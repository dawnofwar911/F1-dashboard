# main.py
import logging
import sys
import os
import threading
import time
import faulthandler
import datetime
import pytz
import atexit
import uuid  # For session IDs if needed, though app_state handles Flask session ID

import dash
from dash import Input, Output, State, html, dcc
import dash_bootstrap_components as dbc

# --- Local Module Imports ---
import app_state  # Uses the new multi-session structure from Response #14
import config
import utils
from app_instance import app, server  # Import app AND server
import fastf1

# Import callbacks so they are registered
import callbacks
# These modules will be refactored to be session-aware in subsequent steps
import signalr_client
import data_processing
import replay
import schedule_page

from layout import main_app_layout

# --- Logging Setup (from your previous main.py) ---

def setup_logging():
    log_formatter = logging.Formatter(config.LOG_FORMAT_DEFAULT)
    actual_root_logger = logging.getLogger()
    actual_root_logger.setLevel(logging.INFO)
    if actual_root_logger.hasHandlers():
        actual_root_logger.handlers.clear()
    root_console_handler = logging.StreamHandler(sys.stdout)
    root_console_handler.setFormatter(log_formatter)
    actual_root_logger.addHandler(root_console_handler)

    f1_app_logger = logging.getLogger("F1App")
    f1_app_logger.setLevel(logging.INFO)
    f1_app_logger.propagate = True

    # Logger for per-session auto-connect (will be dynamically named)
    # For general auto-connect config/module logging:
    logging.getLogger("F1App.AutoConnect").setLevel(logging.DEBUG)
    logging.getLogger("F1App.SessionID").setLevel(logging.INFO)

    logging.getLogger("SignalRCoreClient").setLevel(logging.WARNING)
    logging.getLogger("signalrcore").setLevel(logging.WARNING)

    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(
        logging.ERROR if not config.DASH_DEBUG_MODE else logging.INFO)
    werkzeug_logger.propagate = True
    if werkzeug_logger.hasHandlers():
        werkzeug_logger.handlers.clear()

    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('fastf1').setLevel(logging.INFO)


# --- Initialize FastF1 Cache (from your previous main.py) ---
if hasattr(config, 'FASTF1_CACHE_DIR') and config.FASTF1_CACHE_DIR:
    try:
        config.FASTF1_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        fastf1.Cache.enable_cache(config.FASTF1_CACHE_DIR)
        # print(f"FastF1 Cache enabled at: {config.FASTF1_CACHE_DIR}")
    except Exception as e:
        print(f"Error enabling FastF1 cache at {config.FASTF1_CACHE_DIR}: {e}")
else:
    print("Warning: FASTF1_CACHE_DIR not defined in config.py as a Path object or is None.")

# --- Assign Main App Layout ---
app.layout = main_app_layout


def warm_up_schedule_cache():
    """
    A simple target for a thread that fetches the F1 schedule on startup
    to ensure it's cached for other parts of the application, like auto-connect.
    """
    logger_cache_warmup = logging.getLogger("F1App.Main.CacheWarmer")
    logger_cache_warmup.info("Initiating background schedule cache warm-up...")
    try:
        # This will fetch and cache the data. Subsequent calls will be fast.
        schedule_page.get_current_year_schedule_with_sessions()
        logger_cache_warmup.info("Background schedule cache warm-up completed successfully.")
    except Exception as e:
        logger_cache_warmup.error(f"Background schedule cache warm-up failed: {e}", exc_info=True)


# --- Clientside Timezone Callback (from your previous main.py) ---
app.clientside_callback(
    dash.ClientsideFunction(
        namespace='clientside',
        function_name='getTimezone'
    ),
    Output('user-timezone-store-data', 'data'),
    Input('url', 'pathname'),
)

def global_auto_recorder_service():
    """
    A background service that checks the F1 schedule, updates a global
    status variable, and automatically records live sessions if enabled.
    """
    logger_recorder = logging.getLogger("F1App.AutoRecorder")
    logger_recorder.info("Global Auto-Recorder service started.")
   
    from callbacks import main_controls

    while True:
        time.sleep(60) # Check every 60 seconds
        
        current_live_info = None
        with app_state.CURRENT_LIVE_SESSION_INFO_LOCK:
            current_live_info = app_state.CURRENT_LIVE_SESSION_INFO

        # --- Step A: Check if the currently tracked session is over ---
        if current_live_info:
            # We need to parse the start time from the dictionary
            session_start_time = utils.parse_iso_timestamp_safe(current_live_info.get('start_time_utc'))
            
            # Check if 3 hours have passed since the session started
            if schedule_page.is_session_over(session_start_time):
                logger_recorder.info(f"Session '{current_live_info.get('unique_id')}' is over. Clearing global status.")
                with app_state.CURRENT_LIVE_SESSION_INFO_LOCK:
                    app_state.CURRENT_LIVE_SESSION_INFO = None
                continue # Restart the loop

        # --- Step B: If no session is live, scan for the next one ---
        if not current_live_info:
            settings = utils.load_global_settings()
            if not settings.get('record_live_sessions'):
                continue

            try:
                next_session = schedule_page.find_next_session_to_connect(
                    lead_time_minutes=config.AUTO_CONNECT_LEAD_TIME_MINUTES
                )

                if next_session:
                    # Found a new session! Update global status and start recorder.
                    with app_state.CURRENT_LIVE_SESSION_INFO_LOCK:
                        app_state.CURRENT_LIVE_SESSION_INFO = next_session
                    
                    session_key = next_session.get('SessionKey')
                    if not session_key: continue
                    
                    recorder_session_id = f"auto-recorder-{session_key}"
                    if app_state.get_session_state(recorder_session_id): continue

                    logger_recorder.info(f"Time to connect for {next_session.get('session_name')}. Starting recorder session.")
                    recorder_session_state = app_state.get_or_create_session_state(recorder_session_id)
                    
                    if recorder_session_state:
                        with recorder_session_state.lock:
                            recorder_session_state.session_details.update(next_session['SessionInfo'])
                        main_controls.start_live_connection(recorder_session_state, trigger_source="global_auto_recorder")

            except Exception as e:
                logger_recorder.error(f"Error in auto-recorder service scan: {e}", exc_info=True)


def session_garbage_collector():
    """A background thread to remove stale sessions."""
    while True:
        time.sleep(config.SESSION_CLEANUP_INTERVAL_MINUTES * 60)

        stale_sessions = []
        timeout_seconds = config.SESSION_TIMEOUT_HOURS * 3600

        with app_state.SESSIONS_STORE_LOCK:
            for session_id, session in app_state.SESSIONS_STORE.items():
                if (time.time() - session.last_accessed_time) > timeout_seconds:
                    stale_sessions.append(session_id)

        if stale_sessions:
            print(f"Garbage Collector: Found {len(stale_sessions)} stale sessions. Removing them.")
            for session_id in stale_sessions:
                # Here we call the existing cleanup function
                app_state.remove_session_state(session_id)

# --- Shutdown Hook (Updated for per-session auto_connect_thread) ---


def shutdown_application():
    logger_shutdown = logging.getLogger("F1App.Main.Shutdown")
    logger_shutdown.info(
        "Initiating application shutdown sequence via atexit...")

    active_session_ids = []
    with app_state.SESSIONS_STORE_LOCK:
        active_session_ids = list(app_state.SESSIONS_STORE.keys())
    logger_shutdown.info(
        f"Found {len(active_session_ids)} active session(s) to clean up.")

    for session_id in active_session_ids:
        session_state = app_state.get_session_state(session_id)
        if session_state:
            logger_shutdown.info(f"Cleaning up session: {session_id}...")
            with session_state.lock:
                session_state.stop_event.set()  # Signal all threads for this session

                threads_to_join = []
                if session_state.connection_thread and session_state.connection_thread.is_alive():
                    threads_to_join.append(
                        ("SignalR Connection", session_state.connection_thread))
                if session_state.replay_thread and session_state.replay_thread.is_alive():
                    threads_to_join.append(
                        ("Replay", session_state.replay_thread))
                if session_state.data_processing_thread and session_state.data_processing_thread.is_alive():
                    threads_to_join.append(
                        ("Data Processing", session_state.data_processing_thread))
                if session_state.auto_connect_thread and session_state.auto_connect_thread.is_alive():  # ADDED
                    threads_to_join.append(
                        ("Auto-Connect Monitor", session_state.auto_connect_thread))
                if session_state.track_data_fetch_thread and session_state.track_data_fetch_thread.is_alive(): # NEW
                    threads_to_join.append(
                        ("Track Data Fetch", session_state.track_data_fetch_thread))
                if session_state.hub_connection:  # Attempt to stop hub directly if part of this session's state
                    try:
                        logger_shutdown.debug(
                            f"Session {session_id}: Attempting to stop session's hub_connection directly.")
                        session_state.hub_connection.stop()
                    except Exception as e_hub_stop:
                        logger_shutdown.error(
                            f"Session {session_id}: Error stopping session's hub_connection: {e_hub_stop}")

            for thread_name, thread_obj in threads_to_join:
                logger_shutdown.info(
                    f"Session {session_id}: Waiting for {thread_name} thread ({thread_obj.name}) to join...")
                thread_obj.join(timeout=5.0)  # Standard timeout
                if thread_obj.is_alive():
                    logger_shutdown.warning(
                        f"Session {session_id}: Thread {thread_obj.name} did not exit cleanly.")
                else:
                    logger_shutdown.info(
                        f"Session {session_id}: Thread {thread_obj.name} joined successfully.")

            with session_state.lock:  # Re-acquire lock to nullify handles and close files
                session_state.connection_thread = None
                session_state.replay_thread = None
                session_state.data_processing_thread = None
                session_state.auto_connect_thread = None
                session_state.hub_connection = None
                session_state.track_data_fetch_thread = None #

                if session_state.live_data_file and not session_state.live_data_file.closed:
                    try:
                        session_state.live_data_file.close()
                        logger_shutdown.info(
                            f"Session {session_id}: Closed live_data_file.")
                    except Exception as e:
                        logger_shutdown.error(
                            f"Session {session_id}: Error closing live_data_file: {e}")
                session_state.live_data_file = None  # Ensure it's cleared

    with app_state.SESSIONS_STORE_LOCK:
        if app_state.SESSIONS_STORE:  # Only log if there was something to clear
            app_state.SESSIONS_STORE.clear()
            logger_shutdown.info("Cleared all sessions from SESSIONS_STORE.")
        else:
            logger_shutdown.info("SESSIONS_STORE was already empty.")

    logger_shutdown.info("Application shutdown sequence complete.")


# --- Module Level Execution ---
faulthandler.enable()
setup_logging()  # Call your logging setup
logger_main_module = logging.getLogger("F1App.Main.ModuleLevel")

logger_main_module.info(
    "main.py (multi-session structure) module loaded. Initializing...")

if hasattr(config, 'REPLAY_DIR') and config.REPLAY_DIR:
    try:
        config.REPLAY_DIR.mkdir(parents=True, exist_ok=True)
        logger_main_module.info(
            f"Replay directory checked/created: {config.REPLAY_DIR}")
    except Exception as e:
        logger_main_module.error(
            f"Could not create replay directory {config.REPLAY_DIR}: {e}")

atexit.register(shutdown_application)
logger_main_module.info("Session-aware shutdown handler registered.")

# Start the cache warmer thread
threading.Thread(target=warm_up_schedule_cache, daemon=True, name="ScheduleCacheWarmer").start()

# Start the new background services here so Gunicorn will execute them.
logger_main_module.info("Starting background services (Garbage Collector and Auto-Recorder)...")

cleanup_thread = threading.Thread(target=session_garbage_collector, daemon=True, name="SessionGarbageCollector")
cleanup_thread.start()

recorder_thread = threading.Thread(target=global_auto_recorder_service, daemon=True, name="GlobalAutoRecorder")
recorder_thread.start()

logger_main_module.info("Background services started.")
logger_main_module.info(
    f"To run with Waitress/Gunicorn, target this 'server' object: app_instance.server")


# --- Main Execution Logic (for direct `python main.py` run) ---
if __name__ == '__main__':
    logger_main_module.info(
        f"Running Dash development server on http://{config.DASH_HOST}:{config.DASH_PORT}")
    logger_main_module.warning(
        "This development mode is for testing. For production, use a WSGI server like Waitress or Gunicorn.")

    try:
        # use_reloader=False is critical when managing threads at the module/application level
        # or per-session threads that should persist across Dash's internal reloads.
        app.run(
            host=config.DASH_HOST,
            port=config.DASH_PORT,
            debug=config.DASH_DEBUG_MODE,
            use_reloader=False
        )
    except KeyboardInterrupt:
        logger_main_module.info(
            "KeyboardInterrupt detected in development server. Shutdown will be handled by atexit.")
    except Exception as main_err:
        logger_main_module.error(
            f"Critical error during development server run: {main_err}", exc_info=True)

    logger_main_module.info("Development server has finished.")

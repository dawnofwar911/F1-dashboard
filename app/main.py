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
from app import app_state  # Uses the new multi-session structure from Response #14
from app import settings, constants, api, config
from app import utils
from app.app_instance import app, server  # Import app AND server
import fastf1

# Import callbacks so they are registered
from app import callbacks
# These modules will be refactored to be session-aware in subsequent steps
from app import signalr_client
from app import data_processing
from app import replay
from app import schedule_page

from app.layout import main_app_layout
from app.shutdown import controlled_shutdown # Import the new shutdown function

# --- Initialize FastF1 Cache (from your previous main.py) ---
if hasattr(settings, 'FASTF1_CACHE_DIR') and settings.FASTF1_CACHE_DIR:
    try:
        settings.FASTF1_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        fastf1.Cache.enable_cache(settings.FASTF1_CACHE_DIR)
        # print(f"FastF1 Cache enabled at: {config.FASTF1_CACHE_DIR}")
    except Exception as e:
        print(f"Error enabling FastF1 cache at {settings.FASTF1_CACHE_DIR}: {e}")
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
   
    from app.callbacks import main_controls

    first_check = True
    while True:
        if first_check:
            time.sleep(1) # Initial quick check
            first_check = False
        else:
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
                    lead_time_minutes=constants.AUTO_CONNECT_LEAD_TIME_MINUTES
                )

                if next_session:
                    # Found a new session! Update global status and start recorder.
                    with app_state.CURRENT_LIVE_SESSION_INFO_LOCK:
                        app_state.CURRENT_LIVE_SESSION_INFO = next_session
                    
                    session_key = next_session.get('SessionKey')
                    if not session_key: continue
                    
                    recorder_session_id = f"auto-recorder-{session_key}"
                    existing_recorder_session = app_state.get_session_state(recorder_session_id)
                    if existing_recorder_session:
                        # Check if any of its threads are still alive
                        any_thread_alive = False
                        with existing_recorder_session.lock: # Acquire lock to safely access thread objects
                            if existing_recorder_session.connection_thread and existing_recorder_session.connection_thread.is_alive():
                                any_thread_alive = True
                            elif existing_recorder_session.replay_thread and existing_recorder_session.replay_thread.is_alive():
                                any_thread_alive = True
                            elif existing_recorder_session.data_processing_thread and existing_recorder_session.data_processing_thread.is_alive():
                                any_thread_alive = True
                            elif existing_recorder_session.auto_connect_thread and existing_recorder_session.auto_connect_thread.is_alive():
                                any_thread_alive = True
                            elif existing_recorder_session.track_data_fetch_thread and existing_recorder_session.track_data_fetch_thread.is_alive():
                                any_thread_alive = True

                        if any_thread_alive:
                            logger_recorder.info(f"Recorder session {recorder_session_id[:8]} already exists and its threads are alive. Skipping.")
                            continue # Skip if an active recorder session already exists
                        else:
                            logger_recorder.warning(f"Stale recorder session {recorder_session_id[:8]} found (no active threads). Removing it. Attempting to create new one.")
                            app_state.remove_session_state(recorder_session_id) # Remove the stale one

                    logger_recorder.info(f"Time to connect for {next_session.get('session_name')}. Starting recorder session.")
                    recorder_session_state = app_state.get_or_create_session_state(recorder_session_id)
                    
                    if recorder_session_state:
                        with recorder_session_state.lock:
                            recorder_session_state.session_details.update(next_session['SessionInfo'])
                        main_controls.start_live_connection(recorder_session_state, trigger_source="global_auto_recorder")
                    else:
                        logger_recorder.error(f"Failed to get or create session state for {recorder_session_id[:8]}. Cannot start recorder.")

            except Exception as e:
                logger_recorder.error(f"Error in auto-recorder service scan: {e}", exc_info=True)


def session_garbage_collector():
    """A background thread to remove stale sessions."""
    while True:
        time.sleep(settings.SESSION_CLEANUP_INTERVAL_MINUTES * 60)

        stale_sessions = []
        timeout_seconds = settings.SESSION_TIMEOUT_HOURS * 3600

        with app_state.SESSIONS_STORE_LOCK:
            for session_id, session in app_state.SESSIONS_STORE.items():
                if (time.time() - session.last_accessed_time) > timeout_seconds:
                    stale_sessions.append(session_id)

        if stale_sessions:
            logging.info(f"Garbage Collector: Found {len(stale_sessions)} stale sessions. Removing them.")
            for session_id in stale_sessions:
                # Here we call the existing cleanup function
                app_state.remove_session_state(session_id)

# --- Module Level Execution ---
faulthandler.enable()
utils.setup_logging()  # Call your logging setup
logger_main_module = logging.getLogger("F1App.Main.ModuleLevel")

logger_main_module.info(
    "main.py (multi-session structure) module loaded. Initializing...")

if hasattr(settings, 'REPLAY_DIR') and settings.REPLAY_DIR:
    try:
        settings.REPLAY_DIR.mkdir(parents=True, exist_ok=True)
        logger_main_module.info(
            f"Replay directory checked/created: {settings.REPLAY_DIR}")
    except Exception as e:
        logger_main_module.error(
            f"Could not create replay directory {settings.REPLAY_DIR}: {e}")

# Registering the controlled shutdown for production environments (like Gunicorn/Waitress)
# For local development, the finally block in __main__ is more reliable.
# atexit.register(controlled_shutdown)

# Start the cache warmer thread
threading.Thread(target=warm_up_schedule_cache, daemon=True, name="ScheduleCacheWarmer").start()

def start_background_services():
    logger_main_module.info("Starting background services (Garbage Collector and Auto-Recorder)...")

    global cleanup_thread, recorder_thread # Declare as global if they are accessed outside this function
    cleanup_thread = threading.Thread(target=session_garbage_collector, daemon=True, name="SessionGarbageCollector")
    cleanup_thread.start()

    recorder_thread = threading.Thread(target=global_auto_recorder_service, daemon=True, name="GlobalAutoRecorder")
    recorder_thread.start()

    logger_main_module.info("Background services started.")

# Start background services at the module level
# When testing with pytest, we don't want these running in the background.
if os.environ.get('F1_DASHBOARD_IS_TESTING') != 'True':
    start_background_services()

# When running with Gunicorn, a gunicorn.conf.py file is used to hook into the
# worker_exit signal to call the controlled_shutdown function.
# When running locally, the try...finally block in __main__ handles the shutdown.
logger_main_module.info(
    f"To run with Waitress/Gunicorn, target this 'server' object: app_instance.server")



# --- Main Execution Logic (for direct `python main.py` run) ---
if __name__ == '__main__':
    logger_main_module.info(
        f"Running Dash development server on http://{settings.DASH_HOST}:{settings.DASH_PORT}")
    logger_main_module.warning(
        "This development mode is for testing. For production, use a WSGI server like Waitress or Gunicorn.")

    try:
        # use_reloader=False is critical when managing threads at the module/application level
        # or per-session threads that should persist across Dash's internal reloads.
        app.run(
            host=settings.DASH_HOST,
            port=settings.DASH_PORT,
            debug=settings.DASH_DEBUG_MODE,
            use_reloader=False
        )
    except KeyboardInterrupt:
        logger_main_module.info(
            "KeyboardInterrupt detected in development server. Initiating shutdown...")
    except Exception as main_err:
        logger_main_module.error(
            f"Critical error during development server run: {main_err}", exc_info=True)
    finally:
        # This ensures a clean shutdown when running locally
        logger_main_module.info("Development server is stopping. Running controlled shutdown.")
        controlled_shutdown()
        logger_main_module.info("Development server has finished.")

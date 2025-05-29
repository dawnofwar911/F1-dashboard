# main.py
import logging
import sys
import os 
import threading
import time
import faulthandler # Already in your file
import datetime # Add this
import pytz     # Add this
import atexit

import dash
from dash import Input, Output, html, dcc # dcc needed for Location
import dash_bootstrap_components as dbc

# --- Local Module Imports ---
import app_state
import config 
import utils
from app_instance import app, server # Import app AND server from app_instance
import fastf1

# Import the new main_app_layout and the specific page layouts
from layout import main_app_layout, dashboard_content_layout # dashboard_content_layout is your original dashboard content
from schedule_page import schedule_page_layout, get_current_year_schedule_with_sessions # Import the function

# Import callbacks so they are registered
import callbacks  # Your existing dashboard callbacks
# schedule_page.py also contains its own callbacks, they are registered when schedule_page is imported above.
import signalr_client # Keep if it has setup logic, though usually just functions
import data_processing # Your existing data_processing
import replay # Your existing replay

# --- Logging Setup (from your existing main.py) ---
# raw_message_formatter is defined but seems unused later, can be removed if so.
# raw_message_formatter = logging.Formatter(config.LOG_FORMAT_RAW_MESSAGE) 
def setup_logging():
    log_formatter = logging.Formatter(config.LOG_FORMAT_DEFAULT)
    actual_root_logger = logging.getLogger()
    actual_root_logger.setLevel(logging.INFO) 
    if actual_root_logger.hasHandlers():
        # print(f"Root logger initially has handlers: {actual_root_logger.handlers}. Clearing them.", file=sys.stderr) # DEBUG
        actual_root_logger.handlers.clear()
    root_console_handler = logging.StreamHandler(sys.stdout)
    root_console_handler.setFormatter(log_formatter)
    actual_root_logger.addHandler(root_console_handler)
    # print(f"Root logger configured with handler: {actual_root_logger.handlers}", file=sys.stderr) # DEBUG

    f1_app_logger = logging.getLogger("F1App")
    f1_app_logger.setLevel(logging.INFO) 
    f1_app_logger.propagate = True 
    # f1_app_logger.info("F1App application logger level set. Will use root handler.") # DEBUG

    logging.getLogger("SignalRCoreClient").setLevel(logging.WARNING)
    logging.getLogger("signalrcore").setLevel(logging.WARNING)
    
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.ERROR if not config.DASH_DEBUG_MODE else logging.INFO)
    werkzeug_logger.propagate = True 
    if werkzeug_logger.hasHandlers():
        # module_logger_temp = logging.getLogger("F1App") 
        # module_logger_temp.info(f"Werkzeug logger has handlers: {werkzeug_logger.handlers}. Clearing them.") # DEBUG
        werkzeug_logger.handlers.clear()
    # f1_app_logger.info(f"Werkzeug logger level set to {logging.getLevelName(werkzeug_logger.getEffectiveLevel())}. Will use root handler.") # DEBUG

    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('fastf1').setLevel(logging.INFO) # Keep FastF1 INFO for useful messages
    # actual_root_logger.info("Root logger configured. Specific loggers will propagate to it.") # DEBUG

# --- Initialize FastF1 Cache ---
# This is good to do early. config.FASTF1_CACHE_DIR should be a Path object from your config.
if hasattr(config, 'FASTF1_CACHE_DIR') and config.FASTF1_CACHE_DIR:
    try:
        config.FASTF1_CACHE_DIR.mkdir(parents=True, exist_ok=True) # Ensure it exists
        fastf1.Cache.enable_cache(config.FASTF1_CACHE_DIR)
        # print(f"FastF1 Cache enabled at: {config.FASTF1_CACHE_DIR}") # DEBUG
    except Exception as e:
        print(f"Error enabling FastF1 cache at {config.FASTF1_CACHE_DIR}: {e}")
else:
    print("Warning: FASTF1_CACHE_DIR not defined in config.py as a Path object or is None.")


# --- Assign Main App Layout ---
app.layout = main_app_layout # This now includes the sidebar and the content area

# --- Callback to Update Page Content Based on URL ---
@app.callback(
    Output("page-content", "children"),
    [Input("url", "pathname")]
)
def display_page(pathname):
    if pathname == "/schedule":
        return schedule_page_layout
    elif pathname == "/": # Default to dashboard
        return dashboard_content_layout
    else: # Handle 404 Not Found
        return dbc.Container([
            html.H1("404: Not found", className="text-danger display-3 mt-5"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised.", className="lead"),
            dbc.Button("Go to Dashboard", href="/", color="primary", size="lg")
        ], fluid=True, className="py-5 text-center bg-dark text-light vh-100")
        
        
app.clientside_callback(
    dash.ClientsideFunction(
        namespace='clientside',
        function_name='getTimezone' # This JS function must exist in assets/custom_script.js
    ),
    Output('user-timezone-store-data', 'data'), # This store MUST be in your main_app_layout
    Input('url', 'pathname') # Triggered on page load/navigation
)

AUTO_CONNECT_POLL_INTERVAL_SECONDS = 60
AUTO_CONNECT_LEAD_TIME_MINUTES = 5
INITIAL_AUTO_CONNECT_DELAY_SECONDS = 5 # Short delay on startup

def auto_connect_monitor():
    logger_auto_connect = logging.getLogger("F1App.AutoConnect")
    logger_auto_connect.info("Auto-connect monitor thread started.")

    # Give other parts of the app a moment to initialize on first startup
    if app_state.stop_event.wait(timeout=INITIAL_AUTO_CONNECT_DELAY_SECONDS): # MODIFIED
        logger_auto_connect.info("Auto-connect monitor: Stop event received during initial delay. Exiting.")
        return

    while not app_state.stop_event.is_set():
        try:
            with app_state.app_state_lock:
                current_app_s = app_state.app_status["state"]
                auto_connect_attempted_for = app_state.app_status.get("auto_connect_attempted_for_session")

            if current_app_s not in ["Idle", "Stopped", "Error", "Playback Complete"]:
                logger_auto_connect.debug(f"Auto-connect: App state is '{current_app_s}', monitor will sleep.")
                if app_state.stop_event.wait(timeout=AUTO_CONNECT_POLL_INTERVAL_SECONDS): # MODIFIED
                    break # Exit loop if stop event is set during sleep
                continue

            logger_auto_connect.debug("Auto-connect: Attempting to fetch schedule...")
            # get_current_year_schedule_with_sessions() is a blocking call.
            # It's hard to make this call itself interruptible without modifying FastF1.
            # If this call takes longer than the shutdown timeout (5s), the thread will still hang.
            # For now, we accept this limitation, but for very long schedule fetches,
            # one might consider running it in a short-lived thread with a timeout.
            full_schedule_data = get_current_year_schedule_with_sessions()

            if app_state.stop_event.is_set(): # Check immediately after the blocking call
                logger_auto_connect.info("Auto-connect: Stop event detected after schedule fetch. Exiting.")
                break

            if not full_schedule_data:
                logger_auto_connect.info("Auto-connect: No schedule data returned. Will retry.")
                if app_state.stop_event.wait(timeout=AUTO_CONNECT_POLL_INTERVAL_SECONDS * 2): # MODIFIED
                    break
                continue

            now_utc = datetime.datetime.now(pytz.utc)
            next_session_to_connect = None
            min_future_start_time = datetime.datetime.max.replace(tzinfo=pytz.utc)

            for event in full_schedule_data:
                if app_state.stop_event.is_set(): break # Check inside the loop
                # ... (event processing logic as before - from Response 7) ...
                event_official_name = event.get('OfficialEventName', event.get('EventName', 'Unknown Event'))
                event_circuit_name = event.get('Location', 'Unknown Circuit')
                event_date_str = event.get('EventDate')
                event_year = now_utc.year
                if event_date_str:
                    parsed_event_date = utils.parse_iso_timestamp_safe(event_date_str)
                    if parsed_event_date: event_year = parsed_event_date.year
                    else: logger_auto_connect.warning(f"Could not parse EventDate string '{event_date_str}' for event '{event_official_name}' to get year. Using current year {event_year}.")
                else: logger_auto_connect.debug(f"Event '{event_official_name}' missing 'EventDate'. Using current year {event_year}.")

                for session in event.get('Sessions', []):
                    if app_state.stop_event.is_set(): break # Check inside the inner loop
                    session_name = session.get('SessionName')
                    session_date_utc_str = session.get('SessionDateUTC')
                    # ... (session processing as before) ...
                    if session_date_utc_str and session_name:
                        try:
                            session_dt_utc = datetime.datetime.fromisoformat(session_date_utc_str.replace('Z', '+00:00'))
                            session_dt_utc = session_dt_utc.astimezone(pytz.utc) if session_dt_utc.tzinfo else pytz.utc.localize(session_dt_utc)
                            if session_dt_utc > now_utc and session_dt_utc < min_future_start_time:
                                min_future_start_time = session_dt_utc
                                session_type_auto = "Unknown"
                                s_name_lower = session_name.lower()
                                if "practice" in s_name_lower: session_type_auto = config.SESSION_TYPE_PRACTICE
                                elif "qualifying" in s_name_lower: session_type_auto = config.SESSION_TYPE_QUALI
                                elif "sprint" in s_name_lower and "qualifying" not in s_name_lower : session_type_auto = config.SESSION_TYPE_SPRINT
                                elif "race" in s_name_lower and "pre-race" not in s_name_lower : session_type_auto = config.SESSION_TYPE_RACE
                                next_session_to_connect = {
                                    'event_name': event_official_name, 'session_name': session_name,
                                    'start_time_utc': session_dt_utc, 'year': event_year,
                                    'circuit_name': event_circuit_name, 'circuit_key': event.get('CircuitKey'),
                                    'session_type': session_type_auto,
                                    'unique_id': f"{event_year}_{event_official_name}_{session_name}"
                                }
                        except ValueError as e_parse:
                            logger_auto_connect.warning(f"Auto-connect: Error parsing session date '{session_date_utc_str}' for '{session_name}' in '{event_official_name}': {e_parse}")
                            continue
                if app_state.stop_event.is_set(): break # After inner loop, before next event
            
            if app_state.stop_event.is_set(): break # After outer loop

            if next_session_to_connect:
                session_unique_id = next_session_to_connect['unique_id']
                time_to_session = next_session_to_connect['start_time_utc'] - now_utc
                logger_auto_connect.debug(
                    f"Next session for auto-connect: {next_session_to_connect['event_name']} - {next_session_to_connect['session_name']} "
                    f"starts in {time_to_session}. Attempted for this ID: {session_unique_id == auto_connect_attempted_for}"
                )

                if time_to_session.total_seconds() <= (AUTO_CONNECT_LEAD_TIME_MINUTES * 60) and \
                   time_to_session.total_seconds() > -300:
                    if auto_connect_attempted_for == session_unique_id:
                        logger_auto_connect.info(f"Auto-connect already attempted for {session_unique_id}. Skipping.")
                    else:
                        logger_auto_connect.info(
                            f"Auto-connecting for: {next_session_to_connect['event_name']} - {next_session_to_connect['session_name']}"
                        )
                        # ... (app_state.update_target_session_details and connection logic as before) ...
                        app_state.update_target_session_details(
                            year=next_session_to_connect['year'], circuit_key=next_session_to_connect.get('circuit_key'),
                            circuit_name=next_session_to_connect['circuit_name'], event_name=next_session_to_connect['event_name'],
                            session_name=next_session_to_connect['session_name'], session_start_time_utc=next_session_to_connect['start_time_utc'].isoformat(),
                            session_type=next_session_to_connect['session_type']
                        )
                        with app_state.app_state_lock:
                            should_record_live = app_state.record_live_data
                            if app_state.stop_event.is_set(): logger_auto_connect.info("Auto-connect: Clearing pre-existing stop_event prior to connection attempt.") # Log added clarity
                            app_state.stop_event.clear() # Clear before potentially long connection attempt
                            app_state.app_status["auto_connect_attempted_for_session"] = session_unique_id
                        
                        # Connection attempt itself can be blocking
                        websocket_url, ws_headers = None, None
                        try:
                            with app_state.app_state_lock: app_state.app_status.update({"state": "Initializing", "connection": config.TEXT_SIGNALR_SOCKET_CONNECTING_STATUS})
                            websocket_url, ws_headers = signalr_client.build_connection_url(config.NEGOTIATE_URL_BASE, config.HUB_NAME)
                            if not websocket_url or not ws_headers: raise ConnectionError("Negotiation failed for auto-connect.")
                        except Exception as e_neg:
                            logger_auto_connect.error(f"Auto-connect: Negotiation error: {e_neg}", exc_info=True)
                            with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": config.TEXT_SIGNALR_NEGOTIATION_ERROR_PREFIX + str(type(e_neg).__name__)})
                        
                        if app_state.stop_event.is_set(): # Re-check after negotiation
                             logger_auto_connect.info("Auto-connect: Stop event detected after negotiation. Aborting connection thread start.")
                        elif websocket_url and ws_headers:
                            if should_record_live:
                                if not replay.init_live_file(): logger_auto_connect.error("Auto-connect: Failed to init recording file.")
                            else:
                                replay.close_live_file()
                            connect_thread = threading.Thread(
                                target=signalr_client.run_connection_manual_neg, args=(websocket_url, ws_headers),
                                name="SignalRAutoConnectionThread", daemon=True
                            )
                            signalr_client.connection_thread = connect_thread
                            connect_thread.start()
                            logger_auto_connect.info("Auto-connect: SignalR connection thread initiated.")
                            # After successful initiation, wait longer (interruptibly)
                            if app_state.stop_event.wait(timeout=AUTO_CONNECT_POLL_INTERVAL_SECONDS * AUTO_CONNECT_LEAD_TIME_MINUTES): # MODIFIED
                                break
            else:
                logger_auto_connect.info("Auto-connect: No upcoming sessions identified in the schedule for connection.")

        except Exception as e_monitor:
            logger_auto_connect.error(f"Error in auto-connect monitor loop: {e_monitor}", exc_info=True)
            if app_state.stop_event.wait(timeout=AUTO_CONNECT_POLL_INTERVAL_SECONDS * 5): # MODIFIED
                break
        
        if app_state.stop_event.is_set():
            break 
        
        # Regular poll interval at the end of the main try-except block
        logger_auto_connect.debug(f"Auto-connect: loop finished, sleeping for {AUTO_CONNECT_POLL_INTERVAL_SECONDS}s.")
        if app_state.stop_event.wait(timeout=AUTO_CONNECT_POLL_INTERVAL_SECONDS): # MODIFIED
            break # Exit if stop event set during the final sleep

    logger_auto_connect.info("Auto-connect monitor thread stopped.")


# --- Global variables for threads (moved from if __name__ == '__main__') ---
processing_thread = None
auto_connect_thread = None


def start_background_tasks():
    global processing_thread, auto_connect_thread
    # Logger for this specific part
    logger = logging.getLogger("F1App.Main.BackgroundTasks")

    if processing_thread is None or not processing_thread.is_alive():
        logger.info("Starting background data processing thread...")
        app_state.stop_event.clear()  # Ensure stop_event is clear before starting
        processing_thread = threading.Thread(
            target=data_processing.data_processing_loop,
            name="DataProcessingThread", daemon=True)
        processing_thread.start()
        logger.info("Data processing thread started.")
    else:
        logger.info("Data processing thread already running.")

    if auto_connect_thread is None or not auto_connect_thread.is_alive():
        logger.info("Starting auto-connect monitor thread...")
        # stop_event should already be cleared by data_processing_loop start, or ensure it here too if needed
        auto_connect_thread = threading.Thread(
            # auto_connect_monitor should be defined before this point
            target=auto_connect_monitor,
            name="AutoConnectMonitorThread", daemon=True
        )
        auto_connect_thread.start()
        logger.info("Auto-connect monitor thread started.")
    else:
        logger.info("Auto-connect monitor thread already running.")


def shutdown_application():
    logger = logging.getLogger("F1App.Main.Shutdown")
    logger.info("Initiating application shutdown sequence via atexit...")
    if not app_state.stop_event.is_set():
        logger.info("Setting global stop event.")
        app_state.stop_event.set()

    # This logic is similar to your original finally block
    with app_state.app_state_lock:
        current_app_mode = app_state.app_status.get("state", "Unknown").lower()

    if "live" in current_app_mode or "connecting" in current_app_mode or "initializing" in current_app_mode:
        logger.info("Shutdown: Stopping SignalR connection...")
        signalr_client.stop_connection()  # Ensure this is thread-safe and idempotent

    logger.info("Shutdown: Stopping Replay (if running)...")
    replay.stop_replay()  # Ensure this is thread-safe and idempotent

    global processing_thread, auto_connect_thread  # Refer to global thread variables

    if processing_thread and processing_thread.is_alive():
        logger.info("Shutdown: Waiting for Data Processing thread to join...")
        processing_thread.join(timeout=5.0)
        if processing_thread.is_alive():
            logger.warning("Data Processing thread did not exit cleanly.")
        else:
            logger.info("Data Processing thread joined successfully.")

    if auto_connect_thread and auto_connect_thread.is_alive():
        logger.info(
            "Shutdown: Waiting for Auto-Connect Monitor thread to join...")
        # Give auto_connect_monitor time to exit its loop
        auto_connect_thread.join(timeout=5.0)
        if auto_connect_thread.is_alive():
            logger.warning("Auto-Connect Monitor thread did not exit cleanly.")
        else:
            logger.info("Auto-Connect Monitor thread joined successfully.")

    logger.info("Shutdown sequence via atexit complete.")


# --- MODULE LEVEL EXECUTION (This will run when Gunicorn imports main.py) ---
faulthandler.enable()
setup_logging()  # Setup logging first
logger_main_module = logging.getLogger("F1App.Main.ModuleLevel")

logger_main_module.info("main.py module loaded. Performing initial setups.")
logger_main_module.info("Checking/Creating replay directory...")
replay.ensure_replay_dir_exists()  # (assuming this function exists)

# Assign layout after app object is created and imported
app.layout = main_app_layout
logger_main_module.info("Dash layout structure assigned.")

# Start background tasks when the module is loaded
start_background_tasks()

# Register the shutdown function to be called on exit
atexit.register(shutdown_application)
logger_main_module.info(
    "Shutdown handler registered. Background tasks initiated.")
logger_main_module.info(
    f"Gunicorn should target this 'server' object: {server}")


# --- Main Execution Logic (for direct `python main.py` run) ---
if __name__ == '__main__':
    # The module-level code above will have already run.
    # setup_logging(), replay.ensure_replay_dir_exists(), start_background_tasks(), atexit.register()
    # are already done.

    logger_main_module.info(
        f"Running Dash development server on http://{config.DASH_HOST}:{config.DASH_PORT}")
    logger_main_module.warning(
        "REMINDER: Background threads are already started at module level.")
    logger_main_module.warning(
        "This mode is for development. For production, use Gunicorn pointing to main:server.")

    # Keep app.run for development convenience, but background tasks are now started at module level.
    # The `atexit` handler will manage shutdown for this mode too.
    try:
        app.run(
            host=config.DASH_HOST,
            port=config.DASH_PORT,
            debug=config.DASH_DEBUG_MODE,
            use_reloader=False  # MUST be False if threads are started at module level
        )
    except KeyboardInterrupt:
        logger_main_module.info(
            "KeyboardInterrupt detected in development server run. Shutdown will be handled by atexit.")
    except Exception as main_err:
        logger_main_module.error(
            f"Critical error in development server run: {main_err}", exc_info=True)
    # The atexit handler will take care of the cleanup defined in shutdown_application()
    # No need for the extensive finally block here anymore if atexit handles it.
    logger_main_module.info(
        "Development server finished. --- App Exited (dev mode) ---")
    
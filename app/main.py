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

# Import layout components
from layout import main_app_layout, dashboard_content_layout
# Import the cached schedule function
from schedule_page import get_current_year_schedule_with_sessions, schedule_page_layout

# Import callbacks so they are registered
import callbacks
# These modules will be refactored to be session-aware in subsequent steps
import signalr_client
import data_processing
import replay

# --- Constants for Auto-Connect (can also be in config.py) ---
# How often to check schedule when idle for a session
AUTO_CONNECT_POLL_INTERVAL_SECONDS = 60
# How often to check status when a session is auto-connected or waiting for disconnect
AUTO_CONNECT_ACTIVE_POLL_INTERVAL_SECONDS = 20
AUTO_CONNECT_LEAD_TIME_MINUTES = 5    # Connect X minutes before F1 session start
# Short delay before first check for a session's auto-connect
INITIAL_SESSION_AUTO_CONNECT_DELAY_SECONDS = 5
# Disconnect an auto-connected session X mins after it ends
AUTO_DISCONNECT_AFTER_SESSION_END_MINUTES = 10

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
    f1_app_logger.setLevel(logging.DEBUG)
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

# --- Callback to Update Page Content Based on URL (from your previous main.py) ---


@app.callback(
    Output("page-content", "children"),
    [Input("url", "pathname")]
)
def display_page(pathname):
    if pathname == "/schedule":
        return schedule_page_layout  # Already imported
    elif pathname == "/":
        return dashboard_content_layout  # Already imported
    else:
        return dbc.Container([
            html.H1("404: Not found", className="text-danger display-3 mt-5"),
            html.Hr(),
            html.P(
                f"The pathname {pathname} was not recognised.", className="lead"),
            dbc.Button("Go to Dashboard", href="/", color="primary", size="lg")
        ], fluid=True, className="py-5 text-center bg-dark text-light vh-100")


# --- Clientside Timezone Callback (from your previous main.py) ---
app.clientside_callback(
    dash.ClientsideFunction(
        namespace='clientside',
        function_name='getTimezone'
    ),
    Output('user-timezone-store-data', 'data'),
    Input('url', 'pathname'),
)

# --- Target function for per-session auto-connect threads ---


def auto_connect_monitor_session_actual_target(session_state: app_state.SessionState):
    logger_s_auto_connect = logging.getLogger(
        f"F1App.AutoConnect.Sess_{session_state.session_id[:8]}")
    logger_s_auto_connect.info(
        f"Session auto-connect monitor thread started for session {session_state.session_id}.")

    if session_state.stop_event.wait(timeout=INITIAL_SESSION_AUTO_CONNECT_DELAY_SECONDS):
        logger_s_auto_connect.info(
            "Stop event received during initial delay. Exiting.")
        with session_state.lock:
            session_state.auto_connect_thread = None  # Clear handle
        return

    while not session_state.stop_event.is_set():
        try:
            with session_state.lock:
                if not session_state.auto_connect_enabled:
                    logger_s_auto_connect.info(
                        "Auto-connect preference disabled. Exiting monitor thread.")
                    break
                current_s_app_status = session_state.app_status["state"]
                s_auto_connected_event_id = session_state.app_status.get(
                    "auto_connected_session_identifier")
                s_auto_session_end_detected_utc = session_state.app_status.get(
                    "auto_connected_session_end_detected_utc")
                s_current_replay_file = session_state.app_status.get(
                    "current_replay_file")

            # --- Auto-disconnection logic ---
            if current_s_app_status == "Live" and s_auto_connected_event_id and not s_current_replay_file:
                # ... (Auto-disconnection logic from Response #23, now calling session-aware stop)
                s_current_session_feed_status = "Unknown"
                current_live_event_details_id = None
                with session_state.lock:
                    s_current_session_feed_status = session_state.session_details.get(
                        'SessionStatus', 'Unknown')
                    # Form current_live_event_details_id from session_state.session_details
                    live_year = session_state.session_details.get('Year')
                    live_event_name = session_state.session_details.get(
                        'EventName')
                    live_session_name = session_state.session_details.get(
                        'SessionName')
                    if live_year and live_event_name and live_session_name:
                        current_live_event_details_id = f"{live_year}_{live_event_name}_{live_session_name}"

                if current_live_event_details_id == s_auto_connected_event_id:
                    ended_statuses = ["Finished",
                                      "Ends", "Aborted", "Inactive"]
                    if s_current_session_feed_status in ended_statuses:
                        if s_auto_session_end_detected_utc is None:
                            # ... (set s_auto_session_end_detected_utc) ...
                            finished_time = datetime.datetime.now(pytz.utc)
                            logger_s_auto_connect.info(
                                f"Auto-connected F1 session '{s_auto_connected_event_id}' status is '{s_current_session_feed_status}' at {finished_time}. Starting disconnect countdown.")
                            with session_state.lock:
                                session_state.app_status["auto_connected_session_end_detected_utc"] = finished_time
                        elif isinstance(s_auto_session_end_detected_utc, datetime.datetime) and \
                                datetime.datetime.now(pytz.utc) >= (s_auto_session_end_detected_utc + datetime.timedelta(minutes=AUTO_DISCONNECT_AFTER_SESSION_END_MINUTES)):
                            logger_s_auto_connect.info(
                                f"Disconnect timer expired for F1 session '{s_auto_connected_event_id}'. Disconnecting user session.")
                            signalr_client.stop_connection_session(
                                session_state)  # ACTUAL SESSION-AWARE CALL
                            # stop_connection_session should update app_status and clear threads
                            with session_state.lock:  # Ensure these are cleared if stop_connection_session doesn't
                                session_state.app_status["auto_connected_session_identifier"] = None
                                session_state.app_status["auto_connected_session_end_detected_utc"] = None
                            logger_s_auto_connect.info(
                                "Auto-disconnected. Monitor will pause and re-scan.")
                            session_state.stop_event.clear()  # Clear for this loop to continue scanning
                            if session_state.stop_event.wait(timeout=AUTO_CONNECT_POLL_INTERVAL_SECONDS):
                                break
                            continue
                    elif s_auto_session_end_detected_utc is not None:  # Session running again
                        with session_state.lock:
                            session_state.app_status["auto_connected_session_end_detected_utc"] = None

                if session_state.stop_event.wait(timeout=AUTO_CONNECT_ACTIVE_POLL_INTERVAL_SECONDS):
                    break
                continue

            # --- Logic for finding and initiating a new auto-connection ---
            if current_s_app_status not in ["Idle", "Stopped", "Error", "Playback Complete"]:
                if session_state.stop_event.wait(timeout=AUTO_CONNECT_POLL_INTERVAL_SECONDS):
                    break
                continue

            full_schedule_data = get_current_year_schedule_with_sessions()  # Cached
            if session_state.stop_event.is_set() or not full_schedule_data:
                if not full_schedule_data:
                    logger_s_auto_connect.info(
                        "No schedule data for auto-connect scan.")
                if session_state.stop_event.wait(timeout=AUTO_CONNECT_POLL_INTERVAL_SECONDS * 2):
                    break
                continue

            now_utc = datetime.datetime.now(pytz.utc)
            next_f1_session_to_connect = None
            # ... (Logic to find next_f1_session_to_connect from full_schedule_data - from Response #23) ...
            min_future_start_time = datetime.datetime.max.replace(
                tzinfo=pytz.utc)
            for event in full_schedule_data:
                if session_state.stop_event.is_set():
                    break
                event_official_name = event.get(
                    'OfficialEventName', event.get('EventName', 'Unknown Event'))
                event_year_from_schedule = utils.parse_iso_timestamp_safe(event.get('EventDate')).year if event.get(
                    'EventDate') and utils.parse_iso_timestamp_safe(event.get('EventDate')) else now_utc.year
                for session_detail in event.get('Sessions', []):
                    if session_state.stop_event.is_set():
                        break
                    session_name = session_detail.get('SessionName')
                    session_date_utc_str = session_detail.get('SessionDateUTC')
                    if session_date_utc_str and session_name:
                        session_dt_utc = utils.parse_iso_timestamp_safe(
                            session_date_utc_str)
                        if session_dt_utc and session_dt_utc > now_utc and session_dt_utc < min_future_start_time:
                            min_future_start_time = session_dt_utc
                            session_type_auto = utils.determine_session_type_from_name(
                                session_name)
                            next_f1_session_to_connect = {
                                'event_name': event_official_name, 'session_name': session_name,
                                'start_time_utc': session_dt_utc, 'year': event_year_from_schedule,
                                'circuit_name': event.get('Location', "N/A"), 'circuit_key': event.get('CircuitKey'),
                                'session_type': session_type_auto,
                                'unique_id': f"{event_year_from_schedule}_{event_official_name}_{session_name}"}
                if session_state.stop_event.is_set():
                    break
            if session_state.stop_event.is_set():
                break

            if next_f1_session_to_connect:
                f1_session_unique_id = next_f1_session_to_connect['unique_id']
                time_to_f1_session = next_f1_session_to_connect['start_time_utc'] - now_utc

                with session_state.lock:
                    is_already_handled_event = (session_state.app_status.get(
                        "auto_connected_session_identifier") == f1_session_unique_id)

                if time_to_f1_session.total_seconds() <= (AUTO_CONNECT_LEAD_TIME_MINUTES * 60) and \
                   time_to_f1_session.total_seconds() > -300 and not is_already_handled_event:

                    logger_s_auto_connect.info(
                        f"Auto-connecting user session {session_state.session_id} to F1 session: {f1_session_unique_id}")

                    with session_state.lock:
                        session_state.session_details.update({
                            'Year': next_f1_session_to_connect['year'], 'CircuitKey': next_f1_session_to_connect.get('circuit_key'),
                            'CircuitName': next_f1_session_to_connect['circuit_name'], 'EventName': next_f1_session_to_connect['event_name'],
                            'SessionName': next_f1_session_to_connect['session_name'],
                            'SessionStartTimeUTC': next_f1_session_to_connect['start_time_utc'].isoformat(),
                            'Type': next_f1_session_to_connect['session_type']})
                        session_state.app_status.update({
                            "state": "Initializing", "connection": config.TEXT_SIGNALR_SOCKET_CONNECTING_STATUS,
                            "auto_connected_session_identifier": f1_session_unique_id,
                            "auto_connected_session_end_detected_utc": None,
                            "current_replay_file": None})  # Ensure not in replay mode
                        session_state.stop_event.clear()

                    # ACTUAL CONNECTION START
                    websocket_url, ws_headers = signalr_client.build_connection_url(
                        config.NEGOTIATE_URL_BASE, config.HUB_NAME)

                    if session_state.stop_event.is_set():  # Recheck after potentially blocking negotiation
                        logger_s_auto_connect.info(
                            "Stop event after negotiation. Aborting connection start for this cycle.")
                    elif websocket_url and ws_headers:
                        if session_state.record_live_data:  # Check session specific preference
                            # SESSION-AWARE
                            if not replay.init_live_file_session(session_state):
                                logger_s_auto_connect.error(
                                    f"Failed to initialize live recording file for session {session_state.session_id}.")
                        
                        sess_id_log = session_state.session_id[:8]
                        conn_thread = threading.Thread(
                            target=signalr_client.run_connection_session,  # SESSION-AWARE
                            args=(session_state, websocket_url, ws_headers),
                            name=f"SigRConn_Sess_{sess_id_log}", daemon=True)

                        dp_thread = threading.Thread(
                            target=data_processing.data_processing_loop_session,  # SESSION-AWARE
                            args=(session_state,),
                            name=f"DataProc_Sess_{sess_id_log}", daemon=True)

                        with session_state.lock:
                            session_state.connection_thread = conn_thread
                            session_state.data_processing_thread = dp_thread

                        conn_thread.start()
                        dp_thread.start()
                        logger_s_auto_connect.info(
                            "Session-specific SignalR connection and Data Processing threads initiated by auto-connect.")
                        # Once connection attempt is made, wait longer or use active poll interval
                        if session_state.stop_event.wait(timeout=AUTO_CONNECT_ACTIVE_POLL_INTERVAL_SECONDS):
                            break
                        continue  # Loop to active poll state
                    else:
                        logger_s_auto_connect.error(
                            f"Negotiation failed for F1 session {f1_session_unique_id}. Will retry scan.")
                        with session_state.lock:
                            session_state.app_status.update({"state": "Error", "connection": "Negotiation Failed (Auto)",
                                                             "auto_connected_session_identifier": None})

            if session_state.stop_event.wait(timeout=AUTO_CONNECT_POLL_INTERVAL_SECONDS):
                break

        except Exception as e_monitor:
            logger_s_auto_connect.error(
                f"Error in session auto-connect monitor loop: {e_monitor}", exc_info=True)
            if session_state.stop_event.wait(timeout=AUTO_CONNECT_POLL_INTERVAL_SECONDS * 3):
                break

        if session_state.stop_event.is_set():
            break

    logger_s_auto_connect.info(
        f"Session auto-connect monitor thread stopped for session {session_state.session_id}.")
    with session_state.lock:
        session_state.auto_connect_thread = None
        # If thread stops unexpectedly (not by user disabling), set enabled to false.
        # The toggle callback handles enabling.
        # session_state.auto_connect_enabled = False # Or let the toggle callback manage this

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

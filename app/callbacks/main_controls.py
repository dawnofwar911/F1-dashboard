# app/callbacks/main_controls.py
"""
Callbacks for main application controls like session start/stop, replay,
and user-configurable settings.
"""
import logging
import threading
import time
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime, timezone, timedelta
import pytz
import pandas as pd

import dash
from dash.dependencies import Input, Output, State
from dash import no_update, Patch, dcc
from dash.exceptions import PreventUpdate

from app_instance import app
import app_state
import config
import replay
import signalr_client
import utils
import data_processing
from schedule_page import get_current_year_schedule_with_sessions

logger = logging.getLogger(__name__)

# Add this new, reusable function to main_controls.py

def start_live_connection(session_state: app_state.SessionState, trigger_source: str = "unknown"):
    """
    The core logic for stopping any previous activity, resetting state,
    and starting a new live SignalR connection and data processing threads.
    This function is designed to be called from multiple places.
    """
    sess_id_log = session_state.session_id[:8]
    logger.info(f"Session {sess_id_log}: Executing start_live_connection, triggered by: '{trigger_source}'.")

    if session_state.replay_thread and session_state.replay_thread.is_alive():
        logger.info(f"Session {sess_id_log}: Active replay thread found. Stopping to clear the way for live connection.")
        replay.stop_replay_session(session_state)
        time.sleep(0.1)

    with session_state.lock:
        current_s_state = session_state.app_status["state"]
        if current_s_state not in ["Idle", "Stopped", "Error", "Playback Complete", "Initializing"]:
            logger.warning(f"Session {sess_id_log}: State is '{current_s_state}', not suitable for new live connection. Aborting.")
            return False

        logger.info(f"Session {sess_id_log}: Proceeding with connect-live state setup. Current state: {current_s_state}")
        session_state.stop_event.clear()
        session_state.reset_state_variables()
        session_state.app_status.update({
            "state": "Initializing",
            "connection": config.TEXT_SIGNALR_SOCKET_PRE_NEGOTIATE_STATUS,
        })
        logger.info(f"Session {sess_id_log}: Session state reset and app_status set to Initializing.")

    logger.info(f"Session {sess_id_log}: Attempting to build connection URL...")
    websocket_url, ws_headers = signalr_client.build_connection_url(config.NEGOTIATE_URL_BASE, config.HUB_NAME)

    if not (websocket_url and ws_headers):
        logger.error(f"Session {sess_id_log}: Negotiation failed for trigger '{trigger_source}'. Cannot start live connection.")
        with session_state.lock:
            session_state.app_status.update({"state": "Error", "connection": "Negotiation Failed"})
        return False

    logger.info(f"Session {sess_id_log}: Connection URL built. Starting threads.")
    replay.init_live_file_session(session_state)

    conn_thread = threading.Thread(
        target=signalr_client.run_connection_session,
        args=(session_state, websocket_url, ws_headers),
        name=f"SigRConn_Sess_{sess_id_log}", daemon=True
    )
    dp_thread = threading.Thread(
        target=data_processing.data_processing_loop_session,
        args=(session_state,),
        name=f"DataProc_Live_{sess_id_log}", daemon=True
    )
    with session_state.lock:
        session_state.connection_thread = conn_thread
        session_state.data_processing_thread = dp_thread

    conn_thread.start()
    dp_thread.start()
    logger.info(f"Session {sess_id_log}: Live connection and data processing threads started.")
    return True

def auto_connect_monitor_session_actual_target(session_state: app_state.SessionState):
    """
    Monitors the global live session status and connects this specific user
    session if its auto-connect preference is enabled.
    It does NOT scan the schedule itself.
    """
    logger_s_auto_connect = logging.getLogger(f"F1App.AutoConnect.Sess_{session_state.session_id[:8]}")
    logger_s_auto_connect.info("User auto-connect monitor started.")

    while not session_state.stop_event.wait(15):
        try:
            with session_state.lock:
                if not session_state.auto_connect_enabled:
                    logger_s_auto_connect.info("User disabled auto-connect preference. Exiting monitor.")
                    break

            is_connected = session_state.app_status["state"] in ["Live", "Replaying", "Connecting", "Initializing"]
            if is_connected:
                continue

            live_info = None
            with app_state.CURRENT_LIVE_SESSION_INFO_LOCK:
                live_info = app_state.CURRENT_LIVE_SESSION_INFO

            if live_info and isinstance(live_info, dict) and 'unique_id' in live_info:
                # Because of the check above, this line is now 100% safe
                logger_s_auto_connect.info(f"Global service reports live session '{live_info['unique_id']}'. Auto-connecting this user session for viewing.")
                
                with session_state.lock:
                    session_state.session_details.update(live_info['SessionInfo'])
                
                start_live_connection(session_state, trigger_source="user_auto_connect")
                
                # Once connected, we can stop this thread for this session
                break

        except Exception as e:
            logger_s_auto_connect.error(f"Error in user auto-connect monitor loop: {e}", exc_info=True)

    logger_s_auto_connect.info("User auto-connect monitor stopped.")
    with session_state.lock:
        session_state.auto_connect_thread = None

@app.callback(
    [Output('dummy-output-for-controls', 'children', allow_duplicate=True),
     Output('track-map-graph', 'figure', allow_duplicate=True),
     Output('car-positions-store', 'data', allow_duplicate=True)],
    [Input('connect-button', 'n_clicks'),
     Input('replay-button', 'n_clicks'),
     Input('stop-reset-button', 'n_clicks')],
    [State('replay-file-selector', 'value'),
     State('replay-speed-slider', 'value'),
     State('session-preferences-store', 'data')], # CHANGED
    prevent_initial_call=True
)
def handle_control_clicks(connect_clicks, replay_clicks, stop_reset_clicks,
                          selected_replay_file, replay_speed_value,
                          session_prefs: Optional[dict]): # CHANGED
    ctx = dash.callback_context
    if not ctx.triggered or ctx.triggered[0]['value'] is None or ctx.triggered[0]['value'] < 1:
        return dash.no_update, dash.no_update, dash.no_update
    
    session_state = app_state.get_or_create_session_state()
    if not session_state:
        return dash.no_update, dash.no_update, dash.no_update

    # --- Read record preference from the store ---
    session_prefs = session_prefs or {}
    record_pref = session_prefs.get('record_data', False)
    with session_state.lock:
        session_state.record_live_data = record_pref
    # ---

    button_id = ctx.triggered_id

    # --- FIXED: Robust Guard Clause ---
    # This check prevents the callback from running on initial page load,
    # even if `prevent_initial_call=True` is behaving unexpectedly.
    # It ensures a button has been physically clicked (n_clicks >= 1).
    if not ctx.triggered or ctx.triggered[0]['value'] is None or ctx.triggered[0]['value'] < 1:
        logger.info(f"Control callback fired for '{ctx.triggered_id}' but it was not a user click (n_clicks={ctx.triggered[0]['value']}). Ignoring.")
        return dash.no_update, dash.no_update, dash.no_update
    # --- END OF FIX ---
    button_id = ctx.triggered_id if ctx.triggered else None
    sess_id_log = session_state.session_id[:8]
    logger.info(f"Session {sess_id_log}: Control button clicked: {button_id}")

    # Default outputs
    dummy_output = dash.no_update
    track_map_output = dash.no_update # Use specific map reset when needed
    car_pos_store_output = dash.no_update

    if button_id == 'connect-button':
        logger.info(f"LiveConnSess {sess_id_log}: 'connect-button' pressed by user.")

        start_live_connection(session_state, trigger_source="connect_button")
            
        track_map_output = utils.create_empty_figure_with_message(config.TRACK_MAP_WRAPPER_HEIGHT, f"map_connect_{time.time()}", config.TEXT_TRACK_MAP_LOADING, config.TRACK_MAP_MARGINS)
        car_pos_store_output = {'status': 'reset_map_display', 'timestamp': time.time()}

    elif button_id == 'replay-button':
        if not selected_replay_file:
            logger.warning(f"Session {sess_id_log}: Start Replay: {config.TEXT_REPLAY_SELECT_FILE}")
            return dummy_output, track_map_output, car_pos_store_output

        logger.info(f"ReplaySess {sess_id_log}: In 'replay-button' logic. Selected file: {selected_replay_file}.")

        # --- Stop existing activities for THIS session WITHOUT holding the main session lock during joins ---
        # It's better if stop_connection_session and stop_replay_session manage their own locking carefully
        # or if they signal threads and the actual join happens outside critical lock sections.
        
        # Get current threads to check if they are alive, without lock initially if possible,
        # or briefly acquire lock just to get handles.
        _conn_thread = None
        _repl_thread = None
        with session_state.lock:
            _conn_thread = session_state.connection_thread
            _repl_thread = session_state.replay_thread

        if _conn_thread and _conn_thread.is_alive():
            logger.info(f"ReplaySess {sess_id_log}: Stopping active live connection (from handle_control_clicks) to start replay.")
            signalr_client.stop_connection_session(session_state) # This function should handle its own join and lock release.
            # No sleep here while holding the main callback's lock.
            # Wait for it to actually stop if necessary, or ensure stop_connection_session is fully synchronous.
            # For simplicity, assume stop_connection_session blocks until done or times out.

        if _repl_thread and _repl_thread.is_alive():
             logger.info(f"ReplaySess {sess_id_log}: Stopping previous replay (from handle_control_clicks) to start new one.")
             replay.stop_replay_session(session_state) # This function should handle its own join.
             # No sleep here.

        # Brief sleep outside any specific session lock, to allow threads to react if needed.
        # This is a bit of a pragmatic measure; ideally, thread stopping is fully synchronous.
        time.sleep(0.1) 

        # Set replay speed (can be done under lock before calling start_replay_session, or pass as arg)
        current_replay_speed_val = 1.0 # Default
        with session_state.lock:
            try:
                speed_val_from_slider = float(replay_speed_value if replay_speed_value is not None else 1.0)
                current_replay_speed_val = max(0.1, speed_val_from_slider)
                session_state.replay_speed = current_replay_speed_val # Update session state here
            except:
                logger.warning(f"ReplaySess {sess_id_log}: Could not parse replay_speed_value '{replay_speed_value}', defaulting to 1.0x")
                session_state.replay_speed = 1.0
                current_replay_speed_val = 1.0

        full_replay_path = Path(config.REPLAY_DIR) / selected_replay_file
        logger.info(f"ReplaySess {sess_id_log}: Preparing to call replay.start_replay_session with path: {full_replay_path}, speed: {current_replay_speed_val}")
        
        # replay.start_replay_session will now handle reset_state_variables, app_status updates, and starting its threads
        if replay.start_replay_session(session_state, full_replay_path, current_replay_speed_val):
            logger.info(f"ReplaySess {sess_id_log}: Replay initiation for {full_replay_path.name} reported success by start_replay_session.")
        else:
            logger.error(f"ReplaySess {sess_id_log}: Replay initiation for {full_replay_path.name} reported failure by start_replay_session.")
            # Ensure UI reflects error if start_replay_session fails
            with session_state.lock:
                if session_state.app_status["state"] != "Error": # If start_replay_session didn't set it
                    session_state.app_status.update({"state": "Error", "connection": "Replay Start Failed"})
        
        track_map_output = utils.create_empty_figure_with_message(config.TRACK_MAP_WRAPPER_HEIGHT, f"map_replay_{time.time()}", config.TEXT_TRACK_MAP_LOADING, config.TRACK_MAP_MARGINS)
        car_pos_store_output = {'status': 'reset_map_display', 'timestamp': time.time()}


    elif button_id == 'stop-reset-button':
        logger.info(f"Session {sess_id_log}: Stop & Reset Session button clicked.")
        
        # Stop live connection (if any)
        # signalr_client.stop_connection_session should handle its DP thread.
        logger.info(f"Session {sess_id_log}: Stopping SignalR connection (if any)...")
        signalr_client.stop_connection_session(session_state) 
    
        # Stop replay (if any)
        # replay.stop_replay_session should handle its DP thread.
        logger.info(f"Session {sess_id_log}: Stopping replay (if any)...")
        replay.stop_replay_session(session_state) 
        
        # Stop auto-connect thread if running for this session
        _auto_connect_thread_to_join = None
        with session_state.lock:
            if session_state.auto_connect_thread and session_state.auto_connect_thread.is_alive():
                logger.info(f"Session {sess_id_log}: Signalling auto-connect thread to stop...")
                session_state.stop_event.set() 
                _auto_connect_thread_to_join = session_state.auto_connect_thread
            session_state.auto_connect_enabled = False 
        
        if _auto_connect_thread_to_join:
            logger.info(f"Session {sess_id_log}: Joining auto-connect thread {_auto_connect_thread_to_join.name}...")
            _auto_connect_thread_to_join.join(timeout=3.0) 
            with session_state.lock:
                if _auto_connect_thread_to_join.is_alive():
                    logger.warning(f"Session {sess_id_log}: Auto-connect thread {_auto_connect_thread_to_join.name} did not join cleanly.")
                if session_state.auto_connect_thread is _auto_connect_thread_to_join:
                    session_state.auto_connect_thread = None
        
        # After specific stop functions have run, the DP thread handles should ideally be None.
        # A brief pause to allow threads to fully terminate if their join returned slightly before full cleanup.
        time.sleep(0.2) # Small delay
    
        logger.info(f"Session {sess_id_log}: Resetting session state variables...")
        session_state.reset_state_variables() # This will clear all thread handles to None again.
        
        with session_state.lock: # Ensure status is correctly set after reset
             session_state.app_status.update({
                 "state": "Idle", 
                 "connection": config.TEXT_SIGNALR_DISCONNECTED_STATUS,
                 "current_replay_file": None,
                 "auto_connected_session_identifier": None,
                 "auto_connected_session_end_detected_utc": None
            })
        session_state.stop_event.clear()
    
        map_reset_fig = utils.create_empty_figure_with_message(
            config.TRACK_MAP_WRAPPER_HEIGHT, f"reset_map_sess_{sess_id_log}_{time.time()}",
            config.TEXT_TRACK_MAP_DATA_WILL_LOAD, config.TRACK_MAP_MARGINS
        )
        map_reset_fig.update_layout(plot_bgcolor='rgb(30,30,30)', paper_bgcolor='rgba(0,0,0,0)')
        track_map_output = map_reset_fig
        car_pos_store_output = {'status': 'reset_map_display', 'timestamp': time.time()}
        logger.info(f"Session {sess_id_log}: Stop & Reset processing finished.")
    
    return dummy_output, track_map_output, car_pos_store_output
    

# This callback loads the simple display preferences from the store.
@app.callback(
    Output('hide-retired-drivers-switch', 'value'),
    Output('use-mph-switch', 'value'),
    Input('session-preferences-store', 'data'),
    Input('user-session-id', 'data')  # <-- ADD THIS INPUT
)
def load_display_preferences_from_store(store_data: Optional[dict], session_id: Optional[str]): # <-- ADD THIS ARGUMENT
    """
    Runs on app load to read preferences from dcc.Store
    and set the initial state of the switches.
    """
    store_data = store_data or {}
    hide_retired = store_data.get('hide_retired', config.HIDE_RETIRED_DRIVERS)
    use_mph = store_data.get('use_mph', config.USE_MPH)
    
    # Also update the in-memory session state for recording
    
    return hide_retired, use_mph # ADDED

# Replace save_display_preferences_to_store with this new version
@app.callback(
    Output('session-preferences-store', 'data', allow_duplicate=True),
    Input('hide-retired-drivers-switch', 'value'),
    Input('use-mph-switch', 'value'),
    prevent_initial_call=True
)
def save_display_preferences_to_store(hide_retired_val: bool, use_mph_val: bool) -> Patch:
    """
    Runs when the user toggles settings switches and saves the new values.
    """
    patched_prefs = Patch()
    patched_prefs['hide_retired'] = bool(hide_retired_val)
    patched_prefs['use_mph'] = bool(use_mph_val)
    return patched_prefs

# --- Auto-Connect Feature Callbacks ---

@app.callback(
    Output('dummy-output-for-autostart-thread', 'children'),
    Input('user-session-id', 'data'),
    State('session-preferences-store', 'data'),
    prevent_initial_call=True # Only run when session ID is created
)
def manage_auto_connect_thread_on_load(session_id, store_data):
    """
    This callback runs once when a new session is created. It reads the
    user's stored auto-connect preference and starts the background
    monitoring thread if needed. This is decoupled from any specific page UI.
    """
    if not session_id:
        return dash.no_update

    logger.info("Running auto-connect thread manager on session load.")
    
    store_data = store_data or {}
    auto_connect_pref = store_data.get('auto_connect_f1mv', False)

    session_state = app_state.get_session_state(session_id)
    if not session_state:
        logger.error(f"Could not find session state for {session_id} in thread manager.")
        return dash.no_update

    with session_state.lock:
        session_state.auto_connect_enabled = auto_connect_pref
        thread_is_running = session_state.auto_connect_thread and session_state.auto_connect_thread.is_alive()

        if auto_connect_pref and not thread_is_running:
            logger.info(f"Sess {session_state.session_id[:8]}: Stored preference is ON. Starting auto-connect monitor thread on app load.")
            session_state.stop_event.clear()
            thread = threading.Thread(
                target=auto_connect_monitor_session_actual_target,
                args=(session_state,),
                name=f"AutoConnectMon_Load_{session_state.session_id[:8]}",
                daemon=True
            )
            session_state.auto_connect_thread = thread
            thread.start()
        elif not auto_connect_pref and thread_is_running:
            # This is a safety check, unlikely to happen on initial load
            logger.info(f"Sess {session_state.session_id[:8]}: Stored preference is OFF. Stopping auto-connect monitor thread on app load.")
            session_state.stop_event.set()
    
    return f"Auto-connect thread managed for session {session_id}"

# This is the target function for the auto-connect background thread. It remains unchanged.


# This callback loads the auto-connect preference and initializes the thread state on page load.
@app.callback(
    Output('session-auto-connect-switch', 'value'),
    Input('session-preferences-store', 'data'),
    Input('url', 'pathname') # Trigger when the page changes
)
def update_auto_connect_switch_from_store(store_data, pathname):
    """
    This callback's ONLY job is to visually update the toggle switch
    on the settings page to match the user's stored preference.
    It does NOT manage the background thread.
    """
    if pathname != '/settings':
        # Don't do anything if the settings page is not visible.
        return dash.no_update
    
    logger.debug("Settings page loaded. Updating auto-connect switch to match stored preference.")
    store_data = store_data or {}
    auto_connect_pref = store_data.get('auto_connect_f1mv', False)
    return auto_connect_pref
    
# This callback handles the user TOGGLING the auto-connect switch.
@app.callback(
    Output('session-preferences-store', 'data', allow_duplicate=True),
    Input('session-auto-connect-switch', 'value'),
    prevent_initial_call=True
)
def toggle_session_auto_connect(switch_is_on: Optional[bool]) -> Patch:
    """
    This callback is for user interaction only. It starts/stops the background
    thread and then saves the new state to the dcc.Store.
    """
    session_state = app_state.get_or_create_session_state()
    if not session_state:
        logger.error("Callback 'toggle_session_auto_connect': Could not get/create session state.")
        return dash.no_update

    sess_id_log = session_state.session_id[:8]
    new_enabled_state = bool(switch_is_on)
    logger.info(f"Callback 'toggle_session_auto_connect' for Sess {sess_id_log}. User toggled. Desired state: {new_enabled_state}.")

    thread_to_join = None
    with session_state.lock:
        if new_enabled_state != session_state.auto_connect_enabled:
            session_state.auto_connect_enabled = new_enabled_state
            logger.info(f"Sess {sess_id_log}: In-memory auto_connect_enabled set to {new_enabled_state}")

            thread_is_running = session_state.auto_connect_thread and session_state.auto_connect_thread.is_alive()

            if new_enabled_state and not thread_is_running:
                logger.info(f"Sess {sess_id_log}: Starting auto-connect monitor thread...")
                session_state.stop_event.clear()
                thread = threading.Thread(
                    target=auto_connect_monitor_session_actual_target,
                    args=(session_state,),
                    name=f"AutoConnectMon_Sess_{sess_id_log}",
                    daemon=True
                )
                session_state.auto_connect_thread = thread
                thread.start()
            elif not new_enabled_state and thread_is_running:
                logger.info(f"Sess {sess_id_log}: Signalling auto-connect monitor thread to stop...")
                session_state.stop_event.set()
                thread_to_join = session_state.auto_connect_thread

    if thread_to_join:
        logger.info(f"Sess {sess_id_log}: Attempting to join auto-connect thread {thread_to_join.name}...")
        thread_to_join.join(timeout=7.0)
        with session_state.lock:
            if thread_to_join.is_alive():
                logger.warning(f"Sess {sess_id_log}: Auto-connect thread {thread_to_join.name} did not join cleanly.")
            if session_state.auto_connect_thread is thread_to_join:
                session_state.auto_connect_thread = None

    patched_session_prefs = Patch()
    patched_session_prefs['auto_connect_f1mv'] = new_enabled_state
    logger.info(f"Sess {sess_id_log}: Updating 'session-preferences-store' with auto_connect_f1mv: {new_enabled_state}")

    return patched_session_prefs
    
@app.callback(
    Output('session-preferences-store', 'data', allow_duplicate=True),
    Input('replay-speed-slider', 'value'),
    prevent_initial_call=True
)
def update_replay_speed_state(new_speed_value: Optional[float]) -> Patch:
    """
    This callback fires when the user changes the replay speed slider.
    It performs two actions:
    1. Updates the in-memory session state, re-anchoring the session timer
       to account for the new speed.
    2. Saves the new speed value to the persistent dcc.Store.
    """
    session_state = app_state.get_or_create_session_state()
    if new_speed_value is None:
        return dash.no_update

    try:
        new_speed = float(new_speed_value)
        if not (0.1 <= new_speed <= 100.0):
            return dash.no_update
    except (ValueError, TypeError):
        return dash.no_update

    with session_state.lock:
        old_speed = session_state.replay_speed
        if abs(old_speed - new_speed) < 0.01:
            # If the value hasn't meaningfully changed, just save and exit.
            patched_prefs = Patch()
            patched_prefs['replay_speed'] = new_speed
            return patched_prefs

        # Perform the re-anchoring logic for live timers
        session_type = session_state.session_details.get('Type', "Unknown").lower()
        q_state = session_state.qualifying_segment_state
        current_official_remaining_s_at_anchor = q_state.get("official_segment_remaining_seconds")
        last_capture_utc_anchor = q_state.get("last_official_time_capture_utc")
        now_utc = datetime.now(timezone.utc)
        calculated_current_true_remaining_s = None

        if session_type.startswith("practice"):
            practice_start_utc = session_state.practice_session_actual_start_utc
            practice_duration_s = session_state.practice_session_scheduled_duration_seconds
            if practice_start_utc and practice_duration_s is not None:
                wall_time_elapsed_practice = (now_utc - practice_start_utc).total_seconds()
                session_time_elapsed_practice = wall_time_elapsed_practice * old_speed
                calculated_current_true_remaining_s = practice_duration_s - session_time_elapsed_practice
        
        if calculated_current_true_remaining_s is None and last_capture_utc_anchor and current_official_remaining_s_at_anchor is not None:
            wall_time_since_last_anchor = (now_utc - last_capture_utc_anchor).total_seconds()
            session_time_elapsed_since_anchor = wall_time_since_last_anchor * old_speed
            calculated_current_true_remaining_s = current_official_remaining_s_at_anchor - session_time_elapsed_since_anchor

        if calculated_current_true_remaining_s is not None:
            new_anchor_remaining_s = max(0, calculated_current_true_remaining_s)
            q_state["official_segment_remaining_seconds"] = new_anchor_remaining_s
            q_state["last_official_time_capture_utc"] = now_utc
            q_state["last_capture_replay_speed"] = new_speed
            logger.info(f"Re-anchored session timer for new replay speed {new_speed:.2f}x.")

        # Update the in-memory state and prepare the patch for the store
        session_state.replay_speed = new_speed
        patched_prefs = Patch()
        patched_prefs['replay_speed'] = new_speed
        logger.debug(f"Replay speed updated in session_state and store to: {new_speed}")

    return patched_prefs
    
@app.callback(
    Output('replay-speed-slider', 'value'),
    Input('session-preferences-store', 'data'),
    State('url', 'pathname')
)
def load_replay_speed_from_store(store_data: Optional[dict], pathname: str):
    """
    When the dashboard page loads, this sets the replay speed slider to its
    last saved value. It does nothing on other pages.
    """
    if pathname != '/':
        return dash.no_update

    store_data = store_data or {}
    # Default to 1.0 if the setting isn't in the store yet
    speed = store_data.get('replay_speed', 1.0)
    return speed
    
@app.callback(
    Output('user-session-id', 'data'),
    Input('url', 'pathname'), # This input ensures the callback runs once on any page load
)
def initialize_user_session(pathname):
    """
    This is the PRIMARY callback for session creation and reconciliation.
    It runs on every page load to establish a valid session state on the
    server and sync the ID back to the client, solving stale ID issues
    after server restarts.
    """
    session_state = app_state.get_or_create_session_state()
    
    if session_state:
        # We always return the valid, current session_id from the server.
        # This will overwrite any stale ID the client might have had in its
        # dcc.Store from a previous server instance.
        logger.info(f"Session reconciled. Active server session ID: {session_state.session_id}")
        return session_state.session_id
    
    # This should ideally never happen
    logger.critical("CRITICAL: Could not get or create a session state. App may not function.")
    return dash.no_update
    
@app.callback(
    Output('session-preferences-store', 'data', allow_duplicate=True),
    Input('replay-file-selector', 'value'),
    prevent_initial_call=True
)
def save_replay_file_to_store(selected_file: Optional[str]) -> Patch:
    """
    Fires when the user selects a file from the replay dropdown.
    Saves the filename to the persistent dcc.Store.
    """
    if not selected_file:
        return dash.no_update # Don't save if the dropdown is cleared
        
    patched_prefs = Patch()
    patched_prefs['replay_file'] = selected_file
    logger.info(f"Saved replay file preference to store: {selected_file}")
    return patched_prefs


# This callback loads the last selected replay file from the store when the page loads
@app.callback(
    Output('replay-file-selector', 'value'),
    Input('session-preferences-store', 'data'),
    State('url', 'pathname')
)
def load_replay_file_from_store(store_data: Optional[dict], pathname: str):
    """
    When the dashboard page loads, this sets the replay file dropdown
    to its last saved value. It does nothing on other pages.
    """
    if pathname != '/':
        return dash.no_update

    store_data = store_data or {}
    # Return the saved filename, or None to show the placeholder
    return store_data.get('replay_file', None)
    

@app.callback(
    Output('replay-file-selector', 'options'),
    Input('interval-component-slow', 'n_intervals')
)
def update_replay_options(n_intervals):
     return replay.get_replay_files(config.REPLAY_DIR) #
     
@app.callback(
    Output("download-timing-data-csv", "data"),
    Input("export-csv-button", "n_clicks"),
    prevent_initial_call=True,
)
def export_timing_data_to_csv(n_clicks):
    """
    Handles the click event for the export button.
    Gathers the current timing data, formats it as a CSV, and sends it for download.
    """
    session_state = app_state.get_or_create_session_state()
    if not session_state:
        return dash.no_update

    with session_state.lock:
        # Create a snapshot of the timing state to avoid holding the lock
        timing_state_snapshot = dict(session_state.timing_state)

    if not timing_state_snapshot:
        logger.warning("Export to CSV clicked, but no timing data is available.")
        return dash.no_update

    # Convert the dictionary of driver data into a list
    data_list = list(timing_state_snapshot.values())
    
    # Create a pandas DataFrame
    df = pd.DataFrame(data_list)

    # Clean up complex columns (like dictionaries) into simple text
    if 'LastLapTime' in df.columns:
        df['LastLapTime'] = df['LastLapTime'].apply(lambda x: x.get('Value') if isinstance(x, dict) else x)
    if 'BestLapTime' in df.columns:
        df['BestLapTime'] = df['BestLapTime'].apply(lambda x: x.get('Value') if isinstance(x, dict) else x)
    
    # Select and reorder columns for a clean output
    columns_to_export = [
        'Position', 'RacingNumber', 'Tla', 'FullName', 'TeamName',
        'Time', 'GapToLeader', 'LastLapTime', 'BestLapTime',
        'TyreCompound', 'TyreAge', 'NumberOfPitStops', 'Status'
    ]
    
    # Filter the DataFrame to only include columns that actually exist
    df_export = df[[col for col in columns_to_export if col in df.columns]]

    # Generate a dynamic filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"f1_dashboard_timing_{timestamp}.csv"

    # Use dcc.send_data_frame to send the CSV to the browser
    return dcc.send_data_frame(df_export.to_csv, filename, index=False)

# callbacks.py
"""
Contains all the Dash callback functions for the application.
Handles UI updates, user actions, and plot generation.
"""
from datetime import timezone, timedelta, datetime
import logging
import json
import time
import inspect
import copy
import threading
from pathlib import Path
import flask  # For accessing flask.session
from typing import Dict, List, Optional, Any, Tuple  # For type hints

import dash
from dash.dependencies import Input, Output, State, ClientsideFunction
from dash import dcc, html, dash_table, no_update
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
# import numpy as np # Uncomment if complex numpy operations are re-introduced

from app_instance import app  # Dash app instance
import app_state  # For get_or_create_session_state and SessionState type hint
import config
import utils
# Import a_s_m_a_t from main for the auto-connect callback
#from main import auto_connect_monitor_session_actual_target

# These modules now contain session-aware functions
import signalr_client
import replay
import data_processing  # For starting session-specific data processing loop


logger = logging.getLogger("F1App.Callbacks")

# Note: UI revision, height, and margin constants are now in config.py
# Note: TRACK_STATUS_STYLES and WEATHER_ICON_MAP are now in config.py

# --- Per-User Auto-Connect Toggle Callback ---


@app.callback(
    Output('session-auto-connect-switch', 'value'),
    Input('session-auto-connect-switch', 'value'),
    prevent_initial_call=True
)
def toggle_session_auto_connect(switch_is_on: bool) -> bool:
    callback_name = "toggle_session_auto_connect"
    logger.info(
        f"Callback '{callback_name}': User toggled auto-connect switch. Desired state: {'On' if switch_is_on else 'Off'}")

    session_state = app_state.get_or_create_session_state()
    if not session_state:
        logger.error(
            f"Callback '{callback_name}': Could not get/create session state.")
        return False  # Default to off if session state fails

    with session_state.lock:
        current_auto_connect_enabled = session_state.auto_connect_enabled
        new_target_state = bool(switch_is_on)
        thread_is_running = session_state.auto_connect_thread and session_state.auto_connect_thread.is_alive()

        if new_target_state == current_auto_connect_enabled and new_target_state == thread_is_running:
            logger.info(
                f"Session {session_state.session_id[:8]}: Auto-connect already in desired state ({new_target_state}).")
            return new_target_state

        session_state.auto_connect_enabled = new_target_state
        logger.info(
            f"Session {session_state.session_id[:8]}: auto_connect_enabled preference set to {session_state.auto_connect_enabled}")

        if session_state.auto_connect_enabled:
            if not thread_is_running:
                logger.info(
                    f"Session {session_state.session_id[:8]}: Starting auto-connect monitor thread...")
                # Use the session's main stop_event for now for auto-connect thread
                session_state.stop_event.clear()

                thread = threading.Thread(
                    target=auto_connect_monitor_session_actual_target,
                    args=(session_state,),
                    name=f"AutoConnectMon_Sess_{session_state.session_id[:8]}"
                )
                thread.daemon = True
                session_state.auto_connect_thread = thread
                thread.start()
            # else: thread already running
        else:  # Disable auto-connect
            if thread_is_running and session_state.auto_connect_thread:
                logger.info(
                    f"Session {session_state.session_id[:8]}: Stopping auto-connect monitor thread...")
                session_state.stop_event.set()  # Signal this session's auto_connect thread to stop

                session_state.auto_connect_thread.join(timeout=7.0)
                if session_state.auto_connect_thread.is_alive():
                    logger.warning(
                        f"Session {session_state.session_id[:8]}: Auto-connect thread did not join cleanly.")
                session_state.auto_connect_thread = None

            # Determine if the main session stop_event should be cleared
            s_conn_thread_alive = session_state.connection_thread and session_state.connection_thread.is_alive()
            s_replay_thread_alive = session_state.replay_thread and session_state.replay_thread.is_alive()
            if not s_conn_thread_alive and not s_replay_thread_alive:  # If no other major tasks using it
                logger.debug(
                    f"Session {session_state.session_id[:8]}: Clearing main stop_event for session.")
                session_state.stop_event.clear()  # Okay to clear if only auto-connect was using it

    return session_state.auto_connect_enabled

@app.callback(  # If app is not defined here, this will error. Move to callbacks.py if needed.
    [Output("sidebar", "style"),
     Output("page-content", "style", allow_duplicate=True),
     Output("sidebar-state-store", "data"),
     Output("sidebar-toggle-signal", "data")],
    [Input("sidebar-toggle", "n_clicks")],
    [State("sidebar-state-store", "data")],
    # Added to prevent firing on page load before n_clicks is defined
    prevent_initial_call=True
)
def toggle_sidebar(n_clicks, sidebar_state_data):
    is_open_currently = sidebar_state_data.get(
        'is_open', False)  # Default to False if no data

    # if n_clicks is None or n_clicks == 0: # This might prevent toggling if n_clicks starts at 0 and is then 1
    if not n_clicks:  # Simpler check if n_clicks is None or 0 (initial state)
        # Don't toggle on initial load automatically
        new_is_open_state = is_open_currently
    else:
        new_is_open_state = not is_open_currently

    if new_is_open_state:
        sidebar_style_to_apply = config.SIDEBAR_STYLE_VISIBLE
        content_style_to_apply = config.CONTENT_STYLE_WITH_SIDEBAR
    else:
        sidebar_style_to_apply = config.SIDEBAR_STYLE_HIDDEN
        content_style_to_apply = config.CONTENT_STYLE_FULL_WIDTH

    current_store_val = {'is_open': new_is_open_state}

    # Only generate a new signal if there was a click that caused a toggle
    signal_data = dash.no_update
    if n_clicks:  # and (new_is_open_state != is_open_currently): # If state actually changed
        signal_data = time.time()

    return sidebar_style_to_apply, content_style_to_apply, current_store_val, signal_data

@app.callback(
    Output('timing-data-actual-table', 'columns'),
    Input('interval-component-medium', 'n_intervals') # Trigger based on an interval
    # Consider adding State('session-info-display', 'children') or a dcc.Store
    # if you want to trigger more specifically on session type changes,
    # but interval-component-medium should catch session updates.
)
def update_timing_table_columns(n_intervals):
    """
    Dynamically sets the columns for the timing table based on the session type.
    The 'Pits' column is only shown for Race or Sprint sessions.
    """
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    with session_state.lock:
        session_type = session_state.session_details.get('Type', None)

    # Assuming config.TIMING_TABLE_COLUMNS_CONFIG is a list of dicts,
    # where each dict has at least an 'id' and 'name' key.
    all_columns = config.TIMING_TABLE_COLUMNS_CONFIG

    # Define columns that are primarily relevant for Race/Sprint sessions
    race_sprint_specific_column_ids = ['Pits', 'IntervalGap']

    if session_type is None:
        logger.debug("Session type is None, hiding race/sprint specific columns by default.")
        columns_to_display = [
            col for col in all_columns if col.get('id') not in race_sprint_specific_column_ids
        ]
        return columns_to_display

    if session_type in [config.SESSION_TYPE_RACE, config.SESSION_TYPE_SPRINT]:
        logger.debug(f"Session is '{session_type}', showing all relevant columns including Pits, Gap.")
        return all_columns
    else:
        logger.debug(f"Session is '{session_type}', hiding Pits, Gap columns.")
        columns_to_display = [
            col for col in all_columns if col.get('id') not in race_sprint_specific_column_ids
        ]
        return columns_to_display

@app.callback(
    Output('team-radio-display', 'children'),
    Input('interval-component-medium', 'n_intervals') # Update periodically
)
def update_team_radio_display(n_intervals):
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    try:
        with session_state.lock:
            # Make a copy of the deque for safe iteration
            radio_messages_snapshot = list(session_state.team_radio_messages)
            session_path = session_state.session_details.get('Path') # Needed for the audio URL

        if not radio_messages_snapshot:
            return html.Em(config.TEXT_TEAM_RADIO_AWAITING, style={'color': 'grey'})

        if not session_path:
            logger.warning("Team Radio: Session Path not found in session_details. Cannot build audio URLs.")
            return html.Em(config.TEXT_TEAM_RADIO_NO_SESSION_PATH, style={'color': 'orange'})

        # The base URL for audio files, constructed from config and session_path
        # Example: "https://livetiming.formula1.com/static/2023/2023-11-26_Abu_Dhabi_Grand_Prix/..."
        base_audio_url = f"https://{config.F1_LIVETIMING_BASE_URL}/static/{session_path}"

        display_elements = []
        # radio_messages_snapshot is already newest first due to appendleft in data_processing

        for msg in radio_messages_snapshot: # Iterate over the copy
            utc_time_str = msg.get('Utc', 'N/A')
            # Convert UTC to a more readable format (HH:MM:SS)
            try:
                # Parse timestamp, handling potential milliseconds and 'Z'
                if '.' in utc_time_str:
                    utc_time_str = utc_time_str.split('.')[0]
                dt_obj = datetime.strptime(utc_time_str.replace('Z', ''), "%Y-%m-%dT%H:%M:%S")
                time_display = dt_obj.strftime("%H:%M:%S")
            except ValueError as e_time:
                logger.warning(f"Could not parse radio timestamp '{msg.get('Utc', 'N/A')}': {e_time}")
                time_display = msg.get('Utc', 'N/A') # Fallback to raw string

            driver_tla = msg.get('DriverTla', msg.get('RacingNumber', 'Unknown'))
            audio_file_path = msg.get('Path') # Relative path like "TeamRadio/MAXVER01_..."

            if not audio_file_path:
                logger.debug(f"Skipping radio message due to missing audio path: {msg}")
                continue

            # Ensure no double slashes if audio_file_path might start with one (it shouldn't based on example)
            full_audio_url = f"{base_audio_url.rstrip('/')}/{audio_file_path.lstrip('/')}"

            message_style = {
                'padding': '6px 2px', # Adjusted padding
                'borderBottom': '1px solid #383838', # Slightly lighter separator
                'display': 'flex',
                'alignItems': 'center',
                'justifyContent': 'space-between', # Pushes items apart
                'gap': '8px'
            }

            text_info_style = {
                'fontSize': '0.7rem', # Smaller font for timestamp/TLA
                'whiteSpace': 'nowrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'flexShrink': '1', # Allow text to shrink
                 'minWidth': '80px' # Ensure some space for text
            }

            audio_player_style = {
                'height': '28px', # Slightly smaller player
                'flexGrow': '1', # Allow player to take available space
                'maxWidth': 'calc(100% - 90px)' # Max width considering text part
            }

            timestamp_tla_span = html.Span(f"[{time_display}] {driver_tla}:", style=text_info_style)

            audio_player = html.Audio(
                src=full_audio_url,
                controls=True,
                style=audio_player_style
            )

            # Append to the start to keep newest at the top if not using appendleft initially
            # Since we are iterating a snapshot of a deque that had appendleft, this order is fine.
            display_elements.append(html.Div([timestamp_tla_span, audio_player], style=message_style))

        if not display_elements: # If after filtering, nothing is left
             return html.Em(config.TEXT_TEAM_RADIO_AWAITING, style={'color': 'grey'})
        
        logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
        return html.Div(display_elements) # Wrap all messages in a parent Div

    except Exception as e:
        logger.error(f"Error updating team radio display: {e}", exc_info=True)
        return html.Em(config.TEXT_TEAM_RADIO_ERROR, style={'color': 'red'})


@app.callback(
    [Output('lap-counter', 'children'),
     Output('lap-counter-div', 'style'),
     Output('session-timer-label', 'children'),
     Output('session-timer', 'children'),
     Output('session-timer-div', 'style')],
    [Input('interval-component-fast', 'n_intervals')]
)
def update_lap_and_session_info(n_intervals):
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    # Default values
    lap_value_str = "--/--" #
    lap_counter_div_style = {'display': 'none'} #
    session_timer_label_text = "" #
    session_time_str = "" # Initialize to empty for "Next Up" cases #
    session_timer_div_style = {'display': 'none'} #

    try:
        lock_acquisition_start_time = time.monotonic()
        with session_state.lock: #
            lock_acquired_time = time.monotonic()
            logger.debug(f"Lock in '{func_name}' - ACQUIRED. Wait: {lock_acquired_time - lock_acquisition_start_time:.4f}s")
            critical_section_start_time = time.monotonic()
            current_app_overall_status = session_state.app_status.get("state", "Idle") #

            if current_app_overall_status not in ["Live", "Replaying"]: #
                return lap_value_str, lap_counter_div_style, session_timer_label_text, session_time_str, session_timer_div_style #

            session_type_from_state = session_state.session_details.get('Type', "Unknown") #
            current_session_feed_status = session_state.session_details.get('SessionStatus', 'Unknown') #
            current_replay_speed = session_state.replay_speed # Used for LIVE extrapolation, replay speed is inherent in feed pace #

            lap_count_data_payload = session_state.data_store.get('LapCount', {}) #
            lap_count_data = lap_count_data_payload.get('data', {}) if isinstance(lap_count_data_payload, dict) else {} #
            if not isinstance(lap_count_data, dict): lap_count_data = {} #
            current_lap_from_feed = lap_count_data.get('CurrentLap') #
            total_laps_from_feed = lap_count_data.get('TotalLaps') #
            if total_laps_from_feed is not None and total_laps_from_feed != '-': #
                try: session_state.last_known_total_laps = int(total_laps_from_feed) #
                except (ValueError, TypeError): pass #
            actual_total_laps_to_display = session_state.last_known_total_laps if session_state.last_known_total_laps is not None else '--' #
            current_lap_to_display = str(current_lap_from_feed) if current_lap_from_feed is not None else '-' #

            session_type_lower = session_type_from_state.lower() #

            # q_state is for LIVE timer extrapolation and Q REPLAY pause states
            q_state_live_anchor = session_state.qualifying_segment_state.copy() #

            # For Practice LIVE timing
            practice_start_utc_live = session_state.practice_session_actual_start_utc #
            practice_overall_duration_s = session_state.practice_session_scheduled_duration_seconds #

            # For REPLAY feed-paced timing (Practice and Q)
            current_feed_ts_dt_replay = session_state.current_processed_feed_timestamp_utc_dt if current_app_overall_status == "Replaying" else None #
            start_feed_ts_dt_replay = session_state.session_start_feed_timestamp_utc_dt if current_app_overall_status == "Replaying" else None #
            segment_duration_s_replay = session_state.current_segment_scheduled_duration_seconds if current_app_overall_status == "Replaying" else None #

            session_name_from_details = session_state.session_details.get('Name', '') #
            extrapolated_clock_remaining = session_state.extrapolated_clock_info.get("Remaining") if hasattr(session_state, 'extrapolated_clock_info') else None #
            logger.debug(f"Lock in '{func_name}' - HELD for critical section: {time.monotonic() - critical_section_start_time:.4f}s")

        # --- Logic for displaying session type specific info ---

        if session_type_lower in [config.SESSION_TYPE_RACE.lower(), config.SESSION_TYPE_SPRINT.lower()]: #
            lap_counter_div_style = {'display': 'inline-block', 'margin-right': '20px'} #
            session_timer_div_style = {'display': 'none'} # session_timer is hidden for Race/Sprint
            lap_value_str = f"{current_lap_to_display}/{actual_total_laps_to_display}" if current_lap_to_display != '-' else "Awaiting Data..." #

        elif session_type_lower.startswith("practice"): #
            lap_counter_div_style = {'display': 'none'} #
            session_timer_div_style = {'display': 'inline-block'} #
            session_timer_label_text = "Time Left:" # Default
            session_time_str = "" # Default to empty

            is_next_up_practice = False
            if current_app_overall_status == "Live":
                is_next_up_practice = (not practice_start_utc_live and \
                                       current_session_feed_status not in ["Finished", "Ends", "Started"]) or \
                                      current_session_feed_status == "NotStarted" #
            elif current_app_overall_status == "Replaying":
                is_next_up_practice = (not start_feed_ts_dt_replay and \
                                       current_session_feed_status not in ["Finished", "Ends", "Started"]) or \
                                      current_session_feed_status == "NotStarted" #

            if is_next_up_practice:
                actual_session_name = session_name_from_details if session_name_from_details else "Practice" #
                session_timer_label_text = f"Next Up: {actual_session_name}"
            elif current_session_feed_status in ["Finished", "Ends"]: #
                session_time_str = "00:00:00" #
            elif current_app_overall_status == "Replaying":
                # Use practice_overall_duration_s for Practice replay duration
                duration_to_use_for_practice_replay = practice_overall_duration_s if practice_overall_duration_s is not None else segment_duration_s_replay

                if start_feed_ts_dt_replay and current_feed_ts_dt_replay and duration_to_use_for_practice_replay is not None:
                    elapsed_feed_time = (current_feed_ts_dt_replay - start_feed_ts_dt_replay).total_seconds()
                    displayed_remaining_seconds = max(0, duration_to_use_for_practice_replay - elapsed_feed_time)
                    session_time_str = utils.format_seconds_to_time_str(displayed_remaining_seconds) #
                else:
                    session_time_str = "Awaiting Feed Sync"
            elif current_app_overall_status == "Live":
                if practice_start_utc_live and practice_overall_duration_s is not None: #
                    now_utc = datetime.now(timezone.utc) #
                    elapsed_wall_time_seconds = (now_utc - practice_start_utc_live).total_seconds() #
                    effective_elapsed_session_time = elapsed_wall_time_seconds * current_replay_speed # For live, current_replay_speed is 1.0
                    displayed_remaining_seconds = max(0, practice_overall_duration_s - effective_elapsed_session_time) #
                    session_time_str = utils.format_seconds_to_time_str(displayed_remaining_seconds) #
                else:
                    session_time_str = "Awaiting Start/Duration"
            else:
                session_time_str = "Awaiting Status"

        elif session_type_lower in ["qualifying", "sprint shootout"]: #
            lap_counter_div_style = {'display': 'none'} #
            session_timer_div_style = {'display': 'inline-block'} #
            segment_label = q_state_live_anchor.get("current_segment", "") # Current segment from q_state #
            session_time_str = "" # Default to empty

            if segment_label == "Between Segments": #
                old_q_segment = q_state_live_anchor.get("old_segment") #
                next_q_segment_name = ""
                if session_type_lower == "qualifying": #
                    if old_q_segment == "Q1": next_q_segment_name = "Q2" #
                    elif old_q_segment == "Q2": next_q_segment_name = "Q3" #
                elif session_type_lower == "sprint shootout": #
                    if old_q_segment == "SQ1": next_q_segment_name = "SQ2" #
                    elif old_q_segment == "SQ2": next_q_segment_name = "SQ3" #
                session_timer_label_text = f"Next Up: {next_q_segment_name}" if next_q_segment_name else "Next Up: ..."

            elif segment_label == "Ended": #
                session_timer_label_text = "Session Ended:" #

            elif segment_label and segment_label not in ["Unknown", "Between Segments", "Ended"] and \
                 current_session_feed_status not in ["Suspended", "Aborted", "Finished", "Ends", "NotStarted", "Inactive"]: #
                # Actively running Q segment
                session_timer_label_text = f"{segment_label}:" #
                if current_app_overall_status == "Replaying":
                    if start_feed_ts_dt_replay and current_feed_ts_dt_replay and segment_duration_s_replay is not None:
                        elapsed_feed_time_q = (current_feed_ts_dt_replay - start_feed_ts_dt_replay).total_seconds()
                        displayed_remaining_seconds = max(0, segment_duration_s_replay - elapsed_feed_time_q)
                        session_time_str = utils.format_seconds_to_time_str(displayed_remaining_seconds) #
                    else:
                        session_time_str = "Awaiting Feed Sync"
                elif current_app_overall_status == "Live":
                    if q_state_live_anchor.get("last_official_time_capture_utc") and q_state_live_anchor.get("official_segment_remaining_seconds") is not None: #
                        now_utc = datetime.now(timezone.utc) #
                        time_since_last_capture = (now_utc - q_state_live_anchor["last_official_time_capture_utc"]).total_seconds() #
                        adjusted_elapsed_time = time_since_last_capture * current_replay_speed # For live, current_replay_speed is 1.0
                        calculated_remaining = q_state_live_anchor["official_segment_remaining_seconds"] - adjusted_elapsed_time #
                        displayed_remaining_seconds = max(0, calculated_remaining) #
                        session_time_str = utils.format_seconds_to_time_str(displayed_remaining_seconds) #
                    else:
                        session_time_str = "Awaiting..."

            elif segment_label and segment_label not in ["Unknown", "Between Segments", "Ended"] and \
                 current_session_feed_status in ["Suspended", "Aborted", "Inactive"]: # Paused Q segment #
                session_timer_label_text = f"{segment_label} Paused:" #
                # Display the time captured at the point of pause (stored in q_state_live_anchor.official_segment_remaining_seconds)
                if q_state_live_anchor.get("official_segment_remaining_seconds") is not None: #
                    displayed_remaining_seconds = max(0, q_state_live_anchor["official_segment_remaining_seconds"]) #
                    session_time_str = utils.format_seconds_to_time_str(displayed_remaining_seconds) #
                else:
                    session_time_str = "Awaiting..."

            else: # Fallback for Q: NotStarted, Unknown segment etc.
                if session_name_from_details and current_session_feed_status == "NotStarted": #
                    session_timer_label_text = f"Next Up: {session_name_from_details}"
                    # session_time_str remains empty
                else:
                    session_timer_label_text = "Time Left:" #
                    session_time_str = extrapolated_clock_remaining if extrapolated_clock_remaining and extrapolated_clock_remaining != "0" else "Awaiting..."

        else: # Default for unknown/other session types
            lap_counter_div_style = {'display': 'inline-block', 'margin-right': '20px'} #
            session_timer_div_style = {'display': 'inline-block'} #
            lap_value_str = f"{current_lap_to_display}/{actual_total_laps_to_display}" #
            if current_lap_to_display == '-': #
                awaiting_text_value = config.TEXT_LAP_COUNTER_AWAITING.replace("Lap: ", "") if hasattr(config, 'TEXT_LAP_COUNTER_AWAITING') else "Awaiting Data..." #
                lap_value_str = awaiting_text_value #
            session_timer_label_text = "Time:" #
            session_time_str = extrapolated_clock_remaining if extrapolated_clock_remaining and extrapolated_clock_remaining != "0" else "00:00:00" #

    except Exception as e:
        logger.error(f"Error in update_lap_and_session_info: {e}", exc_info=True) #
        return "--/--", {'display': 'none'}, "", "", {'display': 'none'} # Fallback #
    logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
    return lap_value_str, lap_counter_div_style, session_timer_label_text, session_time_str, session_timer_div_style #


@app.callback(
    Output('connection-status', 'children'),
    Output('connection-status', 'style'),
    Input('interval-component-fast', 'n_intervals')
)
def update_connection_status(n):
    """Updates the connection status indicator."""
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    status_text = config.TEXT_CONN_STATUS_DEFAULT # Use constant
    status_style = {'color': 'grey', 'fontWeight': 'bold'}

    try:
        with session_state.lock:
            status = session_state.app_status.get("connection", "Unknown")
            state = session_state.app_status.get("state", "Idle")
            is_rec = session_state.is_saving_active
            rec_file = session_state.current_recording_filename
            rep_file = session_state.app_status.get("current_replay_file")

        status_text = f"State: {state} | Conn: {status}"
        color = 'grey'

        if state == "Live": color = 'lime'
        elif state in ["Connecting", "Initializing"]: color = 'orange'
        elif state in ["Stopped", "Idle"]: color = 'grey'
        elif state == "Error": color = 'red'
        elif state == "Replaying": color = 'dodgerblue'
        elif state == "Playback Complete": color = 'lightblue'
        elif state == "Stopping": color = 'lightcoral'

        if is_rec and rec_file and isinstance(rec_file, (str, Path)):
             try: status_text += f" (REC: {Path(rec_file).name})"
             except Exception as path_e: logger.warning(f"Could not get filename from rec_file '{rec_file}': {path_e}")
        elif state == "Replaying" and rep_file:
             try: status_text += f" (Replay: {Path(rep_file).name})"
             except Exception as path_e: logger.warning(f"Could not get filename from rep_file '{rep_file}': {path_e}")

        status_style = {'color': color, 'fontWeight': 'bold'}

    except Exception as e:
        logger.error(f"Error in update_connection_status: {e}", exc_info=True)
        status_text = config.TEXT_CONN_STATUS_ERROR_UPDATE # Use constant
        status_style = {'color': 'red', 'fontWeight': 'bold'}
    
    logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
    return status_text, status_style

@app.callback(
    Output('session-info-display', 'children'),
    Output('prominent-weather-display', 'children'),
    Output('weather-main-icon', 'children'),
    Output('prominent-weather-card', 'color'),
    Output('prominent-weather-card', 'inverse'),
    Input('interval-component-slow', 'n_intervals')
)
def update_session_and_weather_info(n):
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    session_info_str = config.TEXT_SESSION_INFO_AWAITING
    weather_details_spans = []

    with session_state.lock:
        # Overall condition state
        overall_condition = session_state.last_known_overall_weather_condition
        weather_card_color = session_state.last_known_weather_card_color
        weather_card_inverse = session_state.last_known_weather_card_inverse
        main_weather_icon_key = session_state.last_known_main_weather_icon_key

        # Detailed weather metrics state (these will be our display fallbacks)
        air_temp_to_display = session_state.last_known_air_temp
        track_temp_to_display = session_state.last_known_track_temp
        humidity_to_display = session_state.last_known_humidity
        pressure_to_display = session_state.last_known_pressure
        wind_speed_to_display = session_state.last_known_wind_speed
        wind_direction_to_display = session_state.last_known_wind_direction
        # This specific rainfall value is primarily for the "RAIN" text logic
        rainfall_val_for_text = session_state.last_known_rainfall_val

        # Get current session and new weather data payload
        local_session_details = session_state.session_details.copy()
        raw_weather_payload = session_state.data_store.get('WeatherData', {})
        current_weather_data_payload = raw_weather_payload.get('data', {}) if isinstance(raw_weather_payload, dict) else {}
        if not isinstance(current_weather_data_payload, dict):
            current_weather_data_payload = {}

    # Initialize icon based on persisted overall state
    current_main_weather_icon = config.WEATHER_ICON_MAP.get(main_weather_icon_key, config.WEATHER_ICON_MAP["default"])

    try:
        # Session Info part (remains the same)
        meeting = local_session_details.get('Meeting', {}).get('Name', '?')
        # ... (rest of session info string building) ...
        parts = []
        if local_session_details.get('Circuit', {}).get('ShortName', '?') != '?': parts.append(f"{local_session_details.get('Circuit', {}).get('ShortName', '?')}")
        if meeting != '?': parts.append(f"{meeting}")
        if local_session_details.get('Name', '?') != '?': parts.append(f"Session: {local_session_details.get('Name', '?')}")
        if parts: session_info_str = " | ".join(parts)


        def safe_float(value, default=None):
            if value is None: return default
            try: return float(value)
            except (ValueError, TypeError): return default

        # --- Step 2: Process new weather data if available and update detailed metrics ---
        # These variables will hold values from the CURRENT data stream, or None
        parsed_air_temp = safe_float(current_weather_data_payload.get('AirTemp'))
        parsed_track_temp = safe_float(current_weather_data_payload.get('TrackTemp'))
        parsed_humidity = safe_float(current_weather_data_payload.get('Humidity'))
        parsed_pressure = safe_float(current_weather_data_payload.get('Pressure'))
        parsed_wind_speed = safe_float(current_weather_data_payload.get('WindSpeed'))
        parsed_wind_direction = current_weather_data_payload.get('WindDirection') # String or None
        parsed_rainfall_val = current_weather_data_payload.get('Rainfall')      # String '0', '1' or None

        # Update display values and persisted session_state for each detailed metric
        # if new data for it is valid (not None). Otherwise, retain the loaded last_known value for display.
        with session_state.lock:
            if parsed_air_temp is not None:
                air_temp_to_display = parsed_air_temp
                session_state.last_known_air_temp = parsed_air_temp
            if parsed_track_temp is not None:
                track_temp_to_display = parsed_track_temp
                session_state.last_known_track_temp = parsed_track_temp
            if parsed_humidity is not None:
                humidity_to_display = parsed_humidity
                session_state.last_known_humidity = parsed_humidity
            if parsed_pressure is not None:
                pressure_to_display = parsed_pressure
                session_state.last_known_pressure = parsed_pressure
            if parsed_wind_speed is not None:
                wind_speed_to_display = parsed_wind_speed
                session_state.last_known_wind_speed = parsed_wind_speed
            if parsed_wind_direction is not None: # Allow empty string as valid update
                wind_direction_to_display = parsed_wind_direction
                session_state.last_known_wind_direction = parsed_wind_direction
            if parsed_rainfall_val is not None:
                rainfall_val_for_text = parsed_rainfall_val # Update for current display logic
                session_state.last_known_rainfall_val = parsed_rainfall_val


        # --- Step 3: Determine and persist OVERALL weather condition (icon, card color) ---
        new_overall_condition_determined_this_update = False
        current_cycle_is_raining = parsed_rainfall_val == '1' or parsed_rainfall_val == 1

        # Use parsed_air_temp and parsed_humidity for determining *new* overall condition
        # Fallback to currently displayed (potentially old) air_temp_to_display etc. for condition logic
        # if new parsed values are None, to maintain stability of overall condition.
        # However, it's better to make overall condition determination rely *only* on fresh data if possible.
        # If fresh data is insufficient for overall, the old overall persists.

        temp_overall_condition_candidate = overall_condition # Start with persisted overall
        temp_card_color_candidate = weather_card_color
        temp_card_inverse_candidate = weather_card_inverse

        # Only try to change overall condition if there's relevant new data
        if parsed_rainfall_val is not None or parsed_air_temp is not None or parsed_humidity is not None:
            new_overall_condition_determined_this_update = True # Attempt to determine

            # Default to a neutral if we are re-evaluating based on new partial data
            effective_air_temp_for_condition = parsed_air_temp if parsed_air_temp is not None else air_temp_to_display
            effective_humidity_for_condition = parsed_humidity if parsed_humidity is not None else humidity_to_display

            if current_cycle_is_raining: # Prioritize current rain data
                temp_overall_condition_candidate = "rain"
                temp_card_color_candidate = "info"
                temp_card_inverse_candidate = True
            elif effective_air_temp_for_condition is not None and effective_humidity_for_condition is not None:
                if effective_air_temp_for_condition > 25 and effective_humidity_for_condition < 60:
                    temp_overall_condition_candidate = "sunny"
                    temp_card_color_candidate = "warning"
                    temp_card_inverse_candidate = True
                elif effective_humidity_for_condition >= 75 or effective_air_temp_for_condition < 15:
                    temp_overall_condition_candidate = "cloudy"
                    temp_card_color_candidate = "secondary"
                    temp_card_inverse_candidate = True
                else:
                    temp_overall_condition_candidate = "partly_cloudy"
                    temp_card_color_candidate = "light"
                    temp_card_inverse_candidate = False
            elif effective_air_temp_for_condition is not None: # Only air temp
                if effective_air_temp_for_condition > 28:
                    temp_overall_condition_candidate = "sunny"
                    temp_card_color_candidate = "warning"
                    temp_card_inverse_candidate = True
                elif effective_air_temp_for_condition < 10:
                    temp_overall_condition_candidate = "cloudy"
                    temp_card_color_candidate = "secondary"
                    temp_card_inverse_candidate = True
                else:
                    temp_overall_condition_candidate = "partly_cloudy"
                    temp_card_color_candidate = "light"
                    temp_card_inverse_candidate = False
            elif parsed_rainfall_val is not None and not current_cycle_is_raining:
                # If rainfall data came in and it's explicitly NOT raining,
                # and we couldn't determine based on temp/humidity, default to something sensible
                # This helps clear a "rain" state if rain stops but other data is missing.
                temp_overall_condition_candidate = "default"
                temp_card_color_candidate = "light"
                temp_card_inverse_candidate = False
            else:
                # Not enough new data to change the overall condition from what was persisted
                new_overall_condition_determined_this_update = False


        if new_overall_condition_determined_this_update:
            overall_condition = temp_overall_condition_candidate
            weather_card_color = temp_card_color_candidate
            weather_card_inverse = temp_card_inverse_candidate
            main_weather_icon_key = overall_condition # Key for icon map

            with session_state.lock:
                session_state.last_known_overall_weather_condition = overall_condition
                session_state.last_known_weather_card_color = weather_card_color
                session_state.last_known_weather_card_inverse = weather_card_inverse
                session_state.last_known_main_weather_icon_key = main_weather_icon_key

        current_main_weather_icon = config.WEATHER_ICON_MAP.get(main_weather_icon_key, config.WEATHER_ICON_MAP["default"])

        # --- Step 4: Build weather_details_spans using the 'to_display' values ---
        if air_temp_to_display is not None: weather_details_spans.append(html.Span(f"Air: {air_temp_to_display:.1f}°C", className="me-3"))
        if track_temp_to_display is not None: weather_details_spans.append(html.Span(f"Track: {track_temp_to_display:.1f}°C", className="me-3"))
        if humidity_to_display is not None: weather_details_spans.append(html.Span(f"Hum: {humidity_to_display:.0f}%", className="me-3"))
        if pressure_to_display is not None: weather_details_spans.append(html.Span(f"Press: {pressure_to_display:.0f}hPa", className="me-3"))
        if wind_speed_to_display is not None:
            wind_str = f"Wind: {wind_speed_to_display:.1f}m/s"
            if wind_direction_to_display is not None and str(wind_direction_to_display).strip(): # Check if not None and not empty
                 try: wind_str += f" ({int(wind_direction_to_display)}°)" # Try int conversion
                 except (ValueError, TypeError): wind_str += f" ({wind_direction_to_display})" # Fallback to string if not int
            weather_details_spans.append(html.Span(wind_str, className="me-3"))

        # "RAIN" text display logic, based on the most up-to-date rainfall_val_for_text
        is_raining_for_text_span = rainfall_val_for_text == '1' or rainfall_val_for_text == 1

        rain_text_color_on_light_card = "#007bff"
        rain_text_color_on_dark_card = "white"

        if is_raining_for_text_span:
            weather_details_spans = [s for s in weather_details_spans if not (isinstance(s, html.Span) and getattr(s, 'children', '') == "RAIN")]
            current_rain_text_color = rain_text_color_on_dark_card if weather_card_inverse else rain_text_color_on_light_card
            # Ensure RAIN text color is correct even if overall card is light but it's raining
            if overall_condition == "rain" and weather_card_color == "light":
                 current_rain_text_color = rain_text_color_on_light_card
            weather_details_spans.append(html.Span("RAIN", className="me-2 fw-bold", style={'color': current_rain_text_color}))


        if not weather_details_spans and overall_condition == "default":
            final_weather_display_children = [html.Em(config.TEXT_WEATHER_UNAVAILABLE)]
        elif not weather_details_spans and overall_condition != "default":
             final_weather_display_children = [html.Em(config.TEXT_WEATHER_CONDITION_GENERIC.format(condition=overall_condition.replace("_"," ").title()))]
        else:
            final_weather_display_children = weather_details_spans
        
        logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
        return session_info_str, html.Div(children=final_weather_display_children), current_main_weather_icon, weather_card_color, weather_card_inverse

    except Exception as e:
        logger.error(f"Session/Weather Display Error in callback: {e}", exc_info=True)
        return (config.TEXT_SESSION_INFO_ERROR,
                config.TEXT_WEATHER_ERROR,
                config.WEATHER_ICON_MAP["default"],
                "light",
                False)

@app.callback(
    Output('prominent-track-status-text', 'children'),
    Output('prominent-track-status-card', 'color'),
    Output('prominent-track-status-text', 'style'),
    Input('interval-component-medium', 'n_intervals')
)
def update_prominent_track_status(n):
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    with session_state.lock:
        track_status_code = str(session_state.track_status_data.get('Status', '0'))

    # Use TRACK_STATUS_STYLES from config
    status_info = config.TRACK_STATUS_STYLES.get(track_status_code, config.TRACK_STATUS_STYLES['DEFAULT'])

    label_to_display = status_info["label"]
    text_style = {'fontWeight':'bold', 'padding':'2px 5px', 'borderRadius':'4px', 'color': status_info["text_color"]}
    
    logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
    return label_to_display, status_info["card_color"], text_style


@app.callback(
    Output('other-data-display', 'children'),
    Output('timing-data-actual-table', 'data'),
    Output('timing-data-timestamp', 'children'),
    Input('interval-component-timing', 'n_intervals'),
    # MODIFICATION: Added State for debug mode
    State("debug-mode-switch", "value")
)
# MODIFICATION: Added debug_mode_enabled
def update_main_data_displays(n, debug_mode_enabled):
    session_state = app_state.get_or_create_session_state()
    overall_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    other_elements = []
    table_data = []
    timestamp_text = config.TEXT_WAITING_FOR_DATA
    current_time_for_callbacks = time.time()
    callback_start_time = time.perf_counter()  # For performance logging

    try:
        current_q_segment_from_state = None
        previous_q_segment_from_state = None
        q_state_snapshot_for_live = {}
        current_replay_speed_snapshot = 1.0
        session_feed_status_snapshot = "Unknown"
        app_overall_status = "Idle"
        current_feed_ts_dt_replay_local = None
        start_feed_ts_dt_replay_local = None
        segment_duration_s_replay_local = None
        active_segment_highlight_rule = {
            "type": "NONE", "lower_pos": 0, "upper_pos": 0}
        q1_eliminated_highlight_rule = {
            "type": "NONE", "lower_pos": 0, "upper_pos": 0}
        q2_eliminated_highlight_rule = {
            "type": "NONE", "lower_pos": 0, "upper_pos": 0}
        
        initial_state_copy_start_time = time.monotonic()
        lock_acquisition_start_time = time.monotonic()
        with session_state.lock:
            lock_acquired_time = time.monotonic()
            logger.debug(f"Lock in '{func_name}' - ACQUIRED. Wait: {lock_acquired_time - lock_acquisition_start_time:.4f}s")
            critical_section_start_time = time.monotonic()
            app_overall_status = session_state.app_status.get("state", "Idle")
            # Robustly get session type
            session_type_from_state_str = (
                session_state.session_details.get('Type') or "").lower()
            # MODIFIED: Changed to debug
            logger.debug(
                f"UPDATE_MAIN_DISPLAYS_DEBUG: Read session_type_from_state_str as '{session_type_from_state_str}'")

            q_state_snapshot_for_live = session_state.qualifying_segment_state.copy()
            current_q_segment_from_state = q_state_snapshot_for_live.get(
                "current_segment")
            previous_q_segment_from_state = q_state_snapshot_for_live.get(
                "old_segment")
            current_replay_speed_snapshot = session_state.replay_speed
            session_feed_status_snapshot = (
                session_state.session_details.get('SessionStatus') or 'Unknown')

            if app_overall_status == "Replaying":
                current_feed_ts_dt_replay_local = session_state.current_processed_feed_timestamp_utc_dt
                start_feed_ts_dt_replay_local = session_state.session_start_feed_timestamp_utc_dt
                segment_duration_s_replay_local = session_state.current_segment_scheduled_duration_seconds

            timing_state_copy = session_state.timing_state.copy()
            # MODIFICATION: Conditionally get data_store_copy only if debug mode is enabled
            data_store_copy = session_state.data_store.copy() if debug_mode_enabled else {}
            logger.debug(f"Lock in '{func_name}' - HELD for critical section: {time.monotonic() - critical_section_start_time:.4f}s")
        logger.debug(f"'{func_name}' - Initial lock & state copy: {time.monotonic() - initial_state_copy_start_time:.4f}s")

        # MODIFICATION: Conditionally prepare other_elements
        if debug_mode_enabled:
            # MODIFIED: Changed to debug
            other_elements_prep_start_time = time.monotonic()
            logger.debug("Debug mode is ON, preparing other_elements.")
            excluded_streams = ['TimingData', 'DriverList', 'Position.z', 'CarData.z', 'Position',
                                'TrackStatus', 'SessionData', 'SessionInfo', 'WeatherData', 'Heartbeat']
            # Use the data_store_copy that was conditionally fetched
            sorted_streams = sorted(
                [s for s in data_store_copy.keys() if s not in excluded_streams])
            for stream in sorted_streams:
                value = data_store_copy.get(stream, {})
                data_payload = value.get('data', 'N/A')
                timestamp_str_val = value.get('timestamp', 'N/A')
                try:
                    data_str = json.dumps(data_payload, indent=2)
                except TypeError:
                    data_str = str(data_payload)
                if len(data_str) > 500:
                    data_str = data_str[:500] + "\n...(truncated)"
                other_elements.append(html.Details([
                    html.Summary(f"{stream} ({timestamp_str_val})"),
                    html.Pre(data_str, style={
                             'marginLeft': '15px', 'maxHeight': '200px', 'overflowY': 'auto'})
                ], open=(stream == "LapCount")))
                logger.debug(f"'{func_name}' - Other_elements prep (debug): {time.monotonic() - other_elements_prep_start_time:.4f}s")
        else:
            # MODIFIED: Changed to debug
            logger.debug(
                "Debug mode is OFF, skipping other_elements preparation.")
            other_elements = [
                html.Em("Debug data streams are hidden. Enable debug mode to view.")]

        current_segment_time_remaining_seconds = float('inf')
        is_active_q_segment_for_highlight = (
            session_type_from_state_str in ["qualifying", "sprint shootout"] and
            current_q_segment_from_state and
            current_q_segment_from_state not in [
                "Unknown", "Between Segments", "Ended", "Practice"]
        )
        if is_active_q_segment_for_highlight:
            if app_overall_status == "Replaying":
                if start_feed_ts_dt_replay_local and current_feed_ts_dt_replay_local and segment_duration_s_replay_local is not None:
                    if session_feed_status_snapshot not in ["Suspended", "Aborted", "Inactive", "Finished", "Ends", "NotStarted"]:
                        elapsed_feed_time = (
                            current_feed_ts_dt_replay_local - start_feed_ts_dt_replay_local).total_seconds()
                        current_segment_time_remaining_seconds = max(
                            0, segment_duration_s_replay_local - elapsed_feed_time)
                    elif session_feed_status_snapshot in ["Suspended", "Aborted", "Inactive"]:
                        current_segment_time_remaining_seconds = q_state_snapshot_for_live.get(
                            "official_segment_remaining_seconds", 0.0)
                elif session_feed_status_snapshot in ["Suspended", "Aborted", "Inactive"]:
                    current_segment_time_remaining_seconds = q_state_snapshot_for_live.get(
                        "official_segment_remaining_seconds", 0.0)
            if app_overall_status == "Live" or (app_overall_status == "Replaying" and current_segment_time_remaining_seconds == float('inf')):
                if session_feed_status_snapshot not in ["Suspended", "Aborted", "Finished", "Ends", "NotStarted", "Inactive"]:
                    last_capture_dt = q_state_snapshot_for_live.get(
                        "last_official_time_capture_utc")
                    official_rem_s_at_capture = q_state_snapshot_for_live.get(
                        "official_segment_remaining_seconds")
                    if official_rem_s_at_capture is None or not isinstance(official_rem_s_at_capture, (int, float)):
                        current_segment_time_remaining_seconds = float('inf')
                    elif last_capture_dt is None:
                        current_segment_time_remaining_seconds = official_rem_s_at_capture
                    elif last_capture_dt:
                        now_utc_for_calc = datetime.now(timezone.utc)
                        time_since_last_capture_for_calc = (
                            now_utc_for_calc - last_capture_dt).total_seconds()
                        adjusted_elapsed_time_for_calc = time_since_last_capture_for_calc * \
                            current_replay_speed_snapshot
                        calculated_remaining_for_calc = official_rem_s_at_capture - \
                            adjusted_elapsed_time_for_calc
                        current_segment_time_remaining_seconds = max(
                            0, calculated_remaining_for_calc)
                elif session_feed_status_snapshot in ["Suspended", "Aborted", "Inactive"]:
                    official_rem_s_at_pause = q_state_snapshot_for_live.get(
                        "official_segment_remaining_seconds", 0.0)
                    current_segment_time_remaining_seconds = official_rem_s_at_pause if isinstance(
                        official_rem_s_at_pause, (int, float)) else 0.0
        elif current_q_segment_from_state in ["Between Segments", "Ended"] or session_feed_status_snapshot in ["Finished", "Ends"]:
            current_segment_time_remaining_seconds = 0
        five_mins_in_seconds = 5 * 60
        is_qualifying_type_session = session_type_from_state_str in [
            "qualifying", "sprint shootout"]
        apply_danger_zone_highlight = False
        danger_zone_applies_to_segment = None
        apply_q1_elimination_highlight = False
        apply_q2_elimination_highlight = False
        if is_qualifying_type_session:
            if current_q_segment_from_state in ["Q1", "SQ1", "Q2", "SQ2"]:
                is_session_status_for_running_danger_zone = session_feed_status_snapshot in [
                    "Started", "Running", "Suspended"]
                if is_session_status_for_running_danger_zone and current_segment_time_remaining_seconds <= five_mins_in_seconds:
                    apply_danger_zone_highlight = True
                    danger_zone_applies_to_segment = current_q_segment_from_state
            if not apply_danger_zone_highlight and session_feed_status_snapshot == "Finished":
                if current_q_segment_from_state in ["Q1", "SQ1", "Q2", "SQ2"]:
                    apply_danger_zone_highlight = True
                    danger_zone_applies_to_segment = current_q_segment_from_state
                elif current_q_segment_from_state == "Between Segments" and previous_q_segment_from_state in ["Q1", "SQ1", "Q2", "SQ2"]:
                    apply_danger_zone_highlight = True
                    danger_zone_applies_to_segment = previous_q_segment_from_state
            if apply_danger_zone_highlight and danger_zone_applies_to_segment:
                if danger_zone_applies_to_segment in ["Q1", "SQ1"]:
                    lower_b_q1_active = config.QUALIFYING_CARS_Q1 - \
                        config.QUALIFYING_ELIMINATED_Q1 + 1
                    upper_b_q1_active = config.QUALIFYING_CARS_Q1
                    active_segment_highlight_rule = {
                        "type": "RED_DANGER", "lower_pos": lower_b_q1_active, "upper_pos": upper_b_q1_active}
                elif danger_zone_applies_to_segment in ["Q2", "SQ2"]:
                    lower_b_q2_active = config.QUALIFYING_CARS_Q2 - \
                        config.QUALIFYING_ELIMINATED_Q2 + 1
                    upper_b_q2_active = config.QUALIFYING_CARS_Q2
                    active_segment_highlight_rule = {
                        "type": "RED_DANGER", "lower_pos": lower_b_q2_active, "upper_pos": upper_b_q2_active}
            if (previous_q_segment_from_state in ["Q1", "SQ1"] and current_q_segment_from_state == "Between Segments" and session_feed_status_snapshot == "Inactive"):
                apply_q1_elimination_highlight = True
            elif current_q_segment_from_state in ["Q2", "SQ2", "Q3", "SQ3"]:
                apply_q1_elimination_highlight = True
            elif current_q_segment_from_state == "Between Segments" and previous_q_segment_from_state in ["Q2", "SQ2", "Q3", "SQ3"]:
                apply_q1_elimination_highlight = True
            elif current_q_segment_from_state == "Ended" and previous_q_segment_from_state not in ["Practice", None]:
                apply_q1_elimination_highlight = True
            if (previous_q_segment_from_state in ["Q2", "SQ2"] and current_q_segment_from_state == "Between Segments" and session_feed_status_snapshot == "Inactive"):
                apply_q2_elimination_highlight = True
            elif current_q_segment_from_state in ["Q3", "SQ3"]:
                apply_q2_elimination_highlight = True
            elif current_q_segment_from_state == "Between Segments" and previous_q_segment_from_state in ["Q3", "SQ3"]:
                apply_q2_elimination_highlight = True
            elif current_q_segment_from_state == "Ended" and previous_q_segment_from_state in ["Q2", "SQ2", "Q3", "SQ3"]:
                apply_q2_elimination_highlight = True
            if apply_q1_elimination_highlight:
                q1_eliminated_highlight_rule = {"type": "GREY_ELIMINATED", "lower_pos": config.QUALIFYING_CARS_Q1 -
                                                config.QUALIFYING_ELIMINATED_Q1 + 1, "upper_pos": config.QUALIFYING_CARS_Q1}
            if apply_q2_elimination_highlight:
                q2_eliminated_highlight_rule = {"type": "GREY_ELIMINATED", "lower_pos": config.QUALIFYING_CARS_Q2 -
                                                config.QUALIFYING_ELIMINATED_Q2 + 1, "upper_pos": config.QUALIFYING_CARS_Q2}
        logger.debug(f"HighlightCheck: Seg='{current_q_segment_from_state}', Prev='{previous_q_segment_from_state}', DangerAppliesTo='{danger_zone_applies_to_segment}', RemSecForHighlight={current_segment_time_remaining_seconds:.1f}, Mode='{app_overall_status}', FeedStatus='{session_feed_status_snapshot}', ApplyDanger='{apply_danger_zone_highlight}', ApplyQ1Elim='{apply_q1_elimination_highlight}', ApplyQ2Elim='{apply_q2_elimination_highlight}'")  # MODIFIED: Changed to debug

        # Use data_store_copy that was fetched within the lock if debug_mode_enabled, otherwise it's {}
        # This check is now implicit as sorted_streams will be empty if data_store_copy is {}
        # The original code fetched data_store_copy outside the debug_mode_enabled check.
        # We've moved the copy of session_state.data_store inside the lock and made it conditional.
        # However, timing_data_entry is still needed.
        with session_state.lock:  # Re-acquire lock if needed for data_store if not copied before
            timing_data_entry = session_state.data_store.get(
                'TimingData', {}) if not debug_mode_enabled else data_store_copy.get('TimingData', {})

        timestamp_text = f"Timing TS: {timing_data_entry.get('timestamp', 'N/A')}" if timing_data_entry else config.TEXT_WAITING_FOR_DATA
        table_data_prep_start_time = time.monotonic()
        if timing_state_copy:
            processed_table_data = []
            TERMINAL_RACING_STATUSES = [
                "retired", "crashed", "disqualified", "out of race", "out", "accident"]
            for car_num, driver_state in timing_state_copy.items():
                racing_no = driver_state.get("RacingNumber", car_num)
                tla = driver_state.get("Tla", "N/A")
                pos = driver_state.get('Position', '-')
                pos_str = str(pos)
                compound = driver_state.get('TyreCompound', '-')
                age = driver_state.get('TyreAge', '?')
                is_new = driver_state.get('IsNewTyre', False)
                compound_short = ""
                known_compounds = ["SOFT", "MEDIUM",
                                   "HARD", "INTERMEDIATE", "WET"]
                if compound and compound.upper() in known_compounds:
                    compound_short = compound[0].upper()
                elif compound and compound != '-':
                    compound_short = "?"
                tyre_display_parts = []
                if compound_short:
                    tyre_display_parts.append(compound_short)
                if age != '?':
                    tyre_display_parts.append(f"{str(age)}L")
                tyre_base = " ".join(
                    tyre_display_parts) if tyre_display_parts else "-"
                new_tyre_indicator = "*" if compound_short and compound_short != '?' and not is_new else ""
                tyre = f"{tyre_base}{new_tyre_indicator}"
                if tyre_base == "-":
                    tyre = "-"
                interval_val = utils.get_nested_state(
                    driver_state, 'IntervalToPositionAhead', 'Value', default='-')
                gap_val = driver_state.get('GapToLeader', '-')
                interval_display_text = str(interval_val).strip(
                ) if interval_val not in [None, "", "-"] else "-"
                gap_display_text = str(gap_val).strip() if gap_val not in [
                    None, "", "-"] else "-"
                bold_interval_text = f"**{interval_display_text}**"
                interval_gap_markdown = ""
                is_p1 = (pos_str == '1')
                show_gap = not is_p1 and session_type_from_state_str in [config.SESSION_TYPE_RACE.lower(
                ), config.SESSION_TYPE_SPRINT.lower()] and gap_display_text != "-"
                if show_gap and interval_display_text != "":
                    normal_weight_gap_text = gap_display_text
                    interval_gap_markdown = f"{bold_interval_text}\\\n{normal_weight_gap_text}"
                elif interval_display_text == "" and is_p1:
                    interval_gap_markdown = ""
                else:
                    if interval_display_text == "-":
                        interval_gap_markdown = "-"
                    else:
                        interval_gap_markdown = bold_interval_text
                last_lap_val = utils.get_nested_state(
                    driver_state, 'LastLapTime', 'Value', default='-')
                if last_lap_val is None or last_lap_val == "":
                    last_lap_val = "-"
                best_lap_val = utils.get_nested_state(
                    driver_state, 'PersonalBestLapTime', 'Value', default='-')
                if best_lap_val is None or best_lap_val == "":
                    best_lap_val = "-"
                s1_val = utils.get_nested_state(
                    driver_state, 'Sectors', '0', 'Value', default='-')
                if s1_val is None or s1_val == "":
                    s1_val = "-"
                s2_val = utils.get_nested_state(
                    driver_state, 'Sectors', '1', 'Value', default='-')
                if s2_val is None or s2_val == "":
                    s2_val = "-"
                s3_val = utils.get_nested_state(
                    driver_state, 'Sectors', '2', 'Value', default='-')
                if s3_val is None or s3_val == "":
                    s3_val = "-"
                
                 # --- Pit Stop Display Logic ---
                reliable_stops = driver_state.get('ReliablePitStops', 0)
                timing_data_stops = driver_state.get('NumberOfPitStops', 0)

                pits_count_display = '0'
                if reliable_stops > 0:
                    pits_count_display = str(reliable_stops)
                elif timing_data_stops > 0:
                    pits_count_display = str(timing_data_stops)

                pits_text_to_display = pits_count_display
                pit_display_state_for_style = "SHOW_COUNT"

                driver_status_raw = driver_state.get('Status', 'N/A')
                driver_status_lower = driver_status_raw.lower()

                if driver_status_lower in TERMINAL_RACING_STATUSES:
                    # If driver is terminally out, ensure pit count is shown.
                    # Default of pits_text_to_display = pits_count_display is already correct.
                    pass
                else:
                    # Driver is not in a terminal status, apply normal live pit logic
                    is_in_pit_flag = driver_state.get('InPit', False)
                    entry_wall_time = driver_state.get(
                        'current_pit_entry_system_time')
                    speed_at_entry = driver_state.get(
                        'pit_entry_replay_speed', 1.0)
                    if not isinstance(speed_at_entry, (float, int)) or speed_at_entry <= 0:
                        speed_at_entry = 1.0

                    final_live_pit_text = driver_state.get(
                        'final_live_pit_time_text')
                    final_live_pit_text_ts = driver_state.get(
                        'final_live_pit_time_display_timestamp')

                    if is_in_pit_flag:
                        pit_display_state_for_style = "IN_PIT_LIVE"
                        if entry_wall_time:
                            current_wall_time_elapsed = current_time_for_callbacks - entry_wall_time
                            live_game_time_elapsed = current_wall_time_elapsed * \
                                current_replay_speed_snapshot  # Use snapshot for consistency
                            pits_text_to_display = f"In Pit: {live_game_time_elapsed:.1f}s"
                        else:
                            pits_text_to_display = "In Pit"
                    elif final_live_pit_text and final_live_pit_text_ts and (current_time_for_callbacks - final_live_pit_text_ts < 15):
                        pits_text_to_display = final_live_pit_text
                        pit_display_state_for_style = "SHOW_COMPLETED_DURATION"
                # --- End of Pit Stop Display Logic ---

                car_data = driver_state.get('CarData', {})
                speed_val = car_data.get('Speed', '-')
                gear = car_data.get('Gear', '-')
                rpm = car_data.get('RPM', '-')
                drs_val = car_data.get('DRS')
                drs_map = {8: "E", 10: "On", 12: "On", 14: "ON"}
                drs = drs_map.get(
                    drs_val, 'Off') if drs_val is not None else 'Off'
                is_overall_best_lap_flag = driver_state.get(
                    'IsOverallBestLap', False)
                is_last_lap_personal_best_flag = utils.get_nested_state(
                    driver_state, 'LastLapTime', 'PersonalFastest', default=False)
                is_s1_personal_best_flag = utils.get_nested_state(
                    driver_state, 'Sectors', '0', 'PersonalFastest', default=False)
                is_s2_personal_best_flag = utils.get_nested_state(
                    driver_state, 'Sectors', '1', 'PersonalFastest', default=False)
                is_s3_personal_best_flag = utils.get_nested_state(
                    driver_state, 'Sectors', '2', 'PersonalFastest', default=False)
                is_overall_best_s1_flag = driver_state.get(
                    'IsOverallBestSector', [False]*3)[0]
                is_overall_best_s2_flag = driver_state.get(
                    'IsOverallBestSector', [False]*3)[1]
                is_overall_best_s3_flag = driver_state.get(
                    'IsOverallBestSector', [False]*3)[2]
                is_last_lap_EVENT_overall_best_flag = utils.get_nested_state(
                    driver_state, 'LastLapTime', 'OverallFastest', default=False)
                is_s1_EVENT_overall_best_flag = utils.get_nested_state(
                    driver_state, 'Sectors', '0', 'OverallFastest', default=False)
                is_s2_EVENT_overall_best_flag = utils.get_nested_state(
                    driver_state, 'Sectors', '1', 'OverallFastest', default=False)
                is_s3_EVENT_overall_best_flag = utils.get_nested_state(
                    driver_state, 'Sectors', '2', 'OverallFastest', default=False)
                current_driver_highlight_type = "NONE"
                driver_pos_int = -1
                if pos_str != '-':
                    try:
                        driver_pos_int = int(pos_str)
                    except ValueError:
                        pass
                if q1_eliminated_highlight_rule["type"] == "GREY_ELIMINATED" and driver_pos_int != -1 and q1_eliminated_highlight_rule["lower_pos"] <= driver_pos_int <= q1_eliminated_highlight_rule["upper_pos"]:
                    current_driver_highlight_type = "GREY_ELIMINATED"
                if current_driver_highlight_type == "NONE" and q2_eliminated_highlight_rule["type"] == "GREY_ELIMINATED" and driver_pos_int != -1 and q2_eliminated_highlight_rule["lower_pos"] <= driver_pos_int <= q2_eliminated_highlight_rule["upper_pos"]:
                    current_driver_highlight_type = "GREY_ELIMINATED"
                if current_driver_highlight_type == "NONE":
                    if active_segment_highlight_rule["type"] == "RED_DANGER":
                        if driver_pos_int != -1 and active_segment_highlight_rule["lower_pos"] <= driver_pos_int <= active_segment_highlight_rule["upper_pos"]:
                            current_driver_highlight_type = "RED_DANGER"
                        elif pos_str == '-':
                            current_driver_highlight_type = "RED_DANGER"
                row = {
                    'id': car_num, 'No.': racing_no, 'Car': tla, 'Pos': pos, 'Tyre': tyre,
                    'IntervalGap': interval_gap_markdown, 'Last Lap': last_lap_val, 'Best Lap': best_lap_val,
                    'S1': s1_val, 'S2': s2_val, 'S3': s3_val, 'Pits': pits_text_to_display,
                    'Status': driver_status_raw, 'Speed': speed_val, 'Gear': gear, 'RPM': rpm, 'DRS': drs,
                    'IsOverallBestLap_Str': "TRUE" if is_overall_best_lap_flag else "FALSE",
                    'IsOverallBestS1_Str': "TRUE" if is_overall_best_s1_flag else "FALSE",
                    'IsOverallBestS2_Str': "TRUE" if is_overall_best_s2_flag else "FALSE",
                    'IsOverallBestS3_Str': "TRUE" if is_overall_best_s3_flag else "FALSE",
                    'IsLastLapPersonalBest_Str': "TRUE" if is_last_lap_personal_best_flag else "FALSE",
                    'IsPersonalBestS1_Str': "TRUE" if is_s1_personal_best_flag else "FALSE",
                    'IsPersonalBestS2_Str': "TRUE" if is_s2_personal_best_flag else "FALSE",
                    'IsPersonalBestS3_Str': "TRUE" if is_s3_personal_best_flag else "FALSE",
                    'IsLastLapEventOverallBest_Str': "TRUE" if is_last_lap_EVENT_overall_best_flag else "FALSE",
                    'IsS1EventOverallBest_Str': "TRUE" if is_s1_EVENT_overall_best_flag else "FALSE",
                    'IsS2EventOverallBest_Str': "TRUE" if is_s2_EVENT_overall_best_flag else "FALSE",
                    'IsS3EventOverallBest_Str': "TRUE" if is_s3_EVENT_overall_best_flag else "FALSE",
                    'PitDisplayState_Str': pit_display_state_for_style,
                    'QualiHighlight_Str': current_driver_highlight_type,
                }
                processed_table_data.append(row)
            processed_table_data.sort(key=utils.pos_sort_key)
            table_data = processed_table_data
        else:
            timestamp_text = config.TEXT_WAITING_FOR_DATA
        logger.debug(f"'{func_name}' - Table data prep (loop & sort): {time.monotonic() - table_data_prep_start_time:.4f}s")

        callback_duration = time.perf_counter() - callback_start_time
        if callback_duration > 0.1:  # Log if callback takes more than 100ms
            logger.warning(
                f"update_main_data_displays callback took {callback_duration:.3f} seconds. Debug mode: {debug_mode_enabled}")
        logger.debug(f"Callback '{func_name}' END. Total time: {time.monotonic() - overall_start_time:.4f}s")
        return other_elements, table_data, timestamp_text

    except Exception as e_update:
        logger.error(
            f"Error in update_main_data_displays callback: {e_update}", exc_info=True)
        return no_update, no_update, no_update


@app.callback(
    Output('dummy-output-for-controls', 'children', allow_duplicate=True),
    Input('replay-speed-slider', 'value'),
    prevent_initial_call=True
)
def update_replay_speed_state(new_speed_value): # Removed session_info_children_trigger, get from session_state
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if new_speed_value is None:
        return no_update

    try:
        new_speed = float(new_speed_value)
        if not (0.1 <= new_speed <= 100.0): # Example validation for speed range
            logger.warning(f"Invalid replay speed requested: {new_speed}. Clamping or ignoring.")
            # Clamp or return no_update, e.g., new_speed = max(0.1, min(new_speed, 100.0))
            # For now, let's assume valid input based on slider range or simply ignore if too extreme.
            if new_speed <= 0: return no_update # Definitely ignore non-positive
    except (ValueError, TypeError):
        logger.warning(f"Could not convert replay speed slider value '{new_speed_value}' to float.")
        return no_update


    with session_state.lock:
        old_speed = session_state.replay_speed # Speed active *before* this change

        # If speed hasn't actually changed, do nothing to avoid potential float precision issues
        if abs(old_speed - new_speed) < 0.01: # Tolerance for float comparison
            # Still update session_state.replay_speed to the precise new_speed_value if slider was just wiggled
            session_state.replay_speed = new_speed
            return no_update

        session_type = session_state.session_details.get('Type', "Unknown").lower() #
        q_state = session_state.qualifying_segment_state # Primary timing state dictionary

        current_official_remaining_s_at_anchor = q_state.get("official_segment_remaining_seconds") #
        last_capture_utc_anchor = q_state.get("last_official_time_capture_utc") #

        now_utc = datetime.now(timezone.utc) #
        calculated_current_true_remaining_s = None

        # --- This block calculates the true current remaining time based on OLD speed ---
        if session_type.startswith("practice"):
            # For practice, use its continuous model to find current true remaining time
            practice_start_utc = session_state.practice_session_actual_start_utc #
            practice_duration_s = session_state.practice_session_scheduled_duration_seconds #
            if practice_start_utc and practice_duration_s is not None:
                wall_time_elapsed_practice = (now_utc - practice_start_utc).total_seconds() #
                session_time_elapsed_practice = wall_time_elapsed_practice * old_speed
                calculated_current_true_remaining_s = practice_duration_s - session_time_elapsed_practice

        # For Qualifying (or if Practice didn't have its continuous model vars set yet)
        # Use the q_state anchor point.
        if calculated_current_true_remaining_s is None and \
           last_capture_utc_anchor and current_official_remaining_s_at_anchor is not None:
            wall_time_since_last_anchor = (now_utc - last_capture_utc_anchor).total_seconds() #
            session_time_elapsed_since_anchor = wall_time_since_last_anchor * old_speed
            calculated_current_true_remaining_s = current_official_remaining_s_at_anchor - session_time_elapsed_since_anchor

        # --- Now, re-anchor using this calculated_current_true_remaining_s ---
        if calculated_current_true_remaining_s is not None:
            new_anchor_remaining_s = max(0, calculated_current_true_remaining_s)

            # Update the main anchor point (q_state)
            q_state["official_segment_remaining_seconds"] = new_anchor_remaining_s #
            q_state["last_official_time_capture_utc"] = now_utc #
            # last_capture_replay_speed will be effectively the new_speed for next extrapolation
            # session_status_at_capture might need an update if relevant, but for re-speed, it's less critical
            q_state["last_capture_replay_speed"] = new_speed # Reflect that this anchor is for the new speed

            logger.info(
                f"Replay speed changing from {old_speed:.2f}x to {new_speed:.2f}x. "
                f"Original anchor: {current_official_remaining_s_at_anchor:.2f}s at {last_capture_utc_anchor}. "
                f"Calculated true current remaining: {calculated_current_true_remaining_s:.2f}s. "
                f"New anchor set: {new_anchor_remaining_s:.2f}s at {now_utc}."
            )

            # If it was a Practice session using its continuous model, adjust its effective start time
            # so its formula yields the new_anchor_remaining_s with the new_speed.
            if session_type.startswith("practice") and \
               session_state.practice_session_actual_start_utc and \
               session_state.practice_session_scheduled_duration_seconds is not None:

                duration_s = session_state.practice_session_scheduled_duration_seconds #
                if new_speed > 0: # Avoid division by zero
                    # We want: new_anchor_remaining_s = duration_s - (now_utc - new_practice_start_utc) * new_speed
                    # (now_utc - new_practice_start_utc) * new_speed = duration_s - new_anchor_remaining_s
                    # (now_utc - new_practice_start_utc) = (duration_s - new_anchor_remaining_s) / new_speed
                    # new_practice_start_utc = now_utc - timedelta(seconds = (duration_s - new_anchor_remaining_s) / new_speed)

                    wall_time_offset_for_new_start = (duration_s - new_anchor_remaining_s) / new_speed
                    session_state.practice_session_actual_start_utc = now_utc - timedelta(seconds=wall_time_offset_for_new_start) #
                    logger.info(
                        f"Adjusted practice_session_actual_start_utc to {session_state.practice_session_actual_start_utc} " #
                        f"to maintain {new_anchor_remaining_s:.2f}s remaining at {new_speed:.2f}x."
                    )
        else:
            logger.warning(
                f"Could not re-anchor timer on speed change: insufficient data. "
                f"Old speed: {old_speed}, New speed: {new_speed}. "
                f"Current official remaining: {current_official_remaining_s_at_anchor}, Last capture: {last_capture_utc_anchor}"
            )

        # Finally, update the global replay speed
        session_state.replay_speed = new_speed #
        logger.debug(f"Replay speed updated in session_state to: {session_state.replay_speed}") #
    
    logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
    return no_update

@app.callback(
    [Output('dummy-output-for-controls', 'children', allow_duplicate=True),
     Output('track-map-graph', 'figure', allow_duplicate=True),
     Output('car-positions-store', 'data', allow_duplicate=True)],
    [Input('connect-button', 'n_clicks'),
     Input('replay-button', 'n_clicks'),
     Input('stop-reset-button', 'n_clicks')],
    [State('replay-file-selector', 'value'),
     State('replay-speed-slider', 'value'),
     State('record-data-checkbox', 'value')],
    prevent_initial_call=True
)
def handle_control_clicks(connect_clicks: Optional[int], replay_clicks: Optional[int], stop_reset_clicks: Optional[int],
                          selected_replay_file: Optional[str], replay_speed_value: Optional[float],
                          record_checkbox_input: Any) -> Tuple[Any, Any, Any]:
    
    session_state = app_state.get_or_create_session_state()
    if not session_state:
        logger.error("handle_control_clicks: Critical - Could not get/create session state!")
        return dash.no_update, dash.no_update, dash.no_update

    ctx = dash.callback_context
    button_id = ctx.triggered_id if ctx.triggered else None
    sess_id_log = session_state.session_id[:8]
    logger.info(f"Session {sess_id_log}: Control button clicked: {button_id}")

    # Default outputs
    dummy_output = dash.no_update
    track_map_output = dash.no_update # Use specific map reset when needed
    car_pos_store_output = dash.no_update

    # Update session's record_live_data preference from checkbox
    # dbc.Switch value is boolean, dcc.Checklist is a list
    if isinstance(record_checkbox_input, list): # Handles Checklist
        session_record_pref = bool(record_checkbox_input and record_checkbox_input[0])
    else: # Handles Switch or direct boolean
        session_record_pref = bool(record_checkbox_input)
    
    with session_state.lock:
        session_state.record_live_data = session_record_pref
        logger.debug(f"Session {sess_id_log}: record_live_data preference set to {session_state.record_live_data}")

    if button_id == 'connect-button':
        with session_state.lock:
            current_s_state = session_state.app_status["state"]
            if current_s_state not in ["Idle", "Stopped", "Error", "Playback Complete"]:
                logger.warning(f"Session {sess_id_log}: Connect ignored. Session state: {current_s_state}")
                return dummy_output, track_map_output, car_pos_store_output
            
            # Stop any existing replay for this session first
            if session_state.replay_thread and session_state.replay_thread.is_alive():
                logger.info(f"Session {sess_id_log}: Stopping active replay to connect live.")
                replay.stop_replay_session(session_state) # SESSION-AWARE
                time.sleep(0.3) # Allow time for thread to join
            
            session_state.stop_event.clear()
            session_state.reset_state_variables() # Reset data stores for a fresh session
            session_state.record_live_data = session_record_pref # Re-apply preference after reset

            session_state.app_status.update({
                "state": "Initializing", 
                "connection": config.TEXT_SIGNALR_SOCKET_CONNECTING_STATUS,
                "current_replay_file": None # Ensure no replay file is listed
            })
            logger.info(f"Session {sess_id_log}: State reset and set to Initializing for live connection.")
            # Reset track map related states (will be handled by initialize_track_map on SessionKey change)
            session_state.track_coordinates_cache = app_state.INITIAL_SESSION_TRACK_COORDINATES_CACHE.copy()
            session_state.session_details['SessionKey'] = None # Force map update via key change
            session_state.selected_driver_for_map_and_lap_chart = None


        logger.info(f"Session {sess_id_log}: Initiating live connection. Recording: {session_state.record_live_data}")
        websocket_url, ws_headers = signalr_client.build_connection_url(config.NEGOTIATE_URL_BASE, config.HUB_NAME)
        
        if websocket_url and ws_headers:
            if session_state.record_live_data:
                if not replay.init_live_file_session(session_state): # SESSION-AWARE
                    logger.error(f"Session {sess_id_log}: Failed to initialize live recording file.")
            
            conn_thread = threading.Thread(
                target=signalr_client.run_connection_session, # SESSION-AWARE
                args=(session_state, websocket_url, ws_headers), 
                name=f"SigRConn_Sess_{sess_id_log}", daemon=True
            )
            dp_thread = threading.Thread(
                target=data_processing.data_processing_loop_session, # SESSION-AWARE
                args=(session_state,),
                name=f"DataProc_Sess_{sess_id_log}", daemon=True
            )
            with session_state.lock:
                session_state.connection_thread = conn_thread
                session_state.data_processing_thread = dp_thread
            conn_thread.start()
            dp_thread.start()
            logger.info(f"Session {sess_id_log}: SignalR connection and Data Processing threads initiated.")
        else: 
            with session_state.lock:
                session_state.app_status.update({"state": "Error", "connection": "Negotiation Failed"})
        
        # Signal for map reset (clientside can use timestamp to force update)
        track_map_output = utils.create_empty_figure_with_message(config.TRACK_MAP_WRAPPER_HEIGHT, f"map_connect_{time.time()}", config.TEXT_TRACK_MAP_LOADING, config.TRACK_MAP_MARGINS)
        car_pos_store_output = {'status': 'reset_map_display', 'timestamp': time.time()}

    elif button_id == 'replay-button':
        if not selected_replay_file:
            logger.warning(f"Session {sess_id_log}: Start Replay: {config.TEXT_REPLAY_SELECT_FILE}")
            return dummy_output, track_map_output, car_pos_store_output

        with session_state.lock:
            # Stop any existing live connection or other replay for THIS session
            if session_state.connection_thread and session_state.connection_thread.is_alive():
                logger.info(f"Session {sess_id_log}: Stopping active live connection to start replay.")
                signalr_client.stop_connection_session(session_state) # SESSION-AWARE
                time.sleep(0.3) # Allow time for thread to join
            if session_state.replay_thread and session_state.replay_thread.is_alive(): # If another replay was running
                 logger.info(f"Session {sess_id_log}: Stopping previous replay to start new one.")
                 replay.stop_replay_session(session_state) # SESSION-AWARE
                 time.sleep(0.3)
            
            # Set replay speed for this session before starting replay
            try: speed_val = float(replay_speed_value if replay_speed_value is not None else 1.0)
            except: speed_val = 1.0
            session_state.replay_speed = max(0.1, speed_val)
            
            # Clear relevant states before starting replay (start_replay_session will also do resets)
            session_state.stop_event.clear()
            session_state.reset_state_variables() # Full reset for new replay
            session_state.record_live_data = False # Ensure recording is off for replays
            
            session_state.app_status.update({ # Set initial status before thread starts
                "state": "Initializing",
                "connection": f"Replay Prep: {selected_replay_file}",
                "current_replay_file": selected_replay_file
            })
            # Reset track map related states
            session_state.track_coordinates_cache = app_state.INITIAL_SESSION_TRACK_COORDINATES_CACHE.copy()
            session_state.session_details['SessionKey'] = None 
            session_state.selected_driver_for_map_and_lap_chart = None
        
        full_replay_path = Path(config.REPLAY_DIR) / selected_replay_file
        
        # replay.start_replay_session handles starting its own thread and data processing thread
        if replay.start_replay_session(session_state, full_replay_path, session_state.replay_speed): # SESSION-AWARE
            logger.info(f"Session {sess_id_log}: Replay initiation process for {full_replay_path.name} started.")
        else:
            logger.error(f"Session {sess_id_log}: Failed to start replay for {full_replay_path.name}.")
            # start_replay_session should update session_state.app_status to Error
        
        track_map_output = utils.create_empty_figure_with_message(config.TRACK_MAP_WRAPPER_HEIGHT, f"map_replay_{time.time()}", config.TEXT_TRACK_MAP_LOADING, config.TRACK_MAP_MARGINS)
        car_pos_store_output = {'status': 'reset_map_display', 'timestamp': time.time()}

    elif button_id == 'stop-reset-button':
        logger.info(f"Session {sess_id_log}: Stop & Reset Session button clicked.")
        
        logger.info(f"Session {sess_id_log}: Stopping SignalR connection (if any)...")
        signalr_client.stop_connection_session(session_state) # SESSION-AWARE

        logger.info(f"Session {sess_id_log}: Stopping replay (if any)...")
        replay.stop_replay_session(session_state) # SESSION-AWARE
        
        # Stop auto-connect thread if running for this session
        with session_state.lock:
            if session_state.auto_connect_thread and session_state.auto_connect_thread.is_alive():
                logger.info(f"Session {sess_id_log}: Stopping auto-connect thread...")
                session_state.stop_event.set() # stop_event is shared for now
                session_state.auto_connect_thread.join(timeout=3.0)
                if session_state.auto_connect_thread.is_alive(): logger.warning(f"Session {sess_id_log}: Auto-connect thread did not join.")
                session_state.auto_connect_thread = None
                session_state.auto_connect_enabled = False # Turn off preference as well on manual stop/reset

            # Ensure data processing thread for this session is stopped
            # It should stop when stop_event is set by stop_connection or stop_replay
            if session_state.data_processing_thread and session_state.data_processing_thread.is_alive():
                logger.info(f"Session {sess_id_log}: Ensuring data processing thread is stopped...")
                # session_state.stop_event should already be set
                session_state.data_processing_thread.join(timeout=3.0)
                if session_state.data_processing_thread.is_alive():
                    logger.warning(f"Session {sess_id_log}: Data processing thread did not join cleanly on stop/reset.")
                session_state.data_processing_thread = None

        logger.info(f"Session {sess_id_log}: Resetting session state...")
        session_state.reset_state_variables() # Resets this session's state
        
        with session_state.lock: # Ensure status is correctly set after reset
             session_state.app_status.update({
                 "state": "Idle", 
                 "connection": config.TEXT_SIGNALR_DISCONNECTED_STATUS,
                 "current_replay_file": None,
                 "auto_connected_session_identifier": None,
                 "auto_connected_session_end_detected_utc": None
            })
        session_state.stop_event.clear() # Clear for future operations in this session

        map_reset_fig = utils.create_empty_figure_with_message(
            config.TRACK_MAP_WRAPPER_HEIGHT, f"reset_map_sess_{sess_id_log}_{time.time()}",
            config.TEXT_TRACK_MAP_DATA_WILL_LOAD, config.TRACK_MAP_MARGINS
        )
        map_reset_fig.update_layout(plot_bgcolor='rgb(30,30,30)', paper_bgcolor='rgba(0,0,0,0)')
        track_map_output = map_reset_fig
        car_pos_store_output = {'status': 'reset_map_display', 'timestamp': time.time()}
        logger.info(f"Session {sess_id_log}: Stop & Reset processing finished.")

    return dummy_output, track_map_output, car_pos_store_output

@app.callback(
    Output('record-data-checkbox', 'id', allow_duplicate=True), # Keep id as string
    Input('record-data-checkbox', 'value'),
    prevent_initial_call=True
)
def record_checkbox_callback(checked_value):
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if checked_value is None: return 'record-data-checkbox' # Return existing ID string
    new_state = bool(checked_value)
    logger.debug(f"Record Live Data checkbox set to: {new_state}")
    with session_state.lock: session_state.record_live_data = new_state
    logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
    return 'record-data-checkbox' # Return existing ID string


@app.callback(
    Output('replay-file-selector', 'options'),
    Input('interval-component-slow', 'n_intervals')
)
def update_replay_options(n_intervals):
     return replay.get_replay_files(config.REPLAY_DIR) #


@app.callback(
    Output("collapse-controls", "is_open"),
    [Input("collapse-controls-button", "n_clicks")],
    [State("collapse-controls", "is_open")],
    prevent_initial_call=True,
)
def toggle_controls_collapse(n, is_open):
    if n:
        return not is_open
    return is_open

@app.callback(
    [Output('driver-details-output', 'children'),      # For basic driver Name/Team
     Output('lap-selector-dropdown', 'options'),       # For Telemetry Tab
     Output('lap-selector-dropdown', 'value'),         # For Telemetry Tab
     Output('lap-selector-dropdown', 'disabled'),      # For Telemetry Tab
     Output('telemetry-graph', 'figure'),              # For Telemetry Tab
     Output('stint-history-table', 'data'),            # For Stint History Tab
     Output('stint-history-table', 'columns')],        # For Stint History Tab (if dynamic, else set in layout)
    [Input('driver-select-dropdown', 'value'),         # Driver selected
     Input('driver-focus-tabs', 'active_tab'),         # Which tab is active
     Input('lap-selector-dropdown', 'value')],         # Lap selected for telemetry (if telemetry tab is active)
    [State('telemetry-graph', 'figure'),               # Current telemetry figure state
     State('stint-history-table', 'columns')]          # Current columns for stint table (if needed)
)
def update_driver_focus_content(selected_driver_number, active_tab_id, 
                                selected_lap_for_telemetry, 
                                current_telemetry_figure, current_stint_table_columns):
    session_state = app_state.get_or_create_session_state()
    overall_callback_start_time = time.monotonic() # For overall timing
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START_OVERALL") # Overall start
    
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered and ctx.triggered[0] else 'N/A'
    
    logger.debug(
        f"'{func_name}': Trigger='{triggered_id}', Driver='{selected_driver_number}', "
        f"ActiveTab='{active_tab_id}', SelectedLap='{selected_lap_for_telemetry}'"
    )

    driver_basic_details_children = [html.P(config.TEXT_DRIVER_SELECT, style={'fontSize':'0.8rem', 'padding':'5px'})]
    telemetry_lap_options = config.DROPDOWN_NO_LAPS_OPTIONS
    telemetry_lap_value = None
    telemetry_lap_disabled = True
    fig_telemetry = utils.create_empty_figure_with_message(
        config.TELEMETRY_WRAPPER_HEIGHT, config.INITIAL_TELEMETRY_UIREVISION,
        config.TEXT_DRIVER_SELECT_LAP, config.TELEMETRY_MARGINS_EMPTY
    )
    stint_history_data = []
    stint_history_columns_output = no_update 

    if not selected_driver_number:
        # logger.info(f"Callback '{func_name}' END_OVERALL. No driver. Total Took: {time.monotonic() - overall_callback_start_time:.4f}s")
        # fig_telemetry is already set to the initial empty one. Return it if it wasn't already the initial.
        if current_telemetry_figure and \
           current_telemetry_figure.get('layout', {}).get('uirevision') == config.INITIAL_TELEMETRY_UIREVISION:
            fig_telemetry_output = no_update
        else:
            fig_telemetry_output = fig_telemetry # Return the newly created empty figure
        
        logger.debug(f"Callback '{func_name}' END_OVERALL (No Driver). Total Took: {time.monotonic() - overall_callback_start_time:.4f}s")
        return (driver_basic_details_children, telemetry_lap_options, telemetry_lap_value, telemetry_lap_disabled, fig_telemetry_output,
                stint_history_data, stint_history_columns_output)

    driver_num_str = str(selected_driver_number)
    driver_info_state = {}
    all_stints_for_driver = []
    available_telemetry_laps = []

    # --- Initial Data Fetch (Locking for session_state access) ---
    lock_acquisition_start_time = time.monotonic()
    with session_state.lock:
        lock_acquired_time = time.monotonic()
        logger.debug(f"Lock in '{func_name}' (Initial Fetch) - ACQUIRED. Wait: {lock_acquired_time - lock_acquisition_start_time:.4f}s")
        critical_section_start_time = time.monotonic()
        
        driver_info_state = session_state.timing_state.get(driver_num_str, {}).copy()
        all_stints_for_driver = copy.deepcopy(session_state.driver_stint_data.get(driver_num_str, [])) # Deepcopy if modified later, or if sub-elements are complex
        available_telemetry_laps = sorted(list(session_state.telemetry_data.get(driver_num_str, {}).keys())) # Get keys (lap numbers)
        
        logger.debug(f"Lock in '{func_name}' (Initial Fetch) - HELD for critical section: {time.monotonic() - critical_section_start_time:.4f}s")

    # --- Driver Basic Details ---
    if driver_info_state:
        tla = driver_info_state.get('Tla', '?')
        # ... (rest of your driver_basic_details_children setup) ...
        driver_basic_details_children = [
            html.H6(f"#{driver_info_state.get('RacingNumber', driver_num_str)} {tla} - {driver_info_state.get('FullName', 'Unknown')}", 
                    style={'marginTop': '0px', 'marginBottom':'2px', 'fontSize':'0.9rem'}),
            html.P(f"Team: {driver_info_state.get('TeamName', '?')}", 
                   style={'fontSize':'0.75rem', 'marginBottom':'0px', 'color': 'lightgrey'})
        ]
    else:
        driver_basic_details_children = [html.P(f"Details for driver {driver_num_str} not found.", style={'color':'orange'})]
        tla = driver_num_str # Fallback for uirevision

    # --- Tab Specific Logic ---
    if active_tab_id == "tab-telemetry":
        driver_selected_uirevision_telemetry = f"telemetry_driver_{driver_num_str}_pendinglap" # For "no laps" or "select lap" states
        
        if available_telemetry_laps:
            telemetry_lap_options = [{'label': f'Lap {l}', 'value': l} for l in available_telemetry_laps]
            telemetry_lap_disabled = False
            
            # Determine the lap to plot
            if triggered_id == 'driver-select-dropdown' or \
               triggered_id == 'driver-focus-tabs' or \
               not selected_lap_for_telemetry or \
               selected_lap_for_telemetry not in available_telemetry_laps:
                telemetry_lap_value = available_telemetry_laps[-1] 
            else:
                telemetry_lap_value = selected_lap_for_telemetry
        
            # If telemetry_lap_value is now set (meaning we have a lap to plot)
            if telemetry_lap_value:
                data_plot_uirevision_telemetry = f"telemetry_data_{driver_num_str}_{telemetry_lap_value}" # uirevision for specific data

                # Check if we really need to update the figure
                # (e.g., if only active_tab_id changed to telemetry but figure for this driver/lap already shown)
                if current_telemetry_figure and \
                   current_telemetry_figure.get('layout',{}).get('uirevision') == data_plot_uirevision_telemetry and \
                   triggered_id == 'driver-focus-tabs': # Only no_update if it was just a tab switch to an already rendered exact figure
                    logger.debug(f"'{func_name}': Telemetry figure for {driver_num_str} Lap {telemetry_lap_value} already rendered, no_update on tab switch.")
                    fig_telemetry = no_update
                else:
                    # Fetch specific lap_data for plotting
                    lap_data_fetch_start_time = time.monotonic()
                    lap_data = {}
                    with session_state.lock: # Second, brief lock for specific lap data
                        lap_data_lock_acquired_time = time.monotonic()
                        logger.debug(f"Lock2 in '{func_name}' (Telemetry-LapData) - ACQUIRED. Wait: {lap_data_lock_acquired_time - lap_data_fetch_start_time:.4f}s")
                        lap_data_critical_start_time = time.monotonic()
                        lap_data = copy.deepcopy(session_state.telemetry_data.get(driver_num_str, {}).get(telemetry_lap_value, {}))
                        logger.debug(f"Lock2 in '{func_name}' (Telemetry-LapData) - HELD for lap_data: {time.monotonic() - lap_data_critical_start_time:.4f}s")
                    
                    logger.debug(f"'{func_name}' (Telemetry Tab) - Specific lap_data fetch for {driver_num_str} Lap {telemetry_lap_value} took: {time.monotonic() - lap_data_fetch_start_time:.4f}s (incl. wait & hold)")

                    # Plotting logic
                    if lap_data:
                        logger.debug(f"'{func_name}' (Telemetry Tab) - Starting Plotly figure generation for driver {driver_num_str}, lap {telemetry_lap_value}.")
                        plotly_render_actual_start_time = time.monotonic()
                        
                        timestamps_str = lap_data.get('Timestamps', [])
                        timestamps_dt = [utils.parse_iso_timestamp_safe(ts) for ts in timestamps_str]
                        valid_indices = [i for i, dt_obj in enumerate(timestamps_dt) if dt_obj is not None]

                        if valid_indices:
                            timestamps_plot = [timestamps_dt[i] for i in valid_indices]
                            channels = ['Speed', 'RPM', 'Throttle', 'Brake', 'Gear', 'DRS']
                            fig_telemetry = make_subplots(rows=len(channels), cols=1, shared_xaxes=True,
                                                          subplot_titles=[c[:10] for c in channels], vertical_spacing=0.06)
                            for i, channel in enumerate(channels):
                                y_data_raw = lap_data.get(channel, [])
                                y_data_plot = [(y_data_raw[idx] if idx < len(y_data_raw) else None) for idx in valid_indices]
                                if channel == 'DRS':
                                    drs_plot = [1 if val in [10, 12, 14] else 0 for val in y_data_plot]
                                    fig_telemetry.add_trace(go.Scattergl(x=timestamps_plot, y=drs_plot, mode='lines', name=channel, line_shape='hv', connectgaps=False), row=i+1, col=1)
                                    fig_telemetry.update_yaxes(fixedrange=True, tickvals=[0,1], ticktext=['Off','On'], range=[-0.1,1.1], row=i+1, col=1, title_text="", title_standoff=2, title_font_size=9, tickfont_size=8)
                                else:
                                    fig_telemetry.add_trace(go.Scattergl(x=timestamps_plot, y=y_data_plot, mode='lines', name=channel, connectgaps=False), row=i+1, col=1)
                                    fig_telemetry.update_yaxes(fixedrange=True, row=i+1, col=1, title_text="", title_standoff=2, title_font_size=9, tickfont_size=8)
                            
                            fig_telemetry.update_layout(
                                template='plotly_dark', height=config.TELEMETRY_WRAPPER_HEIGHT,
                                hovermode="x unified", showlegend=False, margin=config.TELEMETRY_MARGINS_DATA,
                                title_text=f"<b>{tla} - Lap {telemetry_lap_value} Telemetry</b>",
                                title_x=0.5, title_y=0.98, title_font_size=12,
                                uirevision=data_plot_uirevision_telemetry, # CRITICAL for performance
                                annotations=[] 
                            )
                            # ... (your axes updates) ...
                        else: # No valid plot data for this lap
                            fig_telemetry = utils.create_empty_figure_with_message(
                                config.TELEMETRY_WRAPPER_HEIGHT, data_plot_uirevision_telemetry,
                                config.TEXT_TELEMETRY_NO_PLOT_DATA_FOR_LAP_PREFIX + str(telemetry_lap_value) + ".",
                                config.TELEMETRY_MARGINS_EMPTY
                            )
                        logger.debug(f"'{func_name}' (Telemetry Tab) - Plotly Figure Generation actual took: {time.monotonic() - plotly_render_actual_start_time:.4f}s")
                    else: # lap_data was empty
                        fig_telemetry = utils.create_empty_figure_with_message(
                            config.TELEMETRY_WRAPPER_HEIGHT, data_plot_uirevision_telemetry,
                            config.TEXT_TELEMETRY_NO_PLOT_DATA_FOR_LAP_PREFIX +
                            str(telemetry_lap_value) + ".",
                            config.TELEMETRY_MARGINS_EMPTY
                        )
            else: # No available_telemetry_laps or telemetry_lap_value could not be set
                no_laps_message = config.TEXT_DRIVER_NO_LAP_DATA_PREFIX + tla + "."
                fig_telemetry = utils.create_empty_figure_with_message(
                    config.TELEMETRY_WRAPPER_HEIGHT, driver_selected_uirevision_telemetry, 
                    no_laps_message, config.TELEMETRY_MARGINS_EMPTY
                )
        else: # No available_telemetry_laps
             no_laps_message = config.TEXT_DRIVER_NO_LAP_DATA_PREFIX + tla + "."
             fig_telemetry = utils.create_empty_figure_with_message(
                config.TELEMETRY_WRAPPER_HEIGHT, driver_selected_uirevision_telemetry, 
                no_laps_message, config.TELEMETRY_MARGINS_EMPTY
            )
        stint_history_data = no_update # Stint history not visible on this tab

    elif active_tab_id == "tab-stint-history":
        # ... (your existing stint history logic - ensure it's efficient if it becomes an issue) ...
        # For now, assuming it's okay.
        # The fig_telemetry should be an empty placeholder for this tab.
        fig_telemetry = utils.create_empty_figure_with_message(
            config.TELEMETRY_WRAPPER_HEIGHT, config.INITIAL_TELEMETRY_UIREVISION, # Use initial uirevision
            "Select Telemetry tab to view lap data.", config.TELEMETRY_MARGINS_EMPTY
        )
        telemetry_lap_options = config.DROPDOWN_NO_LAPS_OPTIONS # Reset telemetry dropdown
        telemetry_lap_value = None
        telemetry_lap_disabled = True
        
        # Process stint data (from your code, seems reasonable)
        if all_stints_for_driver:
            stint_history_data = [] # Clear previous before reprocessing
            for stint_entry in all_stints_for_driver:
                processed_entry = stint_entry.copy()
                processed_entry['is_new_tyre_display'] = 'Y' if stint_entry.get('is_new_tyre') else 'N'
                stint_history_data.append(processed_entry)
        else:
            stint_history_data = [{ # Placeholder for no data
                'stint_number': "No stint data available.", 'start_lap': '-', 'compound': '-', 
                'is_new_tyre_display': '-', 'tyre_age_at_stint_start': '-', 
                'end_lap': '-', 'total_laps_on_tyre_in_stint': '-', 
                'tyre_total_laps_at_stint_end': '-'
            }]

    else: # Unknown tab
        logger.warning(f"'{func_name}': Unknown active tab ID: {active_tab_id}")
        # Return all defaults, including the initial empty telemetry figure
        fig_telemetry = utils.create_empty_figure_with_message(
            config.TELEMETRY_WRAPPER_HEIGHT, config.INITIAL_TELEMETRY_UIREVISION,
            config.TEXT_DRIVER_SELECT_LAP, config.TELEMETRY_MARGINS_EMPTY
        )
        # telemetry_lap_options, telemetry_lap_value, telemetry_lap_disabled already default
        # stint_history_data, stint_history_columns_output already default/no_update

    logger.debug(f"Callback '{func_name}' END_OVERALL. Total Took: {time.monotonic() - overall_callback_start_time:.4f}s")
    return (driver_basic_details_children, telemetry_lap_options, telemetry_lap_value, telemetry_lap_disabled, fig_telemetry,
            stint_history_data, stint_history_columns_output)


@app.callback(
    Output('current-track-layout-cache-key-store', 'data'),
    Input('interval-component-medium', 'n_intervals'),
    State('current-track-layout-cache-key-store', 'data')
)
def update_current_session_id_for_map(n_intervals, existing_session_id_in_store):
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    with session_state.lock:
        year = session_state.session_details.get('Year')
        circuit_key = session_state.session_details.get('CircuitKey')
        app_status_state = session_state.app_status.get("state", "Idle")

    if not year or not circuit_key or app_status_state in ["Idle", "Stopped", "Error"]:
        if existing_session_id_in_store is not None:
            # Clear the selected driver if session changes or becomes invalid
            with session_state.lock:
                if session_state.selected_driver_for_map_and_lap_chart is not None:
                    logger.debug("Clearing selected_driver_for_map_and_lap_chart due to invalid/changed session.")
                    session_state.selected_driver_for_map_and_lap_chart = None
            return None
        return dash.no_update

    current_session_id = f"{year}_{circuit_key}"

    if current_session_id != existing_session_id_in_store:
        logger.debug(
            f"Updating current-track-layout-cache-key-store to: {current_session_id}. Clearing selected driver.")
        with session_state.lock: # Clear selected driver on session change
            session_state.selected_driver_for_map_and_lap_chart = None
        logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
        return current_session_id

    return dash.no_update


@app.callback(
    Output('clientside-update-interval', 'disabled'),
    [Input('connect-button', 'n_clicks'),
     Input('replay-button', 'n_clicks'),
     Input('stop-reset-button', 'n_clicks'),
     Input('interval-component-fast', 'n_intervals')],
    [State('clientside-update-interval', 'disabled'),
     State('replay-file-selector', 'value')]
)
def toggle_clientside_interval(connect_clicks, replay_clicks,
                               stop_reset_clicks,
                               fast_interval_tick, currently_disabled, selected_replay_file):
    session_state = app_state.get_or_create_session_state()
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered and ctx.triggered[0] else None

    if not triggered_id:
        return no_update

    with session_state.lock:
        current_app_s = session_state.app_status.get("state", "Idle")

    if triggered_id in ['connect-button', 'replay-button']:
        if triggered_id == 'replay-button' and not selected_replay_file:
            logger.info(
                f"Replay button clicked, but no file selected ({config.TEXT_REPLAY_SELECT_FILE}). Clientside interval remains disabled.") # Use constant
            return True

        logger.debug(
            f"Attempting to enable clientside-update-interval due to '{triggered_id}'.")
        return False

    elif triggered_id == 'stop-reset-button':
        logger.debug(
            f"Disabling clientside-update-interval due to '{triggered_id}'.")
        return True

    elif triggered_id == 'interval-component-fast':
        if current_app_s in ["Live", "Replaying"]:
            if currently_disabled:
                logger.debug(
                    f"Fast interval: App is '{current_app_s}', enabling clientside interval.")
                return False
            return no_update
        else:
            if not currently_disabled:
                logger.debug(
                    f"Fast interval: App is '{current_app_s}', disabling clientside interval.")
                return True
            return no_update

    logger.warning(f"toggle_clientside_interval: Unhandled triggered_id '{triggered_id}'. Returning no_update.")
    return no_update


@app.callback(
    Output('car-positions-store', 'data'),
    Input('clientside-update-interval', 'n_intervals'),
)
def update_car_data_for_clientside(n_intervals):
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if n_intervals == 0: # Or check if None
        return dash.no_update
    
    lock_acquisition_start_time = time.monotonic()
    with session_state.lock:
        lock_acquired_time = time.monotonic()
        logger.debug(f"Lock in '{func_name}' - ACQUIRED. Wait: {lock_acquired_time - lock_acquisition_start_time:.4f}s")
    
        critical_section_start_time = time.monotonic()
        current_app_status = session_state.app_status.get("state", "Idle")
        timing_state_snapshot = session_state.timing_state.copy()
        # Get the currently selected driver for highlighting
        selected_driver_rno = session_state.selected_driver_for_map_and_lap_chart
        logger.debug(f"Lock in '{func_name}' - HELD for critical section: {time.monotonic() - critical_section_start_time:.4f}s")

    if current_app_status not in ["Live", "Replaying"] or not timing_state_snapshot:
        # Ensure to include selected_driver even if inactive, so JS can clear highlight
        return {'status': 'inactive', 'timestamp': time.time(), 'selected_driver': selected_driver_rno}


    processed_car_data = {}
    for car_num_str, driver_state in timing_state_snapshot.items():
        if not isinstance(driver_state, dict):
            continue

        pos_data = driver_state.get('PositionData')
        if not pos_data or 'X' not in pos_data or 'Y' not in pos_data:
            continue

        try:
            x_val = float(pos_data['X'])
            y_val = float(pos_data['Y'])
        except (TypeError, ValueError):
            continue

        team_colour_hex = driver_state.get('TeamColour', '808080')
        if not team_colour_hex.startswith('#'):
            team_colour_hex = '#' + team_colour_hex

        processed_car_data[car_num_str] = {
            'x': x_val,
            'y': y_val,
            'color': team_colour_hex,
            'tla': driver_state.get('Tla', car_num_str),
            'status': driver_state.get('Status', 'Unknown').lower()
        }

    if not processed_car_data: # If after processing, there's nothing, send no update
        return {'status': 'active_no_cars', 'timestamp': time.time(), 'selected_driver': selected_driver_rno}


    # Add the selected driver information to the output for JS
    output_data = {
        'status': 'active', # Indicate data is active
        'timestamp': time.time(),
        'selected_driver': selected_driver_rno, # Pass the selected driver's racing number
        'cars': processed_car_data
    }
    logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
    return output_data

@app.callback(
    Output('clientside-update-interval', 'interval'),
    Input('replay-speed-slider', 'value'),
    State('clientside-update-interval', 'disabled'),
    prevent_initial_call=True
)
def update_clientside_interval_speed(replay_speed, interval_disabled):
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if interval_disabled or replay_speed is None:
        return dash.no_update

    try:
        speed = float(replay_speed)
        if speed <= 0: speed = 1.0
    except (ValueError, TypeError):
        speed = 1.0

    base_interval_ms = 1250 # Base interval for car marker updates on map
    new_interval_ms = max(350, int(base_interval_ms / speed)) # Ensure it doesn't go too fast

    logger.debug(f"Adjusting clientside-update-interval to {new_interval_ms}ms for replay speed {speed}x")
    logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
    return new_interval_ms

@app.callback(
    Output('track-map-graph', 'figure', allow_duplicate=True),
    Output('track-map-figure-version-store', 'data', allow_duplicate=True),
    Output('track-map-yellow-key-store', 'data'),
    [Input('interval-component-medium', 'n_intervals'),
     Input('current-track-layout-cache-key-store', 'data'),
     Input('sidebar-toggle-signal', 'data')], # <<< ADDED INPUT
    [State('track-map-graph', 'figure'),
     State('track-map-figure-version-store', 'data'),
     State('track-map-yellow-key-store', 'data')],
    prevent_initial_call='initial_duplicate'
)
def initialize_track_map(n_intervals, expected_session_id, sidebar_toggled_signal, # <<< ADDED ARGUMENT
                         current_track_map_figure_state,
                         current_figure_version_in_store_state,
                         previous_rendered_yellow_key_from_store):
    session_state = app_state.get_or_create_session_state()
    overall_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    ctx = dash.callback_context
    triggered_prop_id = ctx.triggered[0]['prop_id'] if ctx.triggered and ctx.triggered[0] else 'Unknown.Trigger' # Defensive access
    triggering_input_id = triggered_prop_id.split('.')[0]

    logger.debug(f"INIT_TRACK_MAP Trigger: {triggering_input_id}, SID: {expected_session_id}, PrevYellowKey: {previous_rendered_yellow_key_from_store}, SidebarSignal: {sidebar_toggled_signal}")

    lock_acquisition_start_time = time.monotonic()
    with session_state.lock:
        lock_acquired_time = time.monotonic()
        logger.debug(f"Lock in '{func_name}' - ACQUIRED. Wait: {lock_acquired_time - lock_acquisition_start_time:.4f}s")
        
        critical_section_start_time = time.monotonic()
        cached_data = session_state.track_coordinates_cache.copy()
        driver_list_snapshot = session_state.timing_state.copy() 
        active_yellow_sectors_snapshot = set(session_state.active_yellow_sectors)
        logger.debug(f"Lock in '{func_name}' - HELD for critical section: {time.monotonic() - critical_section_start_time:.4f}s")

    if not expected_session_id or not isinstance(expected_session_id, str) or '_' not in expected_session_id:
        fig_empty = utils.create_empty_figure_with_message(config.TRACK_MAP_WRAPPER_HEIGHT, f"empty_map_init_{time.time()}", config.TEXT_TRACK_MAP_DATA_WILL_LOAD, config.TRACK_MAP_MARGINS)
        fig_empty.layout.plot_bgcolor = 'rgb(30,30,30)'; fig_empty.layout.paper_bgcolor = 'rgba(0,0,0,0)'
        return fig_empty, f"empty_map_ver_{time.time()}", ""

    is_cache_ready_for_base = (cached_data.get('session_key') == expected_session_id and cached_data.get('x') and cached_data.get('y'))
    if not is_cache_ready_for_base:
        fig_loading = utils.create_empty_figure_with_message(config.TRACK_MAP_WRAPPER_HEIGHT, f"loading_{expected_session_id}_{time.time()}", f"{config.TEXT_TRACK_MAP_LOADING_FOR_SESSION_PREFIX}{expected_session_id}...", config.TRACK_MAP_MARGINS)
        fig_loading.layout.plot_bgcolor = 'rgb(30,30,30)'; fig_loading.layout.paper_bgcolor = 'rgba(0,0,0,0)'
        return fig_loading, f"loading_ver_{time.time()}", ""

    corners_c = len(cached_data.get('corners_data') or [])
    lights_c = len(cached_data.get('marshal_lights_data') or [])
    layout_structure_version = "v3.3_placeholders"
    target_persistent_layout_uirevision = f"trackmap_layout_{expected_session_id}_c{corners_c}_l{lights_c}_{layout_structure_version}"
    active_yellow_sectors_key_for_current_render = "_".join(sorted(map(str, list(active_yellow_sectors_snapshot))))

    needs_full_rebuild = False
    current_layout_uirevision_from_state = current_track_map_figure_state.get('layout', {}).get('uirevision') if current_track_map_figure_state and current_track_map_figure_state.get('layout') else None
    
    is_sidebar_toggle_trigger = triggering_input_id == 'sidebar-toggle-signal'

    if triggering_input_id == 'current-track-layout-cache-key-store': 
        needs_full_rebuild = True
    elif not current_track_map_figure_state or not current_track_map_figure_state.get('data') or not current_track_map_figure_state.get('layout'): # Check data too
        needs_full_rebuild = True
    # If uirevision is different AND it's not a temporary one from a previous sidebar toggle, then rebuild.
    elif current_layout_uirevision_from_state != target_persistent_layout_uirevision and \
         not (current_layout_uirevision_from_state and current_layout_uirevision_from_state.startswith("trackmap_resized_view_")):
        needs_full_rebuild = True

    processed_previous_yellow_key = previous_rendered_yellow_key_from_store
    if previous_rendered_yellow_key_from_store is None: processed_previous_yellow_key = ""

    # If not a full rebuild, and yellow sectors haven't changed, AND sidebar didn't toggle, then no update.
    if not needs_full_rebuild and \
       processed_previous_yellow_key == active_yellow_sectors_key_for_current_render and \
       not is_sidebar_toggle_trigger:
        logger.debug(f"INIT_TRACK_MAP --- No structural change, yellow key same, sidebar not toggled. No Python figure update.")
        return no_update, no_update, no_update

    figure_output: go.Figure
    version_store_output = dash.no_update # Default to no_update for version unless changed
    yellow_key_store_output = active_yellow_sectors_key_for_current_render

    # Determine the uirevision for the output figure
    final_uirevision_for_output_figure = target_persistent_layout_uirevision
    if is_sidebar_toggle_trigger:
        final_uirevision_for_output_figure = f"trackmap_resized_view_{time.time()}" # Unique uirevision for resize
        logger.info(f"Sidebar toggle: Using NEW uirevision for map: {final_uirevision_for_output_figure}")
        version_store_output = f"track_resized_ver_{time.time()}" 


    if needs_full_rebuild or (is_sidebar_toggle_trigger and not current_track_map_figure_state):
        rebuild_start_time = time.monotonic()
        logger.info(f"Performing FULL track map data rebuild. Target Layout uirevision: {final_uirevision_for_output_figure}")
        fig_data = []
        valid_corners = [c for c in (cached_data.get('corners_data') or []) if c.get('x') is not None and c.get('y') is not None]
        valid_lights = [m for m in (cached_data.get('marshal_lights_data') or []) if m.get('x') is not None and m.get('y') is not None]
        
        fig_data.append(go.Scatter(x=list(cached_data['x']), y=list(cached_data['y']), mode='lines', line=dict(color='grey', width=getattr(config, 'TRACK_LINE_WIDTH', 2)), name='Track', hoverinfo='none'))
        if valid_corners:
            fig_data.append(go.Scatter(
                x=[c['x'] for c in valid_corners], y=[c['y'] for c in valid_corners], mode='markers+text', 
                marker=dict(size=config.CORNER_MARKER_SIZE, color=config.CORNER_MARKER_COLOR, symbol='circle-open'),
                text=[str(c['number']) for c in valid_corners], textposition=config.CORNER_TEXT_POSITION,
                textfont=dict(size=config.CORNER_TEXT_SIZE, color=config.CORNER_TEXT_COLOR),
                dx=config.CORNER_TEXT_DX, dy=config.CORNER_TEXT_DY, name='Corners', hoverinfo='text'))
        if valid_lights:
            fig_data.append(go.Scatter(x=[m['x'] for m in valid_lights], y=[m['y'] for m in valid_lights], mode='markers', marker=dict(size=getattr(config, 'MARSHAL_MARKER_SIZE', 5), color=getattr(config, 'MARSHAL_MARKER_COLOR', 'orange'), symbol='diamond'), name='Marshal Posts', hoverinfo='text', text=[f"M{m['number']}" for m in valid_lights]))
        for i in range(config.MAX_YELLOW_SECTOR_PLACEHOLDERS):
            fig_data.append(go.Scatter(x=[None], y=[None], mode='lines', line=dict(color=getattr(config, 'YELLOW_FLAG_COLOR', 'yellow'), width=getattr(config, 'YELLOW_FLAG_WIDTH', 4)), name=f"{config.YELLOW_FLAG_PLACEHOLDER_NAME_PREFIX}{i}", hoverinfo='name', opacity=getattr(config, 'YELLOW_FLAG_OPACITY', 0.7), visible=False))
        for car_num_str_init, driver_state_init in driver_list_snapshot.items():
            if not isinstance(driver_state_init, dict): continue
            tla_init = driver_state_init.get('Tla', car_num_str_init); team_color_hex_init = driver_state_init.get('TeamColour', '808080')
            if not team_color_hex_init.startswith('#'): team_color_hex_init = '#' + team_color_hex_init.replace("#", "")
            if len(team_color_hex_init) not in [4, 7]: team_color_hex_init = '#808080'
            racing_number_for_uid = driver_state_init.get('RacingNumber', car_num_str_init)
            fig_data.append(go.Scatter(x=[None], y=[None], mode='markers+text', name=tla_init, uid=str(racing_number_for_uid), marker=dict(size=getattr(config, 'CAR_MARKER_SIZE', 8), color=team_color_hex_init, line=dict(width=1, color='Black')), textfont=dict(size=getattr(config, 'CAR_MARKER_TEXT_SIZE', 8), color='white'), textposition='middle right', hoverinfo='text', text=tla_init))

        fig_layout = go.Layout(
            template='plotly_dark', 
            uirevision=final_uirevision_for_output_figure, # Use the determined uirevision
            autosize=True,                                
            xaxis=dict(visible=False, fixedrange=True, 
                       range=list(cached_data.get('range_x', [0,1])), 
                       autorange=False, 
                       automargin=True),
            yaxis=dict(visible=False, fixedrange=True, 
                       scaleanchor="x", scaleratio=1, 
                       range=list(cached_data.get('range_y', [0,1])), 
                       autorange=False, 
                       automargin=True),
            showlegend=False, plot_bgcolor='rgb(30,30,30)', paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'), margin=config.TRACK_MAP_MARGINS,
            height=None, width=None, # Explicitly None for autosize
            annotations=[]
        )
        figure_output = go.Figure(data=fig_data, layout=fig_layout)
        if version_store_output is dash.no_update : 
            version_store_output = f"trackbase_rebuilt_{expected_session_id}_{time.time()}"
        logger.debug(f"'{func_name}' - FULL track map rebuild: {time.monotonic() - rebuild_start_time:.4f}s")
    else: # Not a full structural rebuild, but update existing figure (e.g., for yellow flags OR sidebar toggle with existing figure)
        update_existing_start_time = time.monotonic()
        logger.debug(f"Updating existing track map figure. Target uirevision: {final_uirevision_for_output_figure}")
        figure_output = go.Figure(current_track_map_figure_state) 
        
        if not figure_output.layout: # Ensure layout object exists
            figure_output.layout = go.Layout() 

        figure_output.layout.uirevision = final_uirevision_for_output_figure

        # Explicitly re-apply ranges and ensure autorange is False for existing figure
        figure_output.layout.xaxis = figure_output.layout.xaxis or {}
        figure_output.layout.xaxis.range = list(cached_data.get('range_x', [0,1]))
        figure_output.layout.xaxis.autorange = False
        figure_output.layout.xaxis.automargin = True
        
        figure_output.layout.yaxis = figure_output.layout.yaxis or {}
        figure_output.layout.yaxis.range = list(cached_data.get('range_y', [0,1]))
        figure_output.layout.yaxis.autorange = False
        figure_output.layout.yaxis.scaleanchor = "x" 
        figure_output.layout.yaxis.scaleratio = 1
        figure_output.layout.yaxis.automargin = True
            
        figure_output.layout.autosize = True
        figure_output.layout.height = None 
        figure_output.layout.width = None  
        
        if version_store_output is dash.no_update and is_sidebar_toggle_trigger: 
            version_store_output = f"track_sidebar_updated_ver_{time.time()}"
        elif version_store_output is dash.no_update:
            version_store_output = current_figure_version_in_store_state
        logger.debug(f"'{func_name}' - Existing map figure update (pre-yellow): {time.monotonic() - update_existing_start_time:.4f}s")

    # --- COMMON YELLOW FLAG UPDATE LOGIC (Applied to figure_output whether rebuilt or existing) ---
    if figure_output is not dash.no_update and cached_data.get('marshal_sector_segments') and cached_data.get('x'):
        yellow_flag_start_time = time.monotonic()
        track_x_full = cached_data['x']; track_y_full = cached_data['y']
        
        # Determine placeholder_trace_offset based on current figure_output structure
        # This assumes a fixed order: Track, Corners (if any), Lights (if any), then Yellows
        placeholder_trace_offset = 1 # For 'Track'
        if any(trace.name == 'Corners' for trace in figure_output.data):
            placeholder_trace_offset += 1
        if any(trace.name == 'Marshal Posts' for trace in figure_output.data):
            placeholder_trace_offset +=1
        
        logger.debug(f"Recalculated Placeholder Offset: {placeholder_trace_offset}. Active Yellows: {active_yellow_sectors_snapshot}")

        # First, reset all yellow flag placeholders to invisible
        for i in range(config.MAX_YELLOW_SECTOR_PLACEHOLDERS):
            trace_index_for_placeholder = placeholder_trace_offset + i
            if trace_index_for_placeholder < len(figure_output.data) and \
               figure_output.data[trace_index_for_placeholder].name.startswith(config.YELLOW_FLAG_PLACEHOLDER_NAME_PREFIX) or \
               figure_output.data[trace_index_for_placeholder].name.startswith("Yellow Sector"): # Catch renamed ones too
                figure_output.data[trace_index_for_placeholder].x = [None]
                figure_output.data[trace_index_for_placeholder].y = [None]
                figure_output.data[trace_index_for_placeholder].visible = False
                figure_output.data[trace_index_for_placeholder].name = f"{config.YELLOW_FLAG_PLACEHOLDER_NAME_PREFIX}{i}" # Reset name

        # Then, activate the current yellow sectors
        for sector_num_active in active_yellow_sectors_snapshot:
            placeholder_idx_for_sector = sector_num_active - 1 
            if 0 <= placeholder_idx_for_sector < config.MAX_YELLOW_SECTOR_PLACEHOLDERS:
                trace_index_to_update = placeholder_trace_offset + placeholder_idx_for_sector
                if trace_index_to_update < len(figure_output.data): 
                    segment_indices = cached_data['marshal_sector_segments'].get(sector_num_active)
                    if segment_indices:
                        start_idx, end_idx = segment_indices
                        if 0 <= start_idx < len(track_x_full) and 0 <= end_idx < len(track_x_full) and start_idx <= end_idx:
                            x_seg = track_x_full[start_idx : end_idx + 1]; y_seg = track_y_full[start_idx : end_idx + 1]
                            if len(x_seg) >= 1:
                                figure_output.data[trace_index_to_update].x = list(x_seg)
                                figure_output.data[trace_index_to_update].y = list(y_seg)
                                figure_output.data[trace_index_to_update].visible = True
                                figure_output.data[trace_index_to_update].name = f"Yellow Sector {sector_num_active}" # Rename active
                                figure_output.data[trace_index_to_update].mode = 'lines' if len(x_seg) > 1 else 'markers'
                                if len(x_seg) == 1 and hasattr(config, 'YELLOW_FLAG_MARKER_SIZE'): 
                                    figure_output.data[trace_index_to_update].marker = dict(color=getattr(config, 'YELLOW_FLAG_COLOR', 'yellow'), size=getattr(config, 'YELLOW_FLAG_MARKER_SIZE', 8))
    # --- End yellow sector common logic ---
    
        logger.debug(f"'{func_name}' - Yellow flag processing: {time.monotonic() - yellow_flag_start_time:.4f}s")
    
    # Final assurance of layout properties before returning
    if figure_output is not dash.no_update:
        if not hasattr(figure_output, 'layout') or not figure_output.layout:
            figure_output.layout = go.Layout() 
        
        figure_output.layout.autosize = True
        if cached_data.get('range_x'):
            figure_output.layout.xaxis = figure_output.layout.xaxis or {}
            figure_output.layout.xaxis.range = list(cached_data.get('range_x'))
            figure_output.layout.xaxis.autorange = False
        else: # Should not happen if cache is ready
            figure_output.layout.xaxis = figure_output.layout.xaxis or {}
            figure_output.layout.xaxis.autorange = True

        if cached_data.get('range_y'):
            figure_output.layout.yaxis = figure_output.layout.yaxis or {}
            figure_output.layout.yaxis.range = list(cached_data.get('range_y'))
            figure_output.layout.yaxis.autorange = False
        else: # Should not happen if cache is ready
            figure_output.layout.yaxis = figure_output.layout.yaxis or {}
            figure_output.layout.yaxis.autorange = True
        
        figure_output.layout.yaxis.scaleanchor="x" 
        figure_output.layout.yaxis.scaleratio=1   
        
        figure_output.layout.height = None 
        figure_output.layout.width = None  
        
        # If the uirevision wasn't updated due to sidebar toggle, ensure it's the target_persistent_layout_uirevision
        if figure_output.layout.uirevision != final_uirevision_for_output_figure and not is_sidebar_toggle_trigger:
            figure_output.layout.uirevision = target_persistent_layout_uirevision


        logger.debug(f"Outputting map figure. Uirevision: {getattr(figure_output.layout, 'uirevision', 'N/A')}")
    
    logger.debug(f"Callback '{func_name}' END. Total time: {time.monotonic() - overall_start_time:.4f}s")
    return figure_output, version_store_output, yellow_key_store_output


@app.callback(
    Output('driver-select-dropdown', 'options'),
    Input('interval-component-slow', 'n_intervals')
)
def update_driver_dropdown_options(n_intervals):
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    logger.debug("Attempting to update driver dropdown options...")
    options = config.DROPDOWN_NO_DRIVERS_OPTIONS # Use constant
    try:
        with session_state.lock:
            timing_state_copy = session_state.timing_state.copy()

        options = utils.generate_driver_options(timing_state_copy) # This helper already uses config constants for error states
        logger.debug(f"Updating driver dropdown options: {len(options)} options generated.")
    except Exception as e:
         logger.error(f"Error generating driver dropdown options: {e}", exc_info=True)
         options = config.DROPDOWN_ERROR_LOADING_DRIVERS_OPTIONS # Use constant
    logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
    return options

@app.callback(
    Output('lap-time-driver-selector', 'options'),
    Input('interval-component-slow', 'n_intervals')
)
def update_lap_chart_driver_options(n_intervals):
    session_state = app_state.get_or_create_session_state()
    with session_state.lock:
        timing_state_copy = session_state.timing_state.copy()
    # utils.generate_driver_options already handles empty/error cases with config constants
    options = utils.generate_driver_options(timing_state_copy) #
    return options


@app.callback(
    Output('lap-time-progression-graph', 'figure'),
    Input('lap-time-driver-selector', 'value'),
    Input('interval-component-medium', 'n_intervals'),
    State('lap-time-progression-graph', 'figure') # Add current figure as State
)
def update_lap_time_progression_chart(selected_drivers_rnos, n_intervals, current_figure_state):
    session_state = app_state.get_or_create_session_state()
    overall_callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START_OVERALL") # Overall start

    ctx = dash.callback_context
    triggered_input_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else 'N/A'
    logger.debug(f"'{func_name}' triggered by: {triggered_input_id}")

    fig_empty_lap_prog = utils.create_empty_figure_with_message(
        config.LAP_PROG_WRAPPER_HEIGHT, config.INITIAL_LAP_PROG_UIREVISION,
        config.TEXT_LAP_PROG_SELECT_DRIVERS, config.LAP_PROG_MARGINS_EMPTY
    )

    if not selected_drivers_rnos:
        logger.debug(f"Callback '{func_name}' END_OVERALL (No drivers selected). Total Took: {time.monotonic() - overall_callback_start_time:.4f}s")
        return fig_empty_lap_prog

    if not isinstance(selected_drivers_rnos, list):
        selected_drivers_rnos = [selected_drivers_rnos]

    # uirevision based on selected drivers (good for structural identity)
    sorted_selection_key = "_".join(sorted(list(set(str(rno) for rno in selected_drivers_rnos))))
    data_plot_uirevision = f"lap_prog_data_{sorted_selection_key}"

    # --- Data Fetching (already timed well in your logs) ---
    lock_acquisition_start_time = time.monotonic()
    with session_state.lock:
        lock_acquired_time = time.monotonic()
        logger.debug(f"Lock in '{func_name}' - ACQUIRED. Wait: {lock_acquired_time - lock_acquisition_start_time:.4f}s")
        critical_section_start_time = time.monotonic()
        
        # Make deep copies if you plan to modify/filter these snapshots extensively
        # For read-only iteration, shallow copies or direct iteration (carefully) might be okay
        lap_history_snapshot = {rno: list(session_state.lap_time_history.get(rno, [])) for rno in selected_drivers_rnos}
        timing_state_snapshot = {rno: session_state.timing_state.get(rno, {}).copy() for rno in selected_drivers_rnos}
        
        logger.debug(f"Lock in '{func_name}' - HELD for data snapshot: {time.monotonic() - critical_section_start_time:.4f}s")

    # --- Python Data Preparation & Plotly Figure Building ---
    # This combined block was timed by 'figure_building_start_time' in your previous code.
    # Let's keep that, but be mindful of what it includes.
    python_and_plotly_prep_start_time = time.monotonic()

    fig_with_data = go.Figure(layout={
        'template': 'plotly_dark', 'uirevision': data_plot_uirevision,
        'height': config.LAP_PROG_WRAPPER_HEIGHT,
        'margin': config.LAP_PROG_MARGINS_DATA,
        'xaxis_title': 'Lap Number', 'yaxis_title': 'Lap Time (s)',
        'hovermode': 'x unified', 'title_text': 'Lap Time Progression', 'title_x':0.5, 'title_font_size':14,
        'showlegend':True, 'legend_title_text':'Drivers', 'legend_font_size':10,
        'annotations': []
    })

    data_actually_plotted = False
    min_time_overall, max_time_overall, max_laps_overall = float('inf'), float('-inf'), 0
    
    # --- Python Loop for preparing trace data ---
    # This part can be significant if many drivers or many laps per driver
    traces_to_add = [] # Prepare all trace data first

    for driver_rno_str_loop_key in selected_drivers_rnos: # Ensure this key matches snapshot keys
        driver_rno_str = str(driver_rno_str_loop_key) # Ensure string key
        
        driver_laps = lap_history_snapshot.get(driver_rno_str, [])
        if not driver_laps: continue

        driver_info = timing_state_snapshot.get(driver_rno_str, {})
        tla = driver_info.get('Tla', driver_rno_str)
        team_color_hex = driver_info.get('TeamColour', 'FFFFFF')
        if not team_color_hex.startswith('#'): team_color_hex = '#' + team_color_hex

        valid_laps = [lap for lap in driver_laps if lap.get('is_valid', True)]
        if not valid_laps: continue

        data_actually_plotted = True
        lap_numbers = [lap['lap_number'] for lap in valid_laps]
        lap_times_sec = [lap['lap_time_seconds'] for lap in valid_laps]

        if lap_numbers: max_laps_overall = max(max_laps_overall, max(lap_numbers))
        if lap_times_sec:
            min_time_current_driver = min(lap_times_sec)
            max_time_current_driver = max(lap_times_sec)
            min_time_overall = min(min_time_overall, min_time_current_driver)
            max_time_overall = max(max_time_overall, max_time_current_driver)
        
        # Optimized hover text generation (pre-join list of strings)
        hover_texts_parts = []
        for lap in valid_laps:
            total_seconds = lap['lap_time_seconds']
            minutes = int(total_seconds // 60)
            seconds_part = total_seconds % 60
            time_formatted = f"{minutes}:{seconds_part:06.3f}" if minutes > 0 else f"{seconds_part:.3f}"
            hover_texts_parts.append(f"<b>{tla}</b><br>Lap: {lap['lap_number']}<br>Time: {time_formatted}<br>Tyre: {lap['compound']}<extra></extra>")
        
        traces_to_add.append(go.Scatter(
            x=lap_numbers, y=lap_times_sec, mode='lines+markers', name=tla,
            marker=dict(color=team_color_hex, size=5), line=dict(color=team_color_hex, width=1.5),
            hovertext=hover_texts_parts, hoverinfo='text' # Assign pre-built list
        ))
    
    # Add all traces at once
    if traces_to_add:
        for trace in traces_to_add:
            fig_with_data.add_trace(trace)

    logger.debug(f"'{func_name}' - Python Data Prep & Plotly Traces Added took: {time.monotonic() - python_and_plotly_prep_start_time:.4f}s")

    if not data_actually_plotted:
        fig_empty_lap_prog.layout.annotations[0].text = config.TEXT_LAP_PROG_NO_DATA
        fig_empty_lap_prog.layout.uirevision = data_plot_uirevision 
        logger.debug(f"Callback '{func_name}' END_OVERALL (No data plotted). Total Took: {time.monotonic() - overall_callback_start_time:.4f}s")
        return fig_empty_lap_prog

    # --- Update Axes (Relatively fast Plotly operations) ---
    axes_update_start_time = time.monotonic()
    if min_time_overall != float('inf') and max_time_overall != float('-inf'):
        padding = (max_time_overall - min_time_overall) * 0.05 if max_time_overall > min_time_overall else 0.5
        fig_with_data.update_yaxes(visible=True, range=[min_time_overall - padding, max_time_overall + padding], autorange=False)
    else:
        fig_with_data.update_yaxes(visible=True, autorange=True)

    if max_laps_overall > 0:
        fig_with_data.update_xaxes(visible=True, range=[0.5, max_laps_overall + 0.5], autorange=False)
    else:
        fig_with_data.update_xaxes(visible=True, autorange=True)
    logger.debug(f"'{func_name}' - Plotly Axes Update took: {time.monotonic() - axes_update_start_time:.4f}s")
    
    logger.debug(f"Callback '{func_name}' END_OVERALL. Total Took: {time.monotonic() - overall_callback_start_time:.4f}s")
    return fig_with_data

@app.callback(
    Output('race-control-log-display', 'value'),
    Input('interval-component-medium', 'n_intervals') # Update periodically
)
def update_race_control_display(n_intervals):
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    try:
        with session_state.lock:
            # The deque stores messages with newest first due to appendleft
            # To display them chronologically (oldest at top), we reverse.
            # Or, if you want newest at top, just join directly.
            log_messages = list(session_state.race_control_log) # Get a snapshot

        if not log_messages:
            return config.TEXT_RC_WAITING # Use constant

        # To display newest messages at the top of the textarea:
        display_text = "\n".join(log_messages)
        # If you prefer oldest messages at the top (more traditional log):
        # display_text = "\n".join(reversed(log_messages))
        logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
        return display_text
    except Exception as e:
        logger.error(f"Error updating race control display: {e}", exc_info=True)
        return config.TEXT_RC_ERROR # Use constant

@app.callback(
    Output("debug-data-accordion-item", "className"),
    Input("debug-mode-switch", "value"),
)
def toggle_debug_data_visibility(debug_mode_enabled):
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if debug_mode_enabled:
        logger.info("Debug mode enabled: Showing 'Other Data Streams'.")
        return "mt-1" # Bootstrap margin top class
    else:
        logger.info("Debug mode disabled: Hiding 'Other Data Streams'.")
        logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
        return "d-none" # Bootstrap display none class

@app.callback(
    Output('driver-select-dropdown', 'value'),
    Input('clicked-car-driver-number-store', 'data'),
    State('driver-select-dropdown', 'options'),
    prevent_initial_call=True
)
def update_dropdown_from_map_click(click_data_json_str, dropdown_options): # Renamed arg
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if click_data_json_str is None:
        return dash.no_update

    try:
        # The data from the store is now the JSON string written by JS
        click_data = json.loads(click_data_json_str)
        clicked_driver_number_str = str(click_data.get('carNumber')) # Ensure it's a string

        if clicked_driver_number_str is None or clicked_driver_number_str == 'None': # Check for None or 'None' string
            with session_state.lock: # If click is invalid, clear selection
                if session_state.selected_driver_for_map_and_lap_chart is not None:
                    logger.info("Map click invalid, clearing session_state.selected_driver_for_map_and_lap_chart.")
                    session_state.selected_driver_for_map_and_lap_chart = None
            return dash.no_update

        logger.info(f"Map click: Attempting to select driver number: {clicked_driver_number_str} for telemetry dropdown.")

        # Update session_state with the clicked driver
        with session_state.lock:
            session_state.selected_driver_for_map_and_lap_chart = clicked_driver_number_str
            logger.info(f"Updated session_state.selected_driver_for_map_and_lap_chart to: {clicked_driver_number_str}")


        if dropdown_options and isinstance(dropdown_options, list):
            valid_driver_numbers = [opt['value'] for opt in dropdown_options if 'value' in opt]
            if clicked_driver_number_str in valid_driver_numbers:
                logger.info(f"Map click: Setting driver-select-dropdown (telemetry) to: {clicked_driver_number_str}")
                logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
                return clicked_driver_number_str
            else:
                logger.warning(f"Map click: Driver number {clicked_driver_number_str} not found in telemetry dropdown options: {valid_driver_numbers}")
                # Even if not in telemetry dropdown, keep it selected in session_state for map/lap chart
                return dash.no_update # Don't change telemetry dropdown if invalid for it
    except json.JSONDecodeError:
        logger.error(f"update_dropdown_from_map_click: Could not decode JSON from store: {click_data_json_str}")
        with session_state.lock: session_state.selected_driver_for_map_and_lap_chart = None # Clear on error
    except Exception as e:
        logger.error(f"update_dropdown_from_map_click: Error processing click data: {e}")
        with session_state.lock: session_state.selected_driver_for_map_and_lap_chart = None # Clear on error

    return dash.no_update

# NEW CALLBACK to update Lap Progression Chart Driver Selection
@app.callback(
    Output('lap-time-driver-selector', 'value'),
    Input('clicked-car-driver-number-store', 'data'), # Triggered by map click via JS
    State('lap-time-driver-selector', 'options'),   # To check if driver is valid option
    State('lap-time-driver-selector', 'value'),     # Current selection (to potentially keep if multi-select later)
    prevent_initial_call=True
)
def update_lap_chart_driver_selection_from_map_click(click_data_json_str, lap_chart_options, current_lap_chart_selection):
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if click_data_json_str is None:
        return dash.no_update

    try:
        click_data = json.loads(click_data_json_str)
        clicked_driver_number_str = str(click_data.get('carNumber')) # Ensure string

        if clicked_driver_number_str is None or clicked_driver_number_str == 'None':
             # If click is invalid, potentially clear selection or do nothing
            return dash.no_update # Or return [] to clear selection

        logger.info(f"Map click: Attempting to select driver {clicked_driver_number_str} in lap progression chart.")

        if lap_chart_options and isinstance(lap_chart_options, list):
            valid_driver_numbers_for_lap_chart = [opt['value'] for opt in lap_chart_options if 'value' in opt]

            if clicked_driver_number_str in valid_driver_numbers_for_lap_chart:
                # For now, we'll make it select ONLY the clicked driver.
                # If you want to ADD to a multi-selection, the logic would be:
                # current_selection = list(current_lap_chart_selection) if current_lap_chart_selection else []
                # if clicked_driver_number_str not in current_selection:
                #     current_selection.append(clicked_driver_number_str)
                # return current_selection
                logger.info(f"Map click: Setting lap-time-driver-selector to: [{clicked_driver_number_str}]")
                logger.info(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
                return [clicked_driver_number_str] # Lap chart dropdown expects a list for its 'value'
            else:
                logger.warning(f"Map click: Driver {clicked_driver_number_str} not in lap chart options. Lap chart selection unchanged.")
                return dash.no_update # Or return [] to clear if driver not available
        else:
            logger.warning("Map click: No options available for lap chart driver selector.")
            return dash.no_update

    except json.JSONDecodeError:
        logger.error(f"update_lap_chart_driver_selection_from_map_click: Could not decode JSON: {click_data_json_str}")
    except Exception as e:
        logger.error(f"update_lap_chart_driver_selection_from_map_click: Error: {e}")
    return dash.no_update


app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='animateCarMarkers'
    ),
    Output('track-map-graph', 'figure'), # Outputting to figure
    [Input('car-positions-store', 'data'),          # Trigger on new car positions
     Input('track-map-figure-version-store', 'data')], # Trigger if base figure changes (e.g. new track)
    State('track-map-graph', 'figure'),             # Current figure to update
    State('track-map-graph', 'id'),                 # ID of the graph component
    State('clientside-update-interval', 'interval') # Current animation interval speed
)

# Clientside callback for handling resize, if needed (see custom_script.js)
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='setupTrackMapResizeListener' # Ensure this matches your JS function
    ),
    Output('track-map-graph', 'figure', allow_duplicate=True), # Dummy output or can update figure if resize logic is complex
    Input('track-map-graph', 'figure'), # Triggered when figure initially renders or changes
    prevent_initial_call='initial_duplicate' # Avoids running on initial load before figure exists
)

app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='setupClickToFocusListener'
    ),
    Output('track-map-graph', 'id'), # Dummy output, just needs to target something on the graph
    Input('track-map-graph', 'figure'), # Trigger when the figure is first drawn or updated
    prevent_initial_call=False # Allow it to run on initial load
)

app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='pollClickDataAndUpdateStore'
    ),
    Output('clicked-car-driver-number-store', 'data'),
    Input('clientside-click-poll-interval', 'n_intervals'),
    State('js-click-data-holder', 'children'),
    prevent_initial_call=True # Read the data JS wrote
)

print("Callback definitions processed") #
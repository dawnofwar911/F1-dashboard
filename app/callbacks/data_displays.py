# app/callbacks/data_displays.py
"""
Callbacks for updating the main data tables and informational displays.
"""
import logging
import time
import inspect
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

from dash.dependencies import Input, Output, State
from dash import dash_table, html, no_update, dash

from app_instance import app
import app_state
import config
import utils

logger = logging.getLogger(__name__)

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

        elif session_type_lower in [config.SESSION_TYPE_QUALI, config.SESSION_TYPE_SPRINT_SHOOTOUT]: #
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
    Input('interval-component-fast', 'n_intervals'),
    State('connection-status', 'children')
)
def update_connection_status(n, existing_status_text):
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
        
        if status_text == existing_status_text:
            return dash.no_update, dash.no_update

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
    [Output('other-data-display', 'children'),
     Output('timing-data-actual-table', 'data'),
     Output('timing-data-timestamp', 'children')],
    Input('interval-component-timing', 'n_intervals'),
    [State("debug-mode-switch", "value"),
     State('session-preferences-store', 'data')]
)
# MODIFICATION: Added debug_mode_enabled
def update_main_data_displays(n, debug_mode_enabled: bool, session_prefs: Optional[dict]):
    session_state = app_state.get_or_create_session_state()
    overall_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    other_elements = []
    table_data = []
    timestamp_text = config.TEXT_WAITING_FOR_DATA
    current_time_for_callbacks = time.time()
    callback_start_time = time.perf_counter()  # For performance logging
    if session_prefs is None:
        session_prefs = {} # Handle case where store is empty on first load
    
    hide_retired_pref = session_prefs.get('hide_retired', config.HIDE_RETIRED_DRIVERS)

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
                is_retired = driver_state.get('Status', '').lower() in TERMINAL_RACING_STATUSES
                if hide_retired_pref and is_retired:
                    continue # Skip this driver
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
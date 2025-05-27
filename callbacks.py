# callbacks.py
"""
Contains all the Dash callback functions for the application.
Handles UI updates, user actions, and plot generation.
"""
import datetime
from datetime import datetime, timezone, timedelta # Ensure these are imported
import pytz # Not strictly used in this version, but often useful with F1 data
import logging
import json
import time
# from datetime import timezone # Already imported in app_state & utils if needed there
import threading
from pathlib import Path

import dash
from dash.dependencies import Input, Output, State, ClientsideFunction
from dash import dcc, html, dash_table, no_update
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np # Keep if any complex numerical ops remain, else remove
# from shapely.geometry import LineString, Point # Not directly used here, but in utils
# from shapely.ops import nearest_points # Not directly used here

# --- App Import ---
try:
    from app_instance import app
except ImportError:
    print("ERROR: Could not import 'app' for callbacks.")
    raise

# --- Module Imports ---
import app_state
import config # <<< UPDATED: For constants
import utils # <<< UPDATED: For helper functions like create_empty_figure_with_message
import signalr_client
import replay
# data_processing functions are called by the loop, not directly needed here

logger = logging.getLogger("F1App.Callbacks")

# Note: UI revision, height, and margin constants are now in config.py
# Note: TRACK_STATUS_STYLES and WEATHER_ICON_MAP are now in config.py

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
    with app_state.app_state_lock:
        session_type = app_state.session_details.get('Type', None)

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
    try:
        with app_state.app_state_lock:
            # Make a copy of the deque for safe iteration
            radio_messages_snapshot = list(app_state.team_radio_messages)
            session_path = app_state.session_details.get('Path') # Needed for the audio URL

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
    # Default values
    lap_value_str = "--/--" #
    lap_counter_div_style = {'display': 'none'} #
    session_timer_label_text = "" #
    session_time_str = "" # Initialize to empty for "Next Up" cases #
    session_timer_div_style = {'display': 'none'} #

    try:
        with app_state.app_state_lock: #
            current_app_overall_status = app_state.app_status.get("state", "Idle") #

            if current_app_overall_status not in ["Live", "Replaying"]: #
                return lap_value_str, lap_counter_div_style, session_timer_label_text, session_time_str, session_timer_div_style #

            session_type_from_state = app_state.session_details.get('Type', "Unknown") #
            current_session_feed_status = app_state.session_details.get('SessionStatus', 'Unknown') #
            current_replay_speed = app_state.replay_speed # Used for LIVE extrapolation, replay speed is inherent in feed pace #

            lap_count_data_payload = app_state.data_store.get('LapCount', {}) #
            lap_count_data = lap_count_data_payload.get('data', {}) if isinstance(lap_count_data_payload, dict) else {} #
            if not isinstance(lap_count_data, dict): lap_count_data = {} #
            current_lap_from_feed = lap_count_data.get('CurrentLap') #
            total_laps_from_feed = lap_count_data.get('TotalLaps') #
            if total_laps_from_feed is not None and total_laps_from_feed != '-': #
                try: app_state.last_known_total_laps = int(total_laps_from_feed) #
                except (ValueError, TypeError): pass #
            actual_total_laps_to_display = app_state.last_known_total_laps if app_state.last_known_total_laps is not None else '--' #
            current_lap_to_display = str(current_lap_from_feed) if current_lap_from_feed is not None else '-' #

            session_type_lower = session_type_from_state.lower() #

            # q_state is for LIVE timer extrapolation and Q REPLAY pause states
            q_state_live_anchor = app_state.qualifying_segment_state.copy() #

            # For Practice LIVE timing
            practice_start_utc_live = app_state.practice_session_actual_start_utc #
            practice_overall_duration_s = app_state.practice_session_scheduled_duration_seconds #

            # For REPLAY feed-paced timing (Practice and Q)
            current_feed_ts_dt_replay = app_state.current_processed_feed_timestamp_utc_dt if current_app_overall_status == "Replaying" else None #
            start_feed_ts_dt_replay = app_state.session_start_feed_timestamp_utc_dt if current_app_overall_status == "Replaying" else None #
            segment_duration_s_replay = app_state.current_segment_scheduled_duration_seconds if current_app_overall_status == "Replaying" else None #

            session_name_from_details = app_state.session_details.get('Name', '') #
            extrapolated_clock_remaining = app_state.extrapolated_clock_info.get("Remaining") if hasattr(app_state, 'extrapolated_clock_info') else None #

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

    return lap_value_str, lap_counter_div_style, session_timer_label_text, session_time_str, session_timer_div_style #


@app.callback(
    Output('connection-status', 'children'),
    Output('connection-status', 'style'),
    Input('interval-component-fast', 'n_intervals')
)
def update_connection_status(n):
    """Updates the connection status indicator."""
    status_text = config.TEXT_CONN_STATUS_DEFAULT # Use constant
    status_style = {'color': 'grey', 'fontWeight': 'bold'}

    try:
        with app_state.app_state_lock:
            status = app_state.app_status.get("connection", "Unknown")
            state = app_state.app_status.get("state", "Idle")
            is_rec = app_state.is_saving_active
            rec_file = app_state.current_recording_filename
            rep_file = app_state.app_status.get("current_replay_file")

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
    session_info_str = config.TEXT_SESSION_INFO_AWAITING
    weather_details_spans = []

    with app_state.app_state_lock:
        # Overall condition state
        overall_condition = app_state.last_known_overall_weather_condition
        weather_card_color = app_state.last_known_weather_card_color
        weather_card_inverse = app_state.last_known_weather_card_inverse
        main_weather_icon_key = app_state.last_known_main_weather_icon_key

        # Detailed weather metrics state (these will be our display fallbacks)
        air_temp_to_display = app_state.last_known_air_temp
        track_temp_to_display = app_state.last_known_track_temp
        humidity_to_display = app_state.last_known_humidity
        pressure_to_display = app_state.last_known_pressure
        wind_speed_to_display = app_state.last_known_wind_speed
        wind_direction_to_display = app_state.last_known_wind_direction
        # This specific rainfall value is primarily for the "RAIN" text logic
        rainfall_val_for_text = app_state.last_known_rainfall_val

        # Get current session and new weather data payload
        local_session_details = app_state.session_details.copy()
        raw_weather_payload = app_state.data_store.get('WeatherData', {})
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

        # Update display values and persisted app_state for each detailed metric
        # if new data for it is valid (not None). Otherwise, retain the loaded last_known value for display.
        with app_state.app_state_lock:
            if parsed_air_temp is not None:
                air_temp_to_display = parsed_air_temp
                app_state.last_known_air_temp = parsed_air_temp
            if parsed_track_temp is not None:
                track_temp_to_display = parsed_track_temp
                app_state.last_known_track_temp = parsed_track_temp
            if parsed_humidity is not None:
                humidity_to_display = parsed_humidity
                app_state.last_known_humidity = parsed_humidity
            if parsed_pressure is not None:
                pressure_to_display = parsed_pressure
                app_state.last_known_pressure = parsed_pressure
            if parsed_wind_speed is not None:
                wind_speed_to_display = parsed_wind_speed
                app_state.last_known_wind_speed = parsed_wind_speed
            if parsed_wind_direction is not None: # Allow empty string as valid update
                wind_direction_to_display = parsed_wind_direction
                app_state.last_known_wind_direction = parsed_wind_direction
            if parsed_rainfall_val is not None:
                rainfall_val_for_text = parsed_rainfall_val # Update for current display logic
                app_state.last_known_rainfall_val = parsed_rainfall_val


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

            with app_state.app_state_lock:
                app_state.last_known_overall_weather_condition = overall_condition
                app_state.last_known_weather_card_color = weather_card_color
                app_state.last_known_weather_card_inverse = weather_card_inverse
                app_state.last_known_main_weather_icon_key = main_weather_icon_key

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
    with app_state.app_state_lock:
        track_status_code = str(app_state.track_status_data.get('Status', '0'))

    # Use TRACK_STATUS_STYLES from config
    status_info = config.TRACK_STATUS_STYLES.get(track_status_code, config.TRACK_STATUS_STYLES['DEFAULT'])

    label_to_display = status_info["label"]
    text_style = {'fontWeight':'bold', 'padding':'2px 5px', 'borderRadius':'4px', 'color': status_info["text_color"]}

    return label_to_display, status_info["card_color"], text_style

@app.callback(
    Output('other-data-display', 'children'),
    Output('timing-data-actual-table', 'data'),
    Output('timing-data-timestamp', 'children'),
    Input('interval-component-timing', 'n_intervals')
)
def update_main_data_displays(n):
    other_elements = []
    table_data = []
    timestamp_text = config.TEXT_WAITING_FOR_DATA #
    current_time_for_callbacks = time.time()

    try:
        session_type_from_state_str = ""
        current_q_segment_from_state = None
        previous_q_segment_from_state = None
        q_state_snapshot_for_live = {}
        current_replay_speed_snapshot = 1.0
        session_feed_status_snapshot = "Unknown"
        app_overall_status = "Idle"
        current_feed_ts_dt_replay_local = None
        start_feed_ts_dt_replay_local = None
        segment_duration_s_replay_local = None

        active_segment_highlight_rule = {"type": "NONE", "lower_pos": 0, "upper_pos": 0}
        q1_eliminated_highlight_rule = {"type": "NONE", "lower_pos": 0, "upper_pos": 0}
        q2_eliminated_highlight_rule = {"type": "NONE", "lower_pos": 0, "upper_pos": 0}

        with app_state.app_state_lock:
            app_overall_status = app_state.app_status.get("state", "Idle") #
            session_type_from_state_str = app_state.session_details.get('Type', "").lower() #
            q_state_snapshot_for_live = app_state.qualifying_segment_state.copy() #
            current_q_segment_from_state = q_state_snapshot_for_live.get("current_segment") #
            previous_q_segment_from_state = q_state_snapshot_for_live.get("old_segment") #
            current_replay_speed_snapshot = app_state.replay_speed #
            session_feed_status_snapshot = app_state.session_details.get('SessionStatus', 'Unknown') #

            if app_overall_status == "Replaying": #
                current_feed_ts_dt_replay_local = app_state.current_processed_feed_timestamp_utc_dt #
                start_feed_ts_dt_replay_local = app_state.session_start_feed_timestamp_utc_dt #
                segment_duration_s_replay_local = app_state.current_segment_scheduled_duration_seconds #

            timing_state_copy = app_state.timing_state.copy() #
            data_store_copy = app_state.data_store #

        current_segment_time_remaining_seconds = float('inf')
        is_active_q_segment_for_highlight = (
            session_type_from_state_str in ["qualifying", "sprint shootout"] and #
            current_q_segment_from_state and
            current_q_segment_from_state not in ["Unknown", "Between Segments", "Ended", "Practice"] #
        )

        if is_active_q_segment_for_highlight: #
            if app_overall_status == "Replaying": #
                if start_feed_ts_dt_replay_local and current_feed_ts_dt_replay_local and segment_duration_s_replay_local is not None: #
                    if session_feed_status_snapshot not in ["Suspended", "Aborted", "Inactive", "Finished", "Ends", "NotStarted"]: #
                        elapsed_feed_time = (current_feed_ts_dt_replay_local - start_feed_ts_dt_replay_local).total_seconds() #
                        current_segment_time_remaining_seconds = max(0, segment_duration_s_replay_local - elapsed_feed_time) #
                    elif session_feed_status_snapshot in ["Suspended", "Aborted", "Inactive"]: #
                        current_segment_time_remaining_seconds = q_state_snapshot_for_live.get("official_segment_remaining_seconds", 0.0) #
                elif session_feed_status_snapshot in ["Suspended", "Aborted", "Inactive"]: #
                     current_segment_time_remaining_seconds = q_state_snapshot_for_live.get("official_segment_remaining_seconds", 0.0) #
            if app_overall_status == "Live" or \
               (app_overall_status == "Replaying" and current_segment_time_remaining_seconds == float('inf')): #
                if session_feed_status_snapshot not in ["Suspended", "Aborted", "Finished", "Ends", "NotStarted", "Inactive"]: #
                    last_capture_dt = q_state_snapshot_for_live.get("last_official_time_capture_utc") #
                    official_rem_s_at_capture = q_state_snapshot_for_live.get("official_segment_remaining_seconds") #
                    if official_rem_s_at_capture is None or not isinstance(official_rem_s_at_capture, (int, float)): #
                        current_segment_time_remaining_seconds = float('inf') #
                    elif last_capture_dt is None: #
                        current_segment_time_remaining_seconds = official_rem_s_at_capture #
                    elif last_capture_dt: #
                        now_utc_for_calc = datetime.now(timezone.utc) #
                        time_since_last_capture_for_calc = (now_utc_for_calc - last_capture_dt).total_seconds() #
                        adjusted_elapsed_time_for_calc = time_since_last_capture_for_calc * current_replay_speed_snapshot #
                        calculated_remaining_for_calc = official_rem_s_at_capture - adjusted_elapsed_time_for_calc #
                        current_segment_time_remaining_seconds = max(0, calculated_remaining_for_calc) #
                elif session_feed_status_snapshot in ["Suspended", "Aborted", "Inactive"]: #
                    official_rem_s_at_pause = q_state_snapshot_for_live.get("official_segment_remaining_seconds", 0.0) #
                    current_segment_time_remaining_seconds = official_rem_s_at_pause if isinstance(official_rem_s_at_pause, (int, float)) else 0.0 #
        elif current_q_segment_from_state in ["Between Segments", "Ended"] or \
             session_feed_status_snapshot in ["Finished", "Ends"]: #
            current_segment_time_remaining_seconds = 0 #

        five_mins_in_seconds = 5 * 60 #
        is_qualifying_type_session = session_type_from_state_str in ["qualifying", "sprint shootout"] #

        # --- MODIFIED HIGHLIGHTING LOGIC ---
        apply_danger_zone_highlight = False
        danger_zone_applies_to_segment = None
        apply_q1_elimination_highlight = False
        apply_q2_elimination_highlight = False

        if is_qualifying_type_session:
            # Determine DANGER ZONE application
            if current_q_segment_from_state in ["Q1", "SQ1", "Q2", "SQ2"]: #
                is_session_status_for_running_danger_zone = session_feed_status_snapshot in ["Started", "Running", "Suspended"] #
                if is_session_status_for_running_danger_zone and current_segment_time_remaining_seconds <= five_mins_in_seconds: #
                    apply_danger_zone_highlight = True
                    danger_zone_applies_to_segment = current_q_segment_from_state #

            if not apply_danger_zone_highlight and session_feed_status_snapshot == "Finished": #
                if current_q_segment_from_state in ["Q1", "SQ1", "Q2", "SQ2"]: #
                    apply_danger_zone_highlight = True
                    danger_zone_applies_to_segment = current_q_segment_from_state #
                elif current_q_segment_from_state == "Between Segments" and previous_q_segment_from_state in ["Q1", "SQ1", "Q2", "SQ2"]: #
                    apply_danger_zone_highlight = True
                    danger_zone_applies_to_segment = previous_q_segment_from_state #

            # Set active_segment_highlight_rule if danger zone is active
            if apply_danger_zone_highlight and danger_zone_applies_to_segment: #
                if danger_zone_applies_to_segment in ["Q1", "SQ1"]: #
                    lower_b_q1_active = config.QUALIFYING_CARS_Q1 - config.QUALIFYING_ELIMINATED_Q1 + 1 #
                    upper_b_q1_active = config.QUALIFYING_CARS_Q1 #
                    active_segment_highlight_rule = {
                        "type": "RED_DANGER", "lower_pos": lower_b_q1_active, "upper_pos": upper_b_q1_active} #
                elif danger_zone_applies_to_segment in ["Q2", "SQ2"]: #
                    lower_b_q2_active = config.QUALIFYING_CARS_Q2 - config.QUALIFYING_ELIMINATED_Q2 + 1 #
                    upper_b_q2_active = config.QUALIFYING_CARS_Q2 #
                    active_segment_highlight_rule = {
                        "type": "RED_DANGER", "lower_pos": lower_b_q2_active, "upper_pos": upper_b_q2_active} #

            # Conditions for ELIMINATION HIGHLIGHT (GREY)
            # Q1 Eliminations:
            # Trigger: Q1 -> Between Segments + Inactive
            # Maintain: If in Q2, Q3, or Between Segments (after Q1), or Ended (after Q1)
            if (previous_q_segment_from_state in ["Q1", "SQ1"] and
                current_q_segment_from_state == "Between Segments" and
                session_feed_status_snapshot == "Inactive"): #
                apply_q1_elimination_highlight = True  # Trigger point
            elif current_q_segment_from_state in ["Q2", "SQ2", "Q3", "SQ3"]: #
                apply_q1_elimination_highlight = True  # Maintain during Q2/Q3
            elif current_q_segment_from_state == "Between Segments" and \
                 previous_q_segment_from_state in ["Q2", "SQ2", "Q3", "SQ3"]: # # Between Q2/Q3 or Q3/End
                apply_q1_elimination_highlight = True  # Maintain
            elif current_q_segment_from_state == "Ended" and \
                 previous_q_segment_from_state not in ["Practice", None]: # # Session ended after a Q segment
                apply_q1_elimination_highlight = True # Maintain

            # Q2 Eliminations:
            # Trigger: Q2 -> Between Segments + Inactive
            # Maintain: If in Q3, or Between Segments (after Q2), or Ended (after Q2)
            if (previous_q_segment_from_state in ["Q2", "SQ2"] and
                current_q_segment_from_state == "Between Segments" and
                session_feed_status_snapshot == "Inactive"): #
                apply_q2_elimination_highlight = True  # Trigger point
            elif current_q_segment_from_state in ["Q3", "SQ3"]: #
                apply_q2_elimination_highlight = True  # Maintain during Q3
            elif current_q_segment_from_state == "Between Segments" and \
                 previous_q_segment_from_state in ["Q3", "SQ3"]: # # Between Q3/End
                apply_q2_elimination_highlight = True  # Maintain
            elif current_q_segment_from_state == "Ended" and \
                 previous_q_segment_from_state in ["Q2", "SQ2", "Q3", "SQ3"]: # # Session ended after Q2 or Q3
                apply_q2_elimination_highlight = True # Maintain

            # Set elimination highlight rule dicts
            if apply_q1_elimination_highlight:
                q1_eliminated_highlight_rule = {
                    "type": "GREY_ELIMINATED",
                    "lower_pos": config.QUALIFYING_CARS_Q1 - config.QUALIFYING_ELIMINATED_Q1 + 1, #
                    "upper_pos": config.QUALIFYING_CARS_Q1 } #
            if apply_q2_elimination_highlight:
                q2_eliminated_highlight_rule = {
                    "type": "GREY_ELIMINATED",
                    "lower_pos": config.QUALIFYING_CARS_Q2 - config.QUALIFYING_ELIMINATED_Q2 + 1, #
                    "upper_pos": config.QUALIFYING_CARS_Q2 } #
        # --- END OF MODIFIED HIGHLIGHTING LOGIC ---

        logger.debug(
            f"HighlightCheck: Seg='{current_q_segment_from_state}', Prev='{previous_q_segment_from_state}', "
            f"DangerAppliesTo='{danger_zone_applies_to_segment}', RemSecForHighlight={current_segment_time_remaining_seconds:.1f}, "
            f"Mode='{app_overall_status}', FeedStatus='{session_feed_status_snapshot}', "
            f"ApplyDanger='{apply_danger_zone_highlight}', ApplyQ1Elim='{apply_q1_elimination_highlight}', ApplyQ2Elim='{apply_q2_elimination_highlight}'"
        )

        excluded_streams = ['TimingData', 'DriverList', 'Position.z', 'CarData.z', 'Position',
                            'TrackStatus', 'SessionData', 'SessionInfo', 'WeatherData', 'Heartbeat'] #
        sorted_streams = sorted(
            [s for s in data_store_copy.keys() if s not in excluded_streams]) #
        for stream in sorted_streams: #
            value = data_store_copy.get(stream, {}) #
            data_payload = value.get('data', 'N/A') #
            timestamp_str_val = value.get('timestamp', 'N/A') #
            try:
                data_str = json.dumps(data_payload, indent=2) #
            except TypeError:
                data_str = str(data_payload) #
            if len(data_str) > 500: #
                data_str = data_str[:500] + "\n...(truncated)" #
            other_elements.append(html.Details([html.Summary(f"{stream} ({timestamp_str_val})"), #
                                                html.Pre(data_str, style={'marginLeft': '15px', 'maxHeight': '200px', 'overflowY': 'auto'})], #
                                               open=(stream == "LapCount"))) #
        timing_data_entry = data_store_copy.get('TimingData', {}) #
        timestamp_text = f"Timing TS: {timing_data_entry.get('timestamp', 'N/A')}" if timing_data_entry else config.TEXT_WAITING_FOR_DATA #

        if timing_state_copy: #
            processed_table_data = []
            for car_num, driver_state in timing_state_copy.items(): #
                racing_no = driver_state.get("RacingNumber", car_num) #
                tla = driver_state.get("Tla", "N/A") #
                pos = driver_state.get('Position', '-') #
                pos_str = str(pos) #
                compound = driver_state.get('TyreCompound', '-') #
                age = driver_state.get('TyreAge', '?') #
                is_new = driver_state.get('IsNewTyre', False) #
                compound_short = ""
                known_compounds = ["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"] #
                if compound and compound.upper() in known_compounds: #
                    compound_short = compound[0].upper() #
                elif compound and compound != '-': #
                    compound_short = "?" #
                tyre_display_parts = []
                if compound_short: tyre_display_parts.append(compound_short) #
                if age != '?': tyre_display_parts.append(f"{str(age)}L") #
                tyre_base = " ".join(tyre_display_parts) if tyre_display_parts else "-" #
                new_tyre_indicator = "*" if compound_short and compound_short != '?' and not is_new else "" #
                tyre = f"{tyre_base}{new_tyre_indicator}" #
                if tyre_base == "-": tyre = "-" #
                interval_val = utils.get_nested_state(driver_state, 'IntervalToPositionAhead', 'Value', default='-') #
                gap_val = driver_state.get('GapToLeader', '-') #
                interval_display_text = str(interval_val).strip() if interval_val not in [None, "", "-"] else "-" #
                gap_display_text = str(gap_val).strip() if gap_val not in [None, "", "-"] else "-" #
                bold_interval_text = f"**{interval_display_text}**" #
                interval_gap_markdown = ""
                is_p1 = (pos_str == '1') #
                show_gap = not is_p1 and session_type_from_state_str in [config.SESSION_TYPE_RACE.lower(), config.SESSION_TYPE_SPRINT.lower()] and gap_display_text != "-" #
                if show_gap and interval_display_text != "": #
                    normal_weight_gap_text = gap_display_text #
                    interval_gap_markdown = f"{bold_interval_text}\\\n{normal_weight_gap_text}" #
                elif interval_display_text == "" and is_p1: interval_gap_markdown = "" #
                else:
                    if interval_display_text == "-": interval_gap_markdown = "-" #
                    else: interval_gap_markdown = bold_interval_text #
                last_lap_val = utils.get_nested_state(driver_state, 'LastLapTime', 'Value', default='-') #
                if last_lap_val is None or last_lap_val == "": last_lap_val = "-" #
                best_lap_val = utils.get_nested_state(driver_state, 'PersonalBestLapTime', 'Value', default='-') #
                if best_lap_val is None or best_lap_val == "": best_lap_val = "-" #
                s1_val = utils.get_nested_state(driver_state, 'Sectors', '0', 'Value', default='-') #
                if s1_val is None or s1_val == "": s1_val = "-" #
                s2_val = utils.get_nested_state(driver_state, 'Sectors', '1', 'Value', default='-') #
                if s2_val is None or s2_val == "": s2_val = "-" #
                s3_val = utils.get_nested_state(driver_state, 'Sectors', '2', 'Value', default='-') #
                if s3_val is None or s3_val == "": s3_val = "-" #
                is_in_pit_flag = driver_state.get('InPit', False) #
                entry_wall_time = driver_state.get('current_pit_entry_system_time') #
                speed_at_entry = driver_state.get('pit_entry_replay_speed', 1.0) #
                if not isinstance(speed_at_entry, (float, int)) or speed_at_entry <= 0: speed_at_entry = 1.0 #
                final_live_pit_text = driver_state.get('final_live_pit_time_text') #
                final_live_pit_text_ts = driver_state.get('final_live_pit_time_display_timestamp') #
                reliable_stops = driver_state.get('ReliablePitStops', 0) #
                timing_data_stops = driver_state.get('NumberOfPitStops', 0) #
                pits_text_to_display = '0' #
                if reliable_stops > 0: pits_text_to_display = str(reliable_stops) #
                elif timing_data_stops > 0: pits_text_to_display = str(timing_data_stops) #
                pit_display_state_for_style = "SHOW_COUNT" #
                if is_in_pit_flag: #
                    pit_display_state_for_style = "IN_PIT_LIVE" #
                    if entry_wall_time: #
                        current_wall_time_elapsed = current_time_for_callbacks - entry_wall_time #
                        live_game_time_elapsed = current_wall_time_elapsed * speed_at_entry #
                        pits_text_to_display = f"In Pit: {live_game_time_elapsed:.1f}s" #
                    else: pits_text_to_display = "In Pit" #
                elif final_live_pit_text and final_live_pit_text_ts and (current_time_for_callbacks - final_live_pit_text_ts < 15): #
                    pits_text_to_display = final_live_pit_text #
                    pit_display_state_for_style = "SHOW_COMPLETED_DURATION" #
                car_data = driver_state.get('CarData', {}) #
                speed_val = car_data.get('Speed', '-') #
                gear = car_data.get('Gear', '-') #
                rpm = car_data.get('RPM', '-') #
                drs_val = car_data.get('DRS') #
                drs_map = {8: "E", 10: "On", 12: "On", 14: "ON"} #
                drs = drs_map.get(drs_val, 'Off') if drs_val is not None else 'Off' #
                is_overall_best_lap_flag = driver_state.get('IsOverallBestLap', False)  #
                is_last_lap_personal_best_flag = utils.get_nested_state(driver_state, 'LastLapTime', 'PersonalFastest', default=False) #
                is_s1_personal_best_flag = utils.get_nested_state(driver_state, 'Sectors', '0', 'PersonalFastest', default=False) #
                is_s2_personal_best_flag = utils.get_nested_state(driver_state, 'Sectors', '1', 'PersonalFastest', default=False) #
                is_s3_personal_best_flag = utils.get_nested_state(driver_state, 'Sectors', '2', 'PersonalFastest', default=False) #
                is_overall_best_s1_flag = driver_state.get('IsOverallBestSector', [False]*3)[0] #
                is_overall_best_s2_flag = driver_state.get('IsOverallBestSector', [False]*3)[1] #
                is_overall_best_s3_flag = driver_state.get('IsOverallBestSector', [False]*3)[2] #
                is_last_lap_EVENT_overall_best_flag = utils.get_nested_state(driver_state, 'LastLapTime', 'OverallFastest', default=False) #
                is_s1_EVENT_overall_best_flag = utils.get_nested_state(driver_state, 'Sectors', '0', 'OverallFastest', default=False) #
                is_s2_EVENT_overall_best_flag = utils.get_nested_state(driver_state, 'Sectors', '1', 'OverallFastest', default=False) #
                is_s3_EVENT_overall_best_flag = utils.get_nested_state(driver_state, 'Sectors', '2', 'OverallFastest', default=False) #
                current_driver_highlight_type = "NONE" #
                driver_pos_int = -1
                if pos_str != '-': #
                    try: driver_pos_int = int(pos_str) #
                    except ValueError: pass
                if q1_eliminated_highlight_rule["type"] == "GREY_ELIMINATED" and \
                   driver_pos_int != -1 and \
                   q1_eliminated_highlight_rule["lower_pos"] <= driver_pos_int <= q1_eliminated_highlight_rule["upper_pos"]: #
                    current_driver_highlight_type = "GREY_ELIMINATED" #
                if current_driver_highlight_type == "NONE" and \
                   q2_eliminated_highlight_rule["type"] == "GREY_ELIMINATED" and \
                   driver_pos_int != -1 and \
                   q2_eliminated_highlight_rule["lower_pos"] <= driver_pos_int <= q2_eliminated_highlight_rule["upper_pos"]: #
                    current_driver_highlight_type = "GREY_ELIMINATED" #
                if current_driver_highlight_type == "NONE": #
                    if active_segment_highlight_rule["type"] == "RED_DANGER": #
                        if driver_pos_int != -1 and \
                           active_segment_highlight_rule["lower_pos"] <= driver_pos_int <= active_segment_highlight_rule["upper_pos"]: #
                            current_driver_highlight_type = "RED_DANGER" #
                        elif pos_str == '-': #
                            current_driver_highlight_type = "RED_DANGER" #
                row = {
                    'id': car_num, 'No.': racing_no, 'Car': tla, 'Pos': pos, 'Tyre': tyre, #
                    'IntervalGap': interval_gap_markdown, 'Last Lap': last_lap_val, 'Best Lap': best_lap_val, #
                    'S1': s1_val, 'S2': s2_val, 'S3': s3_val, 'Pits': pits_text_to_display, #
                    'Status': driver_state.get('Status', 'N/A'), 'Speed': speed_val, 'Gear': gear, 'RPM': rpm, 'DRS': drs, #
                    'IsOverallBestLap_Str': "TRUE" if is_overall_best_lap_flag else "FALSE", #
                    'IsOverallBestS1_Str': "TRUE" if is_overall_best_s1_flag else "FALSE", #
                    'IsOverallBestS2_Str': "TRUE" if is_overall_best_s2_flag else "FALSE", #
                    'IsOverallBestS3_Str': "TRUE" if is_overall_best_s3_flag else "FALSE", #
                    'IsLastLapPersonalBest_Str': "TRUE" if is_last_lap_personal_best_flag else "FALSE", #
                    'IsPersonalBestS1_Str': "TRUE" if is_s1_personal_best_flag else "FALSE", #
                    'IsPersonalBestS2_Str': "TRUE" if is_s2_personal_best_flag else "FALSE", #
                    'IsPersonalBestS3_Str': "TRUE" if is_s3_personal_best_flag else "FALSE", #
                    'IsLastLapEventOverallBest_Str': "TRUE" if is_last_lap_EVENT_overall_best_flag else "FALSE", #
                    'IsS1EventOverallBest_Str': "TRUE" if is_s1_EVENT_overall_best_flag else "FALSE", #
                    'IsS2EventOverallBest_Str': "TRUE" if is_s2_EVENT_overall_best_flag else "FALSE", #
                    'IsS3EventOverallBest_Str': "TRUE" if is_s3_EVENT_overall_best_flag else "FALSE", #
                    'PitDisplayState_Str': pit_display_state_for_style, #
                    'QualiHighlight_Str': current_driver_highlight_type, #
                }
                processed_table_data.append(row) #

            processed_table_data.sort(key=utils.pos_sort_key) #
            table_data = processed_table_data #
        else:
            timestamp_text = config.TEXT_WAITING_FOR_DATA # Using a config constant as in other places

        return other_elements, table_data, timestamp_text

    except Exception as e_update:
        logger.error(
            f"Error in update_main_data_displays callback: {e_update}", exc_info=True) #
        return no_update, no_update, no_update #




@app.callback(
    Output('dummy-output-for-controls', 'children', allow_duplicate=True),
    Input('replay-speed-slider', 'value'),
    prevent_initial_call=True
)
def update_replay_speed_state(new_speed_value): # Removed session_info_children_trigger, get from app_state
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


    with app_state.app_state_lock:
        old_speed = app_state.replay_speed # Speed active *before* this change

        # If speed hasn't actually changed, do nothing to avoid potential float precision issues
        if abs(old_speed - new_speed) < 0.01: # Tolerance for float comparison
            # Still update app_state.replay_speed to the precise new_speed_value if slider was just wiggled
            app_state.replay_speed = new_speed
            return no_update

        session_type = app_state.session_details.get('Type', "Unknown").lower() #
        q_state = app_state.qualifying_segment_state # Primary timing state dictionary

        current_official_remaining_s_at_anchor = q_state.get("official_segment_remaining_seconds") #
        last_capture_utc_anchor = q_state.get("last_official_time_capture_utc") #

        now_utc = datetime.now(timezone.utc) #
        calculated_current_true_remaining_s = None

        # --- This block calculates the true current remaining time based on OLD speed ---
        if session_type.startswith("practice"):
            # For practice, use its continuous model to find current true remaining time
            practice_start_utc = app_state.practice_session_actual_start_utc #
            practice_duration_s = app_state.practice_session_scheduled_duration_seconds #
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
               app_state.practice_session_actual_start_utc and \
               app_state.practice_session_scheduled_duration_seconds is not None:

                duration_s = app_state.practice_session_scheduled_duration_seconds #
                if new_speed > 0: # Avoid division by zero
                    # We want: new_anchor_remaining_s = duration_s - (now_utc - new_practice_start_utc) * new_speed
                    # (now_utc - new_practice_start_utc) * new_speed = duration_s - new_anchor_remaining_s
                    # (now_utc - new_practice_start_utc) = (duration_s - new_anchor_remaining_s) / new_speed
                    # new_practice_start_utc = now_utc - timedelta(seconds = (duration_s - new_anchor_remaining_s) / new_speed)

                    wall_time_offset_for_new_start = (duration_s - new_anchor_remaining_s) / new_speed
                    app_state.practice_session_actual_start_utc = now_utc - timedelta(seconds=wall_time_offset_for_new_start) #
                    logger.info(
                        f"Adjusted practice_session_actual_start_utc to {app_state.practice_session_actual_start_utc} " #
                        f"to maintain {new_anchor_remaining_s:.2f}s remaining at {new_speed:.2f}x."
                    )
        else:
            logger.warning(
                f"Could not re-anchor timer on speed change: insufficient data. "
                f"Old speed: {old_speed}, New speed: {new_speed}. "
                f"Current official remaining: {current_official_remaining_s_at_anchor}, Last capture: {last_capture_utc_anchor}"
            )

        # Finally, update the global replay speed
        app_state.replay_speed = new_speed #
        logger.debug(f"Replay speed updated in app_state to: {app_state.replay_speed}") #

    return no_update

@app.callback(
    [Output('dummy-output-for-controls', 'children', allow_duplicate=True),
     Output('track-map-graph', 'figure', allow_duplicate=True),
     Output('car-positions-store', 'data', allow_duplicate=True)],
    Input('connect-button', 'n_clicks'),
    Input('replay-button', 'n_clicks'),
    Input('stop-reset-button', 'n_clicks'),
    State('replay-file-selector', 'value'),
    State('replay-speed-slider', 'value'),
    State('record-data-checkbox', 'value'),
    prevent_initial_call=True
)
def handle_control_clicks(connect_clicks, replay_clicks, stop_reset_clicks,
                          selected_replay_file, replay_speed,
                          record_checkbox_value):
    ctx = dash.callback_context
    button_id = ctx.triggered_id if ctx.triggered else None

    dummy_output = no_update
    track_map_figure_output = no_update
    car_positions_store_output = no_update

    if not button_id:
        return dummy_output, track_map_figure_output, car_positions_store_output

    logger.info(f"Control button clicked: {button_id}")

    def generate_reset_track_map_figure():
        unique_reset_uirevision = f"reset_map_rev_{time.time()}"
        logger.debug(f"Generating reset track map with unique uirevision: {unique_reset_uirevision}")
        # Use utils.create_empty_figure_with_message and config constants
        fig = utils.create_empty_figure_with_message(
            height=config.TRACK_MAP_WRAPPER_HEIGHT, uirevision=unique_reset_uirevision,
            message=config.TEXT_TRACK_MAP_DATA_WILL_LOAD, margins=config.TRACK_MAP_MARGINS
        )
        fig.update_layout(yaxis_scaleanchor='x', yaxis_scaleratio=1,
                          plot_bgcolor='rgb(30,30,30)', paper_bgcolor='rgba(0,0,0,0)')
        if fig.layout.annotations: fig.layout.annotations[0].font.size = 10
        return fig

    if button_id == 'connect-button':
        with app_state.app_state_lock:
            current_app_s = app_state.app_status["state"]
            if current_app_s in ["Replaying", "Playback Complete", "Stopped", "Error"]:
                logger.info(f"Connect Live: Transitioning from {current_app_s}. Resetting track map related states for a clean live start.")
                app_state.track_coordinates_cache = app_state.INITIAL_TRACK_COORDINATES_CACHE.copy()
                app_state.session_details['SessionKey'] = None
                app_state.selected_driver_for_map_and_lap_chart = None # Reset selected driver

                track_map_figure_output = generate_reset_track_map_figure()
                car_positions_store_output = {'status': 'reset_map_display', 'timestamp': time.time()}

            if current_app_s not in ["Idle", "Stopped", "Error", "Playback Complete", "Replaying"]:
                logger.warning(f"Connect ignored. App state: {current_app_s}")
                return dummy_output, track_map_figure_output, car_positions_store_output

            if app_state.stop_event.is_set(): logger.info("Connect Live: Clearing pre-existing stop_event.")
            app_state.stop_event.clear()
            should_record_live = app_state.record_live_data # Use the state variable
        logger.info(f"Initiating connection. Recording: {should_record_live}")
        websocket_url, ws_headers = None, None
        try:
            with app_state.app_state_lock: app_state.app_status.update({"state": "Initializing", "connection": config.TEXT_SIGNALR_SOCKET_CONNECTING_STATUS}) # Use constant
            websocket_url, ws_headers = signalr_client.build_connection_url(config.NEGOTIATE_URL_BASE, config.HUB_NAME) #
            if not websocket_url or not ws_headers: raise ConnectionError("Negotiation failed.")
        except Exception as e:
            logger.error(f"Negotiation error: {e}", exc_info=True)
            with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": config.TEXT_SIGNALR_NEGOTIATION_ERROR_PREFIX + str(type(e).__name__)}) # Use constant
            return dummy_output, track_map_figure_output, car_positions_store_output
        if websocket_url and ws_headers:
            if should_record_live:
                if not replay.init_live_file(): logger.error("Failed to init recording.") #
            else: replay.close_live_file() #
            thread_obj = threading.Thread(target=signalr_client.run_connection_manual_neg, args=(websocket_url, ws_headers), name="SignalRConnectionThread", daemon=True) #
            signalr_client.connection_thread = thread_obj; thread_obj.start() #
            logger.info("SignalR connection thread initiated.")


    elif button_id == 'replay-button':
        if selected_replay_file:
            active_live_session = False
            with app_state.app_state_lock:
                state = app_state.app_status["state"]
                app_state.selected_driver_for_map_and_lap_chart = None # Reset selected driver on new replay
                if state in ["Live", "Connecting"]: active_live_session = True
                elif state == "Replaying":
                    logger.warning(config.TEXT_REPLAY_ALREADY_RUNNING) # Use constant
                    return dummy_output, track_map_figure_output, car_positions_store_output
            if active_live_session:
                logger.info("Stopping live feed for replay.")
                try: signalr_client.stop_connection(); time.sleep(0.3) #
                except Exception as e:
                    logger.error(f"Error stopping live feed for replay: {e}", exc_info=True)
                    return dummy_output, track_map_figure_output, car_positions_store_output
            try:
                speed_float = float(replay_speed); speed_float = max(0.1, speed_float)
                full_replay_path = Path(config.REPLAY_DIR) / selected_replay_file #
                if replay.replay_from_file(full_replay_path, speed_float): logger.info(f"Replay initiated for {full_replay_path.name}.") #
                else: logger.error(f"Failed to start replay for {full_replay_path.name}.")
            except Exception as e_replay_start:
                 logger.error(f"Error starting replay: {e_replay_start}", exc_info=True)
                 with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": config.TEXT_REPLAY_ERROR_THREAD_START_FAILED_STATUS}) # Use constant
        else: logger.warning(f"Start Replay: {config.TEXT_REPLAY_SELECT_FILE}") # Use constant


    elif button_id == 'stop-reset-button':
        logger.info("Stop & Reset Session button clicked.")
        any_action_failed = False

        logger.info("Stop & Reset: Attempting to stop SignalR connection (if any)...")
        try: signalr_client.stop_connection(); logger.info("Stop & Reset: signalr_client.stop_connection() completed.") #
        except Exception as e: logger.error(f"Stop & Reset: Error during signalr_client.stop_connection(): {e}", exc_info=True); any_action_failed = True

        logger.info("Stop & Reset: Attempting to stop replay (if any)...")
        try: replay.stop_replay(); logger.info("Stop & Reset: replay.stop_replay() completed.") #
        except Exception as e: logger.error(f"Stop & Reset: Error during replay.stop_replay(): {e}", exc_info=True); any_action_failed = True

        logger.debug("Stop & Reset: Pausing (0.3s) for stop signals...")
        time.sleep(0.3)

        logger.info("Stop & Reset: Resetting application state...")
        try:
            app_state.reset_to_default_state() # This will now also reset selected_driver_for_map_and_lap_chart
            track_map_figure_output = generate_reset_track_map_figure()
            car_positions_store_output = {'status': 'reset_map_display', 'timestamp': time.time()}
            logger.info("Stop & Reset: State reset; track map set to empty; car_positions_store signaled.")
        except Exception as e:
            logger.error(f"Stop & Reset: Error during reset_to_default_state: {e}", exc_info=True); any_action_failed = True

        logger.info("Stop & Reset: Finalizing stop_event and app status...")
        with app_state.app_state_lock:
            current_status_after_actions = app_state.app_status.get("state") # Get state *after* stop calls and reset
            logger.info(f"Stop & Reset: State after reset_to_default_state() and before final adjustment: '{current_status_after_actions}'. Actions failed: {any_action_failed}")
            
            if app_state.stop_event.is_set(): 
                logger.info("Stop & Reset: Global stop_event is SET. Clearing now.")
                app_state.stop_event.clear()
            else: 
                logger.info("Stop & Reset: Global stop_event was already clear.")

            if any_action_failed: # If any action (stop_connection, stop_replay, reset_state) failed
                logger.warning("Stop & Reset: At least one action failed. Forcing app status to 'Error'.")
                app_state.app_status["state"] = "Error"
                app_state.app_status["connection"] = "Reset failed"
            else: # All actions succeeded
                # reset_to_default_state() should have already set it to "Idle"
                if current_status_after_actions == "Idle":
                    logger.info(f"Stop & Reset: Actions succeeded. State is '{current_status_after_actions}'. Connection: '{config.TEXT_SIGNALR_DISCONNECTED_STATUS}'.")
                    app_state.app_status["connection"] = config.TEXT_SIGNALR_DISCONNECTED_STATUS # Ensure connection message is default
                else:
                    # This case should ideally not be hit if reset_to_default_state() works
                    logger.warning(f"Stop & Reset: Actions succeeded, but state is '{current_status_after_actions}' instead of 'Idle'. Forcing to 'Idle'.")
                    app_state.app_status["state"] = "Idle"
                    app_state.app_status["connection"] = config.TEXT_SIGNALR_DISCONNECTED_STATUS
        logger.info("Stop & Reset Session processing finished.")

    else:
        logger.warning(f"Button ID '{button_id}' not handled by handle_control_clicks.")

    return dummy_output, track_map_figure_output, car_positions_store_output


@app.callback(
    Output('record-data-checkbox', 'id', allow_duplicate=True), # Keep id as string
    Input('record-data-checkbox', 'value'),
    prevent_initial_call=True
)
def record_checkbox_callback(checked_value):
    if checked_value is None: return 'record-data-checkbox' # Return existing ID string
    new_state = bool(checked_value)
    logger.debug(f"Record Live Data checkbox set to: {new_state}")
    with app_state.app_state_lock: app_state.record_live_data = new_state
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
    ctx = dash.callback_context
    triggered_id = ctx.triggered_id if ctx.triggered else 'N/A'
    
    logger.debug(
        f"Driver Focus Update: Trigger='{triggered_id}', Driver='{selected_driver_number}', "
        f"ActiveTab='{active_tab_id}', SelectedLap='{selected_lap_for_telemetry}'"
    )

    # --- Initial/Default Outputs ---
    driver_basic_details_children = [html.P(config.TEXT_DRIVER_SELECT, style={'fontSize':'0.8rem', 'padding':'5px'})]
    
    # Telemetry defaults
    telemetry_lap_options = config.DROPDOWN_NO_LAPS_OPTIONS
    telemetry_lap_value = None
    telemetry_lap_disabled = True
    fig_telemetry = utils.create_empty_figure_with_message(
        config.TELEMETRY_WRAPPER_HEIGHT, config.INITIAL_TELEMETRY_UIREVISION,
        config.TEXT_DRIVER_SELECT_LAP, config.TELEMETRY_MARGINS_EMPTY
    )

    # Stint History defaults
    stint_history_data = []
    # Stint history columns are defined in layout, but can be overridden here if needed.
    # For now, we assume they are static from layout, so we can pass no_update for columns.
    stint_history_columns_output = no_update 
    # If columns were dynamic:
    # stint_history_columns_output = [{"name": i, "id": i} for i in ["Stint", "Lap In", "Compound", "Laps"]] # Example


    # --- Handle No Selected Driver ---
    if not selected_driver_number:
        # If telemetry figure is already the initial one, don't update it
        if current_telemetry_figure and \
           current_telemetry_figure.get('layout', {}).get('uirevision') == config.INITIAL_TELEMETRY_UIREVISION:
            fig_telemetry = no_update
        
        return (driver_basic_details_children, telemetry_lap_options, telemetry_lap_value, telemetry_lap_disabled, fig_telemetry,
                stint_history_data, stint_history_columns_output)

    driver_num_str = str(selected_driver_number)

    # --- Get Driver Basic Info (always displayed) ---
    with app_state.app_state_lock:
        driver_info_state = app_state.timing_state.get(driver_num_str, {}).copy()
        # For Stint History Tab
        all_stints_for_driver = app_state.driver_stint_data.get(driver_num_str, [])
        # For Telemetry Tab
        available_telemetry_laps = sorted(app_state.telemetry_data.get(driver_num_str, {}).keys())


    if driver_info_state:
        tla = driver_info_state.get('Tla', '?')
        num = driver_info_state.get('RacingNumber', driver_num_str)
        name = driver_info_state.get('FullName', 'Unknown')
        team = driver_info_state.get('TeamName', '?')
        # Basic details like name/team shown above tabs
        driver_basic_details_children = [
            html.H6(f"#{num} {tla} - {name}", style={'marginTop': '0px', 'marginBottom':'2px', 'fontSize':'0.9rem'}),
            html.P(f"Team: {team}", style={'fontSize':'0.75rem', 'marginBottom':'0px', 'color': 'lightgrey'})
        ]
    else: # Should not happen if driver_num_str is valid, but as a fallback
        driver_basic_details_children = [html.P(f"Details for driver {driver_num_str} not found.", style={'color':'orange'})]


    # --- Handle Active Tab Content ---
    if active_tab_id == "tab-telemetry":
        driver_selected_uirevision_telemetry = f"telemetry_{driver_num_str}_pendinglap"
        
        if available_telemetry_laps:
            telemetry_lap_options = [{'label': f'Lap {l}', 'value': l} for l in available_telemetry_laps]
            telemetry_lap_disabled = False
            if triggered_id == 'driver-select-dropdown' or \
               triggered_id == 'driver-focus-tabs' or \
               not selected_lap_for_telemetry or \
               selected_lap_for_telemetry not in available_telemetry_laps:
                telemetry_lap_value = available_telemetry_laps[-1] # Default to last available lap
            else:
                telemetry_lap_value = selected_lap_for_telemetry
        else: # No telemetry laps available for this driver
            no_laps_message = config.TEXT_DRIVER_NO_LAP_DATA_PREFIX + tla + "."
            if current_telemetry_figure and \
               current_telemetry_figure.get('layout', {}).get('uirevision') == driver_selected_uirevision_telemetry and \
               current_telemetry_figure.get('layout',{}).get('annotations',[{}])[0].get('text','') == no_laps_message:
                fig_telemetry = no_update # Already showing "no laps"
            else:
                fig_telemetry = utils.create_empty_figure_with_message(
                    config.TELEMETRY_WRAPPER_HEIGHT, driver_selected_uirevision_telemetry, 
                    no_laps_message, config.TELEMETRY_MARGINS_EMPTY
                )
            # Keep other telemetry outputs at their defaults (no options, disabled, etc.)
            return (driver_basic_details_children, telemetry_lap_options, None, True, fig_telemetry,
                    stint_history_data, stint_history_columns_output)

        if not telemetry_lap_value: # Should be set if laps were available, but as a safeguard
            select_lap_message = config.TEXT_DRIVER_SELECT_A_LAP_PREFIX + tla + "."
            if current_telemetry_figure and \
               current_telemetry_figure.get('layout', {}).get('uirevision') == driver_selected_uirevision_telemetry and \
               current_telemetry_figure.get('layout',{}).get('annotations',[{}])[0].get('text','') == select_lap_message:
                fig_telemetry = no_update
            else:
                fig_telemetry = utils.create_empty_figure_with_message(
                    config.TELEMETRY_WRAPPER_HEIGHT, driver_selected_uirevision_telemetry, 
                    select_lap_message, config.TELEMETRY_MARGINS_EMPTY
                )
            return (driver_basic_details_children, telemetry_lap_options, telemetry_lap_value, telemetry_lap_disabled, fig_telemetry,
                    stint_history_data, stint_history_columns_output)

        # If we have a driver and a lap for telemetry, proceed to plot
        data_plot_uirevision_telemetry = f"telemetry_data_{driver_num_str}_{telemetry_lap_value}"
        # Check if figure needs update (e.g. if user just switched tabs but data is same)
        if current_telemetry_figure and \
           current_telemetry_figure.get('layout',{}).get('uirevision') == data_plot_uirevision_telemetry and \
           triggered_id not in ['driver-select-dropdown', 'lap-selector-dropdown', 'driver-focus-tabs']:
            fig_telemetry = no_update
        else:
            try:
                with app_state.app_state_lock: # Re-fetch specific lap data
                    lap_data = app_state.telemetry_data.get(driver_num_str, {}).get(telemetry_lap_value, {})
                
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
                        uirevision=data_plot_uirevision_telemetry, annotations=[]
                    )
                    for i, annot in enumerate(fig_telemetry.layout.annotations):
                        annot.font.size = 9; annot.yanchor = 'bottom'; annot.y = annot.y
                    for i_ax in range(len(channels)):
                        fig_telemetry.update_xaxes(
                            showline=(i_ax == len(channels)-1), zeroline=False,
                            showticklabels=(i_ax == len(channels)-1), row=i_ax+1, col=1,
                            tickfont_size=8
                        )
                else: # No valid plot data for this lap
                    fig_telemetry = utils.create_empty_figure_with_message(
                        config.TELEMETRY_WRAPPER_HEIGHT, data_plot_uirevision_telemetry,
                        config.TEXT_TELEMETRY_NO_PLOT_DATA_FOR_LAP_PREFIX + str(telemetry_lap_value) + ".",
                        config.TELEMETRY_MARGINS_EMPTY
                    )
            except Exception as plot_err:
                logger.error(f"Error in telemetry plot: {plot_err}", exc_info=True)
                fig_telemetry = utils.create_empty_figure_with_message(
                    config.TELEMETRY_WRAPPER_HEIGHT, data_plot_uirevision_telemetry,
                    config.TEXT_TELEMETRY_ERROR, config.TELEMETRY_MARGINS_EMPTY
                )
        # Stint history data remains empty for telemetry tab
        stint_history_data = []


    elif active_tab_id == "tab-stint-history":
        if all_stints_for_driver:
            # Process stint data for the table
            for stint_entry in all_stints_for_driver:
                # Create a display version for 'is_new_tyre'
                processed_entry = stint_entry.copy() # Avoid modifying original app_state data
                processed_entry['is_new_tyre_display'] = 'Y' if stint_entry.get('is_new_tyre') else 'N'
                stint_history_data.append(processed_entry)
        else:
            # Add a placeholder row if no stint data, or could be handled by DataTable's `empty_यर` prop
            stint_history_data = [{"stint_number": "No stint data available for this driver."}] 
            # If you use placeholder, ensure all column IDs exist in this placeholder or DataTable might error.
            # For simplicity, let's make sure all expected keys are there, even if None.
            stint_history_data = [{
                'stint_number': "No stint data", 'start_lap': '-', 'compound': '-', 
                'is_new_tyre_display': '-', 'tyre_age_at_stint_start': '-', 
                'end_lap': '-', 'total_laps_on_tyre_in_stint': '-', 
                'tyre_total_laps_at_stint_end': '-'
            }]


        # Telemetry figure remains empty/initial for stint history tab
        fig_telemetry = utils.create_empty_figure_with_message(
            config.TELEMETRY_WRAPPER_HEIGHT, config.INITIAL_TELEMETRY_UIREVISION,
            config.TEXT_DRIVER_SELECT_LAP, config.TELEMETRY_MARGINS_EMPTY # Or a different message like "Switch to Telemetry tab"
        )
        telemetry_lap_options = config.DROPDOWN_NO_LAPS_OPTIONS
        telemetry_lap_value = None
        telemetry_lap_disabled = True

    else: # Unknown tab or no tab selected
        logger.warning(f"Unknown or no active tab ID: {active_tab_id}")
        # Return defaults for all outputs
        return (driver_basic_details_children, telemetry_lap_options, telemetry_lap_value, telemetry_lap_disabled, fig_telemetry,
                stint_history_data, stint_history_columns_output)

    return (driver_basic_details_children, telemetry_lap_options, telemetry_lap_value, telemetry_lap_disabled, fig_telemetry,
            stint_history_data, stint_history_columns_output)

@app.callback(
    Output('current-track-layout-cache-key-store', 'data'),
    Input('interval-component-medium', 'n_intervals'),
    State('current-track-layout-cache-key-store', 'data')
)
def update_current_session_id_for_map(n_intervals, existing_session_id_in_store):
    with app_state.app_state_lock:
        year = app_state.session_details.get('Year')
        circuit_key = app_state.session_details.get('CircuitKey')
        app_status_state = app_state.app_status.get("state", "Idle")

    if not year or not circuit_key or app_status_state in ["Idle", "Stopped", "Error"]:
        if existing_session_id_in_store is not None:
            # Clear the selected driver if session changes or becomes invalid
            with app_state.app_state_lock:
                if app_state.selected_driver_for_map_and_lap_chart is not None:
                    logger.debug("Clearing selected_driver_for_map_and_lap_chart due to invalid/changed session.")
                    app_state.selected_driver_for_map_and_lap_chart = None
            return None
        return dash.no_update

    current_session_id = f"{year}_{circuit_key}"

    if current_session_id != existing_session_id_in_store:
        logger.debug(
            f"Updating current-track-layout-cache-key-store to: {current_session_id}. Clearing selected driver.")
        with app_state.app_state_lock: # Clear selected driver on session change
            app_state.selected_driver_for_map_and_lap_chart = None
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
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered and ctx.triggered[0] else None

    if not triggered_id:
        return no_update

    with app_state.app_state_lock:
        current_app_s = app_state.app_status.get("state", "Idle")

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
    if n_intervals == 0: # Or check if None
        return dash.no_update

    with app_state.app_state_lock:
        current_app_status = app_state.app_status.get("state", "Idle")
        timing_state_snapshot = app_state.timing_state.copy()
        # Get the currently selected driver for highlighting
        selected_driver_rno = app_state.selected_driver_for_map_and_lap_chart

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
    return output_data

@app.callback(
    Output('clientside-update-interval', 'interval'),
    Input('replay-speed-slider', 'value'),
    State('clientside-update-interval', 'disabled'),
    prevent_initial_call=True
)
def update_clientside_interval_speed(replay_speed, interval_disabled):
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
    return new_interval_ms

@app.callback(
    Output('track-map-graph', 'figure', allow_duplicate=True),
    Output('track-map-figure-version-store', 'data', allow_duplicate=True),
    Output('track-map-yellow-key-store', 'data'),
    [Input('interval-component-medium', 'n_intervals'),
     Input('current-track-layout-cache-key-store', 'data')],
    [State('track-map-graph', 'figure'),
     State('track-map-figure-version-store', 'data'),
     State('track-map-yellow-key-store', 'data')],
    prevent_initial_call='initial_duplicate'
)
def initialize_track_map(n_intervals, expected_session_id,
                         current_track_map_figure_state,
                         current_figure_version_in_store_state,
                         previous_rendered_yellow_key_from_store):

    ctx = dash.callback_context
    triggered_prop_id = ctx.triggered[0]['prop_id']
    triggering_input_id = triggered_prop_id.split('.')[0]

    logger.debug(f"INIT_TRACK_MAP Trigger: {triggering_input_id}, SID: {expected_session_id}, PrevYellowKey: {previous_rendered_yellow_key_from_store}")

    with app_state.app_state_lock:
        cached_data = app_state.track_coordinates_cache.copy()
        driver_list_snapshot = app_state.timing_state.copy() # For car marker placeholders
        active_yellow_sectors_snapshot = set(app_state.active_yellow_sectors)

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
    # Increment version due to placeholder strategy
    layout_structure_version = "v3.3_placeholders"
    target_persistent_layout_uirevision = f"trackmap_layout_{expected_session_id}_c{corners_c}_l{lights_c}_{layout_structure_version}"
    active_yellow_sectors_key_for_current_render = "_".join(sorted(map(str, list(active_yellow_sectors_snapshot))))

    needs_full_rebuild = False
    current_layout_uirevision_from_state = current_track_map_figure_state.get('layout', {}).get('uirevision') if current_track_map_figure_state and current_track_map_figure_state.get('layout') else None

    if triggering_input_id == 'current-track-layout-cache-key-store': needs_full_rebuild = True
    elif not current_track_map_figure_state or not current_track_map_figure_state.get('layout'): needs_full_rebuild = True
    elif current_layout_uirevision_from_state != target_persistent_layout_uirevision: needs_full_rebuild = True

    processed_previous_yellow_key = previous_rendered_yellow_key_from_store
    if previous_rendered_yellow_key_from_store is None: processed_previous_yellow_key = ""

    if not needs_full_rebuild and processed_previous_yellow_key == active_yellow_sectors_key_for_current_render:
        logger.debug(f"INIT_TRACK_MAP --- No change for SID '{expected_session_id}', yellow key '{active_yellow_sectors_key_for_current_render}' unchanged. No update.")
        return no_update, no_update, no_update

    figure_output: go.Figure
    version_store_output = no_update
    yellow_key_store_output = active_yellow_sectors_key_for_current_render

    if needs_full_rebuild:
        logger.debug(f"Performing FULL track map rebuild for SID: {expected_session_id}. Target Layout uirevision: {target_persistent_layout_uirevision}")
        fig_data = []
        # 1. Base Track Line
        fig_data.append(go.Scatter(x=list(cached_data['x']), y=list(cached_data['y']), mode='lines', line=dict(color='grey', width=getattr(config, 'TRACK_LINE_WIDTH', 2)), name='Track', hoverinfo='none'))
        # 2. Static Corner Markers
        valid_corners = []
        if cached_data.get('corners_data'):
            valid_corners = [c for c in cached_data['corners_data'] if c.get('x') is not None and c.get('y') is not None]
            if valid_corners:
                fig_data.append(go.Scatter(
                    x=[c['x'] for c in valid_corners],
                    y=[c['y'] for c in valid_corners],
                    mode='markers+text',
                    marker=dict(
                        size=config.CORNER_MARKER_SIZE,
                        color=config.CORNER_MARKER_COLOR,
                        symbol='circle-open'
                    ),
                    text=[str(c['number']) for c in valid_corners],
                    textposition=config.CORNER_TEXT_POSITION,
                    textfont=dict(
                        size=config.CORNER_TEXT_SIZE,
                        color=config.CORNER_TEXT_COLOR
                    ),
                    dx=config.CORNER_TEXT_DX,  # Added dx
                    dy=config.CORNER_TEXT_DY,  # Added dy
                    name='Corners',
                    hoverinfo='text'
                ))
        # 3. Static Marshal Light/Post Markers
        valid_lights = []
        if cached_data.get('marshal_lights_data'):
            valid_lights = [m for m in cached_data['marshal_lights_data'] if m.get('x') is not None and m.get('y') is not None]
            if valid_lights:
                fig_data.append(go.Scatter(x=[m['x'] for m in valid_lights], y=[m['y'] for m in valid_lights], mode='markers', marker=dict(size=getattr(config, 'MARSHAL_MARKER_SIZE', 5), color=getattr(config, 'MARSHAL_MARKER_COLOR', 'orange'), symbol='diamond'), name='Marshal Posts', hoverinfo='text', text=[f"M{m['number']}" for m in valid_lights]))

        # 4. Add Yellow Sector Placeholders (Invisible initially)
        for i in range(config.MAX_YELLOW_SECTOR_PLACEHOLDERS):
            fig_data.append(go.Scatter(
                x=[None], y=[None], mode='lines',
                line=dict(color=getattr(config, 'YELLOW_FLAG_COLOR', 'yellow'), width=getattr(config, 'YELLOW_FLAG_WIDTH', 4)),
                name=f"{config.YELLOW_FLAG_PLACEHOLDER_NAME_PREFIX}{i}",
                hoverinfo='name',
                opacity=getattr(config, 'YELLOW_FLAG_OPACITY', 0.7),
                visible=False
            ))

        # 5. Car Marker Placeholders (using driver_list_snapshot)
        for car_num_str_init, driver_state_init in driver_list_snapshot.items():
            # Ensure driver_state_init is a dict
            if not isinstance(driver_state_init, dict):
                logger.warning(f"Skipping car marker placeholder for {car_num_str_init} due to invalid driver_state_init type: {type(driver_state_init)}")
                continue

            tla_init = driver_state_init.get('Tla', car_num_str_init);
            team_color_hex_init = driver_state_init.get('TeamColour', '808080')
            if not team_color_hex_init.startswith('#'): team_color_hex_init = '#' + team_color_hex_init.replace("#", "")
            if len(team_color_hex_init) not in [4, 7]: team_color_hex_init = '#808080'

            # UID should be the racing number string for consistent identification with JS
            # Make sure car_num_str_init is what you expect (e.g. from timing_state keys)
            racing_number_for_uid = driver_state_init.get('RacingNumber', car_num_str_init)

            fig_data.append(go.Scatter(
                x=[None], y=[None], mode='markers+text', name=tla_init,
                uid=str(racing_number_for_uid), # Ensure UID is string
                marker=dict(
                    size=getattr(config, 'CAR_MARKER_SIZE', 8),
                    color=team_color_hex_init,
                    line=dict(width=1, color='Black')
                ),
                textfont=dict(
                    size=getattr(config, 'CAR_MARKER_TEXT_SIZE', 8),
                    color='white'
                ),
                textposition='middle right', hoverinfo='text', text=tla_init
            ))


        fig_layout = go.Layout(
            template='plotly_dark', uirevision=target_persistent_layout_uirevision,
            xaxis=dict(visible=False, fixedrange=True, range=cached_data.get('range_x'), autorange=False if cached_data.get('range_x') else True),
            yaxis=dict(visible=False, fixedrange=True, scaleanchor="x", scaleratio=1, range=cached_data.get('range_y'), autorange=False if cached_data.get('range_y') else True),
            showlegend=False, plot_bgcolor='rgb(30,30,30)', paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'), margin=config.TRACK_MAP_MARGINS, height=config.TRACK_MAP_WRAPPER_HEIGHT,
            annotations=[]
        )
        figure_output = go.Figure(data=fig_data, layout=fig_layout)
        version_store_output = f"trackbase_{expected_session_id}_{time.time()}"

        # Update placeholders based on current active_yellow_sectors_snapshot for the new figure
        # This logic is now INSIDE the needs_full_rebuild block as figure_output is brand new
        if cached_data.get('marshal_sector_segments') and cached_data.get('x'):
            track_x_full = cached_data['x']; track_y_full = cached_data['y']
            placeholder_trace_offset = (1 + # Track
                                       (1 if valid_corners else 0) +
                                       (1 if valid_lights else 0))

            for sector_num_active in active_yellow_sectors_snapshot:
                placeholder_idx_for_sector = sector_num_active - 1 # Assuming 1-indexed sectors from RCM
                if 0 <= placeholder_idx_for_sector < config.MAX_YELLOW_SECTOR_PLACEHOLDERS:
                    trace_index_to_update = placeholder_trace_offset + placeholder_idx_for_sector
                    if trace_index_to_update < len(figure_output.data): # Check bounds
                        segment_indices = cached_data['marshal_sector_segments'].get(sector_num_active)
                        if segment_indices:
                            start_idx, end_idx = segment_indices
                            if 0 <= start_idx < len(track_x_full) and \
                               0 <= end_idx < len(track_x_full) and \
                               start_idx <= end_idx:
                                x_seg = track_x_full[start_idx : end_idx + 1]
                                y_seg = track_y_full[start_idx : end_idx + 1]
                                if len(x_seg) >= 1:
                                    figure_output.data[trace_index_to_update].x = list(x_seg)
                                    figure_output.data[trace_index_to_update].y = list(y_seg)
                                    figure_output.data[trace_index_to_update].visible = True
                                    figure_output.data[trace_index_to_update].name = f"Yellow Sector {sector_num_active}"
                                    figure_output.data[trace_index_to_update].mode = 'lines' if len(x_seg) > 1 else 'markers'
                                    if len(x_seg) == 1 and hasattr(config, 'YELLOW_FLAG_MARKER_SIZE'): # Check if config exists
                                        figure_output.data[trace_index_to_update].marker = dict(color=getattr(config, 'YELLOW_FLAG_COLOR', 'yellow'), size=getattr(config, 'YELLOW_FLAG_MARKER_SIZE', 8))

    else: # Not a full rebuild: update existing figure's placeholder traces
        figure_output = go.Figure(current_track_map_figure_state)
        figure_output.layout.uirevision = target_persistent_layout_uirevision
        version_store_output = current_figure_version_in_store_state

        if cached_data.get('marshal_sector_segments') and cached_data.get('x') and figure_output.data:
            track_x_full = cached_data['x']; track_y_full = cached_data['y']

            # Calculate offset of first placeholder trace
            placeholder_trace_offset = 0
            temp_valid_corners = [c for c in (cached_data.get('corners_data') or []) if c.get('x') is not None and c.get('y') is not None]
            temp_valid_lights = [m for m in (cached_data.get('marshal_lights_data') or []) if m.get('x') is not None and m.get('y') is not None]

            if True: placeholder_trace_offset +=1 # For 'Track'
            if temp_valid_corners: placeholder_trace_offset +=1
            if temp_valid_lights: placeholder_trace_offset +=1

            logger.debug(f"Updating yellow placeholders on existing figure. Active: {active_yellow_sectors_snapshot}. Placeholder offset: {placeholder_trace_offset}")

            for i in range(config.MAX_YELLOW_SECTOR_PLACEHOLDERS):
                trace_index_for_placeholder = placeholder_trace_offset + i
                if trace_index_for_placeholder < len(figure_output.data):
                    # Assume sector number `i+1` maps to placeholder `i`
                    current_sector_represented_by_placeholder = i + 1

                    if current_sector_represented_by_placeholder in active_yellow_sectors_snapshot:
                        segment_indices = cached_data['marshal_sector_segments'].get(current_sector_represented_by_placeholder)
                        if segment_indices:
                            start_idx, end_idx = segment_indices
                            if 0 <= start_idx < len(track_x_full) and \
                               0 <= end_idx < len(track_x_full) and \
                               start_idx <= end_idx:
                                x_seg = track_x_full[start_idx : end_idx + 1]
                                y_seg = track_y_full[start_idx : end_idx + 1]
                                if len(x_seg) >= 1:
                                    figure_output.data[trace_index_for_placeholder].x = list(x_seg)
                                    figure_output.data[trace_index_for_placeholder].y = list(y_seg)
                                    figure_output.data[trace_index_for_placeholder].visible = True
                                    figure_output.data[trace_index_for_placeholder].name = f"Yellow Sector {current_sector_represented_by_placeholder}"
                                    figure_output.data[trace_index_for_placeholder].mode = 'lines' if len(x_seg) > 1 else 'markers'
                                    if len(x_seg) == 1 and hasattr(config, 'YELLOW_FLAG_MARKER_SIZE'):
                                        figure_output.data[trace_index_for_placeholder].marker = dict(color=getattr(config, 'YELLOW_FLAG_COLOR', 'yellow'), size=getattr(config, 'YELLOW_FLAG_MARKER_SIZE', 8))
                                else: # Segment is empty, hide placeholder
                                    figure_output.data[trace_index_for_placeholder].x = [None]
                                    figure_output.data[trace_index_for_placeholder].y = [None]
                                    figure_output.data[trace_index_for_placeholder].visible = False
                                    figure_output.data[trace_index_for_placeholder].name = f"{config.YELLOW_FLAG_PLACEHOLDER_NAME_PREFIX}{i}"
                            else: # Invalid segment indices
                                figure_output.data[trace_index_for_placeholder].x = [None]; figure_output.data[trace_index_for_placeholder].y = [None]; figure_output.data[trace_index_for_placeholder].visible = False; figure_output.data[trace_index_for_placeholder].name = f"{config.YELLOW_FLAG_PLACEHOLDER_NAME_PREFIX}{i}"
                        else: # No segment definition for this active sector for this placeholder
                            figure_output.data[trace_index_for_placeholder].x = [None]; figure_output.data[trace_index_for_placeholder].y = [None]; figure_output.data[trace_index_for_placeholder].visible = False; figure_output.data[trace_index_for_placeholder].name = f"{config.YELLOW_FLAG_PLACEHOLDER_NAME_PREFIX}{i}"
                    else: # This placeholder is not for an active yellow sector
                        figure_output.data[trace_index_for_placeholder].x = [None]
                        figure_output.data[trace_index_for_placeholder].y = [None]
                        figure_output.data[trace_index_for_placeholder].visible = False
                        figure_output.data[trace_index_for_placeholder].name = f"{config.YELLOW_FLAG_PLACEHOLDER_NAME_PREFIX}{i}"
                else:
                    logger.warning(f"Placeholder index {trace_index_for_placeholder} out of bounds for fig.data len {len(figure_output.data)}")

    if figure_output is not no_update:
        trace_names_in_output_final = [getattr(t, 'name', 'Unnamed') for t in figure_output.data]
        logger.debug(f"FINAL Figure Output Data Traces (Placeholder Method): {trace_names_in_output_final}")
        active_yellow_trace_names = [getattr(t, 'name') for t in figure_output.data if getattr(t, 'name', '').startswith("Yellow Sector ") and getattr(t, 'visible', False)]
        logger.debug(f"Visibly active yellow traces in output: {active_yellow_trace_names} (based on snapshot: {active_yellow_sectors_snapshot})")

    logger.debug(f"INIT_TRACK_MAP --- Outputting. FullRebuild: {needs_full_rebuild}, FigUpdate: {figure_output is not no_update}, PrevYellowKey: '{previous_rendered_yellow_key_from_store}', RenderedYellowKey: '{yellow_key_store_output}', VersionStoreOutIsUpdate: {version_store_output is not no_update and version_store_output != current_figure_version_in_store_state }")
    return figure_output, version_store_output, yellow_key_store_output


@app.callback(
    Output('driver-select-dropdown', 'options'),
    Input('interval-component-slow', 'n_intervals')
)
def update_driver_dropdown_options(n_intervals):
    logger.debug("Attempting to update driver dropdown options...")
    options = config.DROPDOWN_NO_DRIVERS_OPTIONS # Use constant
    try:
        with app_state.app_state_lock:
            timing_state_copy = app_state.timing_state.copy()

        options = utils.generate_driver_options(timing_state_copy) # This helper already uses config constants for error states
        logger.debug(f"Updating driver dropdown options: {len(options)} options generated.")
    except Exception as e:
         logger.error(f"Error generating driver dropdown options: {e}", exc_info=True)
         options = config.DROPDOWN_ERROR_LOADING_DRIVERS_OPTIONS # Use constant
    return options

@app.callback(
    Output('lap-time-driver-selector', 'options'),
    Input('interval-component-slow', 'n_intervals')
)
def update_lap_chart_driver_options(n_intervals):
    with app_state.app_state_lock:
        timing_state_copy = app_state.timing_state.copy()
    # utils.generate_driver_options already handles empty/error cases with config constants
    options = utils.generate_driver_options(timing_state_copy) #
    return options


@app.callback(
    Output('lap-time-progression-graph', 'figure'),
    Input('lap-time-driver-selector', 'value'), # This is the primary input for data
    Input('interval-component-medium', 'n_intervals') # Keep to refresh if data changes for selected drivers
)
def update_lap_time_progression_chart(selected_drivers_rnos, n_intervals):
    # Use utils.create_empty_figure_with_message and config constants
    fig_empty_lap_prog = utils.create_empty_figure_with_message( #
        config.LAP_PROG_WRAPPER_HEIGHT, config.INITIAL_LAP_PROG_UIREVISION, #
        config.TEXT_LAP_PROG_SELECT_DRIVERS, config.LAP_PROG_MARGINS_EMPTY #
    )

    if not selected_drivers_rnos:
        return fig_empty_lap_prog

    # Ensure selected_drivers_rnos is a list for consistent processing
    if not isinstance(selected_drivers_rnos, list):
        selected_drivers_rnos = [selected_drivers_rnos]


    # Create a uirevision based on sorted list of selected drivers to ensure graph redraws if selection changes
    # but not if only data for those drivers changes (handled by plotly's internal diffing if figure structure is same)
    sorted_selection_key = "_".join(sorted(list(set(str(rno) for rno in selected_drivers_rnos))))
    data_plot_uirevision = f"lap_prog_data_{sorted_selection_key}"


    with app_state.app_state_lock:
        # Deep copy might be safer if complex objects were stored, but lists of dicts of primitives is usually fine
        lap_history_snapshot = {rno: list(laps) for rno, laps in app_state.lap_time_history.items()}
        timing_state_snapshot = app_state.timing_state.copy()

    fig_with_data = go.Figure(layout={
        'template': 'plotly_dark', 'uirevision': data_plot_uirevision, # Use selection-based uirevision
        'height': config.LAP_PROG_WRAPPER_HEIGHT, # Use constant
        'margin': config.LAP_PROG_MARGINS_DATA, # Use constant
        'xaxis_title': 'Lap Number', 'yaxis_title': 'Lap Time (s)',
        'hovermode': 'x unified', 'title_text': 'Lap Time Progression', 'title_x':0.5, 'title_font_size':14,
        'showlegend':True, 'legend_title_text':'Drivers', 'legend_font_size':10,
        'annotations': [] # Clear any "select drivers" message
    })

    data_actually_plotted = False
    min_time_overall, max_time_overall, max_laps_overall = float('inf'), float('-inf'), 0

    for driver_rno_str in selected_drivers_rnos: # driver_rno from dropdown is usually string
        # Ensure we use the string version for lookups if lap_history_snapshot keys are strings
        driver_laps = lap_history_snapshot.get(str(driver_rno_str), [])
        if not driver_laps: continue

        driver_info = timing_state_snapshot.get(str(driver_rno_str), {})
        tla = driver_info.get('Tla', str(driver_rno_str))
        team_color_hex = driver_info.get('TeamColour', 'FFFFFF')
        if not team_color_hex.startswith('#'): team_color_hex = '#' + team_color_hex

        # Filter for valid laps as per your existing logic in data_processing for lap_history
        valid_laps = [lap for lap in driver_laps if lap.get('is_valid', True)]
        if not valid_laps: continue

        data_actually_plotted = True
        lap_numbers = [lap['lap_number'] for lap in valid_laps]
        lap_times_sec = [lap['lap_time_seconds'] for lap in valid_laps]

        if lap_numbers: max_laps_overall = max(max_laps_overall, max(lap_numbers))
        if lap_times_sec:
            min_time_overall = min(min_time_overall, min(lap_times_sec))
            max_time_overall = max(max_time_overall, max(lap_times_sec))

        hover_texts = []
        for lap in valid_laps:
            total_seconds = lap['lap_time_seconds']
            minutes = int(total_seconds // 60)
            seconds_part = total_seconds % 60
            time_formatted = f"{minutes}:{seconds_part:06.3f}" if minutes > 0 else f"{seconds_part:.3f}"
            hover_texts.append(f"<b>{tla}</b><br>Lap: {lap['lap_number']}<br>Time: {time_formatted}<br>Tyre: {lap['compound']}<extra></extra>")

        fig_with_data.add_trace(go.Scatter(
            x=lap_numbers, y=lap_times_sec, mode='lines+markers', name=tla,
            marker=dict(color=team_color_hex, size=5), line=dict(color=team_color_hex, width=1.5),
            hovertext=hover_texts, hoverinfo='text'
        ))

    if not data_actually_plotted:
        fig_empty_lap_prog.layout.annotations[0].text = config.TEXT_LAP_PROG_NO_DATA # Use constant
        fig_empty_lap_prog.layout.uirevision = data_plot_uirevision # Match uirevision to avoid broken updates
        return fig_empty_lap_prog

    if min_time_overall != float('inf') and max_time_overall != float('-inf'):
        padding = (max_time_overall - min_time_overall) * 0.05 if max_time_overall > min_time_overall else 0.5
        fig_with_data.update_yaxes(visible=True, range=[min_time_overall - padding, max_time_overall + padding], autorange=False)
    else:
        fig_with_data.update_yaxes(visible=True, autorange=True) # Fallback if only one point or no data

    if max_laps_overall > 0:
        fig_with_data.update_xaxes(visible=True, range=[0.5, max_laps_overall + 0.5], autorange=False)
    else:
        fig_with_data.update_xaxes(visible=True, autorange=True)


    return fig_with_data

@app.callback(
    Output('race-control-log-display', 'value'),
    Input('interval-component-medium', 'n_intervals') # Update periodically
)
def update_race_control_display(n_intervals):
    try:
        with app_state.app_state_lock:
            # The deque stores messages with newest first due to appendleft
            # To display them chronologically (oldest at top), we reverse.
            # Or, if you want newest at top, just join directly.
            log_messages = list(app_state.race_control_log) # Get a snapshot

        if not log_messages:
            return config.TEXT_RC_WAITING # Use constant

        # To display newest messages at the top of the textarea:
        display_text = "\n".join(log_messages)
        # If you prefer oldest messages at the top (more traditional log):
        # display_text = "\n".join(reversed(log_messages))

        return display_text
    except Exception as e:
        logger.error(f"Error updating race control display: {e}", exc_info=True)
        return config.TEXT_RC_ERROR # Use constant

@app.callback(
    Output("debug-data-accordion-item", "className"),
    Input("debug-mode-switch", "value"),
)
def toggle_debug_data_visibility(debug_mode_enabled):
    if debug_mode_enabled:
        logger.info("Debug mode enabled: Showing 'Other Data Streams'.")
        return "mt-1" # Bootstrap margin top class
    else:
        logger.info("Debug mode disabled: Hiding 'Other Data Streams'.")
        return "d-none" # Bootstrap display none class

@app.callback(
    Output('driver-select-dropdown', 'value'),
    Input('clicked-car-driver-number-store', 'data'),
    State('driver-select-dropdown', 'options'),
    prevent_initial_call=True
)
def update_dropdown_from_map_click(click_data_json_str, dropdown_options): # Renamed arg
    if click_data_json_str is None:
        return dash.no_update

    try:
        # The data from the store is now the JSON string written by JS
        click_data = json.loads(click_data_json_str)
        clicked_driver_number_str = str(click_data.get('carNumber')) # Ensure it's a string

        if clicked_driver_number_str is None or clicked_driver_number_str == 'None': # Check for None or 'None' string
            with app_state.app_state_lock: # If click is invalid, clear selection
                if app_state.selected_driver_for_map_and_lap_chart is not None:
                    logger.info("Map click invalid, clearing app_state.selected_driver_for_map_and_lap_chart.")
                    app_state.selected_driver_for_map_and_lap_chart = None
            return dash.no_update

        logger.info(f"Map click: Attempting to select driver number: {clicked_driver_number_str} for telemetry dropdown.")

        # Update app_state with the clicked driver
        with app_state.app_state_lock:
            app_state.selected_driver_for_map_and_lap_chart = clicked_driver_number_str
            logger.info(f"Updated app_state.selected_driver_for_map_and_lap_chart to: {clicked_driver_number_str}")


        if dropdown_options and isinstance(dropdown_options, list):
            valid_driver_numbers = [opt['value'] for opt in dropdown_options if 'value' in opt]
            if clicked_driver_number_str in valid_driver_numbers:
                logger.info(f"Map click: Setting driver-select-dropdown (telemetry) to: {clicked_driver_number_str}")
                return clicked_driver_number_str
            else:
                logger.warning(f"Map click: Driver number {clicked_driver_number_str} not found in telemetry dropdown options: {valid_driver_numbers}")
                # Even if not in telemetry dropdown, keep it selected in app_state for map/lap chart
                return dash.no_update # Don't change telemetry dropdown if invalid for it
    except json.JSONDecodeError:
        logger.error(f"update_dropdown_from_map_click: Could not decode JSON from store: {click_data_json_str}")
        with app_state.app_state_lock: app_state.selected_driver_for_map_and_lap_chart = None # Clear on error
    except Exception as e:
        logger.error(f"update_dropdown_from_map_click: Error processing click data: {e}")
        with app_state.app_state_lock: app_state.selected_driver_for_map_and_lap_chart = None # Clear on error

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
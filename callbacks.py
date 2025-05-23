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
    session_time_str = "" # Initialize to empty, important for "Next Up"
    session_timer_div_style = {'display': 'none'} #

    try:
        # Acquire all necessary states under a single lock
        with app_state.app_state_lock: #
            current_app_overall_status = app_state.app_status.get("state", "Idle") #
            
            if current_app_overall_status not in ["Live", "Replaying"]: #
                return lap_value_str, lap_counter_div_style, session_timer_label_text, session_time_str, session_timer_div_style #

            session_type_from_state = app_state.session_details.get('Type', "Unknown") #
            current_session_feed_status = app_state.session_details.get('SessionStatus', 'Unknown') #
            current_replay_speed = app_state.replay_speed #
            
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
            q_state = app_state.qualifying_segment_state.copy() # Use a copy #
            
            practice_start_utc_local = app_state.practice_session_actual_start_utc #
            practice_duration_s_local = app_state.practice_session_scheduled_duration_seconds #
            session_name_from_details = app_state.session_details.get('Name', '') # Default to empty if not found #
            
            extrapolated_clock_remaining = app_state.extrapolated_clock_info.get("Remaining") if hasattr(app_state, 'extrapolated_clock_info') else None #

        # --- Logic for displaying session type specific info ---

        if session_type_lower in [config.SESSION_TYPE_RACE.lower(), config.SESSION_TYPE_SPRINT.lower()]: #
            lap_counter_div_style = {'display': 'inline-block', 'margin-right': '20px'} #
            # session_timer_div_style is {'display': 'none'} by default for Race/Sprint
            lap_value_str = f"{current_lap_to_display}/{actual_total_laps_to_display}" if current_lap_to_display != '-' else "Awaiting Data..." #

        elif session_type_lower in ["qualifying", "sprint shootout"]: #
            lap_counter_div_style = {'display': 'none'} #
            session_timer_div_style = {'display': 'inline-block'} #
            segment_label = q_state.get("current_segment", "") # "" if not found #
            displayed_remaining_seconds = 0 
            session_time_str = "" # Default to empty for "Next Up" or "Ended"

            if segment_label == "Between Segments": #
                old_q_segment = q_state.get("old_segment") #
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
                session_timer_label_text = f"{segment_label}:" #
                if q_state.get("last_official_time_capture_utc") and q_state.get("official_segment_remaining_seconds") is not None: #
                    now_utc = datetime.now(timezone.utc) #
                    time_since_last_capture = (now_utc - q_state["last_official_time_capture_utc"]).total_seconds() #
                    adjusted_elapsed_time = time_since_last_capture * current_replay_speed #
                    calculated_remaining = q_state["official_segment_remaining_seconds"] - adjusted_elapsed_time #
                    displayed_remaining_seconds = max(0, calculated_remaining) #
                    session_time_str = utils.format_seconds_to_time_str(displayed_remaining_seconds) #
                else: 
                    session_time_str = "Awaiting..."
            
            elif segment_label and segment_label not in ["Unknown", "Between Segments", "Ended"] and \
                 current_session_feed_status in ["Suspended", "Aborted", "Inactive"]: #
                session_timer_label_text = f"{segment_label} Paused:" #
                if q_state.get("official_segment_remaining_seconds") is not None: #
                    displayed_remaining_seconds = max(0, q_state["official_segment_remaining_seconds"]) #
                    session_time_str = utils.format_seconds_to_time_str(displayed_remaining_seconds) #
                else:
                    session_time_str = "Awaiting..."
            
            else: 
                if session_name_from_details and current_session_feed_status == "NotStarted": #
                    session_timer_label_text = f"Next Up: {session_name_from_details}"
                    # session_time_str is already ""
                else: # Fallback for other Q states (e.g. Unknown segment)
                    session_timer_label_text = "Time Left:" #
                    session_time_str = extrapolated_clock_remaining if extrapolated_clock_remaining and extrapolated_clock_remaining != "0" else "Awaiting..."


        elif session_type_lower.startswith("practice"): #
            lap_counter_div_style = {'display': 'none'} #
            session_timer_div_style = {'display': 'inline-block'} #
            session_timer_label_text = "Time Left:" # Default
            session_time_str = "" # Default to empty for "Next Up"

            if (not practice_start_utc_local and \
                current_session_feed_status not in ["Finished", "Ends", "Started"]) or \
               current_session_feed_status == "NotStarted": #
                actual_session_name = session_name_from_details if session_name_from_details else "Practice" #
                session_timer_label_text = f"Next Up: {actual_session_name}"
                # session_time_str remains empty
            elif current_session_feed_status in ["Finished", "Ends"]: #
                displayed_remaining_seconds = 0 #
                session_time_str = utils.format_seconds_to_time_str(displayed_remaining_seconds) #
            elif practice_start_utc_local and practice_duration_s_local is not None: #
                now_utc = datetime.now(timezone.utc) #
                elapsed_wall_time_seconds = (now_utc - practice_start_utc_local).total_seconds() #
                effective_elapsed_session_time = elapsed_wall_time_seconds * current_replay_speed #
                calculated_true_remaining_seconds = practice_duration_s_local - effective_elapsed_session_time #
                displayed_remaining_seconds = max(0, calculated_true_remaining_seconds) #
                session_time_str = utils.format_seconds_to_time_str(displayed_remaining_seconds) #
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
    timestamp_text = config.TEXT_WAITING_FOR_DATA
    current_time_for_callbacks = time.time()

    try:
        session_type_from_state = ""
        current_q_segment_from_state = None
        previous_q_segment_from_state = None
        q_state_snapshot = {}
        current_replay_speed_snapshot = 1.0
        session_feed_status_snapshot = "Unknown"

        # Initialize rule dictionaries
        active_segment_highlight_rule = {
            "type": "NONE", "lower_pos": 0, "upper_pos": 0}
        q1_eliminated_highlight_rule = {
            "type": "NONE", "lower_pos": 0, "upper_pos": 0}
        q2_eliminated_highlight_rule = {
            "type": "NONE", "lower_pos": 0, "upper_pos": 0}

        current_segment_time_remaining_seconds = float('inf')

        with app_state.app_state_lock:
            session_type_from_state = app_state.session_details.get(
                'Type', "").lower()
            current_q_segment_from_state = app_state.qualifying_segment_state.get(
                "current_segment")
            previous_q_segment_from_state = app_state.qualifying_segment_state.get(
                "old_segment")

            q_state_snapshot = app_state.qualifying_segment_state.copy()
            current_replay_speed_snapshot = app_state.replay_speed
            session_feed_status_snapshot = app_state.session_details.get(
                'SessionStatus', 'Unknown')
            timing_state_copy = app_state.timing_state.copy()
            data_store_copy = app_state.data_store

        # --- Calculate current segment time remaining ---
        if q_state_snapshot.get("last_official_time_capture_utc") and \
           q_state_snapshot.get("official_segment_remaining_seconds") is not None and \
           current_q_segment_from_state and \
           current_q_segment_from_state not in ["Unknown", "Between Segments", "Ended"] and \
           session_feed_status_snapshot not in ["Suspended", "Aborted", "Finished", "Ends", "NotStarted"]:
            now_utc_for_calc = datetime.now(timezone.utc)
            time_since_last_capture_for_calc = (
                now_utc_for_calc - q_state_snapshot["last_official_time_capture_utc"]).total_seconds()
            adjusted_elapsed_time_for_calc = time_since_last_capture_for_calc * \
                current_replay_speed_snapshot
            calculated_remaining_for_calc = q_state_snapshot[
                "official_segment_remaining_seconds"] - adjusted_elapsed_time_for_calc
            current_segment_time_remaining_seconds = max(
                0, calculated_remaining_for_calc)
        elif current_q_segment_from_state in ["Between Segments", "Ended"] or \
                session_feed_status_snapshot in ["Finished", "Ends"]:
            current_segment_time_remaining_seconds = 0

        logger.debug(
            f"QualiHighlight: CurrentSeg='{current_q_segment_from_state}', PrevSeg='{previous_q_segment_from_state}', "
            f"TimeRemainingSec={current_segment_time_remaining_seconds:.1f}"
        )

        # --- Determine Highlight Rules based on your specific logic ---
        five_mins_in_seconds = 5 * 60
        is_qualifying_type_session = session_type_from_state in [
            "qualifying", "sprint shootout"]

        if is_qualifying_type_session and current_q_segment_from_state:
            # Q1/SQ1 Logic
            if current_q_segment_from_state in ["Q1", "SQ1"] or \
               (current_q_segment_from_state == "Between Segments" and previous_q_segment_from_state in [None, "Q1", "SQ1"]):
                lower_b_q1_active = config.QUALIFYING_CARS_Q1 - \
                    config.QUALIFYING_ELIMINATED_Q1 + 1  # P16
                upper_b_q1_active = config.QUALIFYING_CARS_Q1  # P20
                if current_segment_time_remaining_seconds <= five_mins_in_seconds:
                    active_segment_highlight_rule = {
                        "type": "RED_DANGER", "lower_pos": lower_b_q1_active, "upper_pos": upper_b_q1_active}

            # Q2/SQ2 Logic
            elif current_q_segment_from_state in ["Q2", "SQ2"] or \
                    (current_q_segment_from_state == "Between Segments" and previous_q_segment_from_state in ["Q2", "SQ2"]):

                # Rule for P16-P20 (Q1 eliminated cars) - ALWAYS GREY_ELIMINATED in Q2
                lower_b_q1_elim = config.QUALIFYING_CARS_Q1 - config.QUALIFYING_ELIMINATED_Q1 + 1
                upper_b_q1_elim = config.QUALIFYING_CARS_Q1
                q1_eliminated_highlight_rule = {  # This rule is for Q1 eliminated
                    "type": "GREY_ELIMINATED",
                    "lower_pos": lower_b_q1_elim,
                    "upper_pos": upper_b_q1_elim
                }

                # Rule for P11-P15 (Q2 active cars) - RED_DANGER if time critical, otherwise NO active highlight
                lower_b_q2_active = config.QUALIFYING_CARS_Q2 - \
                    config.QUALIFYING_ELIMINATED_Q2 + 1
                upper_b_q2_active = config.QUALIFYING_CARS_Q2
                if current_segment_time_remaining_seconds <= five_mins_in_seconds:
                    active_segment_highlight_rule = {
                        "type": "RED_DANGER", "lower_pos": lower_b_q2_active, "upper_pos": upper_b_q2_active}

            # Q3/SQ3 Logic (and "Ended" state if previous was Q3/SQ3)
            elif current_q_segment_from_state in ["Q3", "SQ3", "Ended"] or \
                    (current_q_segment_from_state == "Between Segments" and previous_q_segment_from_state in ["Q3", "SQ3"]):

                # P1-P10 (active Q3 participants) get NO active highlight
                active_segment_highlight_rule = {
                    "type": "NONE", "lower_pos": 0, "upper_pos": 0}

                # Rule for P16-P20 (Q1 eliminated) to be GREY_ELIMINATED
                lower_b_q1_elim_for_q3 = config.QUALIFYING_CARS_Q1 - \
                    config.QUALIFYING_ELIMINATED_Q1 + 1
                upper_b_q1_elim_for_q3 = config.QUALIFYING_CARS_Q1
                q1_eliminated_highlight_rule = {
                    "type": "GREY_ELIMINATED", "lower_pos": lower_b_q1_elim_for_q3, "upper_pos": upper_b_q1_elim_for_q3}

                # Rule for P11-P15 (Q2 eliminated) to be GREY_ELIMINATED
                lower_b_q2_elim_for_q3 = config.QUALIFYING_CARS_Q2 - \
                    config.QUALIFYING_ELIMINATED_Q2 + 1
                upper_b_q2_elim_for_q3 = config.QUALIFYING_CARS_Q2
                q2_eliminated_highlight_rule = {
                    "type": "GREY_ELIMINATED", "lower_pos": lower_b_q2_elim_for_q3, "upper_pos": upper_b_q2_elim_for_q3}

        # === Start of data processing for table (your existing code) ===
        # ... (excluded_streams, sorted_streams, other_elements loop - this remains unchanged) ...
        excluded_streams = ['TimingData', 'DriverList', 'Position.z', 'CarData.z', 'Position',
                            'TrackStatus', 'SessionData', 'SessionInfo', 'WeatherData', 'RaceControlMessages', 'Heartbeat']
        # ... (rest of other_elements population) ...
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
            other_elements.append(html.Details([html.Summary(f"{stream} ({timestamp_str_val})"),
                                                html.Pre(data_str, style={'marginLeft': '15px', 'maxHeight': '200px', 'overflowY': 'auto'})],
                                               open=(stream == "LapCount")))
        timing_data_entry = data_store_copy.get('TimingData', {})
        timestamp_text = f"Timing TS: {timing_data_entry.get('timestamp', 'N/A')}" if timing_data_entry else config.TEXT_WAITING_FOR_DATA

        if timing_state_copy:
            processed_table_data = []
            for car_num, driver_state in timing_state_copy.items():
                racing_no = driver_state.get("RacingNumber", car_num)
                tla = driver_state.get("Tla", "N/A")
                pos = driver_state.get('Position', '-')
                pos_str = str(pos)
                # status_driver = driver_state.get('Status', 'N/A').lower() # Not used if is_status_out check removed

                # ... (all your other logic from your function for tyre, interval, laps, etc. - this part should be complete in your actual code) ...
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
                show_gap = not is_p1 and session_type_from_state in [config.SESSION_TYPE_RACE.lower(
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
                reliable_stops = driver_state.get('ReliablePitStops', 0)
                timing_data_stops = driver_state.get('NumberOfPitStops', 0)
                pits_text_to_display = '0'
                if reliable_stops > 0:
                    pits_text_to_display = str(reliable_stops)
                elif timing_data_stops > 0:
                    pits_text_to_display = str(timing_data_stops)
                pit_display_state_for_style = "SHOW_COUNT"
                if is_in_pit_flag:
                    pit_display_state_for_style = "IN_PIT_LIVE"
                    if entry_wall_time:
                        current_wall_time_elapsed = current_time_for_callbacks - entry_wall_time
                        live_game_time_elapsed = current_wall_time_elapsed * speed_at_entry
                        pits_text_to_display = f"In Pit: {live_game_time_elapsed:.1f}s"
                    else:
                        pits_text_to_display = "In Pit"
                elif final_live_pit_text and final_live_pit_text_ts and (current_time_for_callbacks - final_live_pit_text_ts < 15):
                    pits_text_to_display = final_live_pit_text
                    pit_display_state_for_style = "SHOW_COMPLETED_DURATION"

                car_data = driver_state.get('CarData', {})
                speed_val = car_data.get('Speed', '-')
                gear = car_data.get('Gear', '-')
                rpm = car_data.get('RPM', '-')
                drs_val = car_data.get('DRS')
                drs_map = {8: "E", 10: "On", 12: "On", 14: "ON"}
                drs = drs_map.get(
                    drs_val, 'Off') if drs_val is not None else 'Off'

                # IsOverallBestLap_flag: True if this driver's PersonalBestLapTime is the session's overall best.
                is_overall_best_lap_flag = driver_state.get('IsOverallBestLap', False) 
                
                # IsLastLapPersonalBest_flag: True if driver_state['LastLapTime']['PersonalFastest'] is true.
                is_last_lap_personal_best_flag = utils.get_nested_state(driver_state, 'LastLapTime', 'PersonalFastest', default=False)
                
                # IsPersonalBestS1_flag etc.: True if driver_state['Sectors']['0']['PersonalFastest'] is true.
                is_s1_personal_best_flag = utils.get_nested_state(driver_state, 'Sectors', '0', 'PersonalFastest', default=False)
                is_s2_personal_best_flag = utils.get_nested_state(driver_state, 'Sectors', '1', 'PersonalFastest', default=False)
                is_s3_personal_best_flag = utils.get_nested_state(driver_state, 'Sectors', '2', 'PersonalFastest', default=False)

                # IsOverallBestS1_flag etc.: True if this driver's PersonalBestSectorTime for S1 is the session's overall best for S1.
                # These flags (IsOverallBestSector) should already be correctly set in driver_state by _process_timing_data
                is_overall_best_s1_flag = driver_state.get('IsOverallBestSector', [False]*3)[0]
                is_overall_best_s2_flag = driver_state.get('IsOverallBestSector', [False]*3)[1]
                is_overall_best_s3_flag = driver_state.get('IsOverallBestSector', [False]*3)[2]

                # --- NEW: Flags for THIS SPECIFIC EVENT being an overall session best ---
                # These should come directly from the 'OverallFastest' boolean in the LastLapTime/Sectors objects
                is_last_lap_EVENT_overall_best_flag = utils.get_nested_state(driver_state, 'LastLapTime', 'OverallFastest', default=False)
                is_s1_EVENT_overall_best_flag = utils.get_nested_state(driver_state, 'Sectors', '0', 'OverallFastest', default=False)
                is_s2_EVENT_overall_best_flag = utils.get_nested_state(driver_state, 'Sectors', '1', 'OverallFastest', default=False)
                is_s3_EVENT_overall_best_flag = utils.get_nested_state(driver_state, 'Sectors', '2', 'OverallFastest', default=False)

                # === End of your existing row data population ===

                current_driver_highlight_type = "NONE"
                driver_pos_int = -1
                if pos_str != '-':
                    try:
                        driver_pos_int = int(pos_str)
                    except ValueError:
                        pass

                # Apply highlighting with priority, status check removed for GREY_ELIMINATED
                # Priority 1: Q1 eliminated drivers (P16-P20) if in Q2 or Q3
                if current_q_segment_from_state in ["Q2", "SQ2", "Q3", "SQ3"] and \
                   q1_eliminated_highlight_rule["type"] == "GREY_ELIMINATED" and \
                   driver_pos_int != -1 and \
                   q1_eliminated_highlight_rule["lower_pos"] <= driver_pos_int <= q1_eliminated_highlight_rule["upper_pos"]:
                    current_driver_highlight_type = "GREY_ELIMINATED"

                # Priority 2: Q2 eliminated drivers (P11-P15) if in Q3
                if current_driver_highlight_type == "NONE" and \
                   current_q_segment_from_state in ["Q3", "SQ3"] and \
                   q2_eliminated_highlight_rule["type"] == "GREY_ELIMINATED" and \
                   driver_pos_int != -1 and \
                   q2_eliminated_highlight_rule["lower_pos"] <= driver_pos_int <= q2_eliminated_highlight_rule["upper_pos"]:
                    current_driver_highlight_type = "GREY_ELIMINATED"

                # Priority 3: Active segment's primary highlight (Q1 RED, Q2 RED)
                if current_driver_highlight_type == "NONE":
                    # Changed from current_segment_active_highlight
                    if active_segment_highlight_rule["type"] != "NONE":
                        if driver_pos_int != -1 and \
                           active_segment_highlight_rule["lower_pos"] <= driver_pos_int <= active_segment_highlight_rule["upper_pos"]:  # Changed
                            # Changed
                            current_driver_highlight_type = active_segment_highlight_rule["type"]
                        # Changed
                        elif pos_str == '-' and active_segment_highlight_rule["type"] == "RED_DANGER":
                            current_driver_highlight_type = "RED_DANGER"

                row = {
                    'id': car_num, 'No.': racing_no, 'Car': tla, 'Pos': pos, 'Tyre': tyre,
                    'IntervalGap': interval_gap_markdown,
                    'Last Lap': last_lap_val, 'Best Lap': best_lap_val,
                    'S1': s1_val, 'S2': s2_val, 'S3': s3_val, 'Pits': pits_text_to_display,
                    'Status': driver_state.get('Status', 'N/A'),
                    'Speed': speed_val, 'Gear': gear, 'RPM': rpm, 'DRS': drs,
                    # Flags indicating if the DRIVER HOLDS the session record (for "Best Lap" column primarily)
                    'IsOverallBestLap_Str': "TRUE" if is_overall_best_lap_flag else "FALSE",
                    'IsOverallBestS1_Str': "TRUE" if is_overall_best_s1_flag else "FALSE",
                    'IsOverallBestS2_Str': "TRUE" if is_overall_best_s2_flag else "FALSE",
                    'IsOverallBestS3_Str': "TRUE" if is_overall_best_s3_flag else "FALSE",

                    # Flags indicating if THIS SPECIFIC EVENT was a Personal Best for the driver
                    'IsLastLapPersonalBest_Str': "TRUE" if is_last_lap_personal_best_flag else "FALSE",
                    'IsPersonalBestS1_Str': "TRUE" if is_s1_personal_best_flag else "FALSE",
                    'IsPersonalBestS2_Str': "TRUE" if is_s2_personal_best_flag else "FALSE",
                    'IsPersonalBestS3_Str': "TRUE" if is_s3_personal_best_flag else "FALSE",
                    
                    # --- NEW: String versions of EVENT-SPECIFIC Overall Best flags ---
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
            timestamp_text = "Waiting for DriverList..."

        return other_elements, table_data, timestamp_text

    except Exception as e_update:
        logger.error(
            f"Error in update_main_data_displays callback: {e_update}", exc_info=True)
        return no_update, no_update, no_update

@app.callback(
    Output('race-control-log-display', 'value'),
    Input('interval-component-slow', 'n_intervals')
)
def update_race_control_log(n):
    try:
        with app_state.app_state_lock: log_snapshot = list(app_state.race_control_log)
        display_text = "\n".join(log_snapshot)
        # Use constant
        return display_text if display_text else config.TEXT_RC_WAITING
    except Exception as e:
        logger.error(f"Error updating RC log: {e}", exc_info=True)
        return config.TEXT_RC_ERROR # Use constant


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
            app_state.reset_to_default_state() #
            track_map_figure_output = generate_reset_track_map_figure()
            car_positions_store_output = {'status': 'reset_map_display', 'timestamp': time.time()}
            logger.info("Stop & Reset: State reset; track map set to empty; car_positions_store signaled.")
        except Exception as e:
            logger.error(f"Stop & Reset: Error during reset_to_default_state: {e}", exc_info=True); any_action_failed = True

        logger.info("Stop & Reset: Finalizing stop_event and app status...")
        with app_state.app_state_lock:
            current_status = app_state.app_status.get("state")
            logger.info(f"Stop & Reset: State before final stop_event clear: '{current_status}'. Actions failed: {any_action_failed}")
            if app_state.stop_event.is_set(): logger.info("Stop & Reset: Global stop_event is SET. Clearing now."); app_state.stop_event.clear()
            else: logger.info("Stop & Reset: Global stop_event was already clear.")
            if any_action_failed and current_status != "Error":
                logger.warning("Stop & Reset: Forcing app status to 'Error' due to failures.")
                app_state.app_status["state"] = "Error"; app_state.app_status["connection"] = "Reset failed" 
            elif not any_action_failed and current_status != "Idle":
                 logger.info(f"Stop & Reset: Actions succeeded. Current state '{current_status}' (expected Idle). Ensuring Idle.")
                 app_state.app_status["state"] = "Idle"; app_state.app_status["connection"] = config.TEXT_SIGNALR_DISCONNECTED_STATUS # Use constant
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
    Output('driver-details-output', 'children'),
    Output('lap-selector-dropdown', 'options'),
    Output('lap-selector-dropdown', 'value'),
    Output('lap-selector-dropdown', 'disabled'),
    Output('telemetry-graph', 'figure'),
    Input('driver-select-dropdown', 'value'),
    Input('lap-selector-dropdown', 'value'),
    State('telemetry-graph', 'figure'),
    prevent_initial_call=True
)
def display_driver_details(selected_driver_number, selected_lap, current_telemetry_figure):
    ctx = dash.callback_context
    triggered_id = ctx.triggered_id if ctx.triggered else 'N/A'
    logger.debug(f"Telemetry Update: Trigger={triggered_id}, Driver={selected_driver_number}, Lap={selected_lap}")

    # Use constants
    details_children = [html.P(config.TEXT_DRIVER_SELECT, style={'fontSize':'0.8rem', 'padding':'5px'})] #
    lap_options = config.DROPDOWN_NO_LAPS_OPTIONS #
    current_lap_value_for_dropdown = None
    lap_disabled = True

    # Use utils.create_empty_figure_with_message and config constants
    fig_empty_telemetry = utils.create_empty_figure_with_message( #
        config.TELEMETRY_WRAPPER_HEIGHT, config.INITIAL_TELEMETRY_UIREVISION, #
        config.TEXT_DRIVER_SELECT_LAP, config.TELEMETRY_MARGINS_EMPTY #
    )

    if not selected_driver_number:
        if current_telemetry_figure and \
           current_telemetry_figure.get('layout', {}).get('uirevision') == config.INITIAL_TELEMETRY_UIREVISION: #
            return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, no_update
        return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, fig_empty_telemetry

    driver_num_str = str(selected_driver_number)

    with app_state.app_state_lock:
        available_laps = sorted(app_state.telemetry_data.get(driver_num_str, {}).keys())
        driver_info_state = app_state.timing_state.get(driver_num_str, {}).copy()

    if driver_info_state:
        tla = driver_info_state.get('Tla', '?'); num = driver_info_state.get('RacingNumber', driver_num_str)
        name = driver_info_state.get('FullName', 'Unknown'); team = driver_info_state.get('TeamName', '?')
        details_children = [html.H5(f"#{num} {tla} - {name} ({team})", style={'marginTop': '5px', 'marginBottom':'5px', 'fontSize':'0.9rem'})]
        ll = utils.get_nested_state(driver_info_state, 'LastLapTime', 'Value', default='-') #
        bl = utils.get_nested_state(driver_info_state, 'BestLapTime', 'Value', default='-') #
        tyre_str = f"{driver_info_state.get('TyreCompound','-')} ({driver_info_state.get('TyreAge','?')}L)" if driver_info_state.get('TyreCompound','-') != '-' else '-'
        details_children.append(html.P(f"Last: {ll} | Best: {bl} | Tyre: {tyre_str}", style={'fontSize':'0.75rem', 'marginBottom':'0px'}))

    driver_selected_uirevision = f"telemetry_{driver_num_str}_pendinglap"

    if available_laps:
        lap_options = [{'label': f'Lap {l}', 'value': l} for l in available_laps]
        lap_disabled = False
        if triggered_id == 'driver-select-dropdown' or not selected_lap or selected_lap not in available_laps:
            current_lap_value_for_dropdown = available_laps[-1]
        else:
            current_lap_value_for_dropdown = selected_lap
    else:
        # Use constant
        no_laps_message = config.TEXT_DRIVER_NO_LAP_DATA_PREFIX + driver_info_state.get('Tla', driver_num_str) + "." #
        if current_telemetry_figure and \
           current_telemetry_figure.get('layout', {}).get('uirevision') == driver_selected_uirevision and \
           current_telemetry_figure.get('layout',{}).get('annotations',[{}])[0].get('text','') == no_laps_message:
            return details_children, lap_options, None, True, no_update

        # Use utils.create_empty_figure_with_message and config constants
        fig_no_laps = utils.create_empty_figure_with_message( #
            config.TELEMETRY_WRAPPER_HEIGHT, driver_selected_uirevision, no_laps_message, config.TELEMETRY_MARGINS_EMPTY #
        )
        return details_children, lap_options, None, True, fig_no_laps

    if not current_lap_value_for_dropdown:
        # Use constant
        select_lap_message = config.TEXT_DRIVER_SELECT_A_LAP_PREFIX + driver_info_state.get('Tla', driver_num_str) + "." #
        if current_telemetry_figure and \
           current_telemetry_figure.get('layout', {}).get('uirevision') == driver_selected_uirevision and \
           current_telemetry_figure.get('layout',{}).get('annotations',[{}])[0].get('text','') == select_lap_message:
             return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, no_update

        # Use utils.create_empty_figure_with_message and config constants
        fig_select_lap = utils.create_empty_figure_with_message( #
            config.TELEMETRY_WRAPPER_HEIGHT, driver_selected_uirevision, select_lap_message, config.TELEMETRY_MARGINS_EMPTY #
        )
        return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, fig_select_lap

    data_plot_uirevision = f"telemetry_data_{driver_num_str}_{current_lap_value_for_dropdown}"

    if current_telemetry_figure and \
       current_telemetry_figure.get('layout',{}).get('uirevision') == data_plot_uirevision and \
       triggered_id not in ['driver-select-dropdown', 'lap-selector-dropdown']:
        logger.debug("Telemetry already showing correct data, non-user trigger.")
        return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, no_update


    try:
        with app_state.app_state_lock:
            lap_data = app_state.telemetry_data.get(driver_num_str, {}).get(current_lap_value_for_dropdown, {})
        timestamps_str = lap_data.get('Timestamps', [])
        timestamps_dt = [utils.parse_iso_timestamp_safe(ts) for ts in timestamps_str] #
        valid_indices = [i for i, dt_obj in enumerate(timestamps_dt) if dt_obj is not None]

        if valid_indices:
            timestamps_plot = [timestamps_dt[i] for i in valid_indices]
            channels = ['Speed', 'RPM', 'Throttle', 'Brake', 'Gear', 'DRS'] 
            fig_with_data = make_subplots(rows=len(channels), cols=1, shared_xaxes=True,
                                          subplot_titles=[c[:10] for c in channels], vertical_spacing=0.06)
            for i, channel in enumerate(channels):
                y_data_raw = lap_data.get(channel, [])
                y_data_plot = [(y_data_raw[idx] if idx < len(y_data_raw) else None) for idx in valid_indices]
                if channel == 'DRS':
                    drs_plot = [1 if val in [10, 12, 14] else 0 for val in y_data_plot]
                    fig_with_data.add_trace(go.Scattergl(x=timestamps_plot, y=drs_plot, mode='lines', name=channel, line_shape='hv', connectgaps=False), row=i+1, col=1)
                    fig_with_data.update_yaxes(fixedrange=True, tickvals=[0,1], ticktext=['Off','On'], range=[-0.1,1.1], row=i+1, col=1, title_text="", title_standoff=2, title_font_size=9, tickfont_size=8)
                else:
                    fig_with_data.add_trace(go.Scattergl(x=timestamps_plot, y=y_data_plot, mode='lines', name=channel, connectgaps=False), row=i+1, col=1)
                    fig_with_data.update_yaxes(fixedrange=True, row=i+1, col=1, title_text="", title_standoff=2, title_font_size=9, tickfont_size=8)

            fig_with_data.update_layout(
                template='plotly_dark',
                height=config.TELEMETRY_WRAPPER_HEIGHT, # Use constant
                hovermode="x unified",
                showlegend=False,
                margin=config.TELEMETRY_MARGINS_DATA, # Use constant
                title_text=f"<b>{driver_info_state.get('Tla', driver_num_str)} - Lap {current_lap_value_for_dropdown} Telemetry</b>",
                title_x=0.5, title_y=0.98, title_font_size=12,
                uirevision=data_plot_uirevision,
                annotations=[]
            )

            for i, annot in enumerate(fig_with_data.layout.annotations):
                annot.font.size = 9; annot.yanchor = 'bottom'; annot.y = annot.y

            for i_ax in range(len(channels)):
                fig_with_data.update_xaxes(
                    showline=(i_ax == len(channels)-1), zeroline=False,
                    showticklabels=(i_ax == len(channels)-1), row=i_ax+1, col=1,
                    tickfont_size=8
                )

            return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, fig_with_data
        else:
            # Use constant
            fig_empty_telemetry.layout.annotations[0].text = config.TEXT_TELEMETRY_NO_PLOT_DATA_FOR_LAP_PREFIX + str(current_lap_value_for_dropdown) + "." #
            fig_empty_telemetry.layout.uirevision = data_plot_uirevision
            return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, fig_empty_telemetry
    except Exception as plot_err:
        logger.error(f"Error in telemetry plot: {plot_err}", exc_info=True)
        fig_empty_telemetry.layout.annotations[0].text = config.TEXT_TELEMETRY_ERROR # Use constant
        fig_empty_telemetry.layout.uirevision = data_plot_uirevision
        return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, fig_empty_telemetry

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
            return None
        return dash.no_update

    current_session_id = f"{year}_{circuit_key}"

    if current_session_id != existing_session_id_in_store:
        logger.debug(
            f"Updating current-track-layout-cache-key-store to: {current_session_id}")
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

    if current_app_status not in ["Live", "Replaying"] or not timing_state_snapshot:
        return dash.no_update # Or return {'status': 'inactive', 'timestamp': time.time()}

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
        return dash.no_update

    return processed_car_data

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
    [Output('track-map-graph', 'figure', allow_duplicate=True),
     Output('track-map-figure-version-store', 'data')],
    [Input('interval-component-medium', 'n_intervals'), # Periodic check
     Input('current-track-layout-cache-key-store', 'data')], # Triggered when session ID changes
    State('track-map-graph', 'figure'), # Current figure state
    prevent_initial_call=True
)
def initialize_track_map(n_intervals, expected_session_id, current_track_map_figure):
    ctx = dash.callback_context
    triggered_input_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered and ctx.triggered[0] else "Unknown"
    new_figure_version = time.time() 

    logger.debug(f"INIT_TRACK_MAP --- Triggered by: {triggered_input_id}. Expected Session ID: '{expected_session_id}'")
    current_fig_uirevision = current_track_map_figure.get('layout', {}).get('uirevision') if current_track_map_figure and current_track_map_figure.get('layout') else 'None'
    logger.debug(f"INIT_TRACK_MAP --- Current Figure Uirevision in State: '{current_fig_uirevision}'")

    with app_state.app_state_lock:
        cached_data = app_state.track_coordinates_cache.copy()
        driver_list_snapshot = app_state.timing_state.copy() # For adding car markers initially
        logger.debug(f"INIT_TRACK_MAP --- AppState session_details.SessionKey: '{app_state.session_details.get('SessionKey')}'")
        logger.debug(f"INIT_TRACK_MAP --- AppState track_coordinates_cache.session_key: '{cached_data.get('session_key')}'")

    if not expected_session_id or not isinstance(expected_session_id, str) or '_' not in expected_session_id:
        empty_map_uirevision = f"empty_map_undefined_session_{new_figure_version}" 
        
        if current_fig_uirevision == empty_map_uirevision:
             logger.debug(f"INIT_TRACK_MAP --- Returning NO_UPDATE for figure & version (already showing specific empty map: {empty_map_uirevision})")
             return no_update, no_update

        fig_empty = utils.create_empty_figure_with_message( #
            config.TRACK_MAP_WRAPPER_HEIGHT, empty_map_uirevision, #
            config.TEXT_TRACK_MAP_DATA_WILL_LOAD, config.TRACK_MAP_MARGINS #
        )
        fig_empty.layout.plot_bgcolor = 'rgb(30,30,30)'; fig_empty.layout.paper_bgcolor = 'rgba(0,0,0,0)'
        logger.debug(f"INIT_TRACK_MAP --- Returning EMPTY map (no valid expected_session_id: '{expected_session_id}'). New uirev: {empty_map_uirevision}")
        return fig_empty, new_figure_version

    data_loaded_uirevision = f"tracklayout_{expected_session_id}"
    loading_uirevision = f"loading_{expected_session_id}_{new_figure_version}" 

    logger.debug(f"INIT_TRACK_MAP --- For SID '{expected_session_id}': TargetLoadedUirev='{data_loaded_uirevision}', TargetLoadingUirev='{loading_uirevision}'")

    is_cache_ready_for_session = (
        cached_data.get('session_key') == expected_session_id and
        cached_data.get('x') and cached_data.get('y')
    )

    if is_cache_ready_for_session:
        if current_fig_uirevision == data_loaded_uirevision:
            logger.debug(f"INIT_TRACK_MAP --- Returning NO_UPDATE for figure & version (map already displays correct loaded uirev: {data_loaded_uirevision})")
            return no_update, no_update
        
        logger.debug(f"INIT_TRACK_MAP --- Cache HIT for '{expected_session_id}'. Drawing track. New uirev: {data_loaded_uirevision}")
        fig_data = [
            go.Scatter(x=list(cached_data['x']), y=list(cached_data['y']), mode='lines',
                       line=dict(color='grey', width=2), name='Track', hoverinfo='none')
        ]
        # Add placeholders for car markers - clientside will update positions
        for car_num, driver_state in driver_list_snapshot.items():
            tla = driver_state.get('Tla', car_num)
            team_color = driver_state.get('TeamColour', '808080')
            if not isinstance(team_color, str) or not team_color.startswith('#'):
                team_color = '#' + str(team_color).replace("#","") if isinstance(team_color, str) else '#808080'
                if len(team_color) not in [4, 7]: team_color = '#808080' # Basic validation for hex
            
            fig_data.append(go.Scatter(
                x=[], y=[], # Positions will be updated by clientside
                mode='markers+text', name=tla, uid=str(car_num), # UID for clientside to find trace
                marker=dict(size=8, color=team_color, line=dict(width=1, color='Black')),
                textfont=dict(size=8, color='white'), textposition='middle right',
                hoverinfo='text', # Show TLA on hover
                text=tla 
            ))
        fig_layout = go.Layout(
            template='plotly_dark', uirevision=data_loaded_uirevision, # CRITICAL for clientside updates
            xaxis=dict(visible=False, fixedrange=True, range=cached_data.get('range_x'), autorange=False if cached_data.get('range_x') else True),
            yaxis=dict(visible=False, fixedrange=True, scaleanchor="x", scaleratio=1, range=cached_data.get('range_y'), autorange=False if cached_data.get('range_y') else True),
            showlegend=False, plot_bgcolor='rgb(30,30,30)', paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'), margin=config.TRACK_MAP_MARGINS, height=config.TRACK_MAP_WRAPPER_HEIGHT, #
            annotations=[] # Clear any previous "Loading..." messages
        )
        new_figure = go.Figure(data=fig_data, layout=fig_layout)
        logger.debug(f"INIT_TRACK_MAP --- Returning NEW TRACK figure for '{expected_session_id}'. Final uirev: {new_figure.layout.uirevision}")
        return new_figure, new_figure_version 
    else: 
        if current_fig_uirevision == loading_uirevision.rsplit('_',1)[0]: # Compare against base loading uirevision
            logger.debug(f"INIT_TRACK_MAP --- Returning NO_UPDATE for figure & version (map already shows correct loading uirev prefix: {loading_uirevision.rsplit('_',1)[0]} for {expected_session_id})")
            return no_update, no_update

        fig_loading_specific = utils.create_empty_figure_with_message( #
            config.TRACK_MAP_WRAPPER_HEIGHT, loading_uirevision, #
            f"{config.TEXT_TRACK_MAP_LOADING_FOR_SESSION_PREFIX}{expected_session_id}...", #
            config.TRACK_MAP_MARGINS #
        )
        fig_loading_specific.layout.plot_bgcolor = 'rgb(30,30,30)'; fig_loading_specific.layout.paper_bgcolor = 'rgba(0,0,0,0)'
        logger.debug(f"INIT_TRACK_MAP --- Cache MISS for '{expected_session_id}'. Returning LOADING map. New uirev: {loading_uirevision}")
        return fig_loading_specific, new_figure_version

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
    Input('lap-time-driver-selector', 'value'),
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

print("Callback definitions processed") #
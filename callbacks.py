# callbacks.py
"""
Contains all the Dash callback functions for the application.
"""

import logging
import json
import os
import time
import datetime
import threading
from datetime import timezone # Ensure timezone is imported if used directly

import dash
from dash.dependencies import Input, Output, State
# Import specific components if type checking used, usually not needed just for callbacks
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import numpy as np
import requests # For track map API call

# Import the app object (defined in main.py)
# This assumes main.py defines 'app = Dash(...)' before importing callbacks
try:
    from main import app
except ImportError:
    # Alternative if app is defined elsewhere, e.g., app_instance.py
    # from app_instance import app
     # Fallback for potential partial runs - likely needs fixing if this happens
    print("ERROR: Could not import 'app' for callbacks. Define app in main.py before importing callbacks.")
    # You might need to raise an error or exit here depending on structure
    raise

# Import shared state and lock
import app_state

# Import configuration
import config

# Import utility functions
import utils

# Import functions from other modules called by callbacks
import signalr_client
import replay
import data_processing # Only if directly calling processing helpers? Unlikely needed.

# Get logger
main_logger = logging.getLogger("F1App.Callbacks")
logger = logging.getLogger("F1App.Callbacks") # Use logger consistent with main app setup

# --- Callback Definitions ---

@app.callback(
    Output('status-display', 'children'),
    Output('heartbeat-display', 'children'),
    Output('track-status-display', 'children'),
    Output('live-data-display', 'children'),
    Output('timing-data-actual-table', 'data'),
    Output('timing-data-timestamp', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_output(n):
    # Initialize outputs outside lock
    status_text = "State: Unknown"; heartbeat_text = "Last HB: N/A"; track_display_text = "Track: Unknown"
    other_data_display = []; table_data = []; timing_timestamp_text = "Waiting for TimingData..."
    try: # Add top-level try-except
        with app_state.app_state_lock: # Acquire lock for ALL shared data reads
            # Status, Heartbeat, Track Status
            status_text = f"State: {app_state.app_status['state']} | Conn: {app_state.app_status['connection']}"
            heartbeat_text = f"Last HB: {app_state.app_status['last_heartbeat'] or 'N/A'}"
            track_status_code = app_state.track_status_data.get('Status', '0') # Use app_state
            track_status_map = {'1':"Clear",'2':"Yellow",'3':"Flag",'4':"SC",'5':"Red",'6':"VSC",'7':"VSC End"} # Compact map
            track_display_text = f"Track: {track_status_map.get(track_status_code, f'? ({track_status_code})')}"

            # Other Data Display (Reads app_state.data_store)
            sorted_streams = sorted([s for s in app_state.data_store.keys() if s not in ['TimingData', 'DriverList', 'Position.z', 'CarData.z','Position']]) # Added Position
            for stream in sorted_streams:
                value = app_state.data_store.get(stream, {})
                data_str = json.dumps(value.get('data', 'N/A'), indent=2)
                timestamp_str = value.get('timestamp', 'N/A')
                if len(data_str) > 500: data_str = data_str[:500] + "\n... (truncated)"
                other_data_display.append(html.Details([ # Needs 'from dash import html'
                    html.Summary(f"{stream} ({timestamp_str})"),
                    html.Pre(data_str, style={'marginLeft': '15px', 'maxHeight': '200px', 'overflowY': 'auto'})
                ], style={'marginBottom': '5px'}, open=(stream=="SessionInfo")))

            # Timing Table Timestamp (Reads app_state.data_store, app_state.timing_state)
            if 'TimingData' in app_state.data_store:
                timing_timestamp_text = f"Timing TS: {app_state.data_store['TimingData'].get('timestamp', 'N/A')}"
            elif not app_state.timing_state:
                timing_timestamp_text = "Waiting for DriverList..."

            # Generate Timing Table Data (Reads app_state.timing_state)
            if app_state.timing_state:
                processed_table_data = []
                sorted_driver_numbers = sorted(app_state.timing_state.keys(), key=lambda x: int(x) if x.isdigit() else float('inf'))

                for car_num in sorted_driver_numbers:
                    driver_state = app_state.timing_state.get(car_num)
                    if not driver_state: continue
                    tyre_compound = driver_state.get('TyreCompound', '-')
                    tyre_age = driver_state.get('TyreAge', '?')
                    tyre_display = f"{tyre_compound} ({tyre_age}L)" if tyre_compound != '-' else '-'
                    tla = driver_state.get("Tla")
                    car_display_value = tla if tla and tla != "N/A" else car_num
                    car_data = driver_state.get('CarData', {})
                    row = {
                        'Car': car_display_value, 'Pos': driver_state.get('Position', '-'), "Tyre": tyre_display,
                        'Time': driver_state.get('Time', '-'), 'Gap': driver_state.get('GapToLeader', '-'),
                        'Interval': utils.get_nested_state(driver_state, 'IntervalToPositionAhead', 'Value', default='-'), # Use utils
                        'Last Lap': utils.get_nested_state(driver_state, 'LastLapTime', 'Value', default='-'), # Use utils
                        'Best Lap': utils.get_nested_state(driver_state, 'BestLapTime', 'Value', default='-'), # Use utils
                        'S1': utils.get_nested_state(driver_state, 'Sectors', '0', 'Value', default='-'), 'S2': utils.get_nested_state(driver_state, 'Sectors', '1', 'Value', default='-'), 'S3': utils.get_nested_state(driver_state, 'Sectors', '2', 'Value', default='-'), # Use utils
                        'Status': driver_state.get('Status', 'N/A'),
                        'Speed': car_data.get('Speed', '-'), 'Gear': car_data.get('Gear', '-'), 'RPM': car_data.get('RPM', '-'),
                        'DRS': {8:"Eligible",10:"On",12:"On",14:"ON"}.get(car_data.get('DRS'), 'Off'),
                    }
                    processed_table_data.append(row)
                processed_table_data.sort(key=utils.pos_sort_key) # Use utils
                table_data = processed_table_data

        # Return all outputs
        return (status_text, heartbeat_text, track_display_text,
                other_data_display, table_data, timing_timestamp_text)

    except Exception as e_update: # Catch any error within the callback
        main_logger.error(f"!!! ERROR in update_output callback: {e_update}", exc_info=True)
        # Return NoUpdate for all outputs to prevent Dash error popup
        no_update = dash.no_update
        return no_update, no_update, no_update, no_update, no_update, no_update


@app.callback(
    Output('start-button', 'disabled'), Output('stop-button', 'disabled'),
    Output('replay-button', 'disabled'), Output('replay-file-dropdown', 'disabled'), # Changed ID from replay-file-input
    Output('replay-speed-input', 'disabled'),
    Input('interval-component', 'n_intervals')
)
def update_button_states(n):
    with app_state.app_state_lock: state = app_state.app_status['state']
    is_idle = state in ["Idle", "Stopped", "Error", "Playback Complete"]
    is_running = state in ["Connecting", "Live", "Replaying", "Initializing"]
    is_stopping = state == "Stopping"
    start_disabled = is_running or is_stopping
    replay_disabled = is_running or is_stopping # Can't start replay if live/connecting
    stop_disabled = is_idle # Can only stop if running/connecting/replaying
    # Disable replay inputs if live/connecting/replaying/stopping
    input_disabled = is_running or is_stopping
    return start_disabled, stop_disabled, replay_disabled, input_disabled, input_disabled

@app.callback(
    Output('replay-file-dropdown', 'options'),
    Input('refresh-replay-list-button', 'n_clicks'),
    prevent_initial_call=True
)
def refresh_replay_files(n_clicks):
    if n_clicks > 0:
        main_logger.info("Refreshing replay file list...")
        # Use replay module function and config constant
        new_options = replay.get_replay_files(config.REPLAY_DIR)
        return new_options
    return dash.no_update

@app.callback(
    Output('race-control-log-display', 'value'),
    Input('interval-component', 'n_intervals')
)
def update_race_control_log(n):
    # log_snapshot = [] # Defined later
    try:
        with app_state.app_state_lock:
            log_snapshot = list(app_state.race_control_log) # Read from app_state

        display_text = "\n".join(reversed(log_snapshot))

        if not display_text: return "Waiting for Race Control messages..." # Simpler check
        # elif not display_text: return "Waiting for Race Control messages..."

        return display_text

    except Exception as e:
        main_logger.error(f"Error in update_race_control_log: {e}", exc_info=True)
        return "Error updating Race Control log."


@app.callback(
    Output('session-info-display', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_session_info_display(n):
    session_info_str = "Session: N/A"
    weather_string = "Weather: N/A"
    try:
        with app_state.app_state_lock:
            # Read SessionInfo data directly from app_state.session_details
            # Assuming _process_session_info correctly populates app_state.session_details
            local_session_details = app_state.session_details.copy() # Work with a copy

            # Read Weather data directly from app_state.data_store
            local_weather_data = app_state.data_store.get('WeatherData', {}).get('data', {})
            if not isinstance(local_weather_data, dict): local_weather_data = {}

        # Format Session Details
        meeting_name = local_session_details.get('Meeting', {}).get('Name', '?')
        session_name = local_session_details.get('Name', '?')
        circuit_name = local_session_details.get('Circuit', {}).get('ShortName', '?')
        country_name = local_session_details.get('Country', {}).get('Name', '')
        start_time_str = local_session_details.get('StartDate', '')

        session_info_parts = []
        if circuit_name != '?': session_info_parts.append(f"{circuit_name}")
        if country_name: session_info_parts.append(f"({country_name})")
        if meeting_name != '?': session_info_parts.append(f"Event: {meeting_name}")
        if session_name != '?': session_info_parts.append(f"Session: {session_name}")
        if start_time_str: session_info_parts.append(f"Starts: {start_time_str}")
        if session_info_parts: session_info_str = " | ".join(session_info_parts)

        # Format Weather Details
        weather_elements = []
        air_temp = local_weather_data.get('AirTemp'); track_temp = local_weather_data.get('TrackTemp')
        humidity = local_weather_data.get('Humidity'); pressure = local_weather_data.get('Pressure')
        wind_speed = local_weather_data.get('WindSpeed'); wind_dir = local_weather_data.get('WindDirection')
        rainfall = local_weather_data.get('Rainfall')

        if air_temp is not None: weather_elements.append(f"Air: {air_temp}°C")
        if track_temp is not None: weather_elements.append(f"Track: {track_temp}°C")
        if humidity is not None: weather_elements.append(f"Hum: {humidity}%")
        if pressure is not None: weather_elements.append(f"Press: {pressure} hPa")
        if wind_speed is not None:
             wind_str = f"Wind: {wind_speed} m/s";
             if wind_dir is not None: wind_str += f" ({wind_dir}°)"
             weather_elements.append(wind_str)
        if rainfall is not None and str(rainfall) == '1': weather_elements.append("RAIN")
        if weather_elements: weather_string = " | ".join(weather_elements)

        # Combine Output
        combined_info = dbc.Row([ # Needs import
             dbc.Col(session_info_str, width="auto", style={'paddingRight': '15px'}),
             dbc.Col(weather_string, width="auto")
        ], justify="start", className="ms-1")

        return combined_info

    except Exception as e:
        main_logger.error(f"Error in update_session_info_display callback: {e}", exc_info=True)
        return "Error loading session info..."


@app.callback(
    # Use list for multiple outputs for clarity
    [Output('status-display', 'children', allow_duplicate=True),
     Output('start-button', 'n_clicks')], # Reset clicks to prevent re-trigger if user double clicks? No, causes loop.
    Input('start-button', 'n_clicks'),
    prevent_initial_call=True
)
def start_live_callback(n_clicks):
    # This callback expects TWO return values based on the Outputs defined above.

    if n_clicks is None or n_clicks == 0:
        # --- Fix: Return TWO values ---
        return dash.no_update, dash.no_update

    # Use local logger
    callback_logger = logging.getLogger("F1App.Callbacks") # Ensure logger is defined/imported
    callback_logger.info(f"Start Live clicked (n={n_clicks}). Checking state...")

    with app_state.app_state_lock: # Ensure app_state is imported
        current_state = app_state.app_status["state"]
        should_record = app_state.record_live_data
        if current_state in ["Connecting", "Live", "Replaying", "Initializing", "Stopping"]:
            callback_logger.warning(f"Start Live clicked but already active/stopping (State: {current_state}). No action.")
            conn_status = app_state.app_status.get("connection", "N/A")
            status_msg = f"State: {current_state} | Conn: {conn_status}"
            # --- Fix: Return TWO values ---
            return status_msg, dash.no_update

    callback_logger.info(f"Initiating connection sequence... Recording: {should_record}")

    # --- Manual Negotiation ---
    websocket_url, ws_headers = None, None
    try:
        with app_state.app_state_lock:
             app_state.app_status.update({"state": "Initializing", "connection": "Negotiating..."})
             app_state.stop_event.clear() # Ensure stop_event is in app_state

        # Call function from signalr_client module (ensure imported)
        # Ensure config is imported for config.NEGOTIATE_URL_BASE etc.
        websocket_url, ws_headers = signalr_client.build_connection_url(
            config.NEGOTIATE_URL_BASE, config.HUB_NAME
        )
        if not websocket_url or not ws_headers:
             # Ensure ConnectionError is defined or imported
             raise ConnectionError("Negotiation failed to return URL or Headers.")

    except Exception as e:
        callback_logger.error(f"Error during negotiation/setup: {e}", exc_info=True)
        with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Negotiation Failed"})
        status_msg = f"State: Error | Conn: Negotiation Failed"
        # --- Fix: Return TWO values ---
        return status_msg, dash.no_update

    # --- Start Connection Thread ---
    if websocket_url and ws_headers:
        file_init_ok = True # This flag seems unused later? Remove if not needed.
        if should_record:
            callback_logger.info("Recording enabled, initializing live file/log handler...")
            # Call function from replay module (ensure imported)
            if not replay.init_live_file():
                callback_logger.error("Failed to initialize live data file/handler. Proceeding without recording.")
                # file_init_ok = False # Flag still unused
        else:
            callback_logger.info("Recording disabled, skipping file/log handler initialization.")

        callback_logger.info("Starting connection thread...")
        # Ensure threading is imported
        thread_obj = threading.Thread(
            target=signalr_client.run_connection_manual_neg, # Ensure this target fn exists
            args=(websocket_url, ws_headers),
            name="SignalRConnectionThread",
            daemon=True)

        # Store thread object reference (ensure signalr_client handles this if needed)
        # signalr_client.connection_thread = thread_obj # Or manage thread ref elsewhere

        thread_obj.start()
        callback_logger.info("Connection thread started.")
        status_msg = f"State: Connecting | Conn: Socket Connecting"
        # --- Fix: Return TWO values ---
        return status_msg, dash.no_update

    else: # Should not happen if negotiation check above worked
         callback_logger.error("Internal Error: URL/Headers missing after negotiation success check.")
         with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Internal Setup Error"})
         status_msg = f"State: Error | Conn: Internal Setup Error"
         # --- Fix: Return TWO values ---
         return status_msg, dash.no_update


@app.callback(
    Output('status-display', 'children', allow_duplicate=True),
    Input('stop-button', 'n_clicks'),
    prevent_initial_call=True
)
def handle_stop_button(n_clicks):
    if n_clicks is None or n_clicks == 0: return dash.no_update

    callback_logger = logging.getLogger("F1App.Callbacks")
    callback_logger.info(f"Stop clicked (n={n_clicks}). Determining action...")

    with app_state.app_state_lock: state = app_state.app_status['state']
    triggered_stop_func = None

    # Check which system is active (replay or live)
    if state in ["Replaying", "Initializing", "Playback Complete"]: # Include Initializing/Complete if replay thread might exist
        callback_logger.info("Stopping Replay...")
        replay.stop_replay() # Call function from replay module
        triggered_stop_func = "Replay"
    elif state in ["Live", "Connecting"]: # Include Connecting state
        callback_logger.info("Stopping Live Connection...")
        signalr_client.stop_connection() # Call function from signalr_client module
        triggered_stop_func = "Live Connection"
    elif state == "Stopping":
        callback_logger.info("Stop clicked while already stopping.")
    else: # Idle, Stopped, Error
        callback_logger.warning(f"Stop clicked in inactive state '{state}'. Setting stop event just in case.")
        if not app_state.stop_event.is_set(): app_state.stop_event.set()

    # Read the state *after* the stop function call
    with app_state.app_state_lock: new_state = app_state.app_status['state']; new_conn = app_state.app_status['connection']

    if triggered_stop_func: return f"Stopped {triggered_stop_func}. New: {new_state} | {new_conn}"
    else: return f"Stop ignored/redundant ('{state}'). Current: {new_state} | {new_conn}"


@app.callback(
    Output('status-display', 'children', allow_duplicate=True),
    Input('replay-button', 'n_clicks'),
    State('replay-file-dropdown', 'value'),
    State('replay-speed-input', 'value'),
    prevent_initial_call=True
)
def handle_replay_button(n_clicks, selected_file, speed_val):
    if n_clicks is None or n_clicks == 0: return dash.no_update

    callback_logger = logging.getLogger("F1App.Callbacks")
    callback_logger.info(f"Replay clicked (n={n_clicks})")

    if not selected_file:
        callback_logger.warning("Replay button clicked but no file selected.")
        with app_state.app_state_lock: # Get current state to display message
             return f"State: {app_state.app_status['state']} | Conn: {app_state.app_status['connection']} (No file)"

    try: speed = float(speed_val) if speed_val is not None else 1.0; speed = max(0.1, speed) # Min speed 0.1
    except (ValueError, TypeError): callback_logger.warning(f"Invalid speed '{speed_val}', default 1.0"); speed = 1.0

    # Use config for directory
    replay_file_path = os.path.join(config.REPLAY_DIR, selected_file)
    callback_logger.info(f"Attempting replay: {replay_file_path}, speed: {speed}")

    if not os.path.exists(replay_file_path):
        callback_logger.error(f"Replay file not found: {replay_file_path}")
        with app_state.app_state_lock:
            app_state.app_status.update({"state": "Error", "connection": f"File Not Found"})
            return f"State: Error | Conn: File Not Found: {selected_file}"

    # Call function from replay module
    replay.replay_from_file(replay_file_path, speed)

    # Read state after starting replay
    with app_state.app_state_lock:
        return f"State: {app_state.app_status['state']} | Conn: {app_state.app_status['connection']}"

@app.callback(
    Output('record-data-checkbox', 'value'),
    Input('record-data-checkbox', 'value'),
    prevent_initial_call=True
)
def record_checkbox_callback(checked_value):
    if checked_value is None: return dash.no_update # Should have initial value from layout

    callback_logger = logging.getLogger("F1App.Callbacks")
    new_state = bool(checked_value)

    with app_state.app_state_lock:
        app_state.record_live_data = new_state
        current_app_state = app_state.app_status["state"]
        is_file_setup_active = app_state.is_saving_active # Check if handler/file is setup

        callback_logger.info(f"Record Live Data set to: {new_state}. Current App State: {current_app_state}")

        # If live and recording toggled OFF, close/remove handler
        if current_app_state == "Live" and not new_state and is_file_setup_active:
            callback_logger.info("Recording toggled off during live session, closing file/handler.")
            replay.close_live_file() # Call function from replay module

        # Starting recording while live? init_live_file is only called by start_live_callback currently.
        # If user checks box while live, recording won't start until feed restarts. This seems acceptable.

    return new_state


@app.callback(
    Output('track-map-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_track_map(n):
    """Updates the track map using fetched track layout and live car positions."""
    start_time_map = time.time()
    logger.debug(f"--- update_track_map Tick {n} Start ---")

    # --- Step 1: Determine Session Key and Check Cache Match ---
    current_session_key = None
    needs_api_fetch = False
    year_from_state = None
    circuit_key_from_state = None

    with app_state.app_state_lock:
        # Read session_details from app_state
        # Use .get() defensively
        year_from_state = app_state.session_details.get('Year') # Assuming Year is directly available
        circuit_key_from_state = app_state.session_details.get('CircuitKey') # Assuming CircuitKey is available

        # Fallback if Year/CircuitKey missing (e.g., use Meeting Key / Path from SessionInfo)
        if not year_from_state or not circuit_key_from_state:
             session_info_local = app_state.data_store.get('SessionInfo', {}).get('data',{})
             if isinstance(session_info_local, dict):
                 if not circuit_key_from_state:
                     circuit_key_from_state = session_info_local.get('Circuit', {}).get('Key')
                 if not year_from_state:
                      path = session_info_local.get('Path', '')
                      if path:
                           parts = path.split('/')
                           if len(parts) > 1 and parts[1].isdigit() and len(parts[1]) == 4:
                               year_from_state = parts[1]
                      # Less reliable fallback: Meeting Key might contain year info
                      # elif not year_from_state: year_from_state = session_info_local.get('Meeting', {}).get('Key')

        if year_from_state and circuit_key_from_state:
             current_session_key = f"{year_from_state}_{circuit_key_from_state}"

        # Read cache session key from app_state.track_coordinates_cache
        cached_session_key = app_state.track_coordinates_cache.get('session_key')

    # Decide if fetch needed (outside lock)
    if current_session_key and cached_session_key != current_session_key:
        needs_api_fetch = True
        logger.debug(f"Map Check: Cache miss/new session (Current: {current_session_key}, Cached: {cached_session_key}). Need API fetch.")
    elif not current_session_key:
        logger.debug("Map Check: Could not determine current session key from app_state.")
    else:
        logger.debug(f"Map Check: Cache hit for session key {current_session_key}.")

    # --- Step 2: Fetch API data if needed (OUTSIDE lock) ---
    api_data = None # Holds extracted data from API if successful
    if needs_api_fetch and current_session_key: # Ensure we have key components
        # Use derived year and circuit key
        api_url = f"https://api.multiviewer.app/api/v1/circuits/{circuit_key_from_state}/{year_from_state}"
        logger.info(f"Attempting Track API fetch: {api_url}")
        try:
            # Use a reasonable User-Agent
            headers = { 'User-Agent': 'F1-Dashboard-App/0.2 (Python)' }
            response = requests.get(api_url, headers=headers, timeout=10)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            map_api_data = response.json()

            # Extract data into a temporary dictionary on success
            extracted_data = {}
            temp_track_x = map_api_data.get('x'); temp_track_y = map_api_data.get('y')
            if isinstance(temp_track_x, list) and isinstance(temp_track_y, list):
                 try:
                     extracted_data['x'] = [float(p) for p in temp_track_x]
                     extracted_data['y'] = [float(p) for p in temp_track_y]
                 except (ValueError, TypeError) as e:
                     logger.warning(f"Could not convert track coordinates to float: {e}")
                     extracted_data['x'] = None; extracted_data['y'] = None # Mark as invalid
            else:
                 extracted_data['x'] = None; extracted_data['y'] = None

            # Extract corners (optional, add robustness)
            corners_raw = map_api_data.get('corners')
            if isinstance(corners_raw, list):
                 corner_x_temp, corner_y_temp = [], []
                 for corner in corners_raw:
                      if isinstance(corner, dict):
                           pos = corner.get('trackPosition', {})
                           cx, cy = pos.get('x'), pos.get('y')
                           if cx is not None and cy is not None:
                                try: corner_x_temp.append(float(cx)); corner_y_temp.append(float(cy))
                                except (ValueError, TypeError): pass # Ignore bad corner coords
                 if corner_x_temp: extracted_data['corner_x'] = corner_x_temp; extracted_data['corner_y'] = corner_y_temp

            # Extract rotation
            try: extracted_data['rotation'] = float(map_api_data.get('rotation', 0.0))
            except (ValueError, TypeError): extracted_data['rotation'] = 0.0

            # Calculate ranges only if coordinates are valid
            local_x_range, local_y_range = None, None
            if extracted_data.get('x') and extracted_data.get('y'): # Check if lists are valid floats
                try:
                    x_min, x_max = np.min(extracted_data['x']), np.max(extracted_data['x'])
                    y_min, y_max = np.min(extracted_data['y']), np.max(extracted_data['y'])
                    padding_x = (x_max - x_min) * 0.05; padding_y = (y_max - y_min) * 0.05
                    local_x_range = [x_min - padding_x, x_max + padding_x]
                    local_y_range = [y_min - padding_y, y_max + padding_y]
                except Exception as range_err: logger.error(f"Error calculating track range: {range_err}")
            extracted_data['range_x'] = local_x_range; extracted_data['range_y'] = local_y_range

            # Assign only if we successfully extracted valid track x/y
            if extracted_data.get('x') and extracted_data.get('y'):
                api_data = extracted_data
                logger.info(f"Track API fetch SUCCESS for {current_session_key}")
            else:
                logger.warning(f"Track API fetch for {current_session_key} did not yield valid x/y coordinates.")
                api_data = None # Ensure None if essential parts failed

        except requests.exceptions.RequestException as e: logger.error(f"Track API fetch FAILED for {current_session_key}: {e}"); api_data = None
        except json.JSONDecodeError as e: logger.error(f"Track API fetch FAILED for {current_session_key}: Invalid JSON. {e}"); api_data = None
        except Exception as e: logger.error(f"Unexpected error during Track API fetch/process: {e}", exc_info=True); api_data = None

    # --- Step 3: Acquire lock ONCE to update cache & read ALL data needed for plot ---
    track_x, track_y, x_range, y_range = None, None, None, None
    drivers_x, drivers_y, drivers_text, drivers_color, drivers_opacity = [], [], [], [], []
    plot_session_key = None # Store the key used for this plot's data

    with app_state.app_state_lock:
        # A. Update cache in app_state if fetch was attempted
        if needs_api_fetch:
            if api_data: # API call succeeded and valid data extracted
                # Update the dictionary within app_state
                app_state.track_coordinates_cache = {**api_data, 'session_key': current_session_key}
                logger.debug("Cache updated with NEW API data.")
            else: # API call failed or extraction failed
                # Update key but clear coordinates/ranges to prevent using stale data
                app_state.track_coordinates_cache['session_key'] = current_session_key
                app_state.track_coordinates_cache['x'] = None
                app_state.track_coordinates_cache['y'] = None
                app_state.track_coordinates_cache['range_x'] = None
                app_state.track_coordinates_cache['range_y'] = None
                # Optionally clear corners/rotation too
                logger.debug("Cache session_key updated for FAILED/Invalid API fetch (coords cleared).")

        # B. Read necessary data for plotting from current app_state
        # Read track layout from cache
        plot_session_key = app_state.track_coordinates_cache.get('session_key') # Use the key actually in cache now
        # Only use track data if the cached key matches the session we expect
        if plot_session_key == current_session_key:
            track_x = app_state.track_coordinates_cache.get('x')
            track_y = app_state.track_coordinates_cache.get('y')
            x_range = app_state.track_coordinates_cache.get('range_x')
            y_range = app_state.track_coordinates_cache.get('range_y')
        else:
             logger.warning(f"Plotting skipped track data due to session key mismatch (Current: {current_session_key}, Plotting for Cache: {plot_session_key})")


        # Read car positions from app_state.timing_state
        # Assuming data_processing merges PositionData (X, Y) into timing_state[driver_num]
        timing_state_snapshot = app_state.timing_state.copy() # Work on a copy

    # --- Process Car Data (Outside Lock) ---
    for car_num, driver_state in timing_state_snapshot.items():
        # Use .get() defensively for nested data
        # Position data might be under 'PositionData' or directly at top level
        pos_data = driver_state.get('PositionData', driver_state) # Check both places

        x_val = pos_data.get('X')
        y_val = pos_data.get('Y')
        status_string = driver_state.get('Status', '').lower() # Status from timing data
        tla = driver_state.get('Tla', car_num) # TLA from timing data (DriverList)
        team_color = driver_state.get('TeamColour', '808080') # Default grey if missing

        # Determine visibility/opacity based on status
        is_off_main_track = ('pit' in status_string or 'retired' in status_string or 'out' in status_string or 'stopped' in status_string)

        if x_val is not None and y_val is not None:
             try:
                 x_coord = float(x_val)
                 y_coord = float(y_val)
                 drivers_x.append(x_coord)
                 drivers_y.append(y_coord)
                 drivers_text.append("" if is_off_main_track else tla) # Hide label if off track
                 drivers_color.append(f"#{team_color}")
                 drivers_opacity.append(0.3 if is_off_main_track else 1.0) # Dim if off track
             except (ValueError, TypeError) as coord_err:
                  logger.warning(f"Could not convert position data for car {car_num}: X={x_val}, Y={y_val} - Error: {coord_err}")
        # else: logger.debug(f"Car {car_num} skipped, no valid X/Y found.")


    # --- Step 4: Create Plotly Figure (Outside lock) ---
    figure_data = []
    logger.debug(f"Plotting Map: Session='{plot_session_key}', HasTrack={track_x is not None and track_y is not None}, HasCars={len(drivers_x)>0}, XRange={x_range is not None}")

    try:
        # Add Track Outline Trace if valid data exists
        if track_x and track_y:
            figure_data.append(go.Scatter(
                x=track_x, y=track_y, mode='lines',
                line=dict(color='grey', width=2),
                name='Track', showlegend=False, hoverinfo='none'
            ))

        # Add Car Trace if valid data exists
        if drivers_x:
             figure_data.append(go.Scatter(
                 x=drivers_x, y=drivers_y, mode='markers+text',
                 marker=dict(size=10, color=drivers_color, line=dict(width=1, color='Black'), opacity=drivers_opacity),
                 text=drivers_text, textposition='middle right', name='Cars',
                 hoverinfo='text', # Shows TLA on hover if text is set
                 showlegend=False,
                 textfont=dict(size=9, color='white') # Adjust styling as needed
             ))

        # Define Layout
        xaxis_config = dict(showgrid=False, zeroline=False, showticklabels=False, range=x_range)
        yaxis_config = dict(showgrid=False, zeroline=False, showticklabels=False, range=y_range)
        # Ensure aspect ratio is 1:1 if we have ranges for the track
        if x_range and y_range:
            yaxis_config['scaleanchor'] = "x"
            yaxis_config['scaleratio'] = 1

        layout = go.Layout(
            xaxis=xaxis_config,
            yaxis=yaxis_config,
            showlegend=False, margin=dict(l=5, r=5, t=20, b=5), # Added top margin for title
            # Use the session key consistent with the *track data* for uirevision
            # This prevents map reset when only cars move, but forces redraw if track changes
            uirevision=plot_session_key, # Use the key from the cache that provided track data
            plot_bgcolor='rgb(30,30,30)', paper_bgcolor='rgba(0,0,0,0)', # Darker plot bg
            font=dict(color='white')
        )

        # Create Final Figure or Empty Placeholder
        if not figure_data: # Check if we have neither track nor cars to plot
            empty_title = "Track Map: Waiting for Session Info / Track Data..."
            # Refine message based on state?
            if current_session_key and not (track_x and track_y) and needs_api_fetch:
                 empty_title = f"Track Map: Fetching Track Data ({current_session_key})..."
            elif current_session_key and not (track_x and track_y):
                 empty_title = f"Track Map: Track Data Unavailable ({current_session_key})"
            elif not current_session_key:
                 empty_title = "Track Map: Waiting for Session Info..."

            layout.title = empty_title
            layout.xaxis={'visible': False}; layout.yaxis={'visible': False} # Hide axes
            final_figure = go.Figure(data=[], layout=layout) # Empty data list
        else: # We have at least track data or car data (or both)
            layout.title = f"Track Map ({plot_session_key})" if plot_session_key else "Track Map"
            final_figure = go.Figure(data=figure_data, layout=layout)

    except Exception as fig_err:
         logger.error(f"!!! Error creating Plotly map figure: {fig_err}", exc_info=True)
         error_layout = go.Layout(
             title=f"Error Creating Map: Check Logs",
             paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgb(30,30,30)',
             font=dict(color='red'),
             xaxis={'visible': False}, yaxis={'visible': False}
         )
         final_figure = go.Figure(data=[], layout=error_layout) # Empty data list

    end_time_map = time.time()
    logger.debug(f"--- update_track_map Tick {n} End ({end_time_map - start_time_map:.4f}s) ---")
    return final_figure



print("DEBUG: callbacks module loaded")
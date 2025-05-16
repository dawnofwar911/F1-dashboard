# callbacks.py
"""
Contains all the Dash callback functions for the application.
Handles UI updates, user actions, and plot generation.
"""
from dash.dependencies import Input, Output, State # Ensure State is imported
import datetime
import pytz
import logging
import json
import time
import datetime
from datetime import timezone
import threading
from pathlib import Path

import dash
from dash.dependencies import Input, Output, State, ClientsideFunction
from dash import dcc, html, dash_table, no_update # Import no_update
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots # Import make_subplots
import numpy as np
import requests
from shapely.geometry import LineString, Point # <<< ADD SHAPELY IMPORT
from shapely.ops import nearest_points # <<< For snapping points to line

# --- App Import ---
try:
    from app_instance import app
except ImportError:
    print("ERROR: Could not import 'app' for callbacks.")
    raise

# --- Module Imports ---
import app_state
import config
import utils # Contains helpers
import signalr_client
import replay
# data_processing functions are called internally by the loop, not directly needed here

logger = logging.getLogger("F1App.Callbacks") # Use consistent logger name

# --- Core Display Update Callbacks ---

# --- Constants for initial figure uirevisions (MUST MATCH layout.py) ---
# These are the uirevisions you set in the initial go.Figure() in layout.py
INITIAL_TRACK_MAP_UIREVISION = 'track_map_main_layout'
INITIAL_TELEMETRY_UIREVISION = 'telemetry_main_layout'
INITIAL_LAP_PROG_UIREVISION = 'lap_prog_main_layout'

# --- Heights from layout.py wrapper divs (MUST MATCH layout.py) ---
TRACK_MAP_WRAPPER_HEIGHT = 360 # From your provided layout.py
TELEMETRY_WRAPPER_HEIGHT = 320 # From your provided layout.py
LAP_PROG_WRAPPER_HEIGHT = 320  # From your provided layout.py
# Driver details div height is also relevant for overall column space
DRIVER_DETAILS_HEIGHT = 80

# --- Margins for consistency (MUST MATCH layout.py for initial figures) ---
TRACK_MAP_MARGINS = {'l': 2, 'r': 2, 't': 2, 'b': 2}
TELEMETRY_MARGINS_EMPTY = {'l': 30, 'r': 5, 't': 10, 'b': 20} # For "Select driver" state
TELEMETRY_MARGINS_DATA = {'l': 35, 'r': 10, 't': 30, 'b': 30}  # When data is plotted (can allow more for titles/axes)
LAP_PROG_MARGINS_EMPTY = {'l': 35, 'r': 5, 't': 20, 'b': 30}
LAP_PROG_MARGINS_DATA = {'l': 40, 'r': 10, 't': 30, 'b': 40}

# --- Track Status and Weather specific styling maps ---
TRACK_STATUS_STYLES = {
    '1': {"label": "CLEAR", "card_color": "success", "text_color": "white"}, # Clear
    '2': {"label": "YELLOW", "card_color": "warning", "text_color": "black"}, # Yellow
    '3': {"label": "SC DEPLOYED?", "card_color": "warning", "text_color": "black"}, # SC Expected / Deployed
    '4': {"label": "SAFETY CAR", "card_color": "warning", "text_color": "black"}, # Safety Car
    '5': {"label": "RED FLAG", "card_color": "danger", "text_color": "white"},   # Red Flag
    '6': {"label": "VSC DEPLOYED", "card_color": "info", "text_color": "white"},# VSC Deployed
    '7': {"label": "VSC ENDING", "card_color": "info", "text_color": "white"}, # VSC Ending
    'DEFAULT': {"label": "UNKNOWN", "card_color": "secondary", "text_color": "white"}
}

# --- Weather Icon Mapping (using simple text/emoji for now, can be Bootstrap Icon classes) ---
WEATHER_ICON_MAP = {
    "sunny": "☀️",
    "cloudy": "☁️",
    "overcast": "🌥️", # Or same as cloudy
    "rain": "🌧️",
    "drizzle": "🌦️",
    "windy": "💨",
    "default": "🌡️" # Default temperature icon
}

def create_empty_figure_with_message(height, uirevision, message, margins):
    """Helper to create a consistent empty figure."""
    return go.Figure(layout={
        'template': 'plotly_dark',
        'height': height,
        'margin': margins,
        'uirevision': uirevision, # Use the specific uirevision for empty states
        'xaxis': {'visible': False, 'range': [0,1]}, # Dummy axis
        'yaxis': {'visible': False, 'range': [0,1]}, # Dummy axis
        'annotations': [{'text': message, 'xref': 'paper', 'yref': 'paper',
                         'showarrow': False, 'font': {'size': 12}}]
    })


@app.callback(
    Output('connection-status', 'children'),
    Output('connection-status', 'style'),
    Input('interval-component-fast', 'n_intervals')
)
def update_connection_status(n):
    """Updates the connection status indicator."""
    # --- >>> ADD DEBUG LOGGING <<< ---
    # logger.debug(f"Running update_connection_status (Interval {n})")
    # --- >>> END DEBUG LOGGING <<< ---

    # Initialize defaults
    status_text = "State: ? | Conn: ?"
    status_style = {'color': 'grey', 'fontWeight': 'bold'} # Default style

    try: # Add try-except around state access and logic
        with app_state.app_state_lock:
            status = app_state.app_status.get("connection", "Unknown")
            state = app_state.app_status.get("state", "Idle")
            is_rec = app_state.is_saving_active
            rec_file = app_state.current_recording_filename
            rep_file = app_state.app_status.get("current_replay_file")

            # --- >>> ADD DEBUG LOGGING <<< ---
            # l ogger.debug(f"Read State: {state}, Conn: {status}, Recording: {is_rec}, RecFile: {rec_file}, RepFile: {rep_file}")
            # --- >>> END DEBUG LOGGING <<< ---

        status_text = f"State: {state} | Conn: {status}"
        color = 'grey' # Default color

        # Determine color based on state
        if state == "Live": color = 'lime' # Brighter green
        elif state in ["Connecting", "Initializing"]: color = 'orange'
        elif state in ["Stopped", "Idle"]: color = 'grey'
        elif state == "Error": color = 'red' # Changed Error color
        elif state == "Replaying": color = 'dodgerblue' # Changed Replay color
        elif state == "Playback Complete": color = 'lightblue'
        elif state == "Stopping": color = 'lightcoral'

        # Append Recording/Replay info
        if is_rec and rec_file and isinstance(rec_file, (str, Path)): # Check type before using Path
             try: status_text += f" (REC: {Path(rec_file).name})"
             except Exception as path_e: logger.warning(f"Could not get filename from rec_file '{rec_file}': {path_e}") # Log path error
        elif state == "Replaying" and rep_file:
             try: status_text += f" (Replay: {Path(rep_file).name})" # Use Path here too for consistency
             except Exception as path_e: logger.warning(f"Could not get filename from rep_file '{rep_file}': {path_e}")

        status_style = {'color': color, 'fontWeight': 'bold'}

    except Exception as e:
        logger.error(f"Error in update_connection_status: {e}", exc_info=True)
        # Return a visible error state in the UI
        status_text = "Error updating status!"
        status_style = {'color': 'red', 'fontWeight': 'bold'}

    # logger.debug(f"Updating connection status display: '{status_text}', Style: {status_style}") # Optional: Log output
    return status_text, status_style

@app.callback(
    Output('session-info-display', 'children'), 
    Output('prominent-weather-display', 'children'),
    Output('weather-main-icon', 'children'), # <<< NEW Output for the icon
    Output('prominent-weather-card', 'color'), # <<< NEW Output for card color
    Output('prominent-weather-card', 'inverse'), # Ensure text is readable
    Input('interval-component-slow', 'n_intervals')
)
def update_session_and_weather_info(n):
    session_info_str = "Session Info: Awaiting data..."
    # Weather display elements
    weather_details_spans = []
    main_weather_icon = WEATHER_ICON_MAP["default"]
    weather_card_color = "light" # Default card color
    weather_card_inverse = False # Default: light card, dark text

    try:
        with app_state.app_state_lock:
            local_session_details = app_state.session_details.copy()
            raw_weather_payload = app_state.data_store.get('WeatherData', {}) 
            local_weather_data = raw_weather_payload.get('data', {}) if isinstance(raw_weather_payload, dict) else {}
            if not isinstance(local_weather_data, dict): 
                local_weather_data = {}

        # --- Session Name, Circuit etc. (for original 'session-info-display') ---
        # (Your existing logic for session_info_str from Response 22)
        meeting = local_session_details.get('Meeting', {}).get('Name', '?')
        session_name = local_session_details.get('Name', '?') 
        circuit = local_session_details.get('Circuit', {}).get('ShortName', '?')
        parts = []
        if circuit != '?': parts.append(f"{circuit}")
        if meeting != '?': parts.append(f"{meeting}")
        if session_name != '?': parts.append(f"Session: {session_name}")
        if parts: session_info_str = " | ".join(parts)


        # --- Weather Data ---
        # Helper to safely convert to float, returns None if not possible
        def safe_float(value, default=None):
            if value is None: return default
            try: return float(value)
            except (ValueError, TypeError): return default

        air_temp = safe_float(local_weather_data.get('AirTemp'))
        track_temp = safe_float(local_weather_data.get('TrackTemp'))
        humidity = safe_float(local_weather_data.get('Humidity'))
        pressure = safe_float(local_weather_data.get('Pressure'))
        wind_speed = safe_float(local_weather_data.get('WindSpeed'))
        wind_direction = local_weather_data.get('WindDirection') # Often a string or number
        rainfall_val = local_weather_data.get('Rainfall') # Can be '0', '1', 0, 1
        is_raining = rainfall_val == '1' or rainfall_val == 1

        overall_condition = "default" # Start with default
        weather_card_color = "light"  # Default to light
        weather_card_inverse = False
        
        if is_raining:
            overall_condition = "rain"
            weather_card_color = "info" 
            weather_card_inverse = True
        elif air_temp is not None and humidity is not None: # Only do temp/hum based if we have both
            if air_temp > 25 and humidity < 60 : # Adjusted humidity threshold for "sunny"
                overall_condition = "sunny"
                weather_card_color = "warning" 
                weather_card_inverse = False # Black text on yellow
            elif humidity >= 75 or air_temp < 15: # Example: cloudy/cool/damp
                overall_condition = "cloudy" # Could be more specific if more data points
                weather_card_color = "secondary" # Greyish for cloudy
                weather_card_inverse = True
            # You can add more elif conditions here based on combinations of data
        elif air_temp is not None: # If only air_temp is available
             if air_temp > 28: overall_condition = "sunny" # Hot implies sun
             elif air_temp < 10: overall_condition = "cloudy" # Cool implies clouds

        main_weather_icon = WEATHER_ICON_MAP.get(overall_condition, WEATHER_ICON_MAP["default"])

        # Build the detailed weather string
        if air_temp is not None: weather_details_spans.append(html.Span(f"Air: {air_temp:.1f}°C", className="me-3"))
        if track_temp is not None: weather_details_spans.append(html.Span(f"Track: {track_temp:.1f}°C", className="me-3"))
        if humidity is not None: weather_details_spans.append(html.Span(f"Hum: {humidity:.0f}%", className="me-3"))
        if pressure is not None: weather_details_spans.append(html.Span(f"Press: {pressure:.0f}hPa", className="me-3"))
        if wind_speed is not None:
            wind_str = f"Wind: {wind_speed:.1f}m/s"
            if wind_direction is not None: # Wind direction can be numeric or sometimes 'Variable'
                 try: wind_str += f" ({int(wind_direction)}°)"
                 except (ValueError, TypeError): wind_str += f" ({wind_direction})" # If not int, show as is
            weather_details_spans.append(html.Span(wind_str, className="me-3"))
        
        # Add RAIN text prominently if it's raining and not already clear from icon/card
        if is_raining and overall_condition != "rain": 
            weather_details_spans.append(html.Span("RAIN", className="me-2 fw-bold", 
                                         style={'color':'#007bff'})) # Blue rain text

        if not weather_details_spans and overall_condition == "default":
            final_weather_display_children = [html.Em("Weather data unavailable")]
        elif not weather_details_spans and overall_condition != "default":
             final_weather_display_children = [html.Em(f"{overall_condition.capitalize()} conditions")]
        else:
            final_weather_display_children = weather_details_spans
            
        return session_info_str, html.Div(children=final_weather_display_children), main_weather_icon, weather_card_color, weather_card_inverse

    except Exception as e:
        logger.error(f"Session/Weather Display Error in callback: {e}", exc_info=True)
        return "Error: Session Info", "Error: Weather", WEATHER_ICON_MAP["default"], "light", False

@app.callback(
    Output('prominent-track-status-text', 'children'),
    Output('prominent-track-status-card', 'color'), # To change card background
    Output('prominent-track-status-text', 'style'), # To change text color for contrast
    Input('interval-component-medium', 'n_intervals') # Or fast, depending on desired reactivity
)
def update_prominent_track_status(n):
    with app_state.app_state_lock:
        track_status_code = str(app_state.track_status_data.get('Status', '0')) # Ensure string
        # track_message = app_state.track_status_data.get('Message', '') # We might add this back later

    status_info = TRACK_STATUS_STYLES.get(track_status_code, TRACK_STATUS_STYLES['DEFAULT'])
    
    label_to_display = status_info["label"]
    # if track_message and track_message != 'AllClear' and track_status_code != '1':
    #     label_to_display += f" ({track_message})" # Optionally add message

    text_style = {'fontWeight':'bold', 'padding':'2px 5px', 'borderRadius':'4px', 'color': status_info["text_color"]}
    
    return label_to_display, status_info["card_color"], text_style

@app.callback(
    Output('other-data-display', 'children'),
    Output('timing-data-actual-table', 'data'),
    Output('timing-data-timestamp', 'children'),
    Input('interval-component-timing', 'n_intervals')
)
def update_main_data_displays(n):
    """Updates the timing table and the 'other data' display area (Optimized)."""
    other_elements = []
    table_data = []
    timestamp_text = "Waiting..."
    start_time = time.monotonic()

    try:
        with app_state.app_state_lock:
            timing_state_copy = app_state.timing_state.copy()
            data_store_copy = app_state.data_store # Read only, direct access okay
            
        # --- Other Data Display (Keep previous logic) ---
        excluded_streams = ['TimingData', 'DriverList', 'Position.z', 'CarData.z', 'Position',
                            'TrackStatus', 'SessionData', 'SessionInfo', 'WeatherData', 'RaceControlMessages', 'Heartbeat']
        sorted_streams = sorted(
            [s for s in data_store_copy.keys() if s not in excluded_streams])
        for stream in sorted_streams:
            value = data_store_copy.get(stream, {})
            data_payload = value.get('data', 'N/A')
            timestamp_str_val = value.get('timestamp', 'N/A') # Renamed to avoid conflict
            try:
                data_str = json.dumps(data_payload, indent=2)
            except TypeError:
                data_str = str(data_payload)
            if len(data_str) > 500:
                data_str = data_str[:500] + "\n...(truncated)"
            other_elements.append(html.Details([html.Summary(f"{stream} ({timestamp_str_val})"), html.Pre(data_str, style={
                                  'marginLeft': '15px', 'maxHeight': '200px', 'overflowY': 'auto'})], open=(stream == "LapCount")))

        # --- Timing Table Timestamp ---
        timing_data_entry = data_store_copy.get('TimingData', {})
        timestamp_text = f"Timing TS: {timing_data_entry.get('timestamp', 'N/A')}" if timing_data_entry else "Waiting..."

        # --- Generate Timing Table Data (Optimized Loop) ---
        if timing_state_copy:
            processed_table_data = []
            for car_num, driver_state in timing_state_copy.items():
                # --- Driver Number and TLA ---
                racing_no = driver_state.get("RacingNumber", car_num) # Fallback to car_num if no RacingNumber
                tla = driver_state.get("Tla", "N/A") # TLA for 'Car' column

                pos = driver_state.get('Position', '-')
                
                # --- Tyre Information Formatting ---
                compound = driver_state.get('TyreCompound', '-') 
                age = driver_state.get('TyreAge', '?')      
                is_new = driver_state.get('IsNewTyre', False) 
                compound_short = ""
                known_compounds = ["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"]
                if compound and compound.upper() in known_compounds:
                    compound_short = compound[0].upper()
                elif compound and compound != '-': 
                    compound_short = "?" 
                tyre_display_parts = []
                if compound_short: 
                    tyre_display_parts.append(compound_short)
                if age != '?': 
                    age_str = str(age) 
                    tyre_display_parts.append(f"{age_str}L")
                tyre_base = " ".join(tyre_display_parts) if tyre_display_parts else "-"
                new_tyre_indicator = ""
                if compound_short and compound_short != '?' and not is_new: 
                    new_tyre_indicator = "*"
                tyre = f"{tyre_base}{new_tyre_indicator}"
                if tyre_base == "-": 
                    tyre = "-"

                time_val = driver_state.get('Time', '-') # This is 'Lap Time'
                gap = driver_state.get('GapToLeader', '-')
                interval = utils.get_nested_state(
                    driver_state, 'IntervalToPositionAhead', 'Value', default='-')
                last_lap = utils.get_nested_state(
                    driver_state, 'LastLapTime', 'Value', default='-')
                best_lap = utils.get_nested_state(
                    driver_state, 'BestLapTime', 'Value', default='-')
                s1 = utils.get_nested_state(
                    driver_state, 'Sectors', '0', 'Value', default='-')
                s2 = utils.get_nested_state(
                    driver_state, 'Sectors', '1', 'Value', default='-')
                s3 = utils.get_nested_state(
                    driver_state, 'Sectors', '2', 'Value', default='-')
                
                # --- Pit Stops ---
                reliable_stops = driver_state.get('ReliablePitStops', 0)
                timing_data_stops = driver_state.get('NumberOfPitStops', 0)
                
                pits_display_val = '0' # Default to 0
                if reliable_stops > 0:
                    pits_display_val = str(reliable_stops)
                elif timing_data_stops > 0: # Only if reliable_stops is 0, consider timing_data_stops
                    pits_display_val = str(timing_data_stops)
                # If both are 0, it remains '0'.
                # If StintsData has been processed and ReliablePitStops is correctly 0 (e.g. first stint),
                # it will correctly show 0, even if TimingData_NumberOfPitStops is momentarily 1.

                status = driver_state.get('Status', 'N/A')
                
                car_data = driver_state.get('CarData', {})
                speed = car_data.get('Speed', '-')
                gear = car_data.get('Gear', '-')
                rpm = car_data.get('RPM', '-')
                drs_val = car_data.get('DRS')
                drs_map = {8: "E", 10: "On", 12: "On", 14: "ON"} # Eligible, On
                drs = drs_map.get(drs_val, 'Off') if drs_val is not None else 'Off'

                row = {
                    'No.': racing_no, # Populate 'No.' column
                    'Car': tla,       # Populate 'Car' (TLA) column
                    'Pos': pos,
                    'Tyre': tyre,     # Updated tyre display
                    'Time': time_val, # This is 'Lap Time'
                    'Gap': gap,
                    'Interval': interval,
                    'Last Lap': last_lap,
                    'Best Lap': best_lap,
                    'S1': s1, 'S2': s2, 'S3': s3,
                    'Pits': pits_display_val,     # Populate 'Pits' column
                    'Status': status,
                    'Speed': speed, 'Gear': gear, 'RPM': rpm, 'DRS': drs
                }
                processed_table_data.append(row)

            processed_table_data.sort(key=utils.pos_sort_key)
            table_data = processed_table_data
        else:
            timestamp_text = "Waiting for DriverList..."

        end_time = time.monotonic()
        # logger.debug(f"update_main_data_displays took {end_time - start_time:.4f}s")

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
    # (Logic from Response 22/24)
    try:
        with app_state.app_state_lock: log_snapshot = list(app_state.race_control_log)
        display_text = "\n".join(log_snapshot)
        return display_text if display_text else "Waiting for Race Control messages..."
    except Exception as e: logger.error(f"Error updating RC log: {e}", exc_info=True); return "Error loading RC log."


# --- Control Callbacks ---

@app.callback(
    Output('dummy-output-for-controls', 'children', allow_duplicate=True), # Dummy output
    Input('replay-speed-slider', 'value'),
    prevent_initial_call=True
)
def update_replay_speed_state(new_speed):
    """Updates the shared replay speed state when the slider changes."""
    if new_speed is None:
        return no_update # Should not happen unless slider is cleared?

    logger.info(f"Replay speed slider changed to: {new_speed}")
    try:
        speed_float = float(new_speed)
        with app_state.app_state_lock:
            app_state.replay_speed = speed_float
        return no_update
    except (ValueError, TypeError):
        logger.warning(f"Could not convert slider value '{new_speed}' to float.")
        return no_update
# --- >>> END ADDED CALLBACK <<< ---


# callbacks.py

import logging
import json
import time
import threading # Ensure threading is imported
from pathlib import Path

import dash
from dash.dependencies import Input, Output, State, ClientsideFunction
from dash import dcc, html, dash_table, no_update
import dash_bootstrap_components as dbc
# ... other specific imports like plotly, numpy, requests etc. ...

try:
    from app_instance import app
except ImportError:
    print("ERROR: Could not import 'app' for callbacks.")
    raise

@app.callback(
    Output('dummy-output-for-controls', 'children'), # Main dummy output for this callback
    Input('connect-button', 'n_clicks'),
    Input('disconnect-button', 'n_clicks'),
    Input('replay-button', 'n_clicks'),
    Input('stop-replay-button', 'n_clicks'),
    Input('stop-reset-button', 'n_clicks'), # Added in previous responses
    State('replay-file-selector', 'value'),
    State('replay-speed-slider', 'value'),
    State('record-data-checkbox', 'value'),
    prevent_initial_call=True
)
def handle_control_clicks(connect_clicks, disconnect_clicks,
                          replay_clicks, stop_replay_clicks,
                          stop_reset_clicks,
                          selected_replay_file, replay_speed,
                          record_checkbox_value):
    ctx = dash.callback_context
    if not ctx.triggered:
        logger.debug("handle_control_clicks triggered with no context (e.g., initial load with prevent_initial_call=True)")
        return no_update
        
    button_id = ctx.triggered_id
    if not button_id: # Should not happen if ctx.triggered is true, but as a safeguard
        logger.warning("handle_control_clicks: No button_id in triggered context.")
        return no_update

    logger.info(f"Control button clicked: {button_id}")

    # --- Connect Button Logic ---
    if button_id == 'connect-button':
        logger.info("Connect Live button clicked.")
        with app_state.app_state_lock:
            current_app_s = app_state.app_status["state"]
            if current_app_s not in ["Idle", "Stopped", "Error", "Playback Complete"]:
                logger.warning(f"Connect button ignored. App not in a connectable state (current: {current_app_s})")
                return no_update
            
            # Clear stop_event FOR THE NEW SESSION
            if app_state.stop_event.is_set():
                logger.info("Connect Live: Clearing pre-existing global stop_event.")
                app_state.stop_event.clear()
            else:
                logger.info("Connect Live: Global stop_event was already clear.")
            
            # Update app_state.record_live_data based on checkbox (already handled by record_checkbox_callback)
            # but good to log the decision point
            should_record_live = app_state.record_live_data 
            logger.info(f"Connect Live: Recording decision based on app_state.record_live_data: {should_record_live}")

        # Proceed with connection (websocket negotiation, thread start)
        websocket_url, ws_headers = None, None
        try:
            with app_state.app_state_lock:
                 app_state.app_status.update({"state": "Initializing", "connection": "Negotiating..."})
            
            websocket_url, ws_headers = signalr_client.build_connection_url(
                config.NEGOTIATE_URL_BASE, config.HUB_NAME
            )
            if not websocket_url or not ws_headers:
                 raise ConnectionError("Negotiation failed to return URL or Headers.")
            logger.info("Negotiation successful.")
        except Exception as e:
            logger.error(f"Error during negotiation/setup for Connect Live: {e}", exc_info=True)
            with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Negotiation Failed"})
            return no_update

        if websocket_url and ws_headers:
            if should_record_live: # Use the value retrieved under lock
                logger.info("Recording enabled for live session, initializing live file state...")
                if not replay.init_live_file(): # This sets app_state flags for recording
                    logger.error("Failed to initialize recording state for live session. Proceeding without recording.")
            else:
                logger.info("Recording disabled for live session, ensuring recording state is cleared.")
                replay.close_live_file() # Clears recording flags and closes file if any

            logger.info("Starting SignalR connection thread...")
            thread_obj = threading.Thread(
                target=signalr_client.run_connection_manual_neg,
                args=(websocket_url, ws_headers),
                name="SignalRConnectionThread", daemon=True)
            signalr_client.connection_thread = thread_obj # Store thread reference
            thread_obj.start()
            logger.info("SignalR connection thread initiated.")
        else:
             logger.error("Cannot start SignalR connection thread: URL or Headers missing after negotiation.")
             with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Internal Setup Error (SignalR)"})

    # --- Disconnect Button Logic ---
    elif button_id == 'disconnect-button':
        logger.info("Disconnect Live button clicked.")
        action_failed = False
        try:
            signalr_client.stop_connection() # This function sets app_state.stop_event
            logger.info("signalr_client.stop_connection() called successfully.")
        except Exception as e:
            logger.error(f"Error during 'disconnect-button' -> signalr_client.stop_connection(): {e}", exc_info=True)
            action_failed = True
        
        # Logic to clear stop_event if app is to remain running and Idle
        with app_state.app_state_lock:
            current_main_state_after_disconnect = app_state.app_status.get("state", "Unknown")
            # stop_connection should put state to "Stopped" or "Idle"
            if not action_failed and current_main_state_after_disconnect in ["Stopped", "Idle"]:
                 if app_state.stop_event.is_set(): # Check if stop_connection actually set it
                    logger.info(f"Disconnect Live: State is '{current_main_state_after_disconnect}'. Clearing global stop_event to keep app running.")
                    app_state.stop_event.clear()
                 else:
                    logger.info(f"Disconnect Live: State is '{current_main_state_after_disconnect}'. Global stop_event was already clear or not set by stop_connection.")
            elif app_state.stop_event.is_set(): 
                logger.warning(f"Disconnect Live: State is '{current_main_state_after_disconnect}', Action Failed: {action_failed}. Global stop_event was set but conditions not met to clear it.")
            
            if action_failed: # Ensure UI reflects error if stop_connection itself failed critically
                app_state.app_status["state"] = "Error"
                app_state.app_status["connection"] = "Disconnect Failed"
        logger.info("Disconnect Live button processing finished.")

    # --- Replay Button Logic ---
    elif button_id == 'replay-button':
        logger.info("Start Replay button clicked.")
        if selected_replay_file:
            active_live_session = False
            with app_state.app_state_lock: # Check if live session is running
                current_app_s_for_replay = app_state.app_status["state"]
                if current_app_s_for_replay in ["Live", "Connecting"]:
                    active_live_session = True
                elif current_app_s_for_replay == "Replaying":
                    logger.warning("Start Replay ignored: Another replay is already in progress. Please stop it first.")
                    return no_update
                elif current_app_s_for_replay not in ["Idle", "Stopped", "Error", "Playback Complete"]:
                    logger.warning(f"Start Replay ignored: App not in a suitable state (current: {current_app_s_for_replay})")
                    return no_update

            if active_live_session: # If live, stop it first
                logger.info("Live session is active. Stopping live feed before starting replay...")
                try:
                    signalr_client.stop_connection() # This sets stop_event
                    logger.info("signalr_client.stop_connection() called to make way for replay.")
                    # Brief pause for live connection thread to react to stop_event
                    time.sleep(0.3) 
                    # The stop_event set by stop_connection will be cleared by replay_from_file
                except Exception as e:
                    logger.error(f"Error stopping live feed prior to replay: {e}", exc_info=True)
                    with app_state.app_state_lock:
                        app_state.app_status["state"] = "Error"
                        app_state.app_status["connection"] = "Failed to stop live feed for replay"
                    return no_update
            
            # Proceed with replay (replay_from_file handles its own stop_event.clear())
            try:
                speed_float = float(replay_speed); speed_float = max(0.1, speed_float) # Validate speed
                full_replay_path = Path(config.REPLAY_DIR) / selected_replay_file # Use Path object
                logger.info(f"Attempting to start replay: {full_replay_path}, Initial Speed: {speed_float}x")
                
                # replay_from_file clears stop_event, sets up state, and starts thread
                replay_started_ok = replay.replay_from_file(full_replay_path, speed_float) 

                if replay_started_ok:
                    logger.info(f"Replay initiated successfully for {full_replay_path.name}.")
                else:
                    logger.error(f"replay.replay_from_file reported failure to start replay for {full_replay_path.name}.")
                    # State should have been set to Error or similar by replay_from_file if it failed early
            except (ValueError, TypeError) as ve:
                 logger.error(f"Invalid initial replay speed value from slider: '{replay_speed}'. Error: {ve}. Cannot start replay.")
                 with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Invalid Replay Speed"})
            except Exception as e_replay_start:
                 logger.error(f"Unexpected error trying to start replay: {e_replay_start}", exc_info=True)
                 with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Replay Start Failed"})
        else:
            logger.warning("Start Replay clicked, but no replay file was selected.")
        logger.info("Start Replay button processing finished.")

    # --- Stop Replay Button Logic ---
    elif button_id == 'stop-replay-button':
        logger.info("Stop Replay button clicked.")
        action_failed = False
        try:
            replay.stop_replay() # This function sets app_state.stop_event
            logger.info("replay.stop_replay() called successfully.")
        except Exception as e:
            logger.error(f"Error during 'stop-replay-button' -> replay.stop_replay(): {e}", exc_info=True)
            action_failed = True
        
        # Logic to clear stop_event if app is to remain running and Idle
        with app_state.app_state_lock:
            current_main_state_after_stop_replay = app_state.app_status.get("state", "Unknown")
            # stop_replay should put state to "Stopped", "Idle", or "Playback Complete"
            if not action_failed and current_main_state_after_stop_replay in ["Stopped", "Idle", "Playback Complete"]:
                if app_state.stop_event.is_set(): # Check if stop_replay actually set it
                    logger.info(f"Stop Replay: State is '{current_main_state_after_stop_replay}'. Clearing global stop_event to keep app running.")
                    app_state.stop_event.clear()
                else:
                    logger.info(f"Stop Replay: State is '{current_main_state_after_stop_replay}'. Global stop_event was already clear or not set by stop_replay.")
            elif app_state.stop_event.is_set():
                 logger.warning(f"Stop Replay: State is '{current_main_state_after_stop_replay}', Action Failed: {action_failed}. Global stop_event was set but conditions not met to clear it.")

            if action_failed: # Ensure UI reflects error if stop_replay itself failed critically
                app_state.app_status["state"] = "Error"
                app_state.app_status["connection"] = "Stop Replay Failed"
        logger.info("Stop Replay button processing finished.")

    # --- Stop & Reset Session Button Logic ---
    elif button_id == 'stop-reset-button':
        logger.info("Stop & Reset Session button clicked.")
        any_action_failed = False # Flag to track if any step had an issue

        # Step 1: Stop SignalR connection
        logger.info("Stop & Reset: Executing signalr_client.stop_connection()...")
        try:
            signalr_client.stop_connection() # This will set app_state.stop_event
            logger.info("Stop & Reset: Completed signalr_client.stop_connection().")
        except Exception as e:
            logger.error(f"Stop & Reset: Error during signalr_client.stop_connection(): {e}", exc_info=True)
            any_action_failed = True

        # Step 2: Stop Replay
        logger.info("Stop & Reset: Executing replay.stop_replay()...")
        try:
            replay.stop_replay() # This will also set/confirm app_state.stop_event
            logger.info("Stop & Reset: Completed replay.stop_replay().")
        except Exception as e:
            logger.error(f"Stop & Reset: Error during replay.stop_replay(): {e}", exc_info=True)
            any_action_failed = True

        # Brief pause for threads to acknowledge stop signals and potentially release locks
        logger.debug("Stop & Reset: Pausing for 0.3s for threads to process stop signals...")
        time.sleep(0.3)

        # Step 3: Reset application state to default
        # This function sets app_status["state"] to "Idle" among other things
        logger.info("Stop & Reset: Executing app_state.reset_to_default_state()...")
        try:
            app_state.reset_to_default_state()
            logger.info("Stop & Reset: Completed app_state.reset_to_default_state(). State should now be 'Idle'.")
        except Exception as e:
            logger.error(f"Stop & Reset: Error during app_state.reset_to_default_state(): {e}", exc_info=True)
            any_action_failed = True
            # If reset fails, app state might be inconsistent. app_status["state"] might not be "Idle".

        # Step 4: Ensure global stop_event is cleared so the main application loop in main.py continues
        logger.info("Stop & Reset: Checking and clearing global stop_event to allow application to continue...")
        try:
            with app_state.app_state_lock:
                current_status_after_reset = app_state.app_status.get("state", "Unknown")
                logger.info(f"Stop & Reset: Application state before final stop_event clear: '{current_status_after_reset}'. Prior actions failed: {any_action_failed}")
                
                if app_state.stop_event.is_set():
                    logger.info("Stop & Reset: Global stop_event is currently SET. Clearing it now.")
                    app_state.stop_event.clear()
                else:
                    logger.info("Stop & Reset: Global stop_event was already CLEAR.")
                
                # If any action failed AND the state isn't already Error, set it to Error for clear UI feedback.
                if any_action_failed and current_status_after_reset != "Error":
                    logger.warning("Stop & Reset: One or more actions failed. Forcing app_status to 'Error'.")
                    app_state.app_status["state"] = "Error"
                    app_state.app_status["connection"] = "Stop/Reset failed"
                elif not any_action_failed and current_status_after_reset != "Idle":
                    logger.warning(f"Stop & Reset: Actions reported success, but state is '{current_status_after_reset}' not 'Idle'. This is unexpected.")
                    # Optionally force to Idle if all actions appeared successful
                    # app_state.app_status["state"] = "Idle"
                    # app_state.app_status["connection"] = "Disconnected"


        except Exception as e:
            logger.error(f"Stop & Reset: Critical error during stop_event clear or final state adjustment: {e}", exc_info=True)
            # If this block fails, the app might still exit if stop_event was set and not cleared.

        logger.info("Stop & Reset Session button processing finished.")

    else:
        logger.warning(f"Button ID '{button_id}' not explicitly handled in handle_control_clicks.")

    return no_update # Default return for the dummy output

@app.callback(
    Output('record-data-checkbox', 'id', allow_duplicate=True),
    Input('record-data-checkbox', 'value'),
    prevent_initial_call=True
)
def record_checkbox_callback(checked_value):
    """Updates the app_state.record_live_data flag."""
    if checked_value is None: return 'record-data-checkbox'
    new_state = bool(checked_value)
    logger.debug(f"Record Live Data checkbox set to: {new_state}")
    with app_state.app_state_lock: app_state.record_live_data = new_state
    return 'record-data-checkbox' # Dummy output


@app.callback(
    Output('replay-file-selector', 'options'),
    Input('interval-component-slow', 'n_intervals')
)
def update_replay_options(n_intervals):
     """Updates the replay file dropdown options periodically."""
     # logger.debug("Updating replay file options...")
     return replay.get_replay_files(config.REPLAY_DIR)


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

# --- >>> NEW/MODIFIED: Driver Details & Telemetry Callback <<< ---
@app.callback(
    Output('driver-details-output', 'children'),
    Output('lap-selector-dropdown', 'options'),
    Output('lap-selector-dropdown', 'value'),
    Output('lap-selector-dropdown', 'disabled'),
    Output('telemetry-graph', 'figure'),
    Input('driver-select-dropdown', 'value'),
    Input('lap-selector-dropdown', 'value'),
    State('telemetry-graph', 'figure'), # Get current figure to check uirevision
    prevent_initial_call=True 
)
def display_driver_details(selected_driver_number, selected_lap, current_telemetry_figure):
    ctx = dash.callback_context
    triggered_id = ctx.triggered_id if ctx.triggered else 'N/A'
    logger.debug(f"Telemetry Update: Trigger={triggered_id}, Driver={selected_driver_number}, Lap={selected_lap}")

    details_children = [html.P("Select a driver.", style={'fontSize':'0.8rem', 'padding':'5px'})]
    lap_options = [{'label': 'No Laps', 'value': ''}]
    current_lap_value_for_dropdown = None
    lap_disabled = True
    
    # Default empty figure using the INITIAL uirevision (from layout.py)
    fig_empty_telemetry = create_empty_figure_with_message(
        TELEMETRY_WRAPPER_HEIGHT, INITIAL_TELEMETRY_UIREVISION, 
        "Select driver & lap", TELEMETRY_MARGINS_EMPTY
    )

    if not selected_driver_number:
        # If already showing the initial empty state, no update
        if current_telemetry_figure and \
           current_telemetry_figure.get('layout', {}).get('uirevision') == INITIAL_TELEMETRY_UIREVISION:
            return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, no_update
        return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, fig_empty_telemetry

    driver_num_str = str(selected_driver_number)
    
    with app_state.app_state_lock:
        available_laps = sorted(app_state.telemetry_data.get(driver_num_str, {}).keys())
        driver_info_state = app_state.timing_state.get(driver_num_str, {}).copy()
        
    # Update driver details text part (as in Response 18)
    if driver_info_state:
        tla = driver_info_state.get('Tla', '?'); num = driver_info_state.get('RacingNumber', driver_num_str)
        name = driver_info_state.get('FullName', 'Unknown'); team = driver_info_state.get('TeamName', '?')
        details_children = [html.H5(f"#{num} {tla} - {name} ({team})", style={'marginTop': '5px', 'marginBottom':'5px', 'fontSize':'0.9rem'})]
        ll = utils.get_nested_state(driver_info_state, 'LastLapTime', 'Value', default='-')
        bl = utils.get_nested_state(driver_info_state, 'BestLapTime', 'Value', default='-')
        tyre_str = f"{driver_info_state.get('TyreCompound','-')} ({driver_info_state.get('TyreAge','?')}L)" if driver_info_state.get('TyreCompound','-') != '-' else '-'
        details_children.append(html.P(f"Last: {ll} | Best: {bl} | Tyre: {tyre_str}", style={'fontSize':'0.75rem', 'marginBottom':'0px'}))

    # UIRevision for a state where a driver is selected, but lap might not be, or no laps exist
    driver_selected_uirevision = f"telemetry_{driver_num_str}_pendinglap"

    if available_laps:
        lap_options = [{'label': f'Lap {l}', 'value': l} for l in available_laps]
        lap_disabled = False
        if triggered_id == 'driver-select-dropdown' or not selected_lap or selected_lap not in available_laps:
            current_lap_value_for_dropdown = available_laps[-1]
        else:
            current_lap_value_for_dropdown = selected_lap
    else: # No laps for this driver
        no_laps_message = f"No lap data for {driver_info_state.get('Tla', driver_num_str)}."
        # If already showing this specific "no laps" message for this driver, no update
        if current_telemetry_figure and \
           current_telemetry_figure.get('layout', {}).get('uirevision') == driver_selected_uirevision and \
           current_telemetry_figure.get('layout',{}).get('annotations',[{}])[0].get('text','') == no_laps_message:
            return details_children, lap_options, None, True, no_update
            
        fig_no_laps = create_empty_figure_with_message(
            TELEMETRY_WRAPPER_HEIGHT, driver_selected_uirevision, no_laps_message, TELEMETRY_MARGINS_EMPTY
        )
        return details_children, lap_options, None, True, fig_no_laps

    if not current_lap_value_for_dropdown: # No valid lap selected yet (e.g. after driver change, before auto-select if needed)
        select_lap_message = f"Select a lap for {driver_info_state.get('Tla', driver_num_str)}."
        if current_telemetry_figure and \
           current_telemetry_figure.get('layout', {}).get('uirevision') == driver_selected_uirevision and \
           current_telemetry_figure.get('layout',{}).get('annotations',[{}])[0].get('text','') == select_lap_message:
             return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, no_update

        fig_select_lap = create_empty_figure_with_message(
            TELEMETRY_WRAPPER_HEIGHT, driver_selected_uirevision, select_lap_message, TELEMETRY_MARGINS_EMPTY
        )
        return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, fig_select_lap

    # --- Generate Telemetry Plot ---
    # UIRevision for a plot with specific driver and lap data
    data_plot_uirevision = f"telemetry_data_{driver_num_str}_{current_lap_value_for_dropdown}"

    # Fix for telemetry not displaying on first click:
    # If the callback was triggered by something other than explicit driver/lap selection AND
    # the graph already shows this data, then no_update.
    # This helps if an interval is also an Input to this callback.
    if current_telemetry_figure and \
       current_telemetry_figure.get('layout',{}).get('uirevision') == data_plot_uirevision and \
       triggered_id not in ['driver-select-dropdown', 'lap-selector-dropdown']:
        logger.debug("Telemetry already showing correct data, non-user trigger.")
        return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, no_update


    try:
        with app_state.app_state_lock:
            lap_data = app_state.telemetry_data.get(driver_num_str, {}).get(current_lap_value_for_dropdown, {})
        # ... (Your existing logic for timestamps_str, timestamps_dt, valid_indices)
        timestamps_str = lap_data.get('Timestamps', [])
        timestamps_dt = [utils.parse_iso_timestamp_safe(ts) for ts in timestamps_str]
        valid_indices = [i for i, dt_obj in enumerate(timestamps_dt) if dt_obj is not None]

        if valid_indices:
            # ... (Your existing logic for plotting channels with make_subplots)
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
                    fig_with_data.update_yaxes(fixedrange=True, tickvals=[0,1], ticktext=['Off','On'], range=[-0.1,1.1], row=i+1, col=1, title_text="", title_standoff=2, title_font_size=9, tickfont_size=8) # Remove y-axis title text, use subplot title
                else:
                    fig_with_data.add_trace(go.Scattergl(x=timestamps_plot, y=y_data_plot, mode='lines', name=channel, connectgaps=False), row=i+1, col=1)
                    fig_with_data.update_yaxes(fixedrange=True, row=i+1, col=1, title_text="", title_standoff=2, title_font_size=9, tickfont_size=8) # Remove y-axis title text
            
            fig_with_data.update_layout(
                template='plotly_dark',
                height=TELEMETRY_WRAPPER_HEIGHT,  # Use constant for wrapper height
                hovermode="x unified",
                showlegend=False,
                # Margins: t(top) needs space for main title, l(left) for y-axis ticks if they had text
                margin=TELEMETRY_MARGINS_DATA,  # Use specific margins for data plots

                # Main Title for the whole telemetry plot
                title_text=f"<b>{driver_info_state.get('Tla', driver_num_str)} - Lap {current_lap_value_for_dropdown} Telemetry</b>",
                title_x=0.5,  # Center title
                title_y=0.98,  # Position title at the top, adjust as needed
                title_font_size=12,  # Slightly smaller main title

                uirevision=data_plot_uirevision,
                annotations=[]  # Clear any previous "Select driver/lap" annotations
            )

            for i, annot in enumerate(fig_with_data.layout.annotations):
                annot.font.size = 9  # Smaller subplot titles
                annot.yanchor = 'bottom'
                annot.y = annot.y  # Slight nudge up if needed based on vertical_spacing

            for i_ax in range(len(channels)):
                fig_with_data.update_xaxes(
                    showline=(i_ax == len(channels)-1),
                    zeroline=False,
                    showticklabels=(i_ax == len(channels)-1),
                    row=i_ax+1, col=1,
                    tickfont_size=8
                )
            
            return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, fig_with_data
        else: # No valid plot data for this specific lap
            fig_empty_telemetry.layout.annotations[0].text = f"No plot data for Lap {current_lap_value_for_dropdown}."
            fig_empty_telemetry.layout.uirevision = data_plot_uirevision # Still use dynamic uirevision for this state
            return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, fig_empty_telemetry
    except Exception as plot_err:
        logger.error(f"Error in telemetry plot: {plot_err}", exc_info=True)
        fig_empty_telemetry.layout.annotations[0].text = "Error loading telemetry."
        fig_empty_telemetry.layout.uirevision = data_plot_uirevision # Use dynamic uirevision on error
        return details_children, lap_options, current_lap_value_for_dropdown, lap_disabled, fig_empty_telemetry

@app.callback(
    # This store will hold the YYYY_CircuitKey string
    Output('current-track-layout-cache-key-store', 'data'),
    Input('interval-component-medium', 'n_intervals'),
    State('current-track-layout-cache-key-store', 'data')
)
def update_current_session_id_for_map(n_intervals, existing_session_id_in_store):
    with app_state.app_state_lock:
        # Get Year and CircuitKey from app_state.session_details
        # These are set by _process_session_info
        year = app_state.session_details.get('Year')
        circuit_key = app_state.session_details.get(
            'CircuitKey')  # This is the numeric key
        app_status_state = app_state.app_status.get("state", "Idle")

    if not year or not circuit_key or app_status_state in ["Idle", "Stopped", "Error"]:
        if existing_session_id_in_store is not None:
            # logger.debug("Clearing current-track-layout-cache-key-store (now session-id-store) as session is not active or details missing.")
            return None  # Clear the session ID from the store
        return dash.no_update

    # Construct the session identifier string (e.g., "2023_44")
    # This is the 'session_key' that _process_session_info calculates and stores in
    # app_state.session_details['SessionKey'] and uses for the track_coordinates_cache['session_key']
    current_session_id = f"{year}_{circuit_key}"

    if current_session_id != existing_session_id_in_store:
        logger.info(
            f"Updating current-track-layout-cache-key-store (now session-id-store) to: {current_session_id}")
        return current_session_id  # This is the YYYY_CircuitKey string

    return dash.no_update


@app.callback(
    Output('clientside-update-interval', 'disabled'),
    [Input('connect-button', 'n_clicks'),
     Input('replay-button', 'n_clicks'),
     Input('disconnect-button', 'n_clicks'),
     Input('stop-replay-button', 'n_clicks'),
     Input('interval-component-fast', 'n_intervals')],  # Generic interval to check app_state
    [State('clientside-update-interval', 'disabled'),
     State('replay-file-selector', 'value')]  # Correctly uses 'replay-file-selector'
)
def toggle_clientside_interval(connect_clicks, replay_clicks, disconnect_clicks, stop_replay_clicks,
                               fast_interval_tick, currently_disabled, selected_replay_file):
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split(
        '.')[0] if ctx.triggered else None

    with app_state.app_state_lock:
        current_app_s = app_state.app_status.get("state", "Idle")

    if triggered_id in ['connect-button', 'replay-button']:
        if triggered_id == 'replay-button' and not selected_replay_file:
            logger.info(
                "Replay button clicked, but no file selected. Interval remains disabled.")
            return True

        logger.info(
            f"Attempting to enable clientside-update-interval due to {triggered_id}.")
        return False  # Enable

    elif triggered_id in ['disconnect-button', 'stop-replay-button']:
        logger.info(
            f"Disabling clientside-update-interval due to {triggered_id}.")
        return True   # Disable

    elif triggered_id == 'interval-component-fast':
        if current_app_s in ["Live", "Replaying"]:
            if currently_disabled:
                logger.info(
                    f"Fast interval: App is {current_app_s}, enabling interval.")
                return False
            return dash.no_update
        else:
            if not currently_disabled:
                logger.info(
                    f"Fast interval: App is {current_app_s}, disabling interval.")
                return True
            return dash.no_update

    return dash.no_update


@app.callback(
    Output('car-positions-store', 'data'),
    Input('clientside-update-interval', 'n_intervals'),
    # Or a more direct 'current_session_key' from app_state if available
    # No need for live-mode or replay-file state if timing_state is the single source of truth
)
# Removed session_key_from_dropdown parameter
def update_car_data_for_clientside(n_intervals):
    if n_intervals == 0:
        return dash.no_update

    with app_state.app_state_lock:
        current_app_status = app_state.app_status.get("state", "Idle")
        timing_state_snapshot = app_state.timing_state.copy()

    if current_app_status not in ["Live", "Replaying"] or not timing_state_snapshot:
        # logger.debug("Not updating car-positions-store: App not Live/Replaying or no timing data.")
        return dash.no_update

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

    if not processed_car_data:
        return dash.no_update

    return processed_car_data

@app.callback(
    Output('clientside-update-interval', 'interval'),
    Input('replay-speed-slider', 'value'),
    State('clientside-update-interval', 'disabled'),
    prevent_initial_call=True
)
def update_clientside_interval_speed(replay_speed, interval_disabled):
    """
    Adjusts the clientside-update-interval based on the replay speed.
    Sends updates more frequently at higher speeds.
    """
    if interval_disabled or replay_speed is None:
        # If the main interval is disabled, or no speed, don't change its rate
        return dash.no_update

    try:
        speed = float(replay_speed)
        if speed <= 0:
            speed = 1.0 # Avoid division by zero or negative intervals
    except (ValueError, TypeError):
        speed = 1.0 # Default to 1x speed if conversion fails

    # Define a base interval (e.g., the default 1250ms for 1x speed)
    base_interval_ms = 1250

    # Calculate new interval: faster speed = smaller interval
    # Ensure a minimum interval to prevent overwhelming the system
    new_interval_ms = max(350, int(base_interval_ms / speed)) # Minimum 100ms

    logger.info(f"Adjusting clientside-update-interval to {new_interval_ms}ms for replay speed {speed}x")
    return new_interval_ms

@app.callback(
    Output('track-map-graph', 'figure', allow_duplicate=True),
    # --- Trigger Change ---
    Input('interval-component-medium', 'n_intervals'), # Trigger periodically (e.g., every 1 sec)
    # --- State Inputs ---
    Input('current-track-layout-cache-key-store', 'data'), # Get the expected session ID
    State('track-map-graph', 'figure'), # Get the CURRENT figure shown in the graph.
    prevent_initial_call='initial_duplicate'
)
def initialize_track_map(n_intervals, expected_session_id, current_track_map_figure):
    logger.debug(f"Track Map Update: Triggered. Expected Session ID = {expected_session_id}")

    # Default empty/loading figure, using the INITIAL uirevision
    fig_empty_or_loading_map = create_empty_figure_with_message(
        TRACK_MAP_WRAPPER_HEIGHT, INITIAL_TRACK_MAP_UIREVISION, 
        "Loading track data...", TRACK_MAP_MARGINS
    )
    
    fig_empty_or_loading_map.layout.plot_bgcolor = 'rgb(30,30,30)'
    fig_empty_or_loading_map.layout.paper_bgcolor = 'rgba(0,0,0,0)'

    if not expected_session_id:
        logger.debug("Track Map Update: No expected_session_id. Returning empty map if not already shown.")
        if current_track_map_figure and \
           current_track_map_figure.get('layout', {}).get('uirevision') == INITIAL_TRACK_MAP_UIREVISION:
            return no_update
        return fig_empty_or_loading_map

    # UIRevision for a map WITH a specific track loaded
    data_loaded_uirevision = f"tracklayout_{expected_session_id}"

    # If the map is already showing the correct track, no need to update the base map
    if current_track_map_figure and \
       current_track_map_figure.get('layout', {}).get('uirevision') == data_loaded_uirevision:
        logger.debug(f"Track map already displaying correct layout uirevision: {data_loaded_uirevision}")
        return no_update 

    # --- Read from cache ---
    # Critical: Ensure this read happens correctly and sees the updated cache
    with app_state.app_state_lock:
        # Make a deep copy if there's any chance of modification, otherwise direct access is fine for read
        cached_data = app_state.track_coordinates_cache.copy() 
        driver_list_snapshot = app_state.timing_state.copy() 

    logger.debug(f"Track Map Update: Cache Read. Cached session_key='{cached_data.get('session_key')}', Expected='{expected_session_id}'")

    if cached_data.get('session_key') == expected_session_id and \
       cached_data.get('x') and cached_data.get('y'):
        logger.info(f"Track map: Cache HIT for {expected_session_id}. Drawing track.")
        
        fig_data = [
            go.Scatter(x=list(cached_data['x']), y=list(cached_data['y']), mode='lines', 
                       line=dict(color='grey', width=2), name='Track', hoverinfo='none')
        ]
        for car_num, driver_state in driver_list_snapshot.items():
            tla = driver_state.get('Tla', car_num)
            team_color = driver_state.get('TeamColour', '808080')
            if not team_color.startswith('#'): team_color = '#' + team_color
            fig_data.append(go.Scatter(
                x=[], y=[], mode='markers+text', name=tla, uid=car_num,
                marker=dict(size=8, color=team_color, line=dict(width=1, color='Black')),
                textfont=dict(size=8, color='white'), textposition='middle right',
                hoverinfo='text', text=tla 
            ))

        fig_layout = go.Layout(
            template='plotly_dark',
            uirevision=data_loaded_uirevision, # <<< Use the new dynamic uirevision FOR THIS DATA
            xaxis=dict(visible=False, showgrid=False, zeroline=False, showticklabels=False, 
                       range=cached_data.get('range_x'), autorange=False if cached_data.get('range_x') else True),
            yaxis=dict(visible=False, showgrid=False, zeroline=False, showticklabels=False, 
                       range=cached_data.get('range_y'), autorange=False if cached_data.get('range_y') else True, 
                       scaleanchor="x" if cached_data.get('range_x') and cached_data.get('range_y') else None, 
                       scaleratio=1 if cached_data.get('range_x') and cached_data.get('range_y') else None),
            showlegend=False, 
            plot_bgcolor='rgb(30,30,30)', 
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'), 
            margin=TRACK_MAP_MARGINS, 
            height=TRACK_MAP_WRAPPER_HEIGHT,
            annotations=[] # Ensure no "loading" annotation when track is drawn
        )
        return go.Figure(data=fig_data, layout=fig_layout)
    else: # Cache miss or incomplete data
        logger.info(f"Track map: Cache MISS or data incomplete for {expected_session_id}. Current cache key: {cached_data.get('session_key')}")
        # If already showing the "loading" map for this specific ID, don't keep re-sending it
        current_text = ""
        if current_track_map_figure and current_track_map_figure.get('layout', {}).get('annotations'):
            current_text = current_track_map_figure['layout']['annotations'][0].get('text', '')
        
        expected_loading_text = f"Track data loading for {expected_session_id}..."
        if current_track_map_figure and \
           current_track_map_figure.get('layout', {}).get('uirevision') == INITIAL_TRACK_MAP_UIREVISION and \
           current_text == expected_loading_text:
            return no_update

        fig_empty_or_loading_map.layout.annotations[0].text = expected_loading_text
        return fig_empty_or_loading_map

# --- >>> ADDED: Driver Dropdown Update Callback <<< ---
@app.callback(
    Output('driver-select-dropdown', 'options'),
    Input('interval-component-slow', 'n_intervals') # Update slowly
)
def update_driver_dropdown_options(n_intervals):
    """Updates the driver selection dropdown options."""
    logger.debug("Attempting to update driver dropdown options...")
    options = [{'label': 'No drivers available', 'value': '', 'disabled': True}] # Default
    try:
        with app_state.app_state_lock:
            # --- >>> READ FROM timing_state <<< ---
            timing_state_copy = app_state.timing_state.copy()
            # logger.debug(f"Raw app_state.timing_state keys: {list(timing_state_copy.keys())}")

        # Pass the timing state data to the updated helper function
        options = utils.generate_driver_options(timing_state_copy)
        logger.debug(f"Updating driver dropdown options: {len(options)} options generated.")
    except Exception as e:
         logger.error(f"Error generating driver dropdown options: {e}", exc_info=True)
         options = [{'label': 'Error loading drivers', 'value': '', 'disabled': True}]
    return options
    
@app.callback(
    Output('lap-time-driver-selector', 'options'),
    Input('interval-component-slow', 'n_intervals') # Update driver list slowly
)
def update_lap_chart_driver_options(n_intervals):
    """Updates the driver selection dropdown options for the lap chart."""
    # This can reuse the same utility as the main driver selector if the format is suitable
    # Or create a specific one if needed. For now, reuse.
    with app_state.app_state_lock:
        timing_state_copy = app_state.timing_state.copy()
    
    options = utils.generate_driver_options(timing_state_copy) # Assumes this returns list of {'label': ..., 'value': ...}
    # Set default selected drivers - e.g., top few, or based on some logic
    # For now, we'll let user select.
    return options


@app.callback(
    Output('lap-time-progression-graph', 'figure'),
    Input('lap-time-driver-selector', 'value'), # Renamed to selected_drivers_for_lap_chart
    Input('interval-component-medium', 'n_intervals')
)
def update_lap_time_progression_chart(selected_drivers_rnos, n_intervals):
    fig_empty_lap_prog = create_empty_figure_with_message(
        LAP_PROG_WRAPPER_HEIGHT, INITIAL_LAP_PROG_UIREVISION,
        "Select drivers for lap progression", LAP_PROG_MARGINS_EMPTY
    )

    if not selected_drivers_rnos:
        return fig_empty_lap_prog

    sorted_selection_key = "_".join(sorted(list(set(selected_drivers_rnos))))
    data_plot_uirevision = f"lap_prog_data_{sorted_selection_key}"

    with app_state.app_state_lock:
        lap_history_snapshot = {rno: list(laps) for rno, laps in app_state.lap_time_history.items()}
        timing_state_snapshot = app_state.timing_state.copy()

    # Start with a layout that has the correct uirevision and properties for a data plot
    fig_with_data = go.Figure(layout={
        'template': 'plotly_dark', 'uirevision': data_plot_uirevision,
        'height': LAP_PROG_WRAPPER_HEIGHT, 'margin': LAP_PROG_MARGINS_DATA,
        'xaxis_title': 'Lap Number', 'yaxis_title': 'Lap Time (s)',
        'hovermode': 'x unified', 'title_text':'Lap Time Progression', 'title_x':0.5, 'title_font_size':14,
        'showlegend':True, 'legend_title_text':'Drivers', 'legend_font_size':10,
        'annotations': [] # CRITICAL: Start with no annotations for data plots
    })
    
    data_actually_plotted = False # Flag to check if any traces were added
    min_time_overall, max_time_overall, max_laps_overall = float('inf'), float('-inf'), 0

    for driver_rno in selected_drivers_rnos:
        # ... (your existing logic for fetching tla, team_color, lap_numbers, lap_times_sec, hover_texts)
        driver_laps = lap_history_snapshot.get(driver_rno, [])
        if not driver_laps: continue
        driver_info = timing_state_snapshot.get(driver_rno, {})
        tla = driver_info.get('Tla', driver_rno)
        team_color_hex = driver_info.get('TeamColour', 'FFFFFF')
        if not team_color_hex.startswith('#'): team_color_hex = '#' + team_color_hex
        valid_laps = [lap for lap in driver_laps if lap.get('is_valid', True)]
        if not valid_laps: continue
        
        data_actually_plotted = True
        lap_numbers = [lap['lap_number'] for lap in valid_laps]
        lap_times_sec = [lap['lap_time_seconds'] for lap in valid_laps]
        
        if lap_numbers: max_laps_overall = max(max_laps_overall, max(lap_numbers))
        if lap_times_sec:
            min_time_overall = min(min_time_overall, min(lap_times_sec))
            max_time_overall = max(max_time_overall, max(lap_times_sec))

        hover_texts = [] # Rebuild hover texts
        for lap in valid_laps:
            total_seconds = lap['lap_time_seconds']
            minutes = int(total_seconds // 60)
            seconds_part = total_seconds % 60 # Use seconds_part for clarity
            time_formatted = f"{minutes}:{seconds_part:06.3f}" if minutes > 0 else f"{seconds_part:.3f}"
            hover_texts.append(f"<b>{tla}</b><br>Lap: {lap['lap_number']}<br>Time: {time_formatted}<br>Tyre: {lap['compound']}<extra></extra>")
        
        fig_with_data.add_trace(go.Scatter(
            x=lap_numbers, y=lap_times_sec, mode='lines+markers', name=tla,
            marker=dict(color=team_color_hex, size=5), line=dict(color=team_color_hex, width=1.5),
            hovertext=hover_texts, hoverinfo='text'
        ))

    if not data_actually_plotted:
        # If drivers were selected but no data was plotted (e.g., no valid laps for any of them)
        # Return the standard empty figure with a message
        fig_empty_lap_prog.layout.annotations[0].text = "No lap data for selected driver(s)."
        # Important: The uirevision for this state should be dynamic based on selection,
        # so it doesn't revert to the absolute initial if the selection itself changes.
        fig_empty_lap_prog.layout.uirevision = data_plot_uirevision # Or a specific "no_data_for_selection" uirevision
        return fig_empty_lap_prog

    # Configure axes for the data plot
    if min_time_overall != float('inf') and max_time_overall != float('-inf'):
        padding = (max_time_overall - min_time_overall) * 0.05 if max_time_overall > min_time_overall else 0.5
        fig_with_data.update_yaxes(visible=True, range=[min_time_overall - padding, max_time_overall + padding])
    else:
        fig_with_data.update_yaxes(visible=False) # Hide if no valid range (should be caught by data_actually_plotted)

    if max_laps_overall > 0:
        fig_with_data.update_xaxes(visible=True, range=[0.5, max_laps_overall + 0.5])
    else:
        fig_with_data.update_xaxes(visible=False) # Hide if no laps

    return fig_with_data


@app.callback(
    # Target className to hide/show
    Output("debug-data-accordion-item", "className"),
    # "value" is True or False for dbc.Switch
    Input("debug-mode-switch", "value"),
    # prevent_initial_call=True # Allow to run on load to set initial state
)
def toggle_debug_data_visibility(debug_mode_enabled):
    if debug_mode_enabled:
        logger.info("Debug mode enabled: Showing 'Other Data Streams'.")
        # Return its normal className (or "" if no other classes)
        return "mt-1"
        # "mt-1" was in the Accordion, so keeping it or similar.
        # If your AccordionItem has other classes, include them here.
    else:
        logger.info("Debug mode disabled: Hiding 'Other Data Streams'.")
        return "d-none"  # Bootstrap class to hide the element
    
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',  # Matches window.dash_clientside.clientside
        function_name='animateCarMarkers'  # Matches the function name in custom_script.js
    ),
    # Still need an Output, though JS modifies in place
    Output('track-map-graph', 'figure'),
    Input('car-positions-store', 'data'),
    # Passes the current figure as 'existingFigure' to JS
    State('track-map-graph', 'figure'),
    # Passes the graph's div ID as 'graphDivId' to JS
    State('track-map-graph', 'id'),
    State('clientside-update-interval', 'interval')
)

app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='setupTrackMapResizeListener'
    ),
    Output('track-map-graph', 'figure', allow_duplicate=True), # Dummy output, but must exist
    Input('track-map-graph', 'figure'), # Trigger when the figure is first set or updated
    prevent_initial_call='initial_duplicate'# Allow to run on initial load
)

# --- Final Log ---
logger.info("Callback definitions processed.")
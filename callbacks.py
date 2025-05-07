# callbacks.py
"""
Contains all the Dash callback functions for the application.
Handles UI updates, user actions, and plot generation.
"""

import logging
import json
import time
import datetime
from datetime import timezone
import threading
from pathlib import Path

import dash
from dash.dependencies import Input, Output, State
from dash import dcc, html, dash_table, no_update # Import no_update
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots # Import make_subplots
import numpy as np
import requests
from shapely.geometry import LineString, Point # <<< ADD SHAPELY IMPORT
from shapely.ops import nearest_points # <<< For snapping points to line
import math

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

@app.callback(
    Output('connection-status', 'children'),
    Output('connection-status', 'style'),
    Input('interval-component-fast', 'n_intervals')
)
def update_connection_status(n):
    """Updates the connection status indicator."""
    # --- >>> ADD DEBUG LOGGING <<< ---
    logger.debug(f"Running update_connection_status (Interval {n})")
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
            logger.debug(f"Read State: {state}, Conn: {status}, Recording: {is_rec}, RecFile: {rec_file}, RepFile: {rep_file}")
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
    Output('track-status-display', 'children'),
    Input('interval-component-medium', 'n_intervals')
)
def update_track_status_display(n):
    """Updates the track status display."""
    with app_state.app_state_lock:
        track_status_code = app_state.track_status_data.get('Status', '0')
        track_message = app_state.track_status_data.get('Message', '')
    track_status_map = {'1':"Clear",'2':"Yellow",'3':"SC?",'4':"SC",'5':"Red",'6':"VSC",'7':"VSC End"}
    track_status_label = track_status_map.get(track_status_code, f'? ({track_status_code})')
    display_text = f"Track: {track_status_label}"
    if track_message and track_message != 'AllClear': display_text += f" ({track_message})"
    return display_text

@app.callback(
    Output('session-info-display', 'children'),
    Input('interval-component-slow', 'n_intervals')
)
def update_session_info_display(n):
    """Updates the session info and weather display."""
    # (Logic from Response 22/24, using app_state)
    session_info_str = "Session: N/A"; weather_string = "Weather: N/A"
    try:
        with app_state.app_state_lock:
            local_session_details = app_state.session_details.copy()
            local_weather_data = app_state.data_store.get('WeatherData', {}).get('data', {})
            if not isinstance(local_weather_data, dict): local_weather_data = {}

        meeting = local_session_details.get('Meeting', {}).get('Name', '?')
        session = local_session_details.get('Name', '?')
        circuit = local_session_details.get('Circuit', {}).get('ShortName', '?')
        country = local_session_details.get('Country', {}).get('Name', '')

        parts = [];
        if circuit != '?': parts.append(f"{circuit}")
        if country: parts.append(f"({country})")
        if meeting != '?': parts.append(f"{meeting}")
        if session != '?': parts.append(f"Session: {session}")
        if parts: session_info_str = " | ".join(parts)

        elements = [];
        air = local_weather_data.get('AirTemp'); track = local_weather_data.get('TrackTemp')
        hum = local_weather_data.get('Humidity'); press = local_weather_data.get('Pressure')
        wind_s = local_weather_data.get('WindSpeed'); wind_d = local_weather_data.get('WindDirection')
        rain = local_weather_data.get('Rainfall')

        if air is not None: elements.append(f"Air: {air}°C")
        if track is not None: elements.append(f"Track: {track}°C")
        if hum is not None: elements.append(f"Hum: {hum}%")
        if press is not None: elements.append(f"Press: {press} hPa")
        if wind_s is not None: w_str=f"Wind: {wind_s} m/s"; w_str += f" ({wind_d}°)" if wind_d is not None else ""; elements.append(w_str)
        if rain == '1' or rain == 1: elements.append("RAIN")
        if elements: weather_string = " | ".join(elements)

        combined = dbc.Row([ dbc.Col(session_info_str, width="auto", style={'paddingRight': '15px'}), dbc.Col(weather_string, width="auto") ], justify="start", className="ms-1")
        return combined
    except Exception as e: logger.error(f"Session/Weather Display Error: {e}", exc_info=True); return "Error loading session info..."

@app.callback(
    Output('other-data-display', 'children'),
    Output('timing-data-actual-table', 'data'),
    Output('timing-data-timestamp', 'children'),
    Input('interval-component-fast', 'n_intervals')
)
def update_main_data_displays(n):
    """Updates the timing table and the 'other data' display area (Optimized)."""
    other_elements = []
    table_data = []
    timestamp_text = "Waiting..."
    start_time = time.monotonic()  # Time the callback

    try:
        with app_state.app_state_lock:
            # Copy only needed states under lock
            timing_state_copy = app_state.timing_state.copy()
            # No need to copy if only reading specific keys
            data_store_copy = app_state.data_store

        # --- Other Data Display (Keep previous logic) ---
        excluded_streams = ['TimingData', 'DriverList', 'Position.z', 'CarData.z', 'Position',
                            'TrackStatus', 'SessionData', 'SessionInfo', 'WeatherData', 'RaceControlMessages', 'Heartbeat']
        sorted_streams = sorted(
            [s for s in data_store_copy.keys() if s not in excluded_streams])
        for stream in sorted_streams:
            value = data_store_copy.get(stream, {})
            data_payload = value.get('data', 'N/A')
            timestamp_str = value.get('timestamp', 'N/A')
            try:
                data_str = json.dumps(data_payload, indent=2)
            except TypeError:
                data_str = str(data_payload)
            if len(data_str) > 500:
                data_str = data_str[:500] + "\n...(truncated)"
            other_elements.append(html.Details([html.Summary(f"{stream} ({timestamp_str})"), html.Pre(data_str, style={
                                  'marginLeft': '15px', 'maxHeight': '200px', 'overflowY': 'auto'})], open=(stream == "LapCount")))

        # --- Timing Table Timestamp ---
        timing_data_entry = data_store_copy.get(
            'TimingData', {})  # Read from non-copied dict
        timestamp_text = f"Timing TS: {timing_data_entry.get('timestamp', 'N/A')}" if timing_data_entry else "Waiting..."

        # --- Generate Timing Table Data (Optimized Loop) ---
        if timing_state_copy:  # Process the copied timing state
            processed_table_data = []
            # No need to sort keys here if sorting the final list later
            for car_num, driver_state in timing_state_copy.items():
                # Use .get() with defaults directly where possible
                # Use car_num as fallback for Car column
                tla = driver_state.get("Tla", car_num)
                pos = driver_state.get('Position', '-')
                compound = driver_state.get('TyreCompound', '-')
                age = driver_state.get('TyreAge', '?')
                tyre = f"{compound}({age}L)" if compound != '-' else '-'
                time_val = driver_state.get('Time', '-')
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
                status = driver_state.get('Status', 'N/A')
                # Access CarData sub-dict, default to empty dict if not present
                car_data = driver_state.get('CarData', {})
                speed = car_data.get('Speed', '-')
                gear = car_data.get('Gear', '-')
                rpm = car_data.get('RPM', '-')
                drs_val = car_data.get('DRS')
                drs_map = {8: "E", 10: "On", 12: "On", 14: "ON"}
                drs = drs_map.get(
                    drs_val, 'Off') if drs_val is not None else 'Off'

                row = {'Car': tla, 'Pos': pos, 'Tyre': tyre, 'Time': time_val, 'Gap': gap,
                       'Interval': interval, 'Last Lap': last_lap, 'Best Lap': best_lap,
                       'S1': s1, 'S2': s2, 'S3': s3, 'Status': status,
                       'Speed': speed, 'Gear': gear, 'RPM': rpm, 'DRS': drs}
                processed_table_data.append(row)

            # Sort the final list once
            processed_table_data.sort(key=utils.pos_sort_key)
            table_data = processed_table_data
        else:
            timestamp_text = "Waiting for DriverList..."

        end_time = time.monotonic()
        # Log execution time
        logger.debug(
            f"update_main_data_displays took {end_time - start_time:.4f}s")

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
        display_text = "\n".join(reversed(log_snapshot))
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


@app.callback(
    Output('dummy-output-for-controls', 'children'),
    Input('connect-button', 'n_clicks'), Input('disconnect-button', 'n_clicks'),
    Input('replay-button', 'n_clicks'), Input('stop-replay-button', 'n_clicks'),
    State('replay-file-selector', 'value'),
    State('replay-speed-slider', 'value'),   # <<< CORRECTED ID
    State('record-data-checkbox', 'value'),
    prevent_initial_call=True
)
def handle_control_clicks(connect_clicks, disconnect_clicks,
                          replay_clicks, stop_replay_clicks,
                          selected_replay_file, replay_speed,
                          record_checkbox_value):
    # (Combined Logic from Response 33)    
    ctx = dash.callback_context; button_id = ctx.triggered_id
    if not button_id: return no_update
    logger.info(f"Control button clicked: {button_id}")
    should_record = bool(record_checkbox_value)

    if button_id == 'connect-button':
        should_record = False # Default
        with app_state.app_state_lock:
            state = app_state.app_status["state"]
            # Read the flag set by the separate record_checkbox_callback
            should_record = app_state.record_live_data

        if state not in ["Idle", "Stopped", "Error", "Playback Complete"]:
            logger.warning(f"Connect ignored ({state})"); return no_update

        logger.info(f"Initiating connection sequence... Recording state from app_state: {should_record}")

        # --- Integrate Negotiation and Thread Start (from old start_live_callback) ---
        websocket_url, ws_headers = None, None
        try:
            with app_state.app_state_lock:
                 app_state.app_status.update({"state": "Initializing", "connection": "Negotiating..."})
                 app_state.stop_event.clear()

            # Call build_connection_url from signalr_client
            websocket_url, ws_headers = signalr_client.build_connection_url(
                config.NEGOTIATE_URL_BASE, config.HUB_NAME
            )
            if not websocket_url or not ws_headers:
                 raise ConnectionError("Negotiation failed to return URL or Headers.")

        except Exception as e:
            logger.error(f"Error during negotiation/setup: {e}", exc_info=True)
            with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Negotiation Failed"})
            return no_update # Error handled, update status via interval

        # --- Start Connection Thread ---
        if websocket_url and ws_headers:
            if should_record:
                logger.info("Recording enabled, initializing live file state...")
                if not replay.init_live_file(): # Calls the state-setting version
                    logger.error("Failed to initialize recording state. Proceeding without recording.")
                    # Update app_state flags if init failed? init_live_file should do this.
            else:
                logger.info("Recording disabled, clearing recording state...")
                replay.close_live_file() # Ensure state is cleared

            logger.info("Starting connection thread...")
            thread_obj = threading.Thread(
                target=signalr_client.run_connection_manual_neg, # Target the existing function
                args=(websocket_url, ws_headers),
                name="SignalRConnectionThread", daemon=True)

            # Manage thread reference within signalr_client
            signalr_client.connection_thread = thread_obj
            thread_obj.start()
            logger.info("Connection thread started.")
            # Status will update via interval based on app_state changes made by the thread
        else:
             logger.error("Cannot start connection thread: URL or Headers missing after negotiation.")
             with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Internal Setup Error"})
        # --- End Integrated Connection Logic ---

    # --- Disconnect Logic ---
    elif button_id == 'disconnect-button':
        logger.info("Disconnect button clicked")
        signalr_client.stop_connection() # Handles stopping thread and clearing recording state
        with app_state.app_state_lock: state = app_state.app_status["state"]
        if state == "Replaying": logger.info("Stopping replay due to disconnect click."); replay.stop_replay()
    elif button_id == 'replay-button':
        if selected_replay_file:
            with app_state.app_state_lock: state = app_state.app_status["state"]
            if state in ["Live", "Connecting"]: logger.info("Stopping live feed before replay."); signalr_client.stop_connection(); time.sleep(0.5)
            with app_state.app_state_lock: current_state_after_stop = app_state.app_status["state"]
            if current_state_after_stop != "Replaying":
                try:
                    speed_float = float(replay_speed); speed_float = max(0.1, speed_float)
                    full_replay_path = config.REPLAY_DIR / selected_replay_file
                    logger.info(f"Attempting replay: {full_replay_path}, Initial Speed: {speed_float}")
                    with app_state.app_state_lock: app_state.replay_speed = speed_float

                    replay_started_ok = replay.replay_from_file(full_replay_path, speed_float)
                    logger.debug(f"replay.replay_from_file returned: {replay_started_ok}") # Keep debug log

                    if replay_started_ok:
                        # Read state immediately after successful start attempt
                        with app_state.app_state_lock: current_state = app_state.app_status["state"]; current_conn = app_state.app_status["connection"]
                        status_text_update = f"State: {current_state} | Conn: {current_conn}" # Should be Initializing or Replaying
                        status_style_update = {'color': 'blue', 'fontWeight': 'bold'} # Style for replaying
                    else:
                        logger.error("replay.replay_from_file reported failure.")
                        # Update UI to reflect failure (likely already set to Error in replay_from_file)
                        with app_state.app_state_lock: current_state = app_state.app_status["state"]; current_conn = app_state.app_status["connection"]
                        status_text_update = f"State: {current_state} | Conn: {current_conn}"
                        status_style_update = {'color': 'purple', 'fontWeight': 'bold'}

                except (ValueError, TypeError):
                     logger.error(f"Invalid initial replay speed value from slider: '{replay_speed}'. Cannot start replay.")
                     with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Invalid Replay Speed"})
                     status_text_update = "State: Error | Conn: Invalid Replay Speed"
                     status_style_update = {'color': 'purple', 'fontWeight': 'bold'}
            else: logger.warning("Replay already in progress."); status_text_update = no_update; status_style_update = no_update
        else: logger.warning("Replay clicked, but no file selected."); status_text_update = no_update; status_style_update = no_update
    elif button_id == 'stop-replay-button': logger.info("Stop Replay button clicked"); replay.stop_replay()
    return no_update

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

# --- >>> NEW/MODIFIED: Driver Details & Telemetry Callback <<< ---
@app.callback(
    # Outputs
    Output('driver-details-output', 'children'),    # Basic info display area
    Output('lap-selector-dropdown', 'options'),     # Lap dropdown options
    Output('lap-selector-dropdown', 'value'),       # Selected lap value
    Output('lap-selector-dropdown', 'disabled'),    # Enable/disable lap dropdown
    Output('telemetry-graph', 'figure'),           # Telemetry plot
    # Inputs
    Input('driver-select-dropdown', 'value'),      # Trigger on driver change
    Input('lap-selector-dropdown', 'value'),       # Trigger on lap change
    # Use interval for potential updates while selection is static? Optional.
    # Input('interval-component-medium', 'n_intervals'),
    prevent_initial_call=True
)
def display_driver_details(selected_driver_number, selected_lap): # Removed n_intervals if not needed
    """Displays detailed data and telemetry plots for the selected driver and lap."""
    ctx = dash.callback_context
    triggered_id = ctx.triggered_id if ctx.triggered_id else 'N/A'
    logger.debug(f"display_driver_details triggered by: {triggered_id}")

    # Default outputs
    details_children = [html.P("Select a driver.")]
    lap_options = []
    lap_value = None
    lap_disabled = True
    telemetry_layout_uirevision = f"{selected_driver_number or 'none'}_{selected_lap or 'none'}"
    telemetry_figure = go.Figure(layout={'template': 'plotly_dark', 'height': 400, 'margin': dict(
        t=20, b=30, l=40, r=10), 'title_text': "Select Driver/Lap for Telemetry", 'uirevision': telemetry_layout_uirevision})  # Empty placeholder

    if not selected_driver_number:
        return details_children, lap_options, lap_value, lap_disabled, telemetry_figure

    driver_num_str = str(selected_driver_number)
    driver_changed = triggered_id == 'driver-select-dropdown'

    # --- Get available laps ---
    available_laps = []
    try:
        with app_state.app_state_lock:
            # Check if driver exists and has laps recorded
            if driver_num_str in app_state.telemetry_data and app_state.telemetry_data[driver_num_str]:
                available_laps = sorted(app_state.telemetry_data[driver_num_str].keys())
    except Exception as e: logger.error(f"Error retrieving laps for driver {driver_num_str}: {e}", exc_info=True)

    # --- Update Lap Selector ---
    if available_laps:
        lap_options = [{'label': f'Lap {l}', 'value': l} for l in available_laps]
        lap_disabled = False
        # Determine which lap value to show
        if driver_changed: # Driver just selected, default to latest lap
            lap_value = available_laps[-1]
        elif selected_lap in available_laps: # Lap was selected by user
            lap_value = selected_lap
        else: # Fallback (e.g., interval trigger, invalid previous selection)
             lap_value = available_laps[-1] if available_laps else None
    else: # No lap data found
        lap_options = [{'label': 'No Lap Data', 'value': ''}]
        lap_value = '' # Clear selection
        lap_disabled = True

    # --- Generate Basic Driver Details ---
    details_components = []
    tla = '?' # Default TLA
    try:
        with app_state.app_state_lock: driver_info = app_state.timing_state.get(driver_num_str, {})
        if driver_info:
            tla = driver_info.get('Tla', '?'); num = driver_info.get('RacingNumber', driver_num_str)
            name = driver_info.get('FullName', 'Unknown'); team = driver_info.get('TeamName', '?')
            details_components.append(html.H5(f"#{num} {tla} - {name} ({team})", style={'marginTop': '10px'}))
            # Add other info if needed
            ll = utils.get_nested_state(driver_info, 'LastLapTime', 'Value', default='-')
            bl = utils.get_nested_state(driver_info, 'BestLapTime', 'Value', default='-')
            tyre = f"{driver_info.get('TyreCompound','-')} ({driver_info.get('TyreAge','?')}L)" if driver_info.get('TyreCompound','-') != '-' else '-'
            details_components.append(html.P(f"Last Lap: {ll} | Best Lap: {bl} | Tyre: {tyre}", style={'fontSize':'small'}))
        else: details_components.append(html.P(f"Driver {driver_num_str} info not found."))
        details_children = html.Div(details_components)
    except Exception as e: logger.error(f"Error generating driver details: {e}"); details_children = html.P("Error loading driver details.")


    # --- Generate Telemetry Plot ---
    if lap_value and lap_value in available_laps:
        logger.debug(f"Generating telemetry plot for Driver {driver_num_str}, Lap {lap_value}")
        try:
            with app_state.app_state_lock: lap_data = app_state.telemetry_data.get(driver_num_str, {}).get(lap_value, {})

            timestamps_str = lap_data.get('Timestamps', [])
            timestamps_dt = [utils.parse_iso_timestamp_safe(ts) for ts in timestamps_str]

            # Find valid indices where timestamp parsing worked
            valid_indices = [i for i, dt in enumerate(timestamps_dt) if dt is not None]

            if valid_indices:
                timestamps_plot = [timestamps_dt[i] for i in valid_indices]

                # Define channels and create subplots
                channels = ['Speed', 'RPM', 'Throttle', 'Brake', 'Gear', 'DRS']
                fig = make_subplots(rows=len(channels), cols=1, shared_xaxes=True, subplot_titles=channels, vertical_spacing=0.02)
                fig.update_layout(template='plotly_dark', height=100*len(channels), hovermode="x unified", showlegend=False, margin=dict(t=40, b=30, l=50, r=10), uirevision=telemetry_layout_uirevision)

                for i, channel in enumerate(channels):
                    y_data_raw = lap_data.get(channel, [])
                    # Filter Y data using only valid indices, propagating None gaps
                    y_data_plot = [(y_data_raw[idx] if idx < len(y_data_raw) else None) for idx in valid_indices]
                    
                    if channel == 'DRS':
                        # Convert DRS states (e.g., 10, 12, 14 = ON=1, others=OFF=0)
                        drs_plot_values = []
                        for val in y_data_plot:
                            if val in [10, 12, 14]: # DRS flap open state values
                                drs_plot_values.append(1)
                            # elif val == 8: # Optionally show 'Eligible' state
                            #    drs_plot_values.append(0.5)
                            else: # Off, Ineligible, Error, None
                                drs_plot_values.append(0)
                        y_data_plot = drs_plot_values # Use the converted values

                        fig.add_trace(go.Scattergl(x=timestamps_plot, y=y_data_plot, mode='lines', name=channel,
                                                   line_shape='hv', # Use step shape for on/off
                                                   connectgaps=False), row=i+1, col=1)
                        # Customize Y axis ticks for DRS
                        fig.update_yaxes(tickvals=[0, 1], ticktext=['Off', 'On'], range=[-0.1, 1.1], row=i+1, col=1)
                    # --- >>> End DRS Handling <<< ---
                    else: # Plot other channels normally
                         fig.add_trace(go.Scattergl(x=timestamps_plot, y=y_data_plot, mode='lines', name=channel, connectgaps=False), row=i+1, col=1)
                         
                    # Potentially add axis title to yaxis
                    fig.update_yaxes(title_text=channel, row=i+1, col=1)

                fig.update_layout(title=f"Driver {driver_num_str} ({tla}) - Lap {lap_value} Telemetry")
                fig.update_xaxes(title_text="Time", row=len(channels), col=1) # Title only on bottom axis

                telemetry_figure = fig # Assign the generated figure
            else:
                logger.warning(f"No valid plot data found for Lap {lap_value}, Driver {driver_num_str}")
                telemetry_figure.update_layout(title=f"Lap {lap_value}: No telemetry data with valid timestamps")


        except Exception as plot_err:
             logger.error(f"Error generating telemetry plot: {plot_err}", exc_info=True)
             telemetry_figure.update_layout(title=f"Error generating plot for Lap {lap_value}")

    # Return all outputs in the correct order
    return details_children, lap_options, lap_value, lap_disabled, telemetry_figure

def get_valid_float_coord(pd, k, cn):
    if not isinstance(pd,dict): return None
    val=pd.get(k)
    if val is None: return None
    try: fv=float(val); return fv if not (math.isnan(fv) or math.isinf(fv)) else None
    except: return None

# --- Track Map Callback ---
@app.callback(
    Output('track-map-graph', 'figure'),
    Input('interval-component-slow', 'n_intervals')
)
def update_track_map(n):
    start_time_callback = time.monotonic()
    logger.debug(f"update_track_map (Static Plot v3) Tick {n}")

    empty_layout_dict = {
        'template': 'plotly_dark', 'height': 450, 'margin': dict(t=40, b=30, l=40, r=10),
        'xaxis': {'visible': False, 'showticklabels': False, 'range': [-1,1]}, 
        'yaxis': {'visible': False, 'showticklabels': False, 'range': [-1,1]}, 
        'title_text': "Track Map: Initializing..."
    }
    
    current_session_key = None
    track_x_coords, track_y_coords, x_range, y_range = None, None, None, None
    needs_api_fetch = False

    # --- Step 1: Determine Session Key & Attempt to Load Track Data from Cache ---
    with app_state.app_state_lock:
        year = app_state.session_details.get('Year'); circuit_key = app_state.session_details.get('CircuitKey')
        if not (year and circuit_key): # Fallback
            s_info = app_state.data_store.get('SessionInfo', {}).get('data', {})
            if isinstance(s_info, dict):
                if not circuit_key: circuit_key = s_info.get('Circuit', {}).get('Key')
                if not year: p = s_info.get('Path', ''); ps = p.split('/'); year = ps[1] if len(ps)>1 and ps[1].isdigit() and len(ps[1])==4 else None
        if year and circuit_key: current_session_key = f"{year}_{circuit_key}"
        
        cached = app_state.track_coordinates_cache 
        if current_session_key and cached.get('session_key') == current_session_key and cached.get('x') and cached.get('range_x'):
            track_x_coords=cached.get('x'); track_y_coords=cached.get('y'); x_range=cached.get('range_x'); y_range=cached.get('range_y')
            logger.debug(f"Using cached track coords/ranges for {current_session_key}.")
        elif current_session_key: 
            needs_api_fetch = True
            logger.info(f"Cache miss for session: {current_session_key}")
        else: 
            return go.Figure(layout=empty_layout_dict.update({'title_text': 'Map: Session Info Missing'}))

    # --- Step 2: Fetch API Data if Needed ---
    if needs_api_fetch:
        api_url = f"https://api.multiviewer.app/api/v1/circuits/{circuit_key}/{year}"; logger.info(f"Track API fetch for: {current_session_key}")
        # Reset variables - they will be assigned below if successful
        track_x_coords, track_y_coords, x_range, y_range = None, None, None, None 
        try:
            response=requests.get(api_url,headers={'User-Agent':'F1-Dashboard-App/0.5'},timeout=10, verify=False) 
            response.raise_for_status(); map_api_data = response.json()
            raw_x=map_api_data.get('x',[]); raw_y=map_api_data.get('y',[]); valid_coords_list=[]
            if len(raw_x)==len(raw_y):
                for xi,yi in zip(raw_x,raw_y):
                    try: 
                        xf,yf=float(xi),float(yi);
                        if not(math.isnan(xf) or math.isinf(xf) or math.isnan(yf) or math.isinf(yf)): valid_coords_list.append((xf,yf))
                    except: pass
            
            if len(valid_coords_list) > 1:
                # Assign directly to function-scoped variables
                track_x_coords=[c[0] for c in valid_coords_list]; track_y_coords=[c[1] for c in valid_coords_list]
                try:
                    x_m,x_M=np.min(track_x_coords),np.max(track_x_coords); y_m,y_M=np.min(track_y_coords),np.max(track_y_coords); 
                    padding_x=(x_M-x_m)*0.05; padding_y=(y_M-y_m)*0.05 
                    x_range=[x_m-padding_x,x_M+padding_x]; y_range=[y_m-padding_y,y_M+padding_y]
                    logger.info(f"Track API data successfully processed (coords/ranges): {current_session_key}")
                except Exception as e_range:
                    logger.error(f"Error calculating ranges: {e_range}")
                    x_range, y_range = None, None # Ensure ranges are None if calc fails
            else: 
                logger.warning(f"API no valid x/y pairs: {current_session_key}")
                # Ensure coords/ranges are None
                track_x_coords, track_y_coords, x_range, y_range = None, None, None, None 
            
            # Update cache with the processed values (which might be None)
            with app_state.app_state_lock: 
                app_state.track_coordinates_cache={'session_key':current_session_key,'x':track_x_coords,'y':track_y_coords,'linestring':None,'range_x':x_range,'range_y':y_range}
                logger.debug(f"Cache updated for {current_session_key} after API attempt.")
        except Exception as e: 
            logger.error(f"Track API FAILED: {e}", exc_info=True)
            # Ensure cache reflects failure
            with app_state.app_state_lock:
                 app_state.track_coordinates_cache={'session_key':current_session_key,'x':None,'y':None,'linestring':None,'range_x':None,'range_y':None}
            # Ensure local variables are None after failure
            track_x_coords, track_y_coords, x_range, y_range = None, None, None, None
    
    # --- CRITICAL CHECK for basic plotting (using variables from cache or API) ---
    if not track_x_coords or not track_y_coords or not x_range or not y_range:
        logger.error(f"Map Layout Data Missing/Invalid for {current_session_key} before plotting. Cannot plot map.")
        return go.Figure(layout=empty_layout_dict.update({'title_text': f"Map: Layout Data Missing ({current_session_key or '?'})"}))
    logger.debug(f"Track Coords and Ranges are VALID for {current_session_key}. Plotting static map.")

    # --- Step 3: Prepare STATIC Car Data ---
    # (Same as Response 92) ...
    static_traces = []
    with app_state.app_state_lock: timing_state_snapshot = app_state.timing_state.copy()
    driver_order = sorted(timing_state_snapshot.keys(), key=lambda x: int(x) if x.isdigit() else float('inf'))
    static_car_x = [None]*len(driver_order); static_car_y = [None]*len(driver_order)
    static_car_text = [""]*len(driver_order); static_car_colors = ["#808080"]*len(driver_order)
    static_car_opacities = [1.0]*len(driver_order)
    cars_plotted_count = 0
    for i, car_num_str in enumerate(driver_order):
        ds=timing_state_snapshot.get(car_num_str,{}); cpd=ds.get('PositionData')
        cX=get_valid_float_coord(cpd,'X',car_num_str); cY=get_valid_float_coord(cpd,'Y',car_num_str)
        static_car_x[i], static_car_y[i] = cX, cY
        if static_car_x[i] is not None: cars_plotted_count += 1
        status=ds.get('Status','').lower();tla=ds.get('Tla',car_num_str);static_car_colors[i]=f"#{ds.get('TeamColour','808080')}";off_track=('pit' in status or 'retired' in status or 'out' in status or 'stopped' in status);static_car_text[i]="" if off_track else tla;static_car_opacities[i]=0.3 if off_track else 1.0
    logger.debug(f"Prepared static data for {cars_plotted_count} cars.")
    static_traces.append(go.Scattergl(x=track_x_coords, y=track_y_coords, mode='lines', line=dict(color='grey', width=2), name='Track', hoverinfo='none'))
    static_traces.append(go.Scattergl(x=static_car_x, y=static_car_y, mode='markers+text', text=static_car_text, marker=dict(size=10, color=static_car_colors, line=dict(width=1,color='Black'), opacity=static_car_opacities), textposition='middle right', name='Cars', hoverinfo='text', textfont=dict(size=9, color='white')))
    
    # --- Step 4: Create Plotly Figure Layout ---
    final_layout_dict = go.Layout(
        xaxis=dict(showgrid=False,zeroline=False,showticklabels=False,range=x_range,visible=True), 
        yaxis=dict(showgrid=False,zeroline=False,showticklabels=False,range=y_range,scaleanchor="x",scaleratio=1,visible=True), 
        showlegend=False, uirevision=current_session_key, 
        plot_bgcolor='rgb(30,30,30)', paper_bgcolor='rgba(0,0,0,0)', 
        font=dict(color='white'), margin=dict(l=5,r=5,t=40,b=30), 
        title_text=f"Track Map ({current_session_key or '?'})"
    )
    final_figure = go.Figure(data=static_traces, layout=final_layout_dict) 
    
    logger.debug(f"Map update (Static Plot) took {time.monotonic() - start_time_callback:.4f}s")
    return final_figure

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


# --- Final Log ---
logger.info("Callback definitions processed.")
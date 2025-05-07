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


# --- Track Map Callback ---
@app.callback(
    Output('track-map-graph', 'figure'),
    Input('interval-component-medium', 'n_intervals')
)
def update_track_map(n):
    """Updates the track map with cars animated along the track path using Shapely."""
    start_time_callback = time.monotonic()
    logger.debug(f"update_track_map (Shapely Path Animation) Tick {n} Start")

    # Default empty figure
    empty_layout = go.Layout(template='plotly_dark', height=450, margin=dict(t=30, b=5, l=5, r=5),
                             xaxis={'visible': False}, yaxis={'visible': False}, title_text="Track Map: Loading...")
    current_figure = go.Figure(data=[], layout=empty_layout) # Start with an empty figure

    current_session_key = None
    needs_api_fetch = False
    year_from_state = None
    circuit_key_from_state = None
    track_linestring_obj = None # Will hold the Shapely LineString object
    track_x_coords, track_y_coords = None, None
    x_range, y_range = None, None

    # --- Step 1: Determine Session Key & Get/Prepare Track Layout ---
    with app_state.app_state_lock:
        # ... (logic to get year_from_state, circuit_key_from_state, current_session_key - same as Response 61/66) ...
        year_from_state = app_state.session_details.get('Year'); circuit_key_from_state = app_state.session_details.get('CircuitKey')
        if not year_from_state or not circuit_key_from_state:
             session_info_local = app_state.data_store.get('SessionInfo', {}).get('data',{})
             if isinstance(session_info_local, dict):
                 if not circuit_key_from_state: circuit_key_from_state = session_info_local.get('Circuit', {}).get('Key')
                 if not year_from_state:
                      path = session_info_local.get('Path', ''); parts = path.split('/')
                      if len(parts) > 1 and parts[1].isdigit() and len(parts[1]) == 4: year_from_state = parts[1]
        if year_from_state and circuit_key_from_state: current_session_key = f"{year_from_state}_{circuit_key_from_state}"

        cached_session_key = app_state.track_coordinates_cache.get('session_key')
        if current_session_key and cached_session_key == current_session_key:
            track_x_coords = app_state.track_coordinates_cache.get('x')
            track_y_coords = app_state.track_coordinates_cache.get('y')
            track_linestring_obj = app_state.track_coordinates_cache.get('linestring') # Get cached shapely object
            x_range = app_state.track_coordinates_cache.get('range_x')
            y_range = app_state.track_coordinates_cache.get('range_y')
            if track_linestring_obj is None and track_x_coords and track_y_coords: # Create if missing from cache
                try:
                    track_linestring_obj = LineString(zip(track_x_coords, track_y_coords))
                    app_state.track_coordinates_cache['linestring'] = track_linestring_obj
                except Exception as e_shape:
                    logger.error(f"Error creating shapely LineString from cached coords: {e_shape}")
                    track_linestring_obj = None # Ensure it's None on error

        elif current_session_key: needs_api_fetch = True
        else: logger.debug("Map: No valid session key."); current_figure.update_layout(title_text="Track Map: Waiting for Session Info"); return current_figure

    # --- Step 2: Fetch API data if needed (OUTSIDE lock) ---
    if needs_api_fetch:
        # ... (API fetch logic - same as Response 61/66) ...
        # Ensure it sets track_x_coords, track_y_coords, x_range, y_range
        # And critically, creates and caches track_linestring_obj
        api_url = f"https://api.multiviewer.app/api/v1/circuits/{circuit_key_from_state}/{year_from_state}"; logger.info(f"Track API fetch: {api_url}")
        try:
            response=requests.get(api_url, headers={'User-Agent':'F1-Dash/0.4'}, timeout=10); response.raise_for_status(); map_api_data=response.json()
            temp_x = [float(p) for p in map_api_data.get('x',[])]; temp_y = [float(p) for p in map_api_data.get('y',[])]
            if temp_x and temp_y and len(temp_x)==len(temp_y) and len(temp_x)>1:
                track_x_coords=temp_x; track_y_coords=temp_y; track_linestring_obj=LineString(zip(track_x_coords,track_y_coords))
                x_min,x_max=np.min(track_x_coords),np.max(track_x_coords); y_min,y_max=np.min(track_y_coords),np.max(track_y_coords)
                pad_x=(x_max-x_min)*0.05; pad_y=(y_max-y_min)*0.05; x_range=[x_min-pad_x,x_max+pad_x]; y_range=[y_min-pad_y,y_max+pad_y]
                with app_state.app_state_lock: app_state.track_coordinates_cache={'session_key':current_session_key,'x':track_x_coords,'y':track_y_coords,'linestring':track_linestring_obj,'range_x':x_range,'range_y':y_range}; logger.info(f"Track API SUCCESS: {current_session_key}")
            else: logger.warning(f"Track API no valid x/y: {current_session_key}");
            track_linestring_obj=None; 
            with app_state.app_state_lock: app_state.track_coordinates_cache['session_key']=current_session_key
        except Exception as e: logger.error(f"Track API FAILED: {e}"); track_linestring_obj=None; 
        with app_state.app_state_lock: app_state.track_coordinates_cache['session_key']=current_session_key

    if not track_linestring_obj or not track_x_coords:
        current_figure.update_layout(title_text=f"Track Map: Layout data unavailable ({current_session_key})"); return current_figure

    # --- Step 3: Prepare Car Data and Interpolation ---
    base_traces = []
    animation_frames = []
    N_INTERPOLATION_STEPS = 5 # Number of intermediate steps (total frames = N_INTERPOLATION_STEPS + 1)

    with app_state.app_state_lock:
        timing_state_snapshot = app_state.timing_state.copy()

    # Create a mapping from car_num_str to its index in the plot data for consistent frame updates
    driver_order = sorted(timing_state_snapshot.keys(), key=lambda x: int(x) if x.isdigit() else float('inf'))
    driver_index_map = {driver_num: i for i, driver_num in enumerate(driver_order)}

    # Initialize data lists for the base frame (previous positions)
    base_car_x = [None] * len(driver_order)
    base_car_y = [None] * len(driver_order)
    base_car_text = [""] * len(driver_order)
    base_car_colors = ["#808080"] * len(driver_order) # Default grey
    base_car_opacities = [1.0] * len(driver_order)

    # Populate base frame data
    for i, car_num_str in enumerate(driver_order):
        driver_state = timing_state_snapshot.get(car_num_str, {})
        pos_data_to_plot = driver_state.get('PreviousPositionData', driver_state.get('PositionData')) # Use previous, fallback to current
        if pos_data_to_plot and pos_data_to_plot.get('X') is not None:
            try:
                base_car_x[i] = float(pos_data_to_plot['X'])
                base_car_y[i] = float(pos_data_to_plot['Y'])
            except (TypeError, ValueError): pass # Keep None if conversion fails
        
        status = driver_state.get('Status', '').lower(); tla = driver_state.get('Tla', car_num_str)
        base_car_colors[i] = f"#{driver_state.get('TeamColour','808080')}"
        off_track = ('pit' in status or 'retired' in status or 'out' in status or 'stopped' in status)
        base_car_text[i] = "" if off_track else tla
        base_car_opacities[i] = 0.3 if off_track else 1.0

    # Track outline trace (always first)
    base_traces.append(go.Scattergl(x=track_x_coords, y=track_y_coords, mode='lines', line=dict(color='grey', width=2), name='Track', hoverinfo='none'))
    # Base car positions trace
    base_traces.append(go.Scattergl(x=base_car_x, y=base_car_y, mode='markers+text', text=base_car_text,
                                  marker=dict(size=10, color=base_car_colors, line=dict(width=1,color='Black'), opacity=base_car_opacities),
                                  textposition='middle right', name='Cars', hoverinfo='text', textfont=dict(size=9, color='white')))

    # --- Create animation frames ---
    for k in range(N_INTERPOLATION_STEPS + 1): # From step 0 (previous) to N_INTERPOLATION_STEPS (current)
        frame_x = list(base_car_x) # Start with previous positions for this frame
        frame_y = list(base_car_y)
        fraction = k / N_INTERPOLATION_STEPS # From 0.0 to 1.0

        for i, car_num_str in enumerate(driver_order):
            driver_state = timing_state_snapshot.get(car_num_str)
            if not driver_state: continue

            prev_pos = driver_state.get('PreviousPositionData')
            curr_pos = driver_state.get('PositionData')

            if prev_pos and curr_pos and \
               prev_pos.get('X') is not None and curr_pos.get('X') is not None and \
               prev_pos.get('Timestamp') != curr_pos.get('Timestamp'): # Animate if data is valid and different
                try:
                    prev_point_geom = Point(float(prev_pos['X']), float(prev_pos['Y']))
                    curr_point_geom = Point(float(curr_pos['X']), float(curr_pos['Y']))

                    # Project to track line
                    # _, p_prev_on_track = nearest_points(track_linestring_obj, prev_point_geom)
                    # _, p_curr_on_track = nearest_points(track_linestring_obj, curr_point_geom)
                    # Using project which is more direct for distances
                    dist_prev = track_linestring_obj.project(prev_point_geom)
                    dist_curr = track_linestring_obj.project(curr_point_geom)
                    
                    track_len = track_linestring_obj.length
                    # Handle lap crossover: if current distance is small and previous was near end
                    if dist_curr < track_len * 0.1 and dist_prev > track_len * 0.9:
                        dist_curr += track_len
                    
                    interp_dist_along_track = dist_prev + (dist_curr - dist_prev) * fraction
                    # Ensure interpolated distance is within bounds for .interpolate method
                    interp_dist_normalized = max(0, min(interp_dist_along_track, dist_curr if dist_curr >= dist_prev else dist_curr + track_len)) # Crude clamp
                    
                    # Normalize to actual track length if it went over due to crossover logic
                    final_interp_dist = interp_dist_normalized % track_len

                    interpolated_point = track_linestring_obj.interpolate(final_interp_dist)
                    
                    frame_x[i] = interpolated_point.x
                    frame_y[i] = interpolated_point.y
                except Exception as e_interp:
                    logger.debug(f"Interpolation error car {car_num_str} frame {k}: {e_interp}")
                    # If error, keep at previous position (already in frame_x[i], frame_y[i] from base)
                    if frame_x[i] is None and prev_pos: frame_x[i] = float(prev_pos['X']) # Fallback
                    if frame_y[i] is None and prev_pos: frame_y[i] = float(prev_pos['Y']) # Fallback

            elif curr_pos and curr_pos.get('X') is not None: # No previous or same timestamp, just plot current
                frame_x[i] = float(curr_pos['X'])
                frame_y[i] = float(curr_pos['Y'])

        # Create frame object (only need to update the car trace, which is index 1)
        animation_frames.append(go.Frame(
            data=[go.Scattergl(x=frame_x, y=frame_y)], # Only update x, y of the car trace
            name=f'frame{k}',
            traces=[1] # Specifies that this frame data applies to the second trace (index 1)
        ))


    # --- Create Figure with Animation ---
    xaxis_cfg = dict(showgrid=False, zeroline=False, showticklabels=False, range=x_range)
    yaxis_cfg = dict(showgrid=False, zeroline=False, showticklabels=False, range=y_range)
    if x_range and y_range: yaxis_cfg['scaleanchor']="x"; yaxis_cfg['scaleratio']=1

    # Calculate frame duration based on typical data interval (e.g., 1 sec)
    # If map updates every 1.5s, and data comes at 1s, animation should ideally complete in ~1s.
    # Let's aim for the animation to cover the time until the next expected data update.
    # For 1000ms data interval and 5 steps -> 200ms per frame.
    frame_duration = 800 / N_INTERPOLATION_STEPS # e.g., 1000ms / 5 steps = 200ms

    layout = go.Layout(
        xaxis=xaxis_cfg, yaxis=yaxis_cfg, showlegend=False,
        uirevision=current_session_key, # uirevision based on session for overall view
        plot_bgcolor='rgb(30,30,30)', paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'), margin=dict(l=5,r=5,t=30,b=5),
        title_text=f"Track Map ({current_session_key or '?'})",
        updatemenus=[dict(
            type="buttons", direction="left", x=0.1, xanchor="right", y=0, yanchor="top",
            buttons=[
                dict(label="Play", method="animate",
                     args=[None, {"frame": {"duration": frame_duration, "redraw": True}, # Redraw for each frame
                                  "fromcurrent": True,
                                  "transition": {"duration": frame_duration * 0.8, "easing": "linear"}, # Smooth transition
                                  "mode": "immediate"}]),
                dict(label="Pause", method="animate",
                     args=[[None], {"frame": {"duration": 0, "redraw": False},
                                    "mode": "immediate",
                                    "transition": {"duration": 0}}])
            ]
        )],
        # Optional: Add a slider to manually scrub through frames
        # sliders=[dict(
        #     active=0, currentvalue={"prefix": "Step: "}, pad={"t": 50},
        #     steps=[dict(label=str(k), method="animate",
        #                 args=[[f'frame{k}'], {"mode": "immediate", "frame": {"duration": 100, "redraw": True}}])
        #            for k in range(N_INTERPOLATION_STEPS + 1)]
        # )]
    )

    # Remove the placeholder empty scatter from frames (if any were added by default)
    # The base_traces already define the initial state for trace 1
    valid_animation_frames = [f for f in animation_frames if f.data and f.data[0].x and f.data[0].y]


    if not valid_animation_frames and not base_traces[1].x: # No cars to plot at all
        logger.debug("No car data to animate or plot.")
        final_figure = go.Figure(data=[base_traces[0]] if base_traces else [], layout=layout) # Show only track if no cars
        final_figure.update_layout(title_text=f"Track Map: No Car Data ({current_session_key or '?'})")
    elif not valid_animation_frames and base_traces[1].x: # Cars present but no animation frames (e.g. all static)
        logger.debug("Cars present but no animation frames generated, showing static positions.")
        final_figure = go.Figure(data=base_traces, layout=layout)
    elif valid_animation_frames:
        final_figure = go.Figure(data=base_traces, layout=layout, frames=valid_animation_frames)
    else: # Fallback, should not happen if logic is correct
        logger.warning("Unexpected case in map figure generation.")
        final_figure = go.Figure(data=[base_traces[0]] if base_traces else [], layout=layout)
        final_figure.update_layout(title_text=f"Track Map: Error generating animation ({current_session_key or '?'})")


    logger.debug(f"Map update (Path Animation) took {time.monotonic() - start_time_callback:.4f}s")
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
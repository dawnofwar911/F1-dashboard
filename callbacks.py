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
    with app_state.app_state_lock:
        status = app_state.app_status.get("connection", "Unknown")
        state = app_state.app_status.get("state", "Idle")
        is_rec = app_state.is_saving_active
        rec_file = app_state.current_recording_filename
        rep_file = app_state.app_status.get("current_replay_file")

    status_text = f"State: {state} | Conn: {status}"
    color = 'grey'
    if state == "Live": color = 'green'
    elif state in ["Connecting", "Initializing"]: color = 'orange'
    elif state in ["Stopped", "Idle"]: color = 'grey'
    elif state == "Error": color = 'purple'
    elif state == "Replaying": color = 'blue'
    elif state == "Playback Complete": color = 'lightblue'
    elif state == "Stopping": color = 'lightcoral'

    if is_rec and rec_file: status_text += f" (REC: {Path(rec_file).name})"
    elif state == "Replaying" and rep_file: status_text += f" (Replay: {rep_file})"

    return status_text, {'color': color, 'fontWeight': 'bold'}

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
    Input('interval-component-medium', 'n_intervals')
)
def update_main_data_displays(n):
    """Updates the timing table and the 'other data' display area."""
    # (Logic combined and adapted from user's update_output in Response 24)
    other_elements = []; table_data = []; timestamp_text = "Waiting..."
    try:
        with app_state.app_state_lock:
            timing_state_copy = app_state.timing_state.copy()
            data_store_copy = app_state.data_store.copy()

        # Other Data
        excluded = ['TimingData', 'DriverList', 'Position.z', 'CarData.z', 'Position', 'TrackStatus', 'SessionData', 'SessionInfo', 'WeatherData', 'RaceControlMessages', 'Heartbeat']
        streams = sorted([s for s in data_store_copy.keys() if s not in excluded])
        for s_name in streams:
            val = data_store_copy.get(s_name, {}); data=val.get('data','N/A'); ts=val.get('timestamp','N/A')
            try: d_str = json.dumps(data, indent=2)
            except TypeError: d_str = str(data)
            if len(d_str) > 500: d_str = d_str[:500] + "\n...(truncated)"
            other_elements.append(html.Details([html.Summary(f"{s_name} ({ts})"), html.Pre(d_str, style={'marginLeft':'15px','maxHeight':'200px','overflowY':'auto'})], open=(s_name=='LapCount'))) # Example open

        # Timing Table
        timing_entry = data_store_copy.get('TimingData', {})
        timestamp_text = f"Timing TS: {timing_entry.get('timestamp', 'N/A')}" if timing_entry else "Waiting..."
        if timing_state_copy:
            t_data = []
            drivers = sorted(timing_state_copy.keys(), key=lambda x: int(x) if x.isdigit() else float('inf'))
            for num in drivers:
                state = timing_state_copy.get(num);
                if not state: continue
                tyre = f"{state.get('TyreCompound', '-')}({state.get('TyreAge', '?')}L)" if state.get('TyreCompound', '-') != '-' else '-'
                tla = state.get("Tla", num)
                c_data = state.get('CarData', {}); drs_val = c_data.get('DRS'); drs_map = {8:"E",10:"On",12:"On",14:"ON"}; drs = drs_map.get(drs_val, 'Off') if drs_val is not None else 'Off'
                row = {'Car':tla, 'Pos':state.get('Position', '-'), 'Tyre':tyre, 'Time':state.get('Time', '-'), 'Gap':state.get('GapToLeader', '-'),
                       'Interval':utils.get_nested_state(state,'IntervalToPositionAhead','Value',default='-'), 'Last Lap':utils.get_nested_state(state,'LastLapTime','Value',default='-'),
                       'Best Lap':utils.get_nested_state(state,'BestLapTime','Value',default='-'), 'S1':utils.get_nested_state(state,'Sectors','0','Value',default='-'),
                       'S2':utils.get_nested_state(state,'Sectors','1','Value',default='-'), 'S3':utils.get_nested_state(state,'Sectors','2','Value',default='-'),
                       'Status':state.get('Status','N/A'), 'Speed':c_data.get('Speed','-'), 'Gear':c_data.get('Gear','-'), 'RPM':c_data.get('RPM','-'), 'DRS':drs}
                t_data.append(row)
            t_data.sort(key=utils.pos_sort_key); table_data = t_data
        else: timestamp_text = "Waiting for DriverList..."

        return other_elements, table_data, timestamp_text
    except Exception as e: logger.error(f"Error updating main data: {e}", exc_info=True); return no_update, no_update, no_update


@app.callback(
    Output('race-control-log-display', 'value'),
    Input('interval-component-medium', 'n_intervals')
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

    logger.debug(f"Replay speed slider changed to: {new_speed}")
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
    
    print(f"DEBUG: handle_control_clicks entered!", flush=True)
    
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
            with app_state.app_state_lock: current_state_after_stop = app_state.app_status["state"] # Re-check state
            if current_state_after_stop != "Replaying":
                # --- >>> Set initial speed in app_state <<< ---
                try:
                    speed_float = float(app_state.initial_replay_speed)
                    with app_state.app_state_lock:
                         app_state.replay_speed = speed_float
                    logger.info(f"Attempting replay: {selected_replay_file}, Initial Speed: {speed_float}")
                    # Pass initial speed for logging, but thread will read app_state
                    if not replay.replay_from_file(selected_replay_file, speed_float):
                         logger.error("Replay start failed.")
                except (ValueError, TypeError):
                     logger.error(f"Invalid initial replay speed: {initial_replay_speed}. Cannot start replay.")
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
    telemetry_figure = go.Figure(layout={'template': 'plotly_dark', 'height': 400, 'margin': dict(t=20, b=30, l=40, r=10)}) # Empty placeholder

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
                fig.update_layout(template='plotly_dark', height=200*len(channels), hovermode="x unified", showlegend=False, margin=dict(t=40, b=30, l=50, r=10))

                for i, channel in enumerate(channels):
                    y_data_raw = lap_data.get(channel, [])
                    # Filter Y data using only valid indices, propagating None gaps
                    y_data_plot = [(y_data_raw[idx] if idx < len(y_data_raw) else None) for idx in valid_indices]
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
    """Updates the track map using fetched track layout and live car positions."""
    # (Using the implementation from Response 18/21, adapted for app_state)
    # This needs the full logic for fetching API data and plotting cars
    start_time_map = time.time()
    logger.debug(f"Updating track map (tick {n})...")
    # --- Step 1: Determine Session Key and Check Cache Match ---
    current_session_key = None; needs_api_fetch = False; year_from_state = None; circuit_key_from_state = None
    with app_state.app_state_lock:
        year_from_state = app_state.session_details.get('Year'); circuit_key_from_state = app_state.session_details.get('CircuitKey')
        if not year_from_state or not circuit_key_from_state: # Fallback using SessionInfo from data_store
             session_info_local = app_state.data_store.get('SessionInfo', {}).get('data',{})
             if isinstance(session_info_local, dict):
                 if not circuit_key_from_state: circuit_key_from_state = session_info_local.get('Circuit', {}).get('Key')
                 if not year_from_state:
                      path = session_info_local.get('Path', ''); parts = path.split('/')
                      if len(parts) > 1 and parts[1].isdigit() and len(parts[1]) == 4: year_from_state = parts[1]
        if year_from_state and circuit_key_from_state: current_session_key = f"{year_from_state}_{circuit_key_from_state}"
        cached_session_key = app_state.track_coordinates_cache.get('session_key')
    if current_session_key and cached_session_key != current_session_key: needs_api_fetch = True; logger.debug(f"Map: Cache miss for {current_session_key}.")
    elif not current_session_key: logger.debug("Map: No session key.")
    # --- Step 2: Fetch API data if needed (OUTSIDE lock) ---
    api_data = None
    if needs_api_fetch and current_session_key:
        api_url = f"https://api.multiviewer.app/api/v1/circuits/{circuit_key_from_state}/{year_from_state}"; logger.info(f"Track API fetch: {api_url}")
        try:
            headers = { 'User-Agent': 'F1-Dashboard-App/0.3' }; response = requests.get(api_url, headers=headers, timeout=10); response.raise_for_status(); map_api_data = response.json()
            extracted_data = {}
            try: extracted_data['x'] = [float(p) for p in map_api_data.get('x',[])]; extracted_data['y'] = [float(p) for p in map_api_data.get('y',[])]
            except: extracted_data['x'] = None; extracted_data['y'] = None
            if extracted_data.get('x') and extracted_data.get('y'):
                 try:
                     x_min, x_max = np.min(extracted_data['x']), np.max(extracted_data['x']); y_min, y_max = np.min(extracted_data['y']), np.max(extracted_data['y'])
                     padding_x = (x_max - x_min) * 0.05; padding_y = (y_max - y_min) * 0.05
                     extracted_data['range_x'] = [x_min - padding_x, x_max + padding_x]; extracted_data['range_y'] = [y_min - padding_y, y_max + padding_y]
                 except Exception as range_err: logger.error(f"Map range error: {range_err}"); extracted_data['range_x'] = None; extracted_data['range_y'] = None
                 api_data = extracted_data; logger.info(f"Track API SUCCESS for {current_session_key}")
            else: logger.warning(f"Track API did not yield valid x/y: {current_session_key}"); api_data = None
        except Exception as e: logger.error(f"Track API FAILED: {e}"); api_data = None
    # --- Step 3: Update cache & read plot data (Lock) ---
    track_x, track_y, x_range, y_range = None, None, None, None; drivers_x, drivers_y, drivers_text, drivers_color, drivers_opacity = [], [], [], [], []; plot_session_key = None
    with app_state.app_state_lock:
        if needs_api_fetch:
            if api_data: app_state.track_coordinates_cache = {**api_data, 'session_key': current_session_key}; logger.debug("Cache updated.")
            else: app_state.track_coordinates_cache = {'session_key': current_session_key, 'x':None, 'y':None, 'range_x':None, 'range_y':None}; logger.debug("Cache key updated, coords cleared.")
        plot_session_key = app_state.track_coordinates_cache.get('session_key')
        if plot_session_key == current_session_key: track_x, track_y, x_range, y_range = app_state.track_coordinates_cache.get('x'), app_state.track_coordinates_cache.get('y'), app_state.track_coordinates_cache.get('range_x'), app_state.track_coordinates_cache.get('range_y')
        timing_state_snapshot = app_state.timing_state.copy() # Copy state needed for cars
    # --- Process cars (outside lock) ---
    for car_num, state in timing_state_snapshot.items():
         pos_data = state.get('PositionData', state); x_val, y_val = pos_data.get('X'), pos_data.get('Y')
         if x_val is not None and y_val is not None:
             try:
                 x, y = float(x_val), float(y_val); status = state.get('Status','').lower(); tla = state.get('Tla', car_num); color = f"#{state.get('TeamColour','808080')}"
                 off_track = ('pit' in status or 'retired' in status or 'out' in status or 'stopped' in status)
                 drivers_x.append(x); drivers_y.append(y); drivers_text.append("" if off_track else tla); drivers_color.append(color); drivers_opacity.append(0.3 if off_track else 1.0)
             except (ValueError, TypeError): logger.warning(f"Map: Bad pos data for {car_num}: {x_val}, {y_val}")
    # --- Step 4: Create Plotly Figure ---
    figure_data = []; final_figure = go.Figure(layout={'template':'plotly_dark', 'margin':dict(t=30, b=5, l=5, r=5), 'xaxis':{'visible':False}, 'yaxis':{'visible':False}}) # Default empty
    try:
        if track_x and track_y: figure_data.append(go.Scatter(x=track_x, y=track_y, mode='lines', line=dict(color='grey', width=2), name='Track', hoverinfo='none'))
        if drivers_x: figure_data.append(go.Scatter(x=drivers_x, y=drivers_y, mode='markers+text', marker=dict(size=10, color=drivers_color, line=dict(width=1,color='Black'), opacity=drivers_opacity), text=drivers_text, textposition='middle right', name='Cars', hoverinfo='text', textfont=dict(size=9, color='white')))
        if figure_data:
            xaxis_cfg = dict(showgrid=False, zeroline=False, showticklabels=False, range=x_range); yaxis_cfg = dict(showgrid=False, zeroline=False, showticklabels=False, range=y_range)
            if x_range and y_range: yaxis_cfg['scaleanchor']="x"; yaxis_cfg['scaleratio']=1
            layout = go.Layout(xaxis=xaxis_cfg, yaxis=yaxis_cfg, showlegend=False, uirevision=plot_session_key, plot_bgcolor='rgb(30,30,30)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color='white'), margin=dict(l=5,r=5,t=30,b=5), title=f"Track Map ({plot_session_key})" if plot_session_key else "Track Map")
            final_figure = go.Figure(data=figure_data, layout=layout)
        else: final_figure.update_layout(title="Track Map: Waiting for Data...")
    except Exception as fig_err: logger.error(f"Map figure error: {fig_err}", exc_info=True); final_figure.update_layout(title="Error Creating Map")
    logger.debug(f"Map update took {time.time() - start_time_map:.4f}s")
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
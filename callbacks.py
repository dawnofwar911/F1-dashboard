# callbacks.py
"""
Contains all the Dash callback functions for the application.
Handles UI updates, user actions, and plot generation.
"""
import datetime
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
    session_info_str = config.TEXT_SESSION_INFO_AWAITING # Use constant
    weather_details_spans = []
    main_weather_icon = config.WEATHER_ICON_MAP["default"] # Use constant
    weather_card_color = "light"
    weather_card_inverse = False

    try:
        with app_state.app_state_lock:
            local_session_details = app_state.session_details.copy()
            raw_weather_payload = app_state.data_store.get('WeatherData', {})
            local_weather_data = raw_weather_payload.get('data', {}) if isinstance(raw_weather_payload, dict) else {}
            if not isinstance(local_weather_data, dict):
                local_weather_data = {}

        meeting = local_session_details.get('Meeting', {}).get('Name', '?')
        session_name = local_session_details.get('Name', '?')
        circuit = local_session_details.get('Circuit', {}).get('ShortName', '?')
        parts = []
        if circuit != '?': parts.append(f"{circuit}")
        if meeting != '?': parts.append(f"{meeting}")
        if session_name != '?': parts.append(f"Session: {session_name}")
        if parts: session_info_str = " | ".join(parts)


        def safe_float(value, default=None):
            if value is None: return default
            try: return float(value)
            except (ValueError, TypeError): return default

        air_temp = safe_float(local_weather_data.get('AirTemp'))
        track_temp = safe_float(local_weather_data.get('TrackTemp'))
        humidity = safe_float(local_weather_data.get('Humidity'))
        pressure = safe_float(local_weather_data.get('Pressure'))
        wind_speed = safe_float(local_weather_data.get('WindSpeed'))
        wind_direction = local_weather_data.get('WindDirection')
        rainfall_val = local_weather_data.get('Rainfall')
        is_raining = rainfall_val == '1' or rainfall_val == 1

        overall_condition = "default"
        weather_card_color = "light"
        weather_card_inverse = False

        if is_raining:
            overall_condition = "rain"
            weather_card_color = "info"
            weather_card_inverse = True
        elif air_temp is not None and humidity is not None:
            if air_temp > 25 and humidity < 60 :
                overall_condition = "sunny"
                weather_card_color = "warning"
                weather_card_inverse = False
            elif humidity >= 75 or air_temp < 15:
                overall_condition = "cloudy"
                weather_card_color = "secondary"
                weather_card_inverse = True
        elif air_temp is not None:
             if air_temp > 28: overall_condition = "sunny"
             elif air_temp < 10: overall_condition = "cloudy"

        main_weather_icon = config.WEATHER_ICON_MAP.get(overall_condition, config.WEATHER_ICON_MAP["default"]) # Use constant

        if air_temp is not None: weather_details_spans.append(html.Span(f"Air: {air_temp:.1f}°C", className="me-3"))
        if track_temp is not None: weather_details_spans.append(html.Span(f"Track: {track_temp:.1f}°C", className="me-3"))
        if humidity is not None: weather_details_spans.append(html.Span(f"Hum: {humidity:.0f}%", className="me-3"))
        if pressure is not None: weather_details_spans.append(html.Span(f"Press: {pressure:.0f}hPa", className="me-3"))
        if wind_speed is not None:
            wind_str = f"Wind: {wind_speed:.1f}m/s"
            if wind_direction is not None:
                 try: wind_str += f" ({int(wind_direction)}°)"
                 except (ValueError, TypeError): wind_str += f" ({wind_direction})"
            weather_details_spans.append(html.Span(wind_str, className="me-3"))

        if is_raining and overall_condition != "rain":
            weather_details_spans.append(html.Span("RAIN", className="me-2 fw-bold",
                                         style={'color':'#007bff'}))

        if not weather_details_spans and overall_condition == "default":
            final_weather_display_children = [html.Em(config.TEXT_WEATHER_UNAVAILABLE)] # Use constant
        elif not weather_details_spans and overall_condition != "default":
             final_weather_display_children = [html.Em(config.TEXT_WEATHER_CONDITION_GENERIC.format(condition=overall_condition.capitalize()))] # Use constant
        else:
            final_weather_display_children = weather_details_spans

        return session_info_str, html.Div(children=final_weather_display_children), main_weather_icon, weather_card_color, weather_card_inverse

    except Exception as e:
        logger.error(f"Session/Weather Display Error in callback: {e}", exc_info=True)
        # Use constants
        return config.TEXT_SESSION_INFO_ERROR, config.TEXT_WEATHER_ERROR, config.WEATHER_ICON_MAP["default"], "light", False


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
    timestamp_text = config.TEXT_WAITING_FOR_DATA # Use constant
    start_time = time.monotonic()

    try:
        with app_state.app_state_lock:
            timing_state_copy = app_state.timing_state.copy()
            data_store_copy = app_state.data_store
            # Get a snapshot of overall bests to ensure consistency for this update
            # No specific deep copy needed here as we are just reading primitives
            overall_session_bests_lap_val = app_state.session_bests["OverallBestLapTime"]["Value"]
            overall_session_bests_s1_val = app_state.session_bests["OverallBestSectors"][0]["Value"]
            overall_session_bests_s2_val = app_state.session_bests["OverallBestSectors"][1]["Value"]
            overall_session_bests_s3_val = app_state.session_bests["OverallBestSectors"][2]["Value"]


        excluded_streams = ['TimingData', 'DriverList', 'Position.z', 'CarData.z', 'Position',
                            'TrackStatus', 'SessionData', 'SessionInfo', 'WeatherData', 'RaceControlMessages', 'Heartbeat']
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
            other_elements.append(html.Details([html.Summary(f"{stream} ({timestamp_str_val})"), html.Pre(data_str, style={
                                  'marginLeft': '15px', 'maxHeight': '200px', 'overflowY': 'auto'})], open=(stream == "LapCount")))

        timing_data_entry = data_store_copy.get('TimingData', {})
        timestamp_text = f"Timing TS: {timing_data_entry.get('timestamp', 'N/A')}" if timing_data_entry else config.TEXT_WAITING_FOR_DATA # Use constant

        if timing_state_copy:
            processed_table_data = []
            for car_num, driver_state in timing_state_copy.items():
                racing_no = driver_state.get("RacingNumber", car_num)
                tla = driver_state.get("Tla", "N/A")
                pos = driver_state.get('Position', '-')
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

                time_val = driver_state.get('Time', '-')
                gap = driver_state.get('GapToLeader', '-')
                interval = utils.get_nested_state(
                    driver_state, 'IntervalToPositionAhead', 'Value', default='-')
                
                last_lap_val = utils.get_nested_state(
                    driver_state, 'LastLapTime', 'Value', default='-')
                best_lap_val = utils.get_nested_state( # This is the driver's personal best lap time string
                    driver_state, 'PersonalBestLapTime', 'Value', default='-')

                s1_val = utils.get_nested_state(
                    driver_state, 'Sectors', '0', 'Value', default='-')
                s2_val = utils.get_nested_state(
                    driver_state, 'Sectors', '1', 'Value', default='-')
                s3_val = utils.get_nested_state(
                    driver_state, 'Sectors', '2', 'Value', default='-')

                reliable_stops = driver_state.get('ReliablePitStops', 0)
                timing_data_stops = driver_state.get('NumberOfPitStops', 0)

                pits_display_val = '0'
                if reliable_stops > 0:
                    pits_display_val = str(reliable_stops)
                elif timing_data_stops > 0:
                    pits_display_val = str(timing_data_stops)

                status = driver_state.get('Status', 'N/A')

                car_data = driver_state.get('CarData', {})
                speed = car_data.get('Speed', '-')
                gear = car_data.get('Gear', '-')
                rpm = car_data.get('RPM', '-')
                drs_val = car_data.get('DRS')
                drs_map = {8: "E", 10: "On", 12: "On", 14: "ON"} # Using integer keys
                drs = drs_map.get(drs_val, 'Off') if drs_val is not None else 'Off'

                # <<< ADDED BEST LAP/SECTOR FLAGS FOR STYLING --- START >>>
                is_overall_best_lap_flag = driver_state.get('IsOverallBestLap', False)
                
                # Personal Best Lap for 'Last Lap' column means LastLapTime.PersonalFastest was true
                is_last_lap_personal_best_flag = utils.get_nested_state(driver_state, 'LastLapTime', 'PersonalFastest', default=False)

                # Personal Best Sectors for S1, S2, S3 columns means Sectors[X].PersonalFastest was true
                is_s1_personal_best_flag = utils.get_nested_state(driver_state, 'Sectors', '0', 'PersonalFastest', default=False)
                is_s2_personal_best_flag = utils.get_nested_state(driver_state, 'Sectors', '1', 'PersonalFastest', default=False)
                is_s3_personal_best_flag = utils.get_nested_state(driver_state, 'Sectors', '2', 'PersonalFastest', default=False)

                is_overall_best_s1_flag = driver_state.get('IsOverallBestSector', [False]*3)[0]
                is_overall_best_s2_flag = driver_state.get('IsOverallBestSector', [False]*3)[1]
                is_overall_best_s3_flag = driver_state.get('IsOverallBestSector', [False]*3)[2]
                # <<< ADDED BEST LAP/SECTOR FLAGS FOR STYLING --- END >>>

                row = {
                    'id': car_num, # Add a unique ID for the row, car_num is good
                    'No.': racing_no, 'Car': tla, 'Pos': pos, 'Tyre': tyre,
                    'Time': time_val, 'Gap': gap, 'Interval': interval,
                    'Last Lap': last_lap_val, 'Best Lap': best_lap_val,
                    'S1': s1_val, 'S2': s2_val, 'S3': s3_val, 'Pits': pits_display_val,
                    'Status': status, 'Speed': speed, 'Gear': gear, 'RPM': rpm, 'DRS': drs,

                   # Original boolean flags (can keep them if used elsewhere, or remove if only string versions are needed for table)
                    'IsOverallBestLap': is_overall_best_lap_flag,
                    'IsLastLapPersonalBest': is_last_lap_personal_best_flag,
                    'IsOverallBestS1': is_overall_best_s1_flag,
                    'IsPersonalBestS1': is_s1_personal_best_flag,
                    'IsOverallBestS2': is_overall_best_s2_flag,
                    'IsPersonalBestS2': is_s2_personal_best_flag,
                    'IsOverallBestS3': is_overall_best_s3_flag,
                    'IsPersonalBestS3': is_s3_personal_best_flag,

                    # Add STRING versions of flags for DataTable filtering
                    'IsOverallBestLap_Str': "TRUE" if is_overall_best_lap_flag else "FALSE",
                    'IsLastLapPersonalBest_Str': "TRUE" if is_last_lap_personal_best_flag else "FALSE",
                    
                    'IsOverallBestS1_Str': "TRUE" if is_overall_best_s1_flag else "FALSE",
                    'IsPersonalBestS1_Str': "TRUE" if is_s1_personal_best_flag else "FALSE",
                    
                    'IsOverallBestS2_Str': "TRUE" if is_overall_best_s2_flag else "FALSE",
                    'IsPersonalBestS2_Str': "TRUE" if is_s2_personal_best_flag else "FALSE",

                    'IsOverallBestS3_Str': "TRUE" if is_overall_best_s3_flag else "FALSE",
                    'IsPersonalBestS3_Str': "TRUE" if is_s3_personal_best_flag else "FALSE",
                }
                processed_table_data.append(row)

            processed_table_data.sort(key=utils.pos_sort_key)
            table_data = processed_table_data
        else:
            timestamp_text = "Waiting for DriverList..." # Could be a config constant

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
def update_replay_speed_state(new_speed):
    if new_speed is None:
        return no_update

    logger.info(f"Replay speed slider changed to: {new_speed}")
    try:
        speed_float = float(new_speed)
        with app_state.app_state_lock:
            app_state.replay_speed = speed_float
        return no_update
    except (ValueError, TypeError):
        logger.warning(f"Could not convert slider value '{new_speed}' to float.")
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
        logger.info(
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

        logger.info(
            f"Attempting to enable clientside-update-interval due to '{triggered_id}'.")
        return False

    elif triggered_id == 'stop-reset-button':
        logger.info(
            f"Disabling clientside-update-interval due to '{triggered_id}'.")
        return True

    elif triggered_id == 'interval-component-fast':
        if current_app_s in ["Live", "Replaying"]:
            if currently_disabled:
                logger.info(
                    f"Fast interval: App is '{current_app_s}', enabling clientside interval.")
                return False
            return no_update
        else:
            if not currently_disabled:
                logger.info(
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

    logger.info(f"Adjusting clientside-update-interval to {new_interval_ms}ms for replay speed {speed}x")
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

logger.info("Callback definitions processed (now using constants from config.py and helpers from utils.py, and added best lap/sector flags to table data).") #
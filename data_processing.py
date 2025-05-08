# data_processing.py
"""
Handles processing of data received from the SignalR feed or replay files.
Updates the shared application state defined in app_state.py.
"""

import logging
import time
import queue # Needed for queue.Empty exception
import threading

# Import shared state variables and lock
import app_state
# Import utility functions (if needed by any _process function directly)
import utils

# Get logger
main_logger = logging.getLogger("F1App.DataProcessing")
logger = logging.getLogger("F1App.DataProcessing")

# --- Individual Stream Processing Functions ---
# These functions now read from and write to the variables imported from app_state

def _process_race_control(data):
    """ Helper function to process RaceControlMessages stream """
    # global race_control_log # Removed global
    messages_to_process = []
    # ... (rest of the logic as before, but using app_state.race_control_log) ...
    if isinstance(data, dict) and 'Messages' in data:
        messages_payload = data.get('Messages')
        if isinstance(messages_payload, list):
            messages_to_process = messages_payload
        elif isinstance(messages_payload, dict):
            messages_to_process = messages_payload.values()
        else:
            main_logger.warning(f"RaceControlMessages 'Messages' field was not a list or dict: {type(messages_payload)}")
            return
    elif data:
         main_logger.warning(f"Unexpected RaceControlMessages format received: {type(data)}. Expected dict with 'Messages'.")
         return
    else: return

    new_messages_added = 0
    for i, msg in enumerate(messages_to_process):
        if isinstance(msg, dict):
            try:
                timestamp = msg.get('Utc', 'Timestamp?')
                lap = msg.get('Lap', '-')
                message_text = msg.get('Message', '')
                time_str = "Timestamp?"
                if isinstance(timestamp, str) and 'T' in timestamp:
                     try: time_str = timestamp.split('T')[1].split('.')[0]
                     except: time_str = timestamp
                log_entry = f"[{time_str} L{lap}]: {message_text}"
                app_state.race_control_log.appendleft(log_entry) # Use app_state.race_control_log
                new_messages_added += 1
            except Exception as e:
                 main_logger.error(f"Error processing RC message item #{i+1}: {msg} - Error: {e}", exc_info=True)
        else:
             main_logger.warning(f"Unexpected item type #{i+1} in RaceControlMessages source: {type(msg)}")

def _process_weather_data(data):
    """ Helper function to process WeatherData stream """
    # global data_store # Removed global
    if isinstance(data, dict):
        # Use app_state.data_store
        if 'WeatherData' not in app_state.data_store: app_state.data_store['WeatherData'] = {}
        app_state.data_store['WeatherData'].update(data)
    else:
        main_logger.warning(f"Unexpected WeatherData format received: {type(data)}")

def _process_timing_app_data(data):
    """ Helper function to process TimingAppData stream data (contains Stint/Tyre info) """
    # global timing_state # Removed global
    if not app_state.timing_state: # Use app_state.timing_state
        return

    if isinstance(data, dict) and 'Lines' in data and isinstance(data['Lines'], dict):
        for car_num_str, line_data in data['Lines'].items():
            # Use app_state.timing_state
            driver_current_state = app_state.timing_state.get(car_num_str)
            if driver_current_state and isinstance(line_data, dict):
                # ... (rest of tyre/stint processing logic is the same,
                #      as it modifies the driver_current_state dictionary directly,
                #      which is already retrieved from app_state.timing_state) ...
                current_compound = driver_current_state.get('TyreCompound', '-')
                current_age = driver_current_state.get('TyreAge', '?')
                stints_data = line_data.get('Stints')
                if isinstance(stints_data, dict) and stints_data:
                    try:
                        latest_stint_key = sorted(stints_data.keys(), key=int)[-1]
                        latest_stint_info = stints_data[latest_stint_key]
                        if isinstance(latest_stint_info, dict):
                            compound_value = latest_stint_info.get('Compound')
                            if isinstance(compound_value, str):
                                current_compound = compound_value.upper()

                            age_determined = False
                            total_laps_value = latest_stint_info.get('TotalLaps')
                            if total_laps_value is not None:
                                try:
                                    current_age = int(total_laps_value)
                                    age_determined = True
                                except (ValueError, TypeError):
                                    main_logger.warning(f"Driver {car_num_str}: Could not convert TotalLaps '{total_laps_value}' to int.")

                            if not age_determined:
                                start_laps_value = latest_stint_info.get('StartLaps')
                                num_laps_value = driver_current_state.get('NumberOfLaps')
                                if start_laps_value is not None and num_laps_value is not None:
                                    try:
                                        start_lap = int(start_laps_value)
                                        current_lap_completed = int(num_laps_value)
                                        age_calc = current_lap_completed - start_lap + 1
                                        current_age = age_calc if age_calc >= 0 else '?'
                                        age_determined = True
                                    except (ValueError, TypeError) as e:
                                         main_logger.warning(f"Driver {car_num_str}: Error converting StartLaps/NumberOfLaps for age calculation: {e}")
                        else:
                            main_logger.warning(f"Driver {car_num_str}: Data for Stint {latest_stint_key} is not a dictionary: {type(latest_stint_info)}")
                    except (ValueError, IndexError, KeyError, TypeError) as e:
                         main_logger.error(f"Driver {car_num_str}: Error processing Stints data in TimingAppData: {e} - Data was: {stints_data}", exc_info=False)

                driver_current_state['TyreCompound'] = current_compound
                driver_current_state['TyreAge'] = current_age

    elif data:
         main_logger.warning(f"Unexpected TimingAppData format received: {type(data)}")

def _process_driver_list(data):
    """ Helper to process DriverList data ONLY from the stream """
    # global timing_state # Removed global
    added_count = 0
    updated_count = 0
    processed_count = 0
    if isinstance(data, dict):
        processed_count = len(data)
        for driver_num_str, driver_info in data.items():
            if not isinstance(driver_info, dict):
                if driver_num_str == "_kf": continue
                else: main_logger.warning(f"Skipping invalid driver_info for {driver_num_str} in DriverList: {driver_info}")
                continue

            # Use app_state.timing_state
            is_new_driver = driver_num_str not in app_state.timing_state
            tla_from_stream = driver_info.get("Tla", "N/A")

            if is_new_driver:
                # Add to app_state.timing_state
                app_state.timing_state[driver_num_str] = {
                    # ... (all fields as before) ...
                    "RacingNumber": driver_info.get("RacingNumber", driver_num_str), "Tla": tla_from_stream, "FullName": driver_info.get("FullName", "N/A"), "TeamName": driver_info.get("TeamName", "N/A"), "Line": driver_info.get("Line", "-"), "TeamColour": driver_info.get("TeamColour", "FFFFFF"), "FirstName": driver_info.get("FirstName", ""), "LastName": driver_info.get("LastName", ""), "Reference": driver_info.get("Reference", ""), "CountryCode": driver_info.get("CountryCode", ""),
                    "Position": "-", "Time": "-", "GapToLeader": "-", "IntervalToPositionAhead": {"Value": "-"}, "LastLapTime": {}, "BestLapTime": {}, "Sectors": {}, "Status": "On Track", "InPit": False, "Retired": False, "Stopped": False, "PitOut": False
                }
                added_count += 1
            else:
                # Modify existing entry in app_state.timing_state
                current_driver_state = app_state.timing_state[driver_num_str]
                # ... (rest of update logic modifies current_driver_state directly) ...
                current_tla = current_driver_state.get("Tla")
                if tla_from_stream != "N/A" and (not current_tla or current_tla == "N/A"): current_driver_state["Tla"] = tla_from_stream
                elif tla_from_stream != "N/A" and current_tla != tla_from_stream: current_driver_state["Tla"] = tla_from_stream
                for key in ["RacingNumber", "FullName", "TeamName", "Line", "TeamColour", "FirstName", "LastName", "Reference", "CountryCode"]:
                     if key in driver_info and driver_info[key] is not None: current_driver_state[key] = driver_info[key]
                default_timing_values = { "Position": "-", "Time": "-", "GapToLeader": "-", "IntervalToPositionAhead": {"Value": "-"}, "LastLapTime": {}, "BestLapTime": {}, "Sectors": {}, "Status": "On Track", "InPit": False, "Retired": False, "Stopped": False, "PitOut": False }
                for key, default_val in default_timing_values.items(): current_driver_state.setdefault(key, default_val)
                updated_count += 1

        main_logger.debug(f"Processed DriverList message ({processed_count} entries). Added: {added_count}, Updated: {updated_count}. Total drivers now: {len(app_state.timing_state)}")
    else:
        main_logger.warning(f"Unexpected DriverList stream data format: {type(data)}. Cannot process.")

def _process_timing_data(data):
    """ Helper function to process TimingData stream data """
    # global timing_state # Removed global
    if not app_state.timing_state: # Use app_state
        return

    if isinstance(data, dict) and 'Lines' in data and isinstance(data['Lines'], dict):
        for car_num_str, line_data in data['Lines'].items():
            driver_current_state = app_state.timing_state.get(car_num_str) # Use app_state
            if driver_current_state and isinstance(line_data, dict):
                # ... (rest of update logic modifies driver_current_state directly) ...
                for key in ["Position", "Time", "GapToLeader", "InPit", "Retired", "Stopped", "PitOut", "NumberOfLaps", "NumberOfPitStops"]:
                     if key in line_data: driver_current_state[key] = line_data[key]
                for key in ["IntervalToPositionAhead", "LastLapTime", "BestLapTime"]:
                    if key in line_data:
                        incoming_value = line_data[key]
                        if key not in driver_current_state or not isinstance(driver_current_state[key], dict): driver_current_state[key] = {}
                        if isinstance(incoming_value, dict): driver_current_state[key].update(incoming_value)
                        else:
                            sub_key = 'Value' if key == "IntervalToPositionAhead" else 'Time'; driver_current_state[key][sub_key] = incoming_value
                if "Sectors" in line_data and isinstance(line_data["Sectors"], dict):
                     if "Sectors" not in driver_current_state or not isinstance(driver_current_state["Sectors"], dict): driver_current_state["Sectors"] = {}
                     for sector_idx, sector_data in line_data["Sectors"].items():
                          if sector_idx not in driver_current_state["Sectors"] or not isinstance(driver_current_state["Sectors"][sector_idx], dict): driver_current_state["Sectors"][sector_idx] = {}
                          if isinstance(sector_data, dict): driver_current_state["Sectors"][sector_idx].update(sector_data)
                          else: driver_current_state["Sectors"][sector_idx]['Value'] = sector_data
                if "Speeds" in line_data and isinstance(line_data["Speeds"], dict):
                     if "Speeds" not in driver_current_state or not isinstance(driver_current_state["Speeds"], dict): driver_current_state["Speeds"] = {}
                     driver_current_state["Speeds"].update(line_data["Speeds"])
                status_flags = []
                if driver_current_state.get("Retired"): status_flags.append("Retired")
                if driver_current_state.get("InPit"): status_flags.append("In Pit")
                if driver_current_state.get("Stopped"): status_flags.append("Stopped")
                if driver_current_state.get("PitOut"): status_flags.append("Out Lap")
                if status_flags: driver_current_state["Status"] = ", ".join(status_flags)
                elif driver_current_state.get("Position", "-") != "-": driver_current_state["Status"] = "On Track"

    elif data:
         main_logger.warning(f"Unexpected TimingData format received: {type(data)}")

def _process_track_status(data):
    """Handles TrackStatus data. MUST be called within app_state.app_state_lock."""
    # global track_status_data # Removed global
    if not isinstance(data, dict):
        main_logger.warning(f"TrackStatus handler received non-dict data: {data}")
        return

    # Use app_state.track_status_data
    new_status = data.get('Status', app_state.track_status_data.get('Status', 'Unknown'))
    new_message = data.get('Message', app_state.track_status_data.get('Message', ''))

    if app_state.track_status_data.get('Status') != new_status or app_state.track_status_data.get('Message') != new_message:
        app_state.track_status_data['Status'] = new_status
        app_state.track_status_data['Message'] = new_message
        main_logger.info(f"Track Status Update: Status={new_status}, Message='{new_message}'")

def _process_position_data(data):
    """
    Handles Position data. Updates current and previous position data in app_state.
    MUST be called within app_state.app_state_lock.
    """
    # Use app_state.timing_state
    if not app_state.timing_state:
        # logger.debug("Position data received, but timing_state not yet initialized.")
        return # Cannot process without driver entries in timing_state

    if not isinstance(data, dict) or 'Position' not in data:
        logger.warning(f"Position handler received unexpected format: {type(data)}")
        return

    position_entries_list = data.get('Position', [])
    if not isinstance(position_entries_list, list):
        logger.warning(f"Position data 'Position' key is not a list: {type(position_entries_list)}")
        return

    for entry_group in position_entries_list:
        if not isinstance(entry_group, dict): continue
        timestamp_str = entry_group.get('Timestamp') # Timestamp for this batch of positions
        if not timestamp_str: continue # Need a timestamp

        entries_dict = entry_group.get('Entries', {})
        if not isinstance(entries_dict, dict): continue

        for car_number_str, new_pos_info in entries_dict.items():
            if car_number_str not in app_state.timing_state:
                # logger.debug(f"Position data for unknown driver {car_number_str}")
                continue # Skip if driver isn't known (e.g., not in DriverList yet)

            if isinstance(new_pos_info, dict):
                current_driver_state = app_state.timing_state[car_number_str]

                # --- Shift current to previous ---
                if 'PositionData' in current_driver_state and current_driver_state['PositionData']:
                    # Only copy if PositionData actually existed and had content
                    current_driver_state['PreviousPositionData'] = current_driver_state['PositionData'].copy()
                # else: # First position update for this driver, no previous to set
                #    current_driver_state['PreviousPositionData'] = None # Or empty dict

                # --- Store new current position data ---
                current_driver_state['PositionData'] = {
                    'X': new_pos_info.get('X'),
                    'Y': new_pos_info.get('Y'),
                    'Status': new_pos_info.get('Status'), # e.g., "OnTrack"
                    'Timestamp': timestamp_str # Timestamp for this specific update
                }
                # logger.debug(f"Updated Position for {car_number_str}: New X={new_pos_info.get('X')}, Prev X={current_driver_state.get('PreviousPositionData',{}).get('X')}")

def _process_car_data(data):
    """Handles CarData. MUST be called within app_state.app_state_lock."""
    # global timing_state # Removed global
    # Need access to config for CHANNEL_MAP
    try:
        import config
    except ImportError:
        main_logger.error("Config module not found for CHANNEL_MAP in _process_car_data!")
        # Define a fallback map or return?
        channel_map = {} # Fallback empty map
    else:
        channel_map = config.CHANNEL_MAP

    if 'timing_state' not in app_state.__dict__:
        main_logger.error("Global 'timing_state' not found in app_state for CarData processing.")
        return

    if not isinstance(data, dict) or 'Entries' not in data:
        main_logger.warning(f"CarData handler received unexpected format: {data}")
        return
    # ... (rest of the logic as before, using app_state.timing_state and channel_map) ...
    entries = data.get('Entries', [])
    if not isinstance(entries, list): main_logger.warning(f"CarData 'Entries' is not a list: {entries}"); return

    for entry in entries:
        if not isinstance(entry, dict): continue
        utc_time = entry.get('Utc'); cars_data = entry.get('Cars', {})
        if not isinstance(cars_data, dict): continue

        for car_number, car_details in cars_data.items():
             car_number_str = str(car_number)
             if car_number_str not in app_state.timing_state: continue
             if not isinstance(car_details, dict): continue
             channels = car_details.get('Channels', {})
             if not isinstance(channels, dict): continue

             if 'CarData' not in app_state.timing_state[car_number_str]: app_state.timing_state[car_number_str]['CarData'] = {}
             car_data_dict = app_state.timing_state[car_number_str]['CarData']

             for channel_num_str, data_key in channel_map.items():
                 if channel_num_str in channels: car_data_dict[data_key] = channels[channel_num_str]

             car_data_dict['Utc'] = utc_time
             
             if car_number_str in app_state.timing_state:
                driver_timing_state = app_state.timing_state[car_number_str]

                # Determine Current Lap Number
                completed_laps = driver_timing_state.get('NumberOfLaps', -1)
                try:
                    current_lap = int(completed_laps) + 1
                    if current_lap <= 0: current_lap = 1 # Default to lap 1 if unknown/invalid
                except (ValueError, TypeError):
                    main_logger.warning(f"Cannot determine lap for Driver {car_number_str}, LapInfo='{completed_laps}'. Skip history append.")
                    continue # Cannot store history without lap

                # Ensure structure exists in app_state.telemetry_data
                # { driver_num: { lap_num: { channel: [], Timestamps: [] } } }
                if car_number_str not in app_state.telemetry_data:
                    app_state.telemetry_data[car_number_str] = {}
                if current_lap not in app_state.telemetry_data[car_number_str]:
                    app_state.telemetry_data[car_number_str][current_lap] = {
                        'Timestamps': [], **{key: [] for key in channel_map.values()} # Initialize lists for all mapped channels
                    }
                    main_logger.debug(f"Initialized telemetry storage for Driver {car_number_str}, Lap {current_lap}")

                lap_telemetry_history = app_state.telemetry_data[car_number_str][current_lap]

                # Append Timestamp
                lap_telemetry_history['Timestamps'].append(utc_time)

                # Append Channel Data (get from 'channels' dict)
                for channel_num_str, data_key in channel_map.items():
                    value = channels.get(channel_num_str)
                    # Convert relevant values to numeric types if possible
                    if data_key in ['RPM', 'Speed', 'Gear', 'Throttle', 'Brake', 'DRS']:
                        try: value = int(value) if value is not None else None
                        except (ValueError, TypeError): value = None
                    lap_telemetry_history[data_key].append(value) # Append value or None

def _process_session_data(data):
    """ Processes SessionData updates (like status). MUST be called within app_state.app_state_lock."""
    # global session_details # Removed global
    if not isinstance(data, dict):
        main_logger.warning(f"SessionData handler received non-dict data: {data}")
        return
    try:
        status_series = data.get('StatusSeries')
        if isinstance(status_series, dict):
             for entry_key, status_info in status_series.items():
                 if isinstance(status_info, dict):
                      session_status = status_info.get('SessionStatus')
                      if session_status:
                           app_state.session_details['SessionStatus'] = session_status # Use app_state
                           main_logger.info(f"Session Status Updated: {session_status}")
        # Potentially extract other fields directly into app_state.session_details if needed
    except Exception as e:
        main_logger.error(f"Error processing SessionData: {e}", exc_info=True)

def _process_session_info(data, app_state):
    """ 
    Processes SessionInfo data. MUST be called within app_state.app_state_lock.
    Starts a background thread for proactive track data fetch if session changes.
    """
    if not isinstance(data, dict): main_logger.warning(f"SessionInfo non-dict: {data}"); return
    try:
        # Extract info safely (ensure circuit_info is always a dict)
        meeting_info = data.get('Meeting', {}); circuit_info = meeting_info.get('Circuit', {}); country_info = meeting_info.get('Country', {})
        if not isinstance(circuit_info, dict): circuit_info = {} 
        if not isinstance(meeting_info, dict): meeting_info = {} 
        if not isinstance(country_info, dict): country_info = {} 

        # Extract Year safely
        year_str = None; start_date_str = data.get('StartDate')
        if start_date_str and isinstance(start_date_str, str) and len(start_date_str) >= 4:
             try: int(start_date_str[:4]); year_str = start_date_str[:4]
             except ValueError: main_logger.warning(f"Invalid year in StartDate: {start_date_str}")
        
        circuit_key = circuit_info.get('Key') 

        # Construct new session key if possible
        new_session_key = f"{year_str}_{circuit_key}" if year_str and circuit_key is not None and str(circuit_key).strip() else None

        # Get OLD session key BEFORE updating session_details
        old_session_key = app_state.session_details.get('SessionKey')

        # Update session_details 
        app_state.session_details['Year'] = year_str
        app_state.session_details['CircuitKey'] = circuit_key
        app_state.session_details['SessionKey'] = new_session_key
        app_state.session_details['Meeting'] = meeting_info
        app_state.session_details['Circuit'] = circuit_info
        app_state.session_details['Country'] = country_info
        app_state.session_details['Name'] = data.get('Name')
        app_state.session_details['Type'] = data.get('Type')
        app_state.session_details['StartDate'] = start_date_str
        app_state.session_details['EndDate'] = data.get('EndDate')
        app_state.session_details['GmtOffset'] = data.get('GmtOffset')
        app_state.session_details['Path'] = data.get('Path')
        
        # Check if fetch is needed and start background thread
        needs_fetch = False
        if new_session_key:
            main_logger.info(f"DataProcessing: SessionKey '{new_session_key}' set.")
            cached_session_key = app_state.track_coordinates_cache.get('session_key')
            if old_session_key != new_session_key or cached_session_key != new_session_key:
                 main_logger.info(f"DataProcessing: Proactive track fetch needed for {new_session_key}")
                 needs_fetch = True
            else:
                 main_logger.debug(f"DataProcessing: Cache key {new_session_key} OK. No fetch needed.")
        else: # Invalid new session key
             main_logger.warning(f"DataProcessing: Could not construct valid SessionKey. Clearing cache.")
             app_state.session_details.pop('SessionKey', None)
             app_state.track_coordinates_cache = {} # Clear track cache

        # Start fetch in background thread *after* releasing the main lock if needed
        if needs_fetch:
             fetch_thread = threading.Thread(
                  target=utils._background_track_fetch_and_update, 
                  args=(new_session_key, year_str, circuit_key, app_state),
                  daemon=True # Allows main program to exit even if thread is running
             )
             fetch_thread.start()
             main_logger.info(f"DataProcessing: Background fetch thread started for {new_session_key}.")

        # Log summary
        session_key_log = app_state.session_details.get('SessionKey', 'N/A') 
        main_logger.info(f"Processed SessionInfo: ... -> Stored SessionKey: {session_key_log}") # Concise log

    except Exception as e: main_logger.error(f"Error processing SessionInfo: {e}", exc_info=True)

# --- Main Processing Loop ---
def data_processing_loop():
    # Remove global statements
    processed_count = 0
    last_log_time = time.monotonic()
    log_interval_seconds = 15
    log_interval_items = 500
    loop_counter = 0

    main_logger.info("Data processing thread started.")

    while not app_state.stop_event.is_set(): # Use app_state.stop_event
        loop_counter += 1
        # ... (Periodic logging using app_state.data_queue.qsize()) ...
        if loop_counter % 50 == 0: main_logger.debug(f"Data processing loop is running (Iteration {loop_counter})...")
        current_time = time.monotonic()
        if (current_time - last_log_time > log_interval_seconds) or \
           (processed_count > 0 and processed_count % log_interval_items == 0):
            try:
                qsize = app_state.data_queue.qsize()
                main_logger.debug(f"Data processing loop status: Processed={processed_count}, Queue Size={qsize}")
                last_log_time = current_time
            except Exception as q_err:
                 main_logger.warning(f"Could not get queue size: {q_err}")

        item = None
        try:
            item = app_state.data_queue.get(block=True, timeout=0.2) # Use app_state.data_queue

            # --- Optional: Add structured data writing here if desired ---
            # try:
            #     if app_state.is_saving_active and app_state.live_data_file: # Check flags/handle
            #         json.dump(item, app_state.live_data_file)
            #         app_state.live_data_file.write('\n')
            # except Exception as write_err:
            #      main_logger.error(f"Error writing processed item to file: {write_err}", exc_info=False)
            # --- End optional writing ---

            if not isinstance(item, dict) or 'stream' not in item or 'data' not in item:
                main_logger.warning(f"Skipping queue item with unexpected structure: {type(item)}")
                if item is not None: app_state.data_queue.task_done()
                continue

            stream_name = item['stream']
            actual_data = item['data']
            timestamp = item.get('timestamp')

            # Acquire lock from app_state
            with app_state.app_state_lock:
                # Update data_store in app_state
                app_state.data_store[stream_name] = {"data": actual_data, "timestamp": timestamp}

                # Call specific processing functions (which modify app_state variables)
                try:
                    if stream_name == "Heartbeat":
                         app_state.app_status["last_heartbeat"] = timestamp # Update dict in app_state
                    elif stream_name == "DriverList": _process_driver_list(actual_data)
                    elif stream_name == "TimingData": _process_timing_data(actual_data)
                    elif stream_name == "SessionInfo": _process_session_info(actual_data, app_state)
                    elif stream_name == "SessionData": _process_session_data(actual_data)
                    elif stream_name == "TimingAppData": _process_timing_app_data(actual_data)
                    elif stream_name == "TrackStatus": _process_track_status(actual_data)
                    elif stream_name == "CarData": _process_car_data(actual_data)
                    elif stream_name == "Position": _process_position_data(actual_data)
                    elif stream_name == "WeatherData": _process_weather_data(actual_data)
                    elif stream_name == "RaceControlMessages": _process_race_control(actual_data)
                    # Add LapCount if needed
                    # elif stream_name == "LapCount": _process_lap_count(actual_data) # Need to create this handler
                    else: main_logger.debug(f"No specific handler for stream: {stream_name}")
                except Exception as proc_ex:
                     main_logger.error(f"  ERROR processing stream '{stream_name}': {proc_ex}", exc_info=True)

            app_state.data_queue.task_done() # Use app_state.data_queue

        except queue.Empty:
            continue
        except Exception as e:
            main_logger.error(f"!!! Unhandled exception in data_processing_loop !!! Error: {e}", exc_info=True)
            if item is not None:
                try: app_state.data_queue.task_done()
                except: pass
            time.sleep(0.5)

    main_logger.info("Data processing thread finished cleanly (stop_event set).")

print("DEBUG: data_processing module loaded")
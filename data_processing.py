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
import utils # Contains helpers
import config 

# Get logger
logger = logging.getLogger("F1App.DataProcessing") 

# --- Individual Stream Processing Functions ---

def _process_race_control(data):
    """ Helper function to process RaceControlMessages stream """
    messages_to_process = []
    if isinstance(data, dict) and 'Messages' in data:
        messages_payload = data.get('Messages')
        if isinstance(messages_payload, list):
            messages_to_process = messages_payload
        elif isinstance(messages_payload, dict): 
            messages_to_process = messages_payload.values()
        else:
            logger.warning(f"RaceControlMessages 'Messages' field was not a list or dict: {type(messages_payload)}")
            return
    elif data: 
         logger.warning(f"Unexpected RaceControlMessages format received: {type(data)}. Expected dict with 'Messages'.")
         return
    else: 
        return

    new_messages_added = 0
    for i, msg in enumerate(messages_to_process):
        if isinstance(msg, dict):
            try:
                timestamp = msg.get('Utc', 'Timestamp?')
                lap_num_str = str(msg.get('Lap', '-')) 
                message_text = msg.get('Message', '')
                time_str = "Timestamp?"
                if isinstance(timestamp, str) and 'T' in timestamp:
                     try: time_str = timestamp.split('T')[1].split('.')[0] 
                     except: time_str = timestamp 
                log_entry = f"[{time_str} L{lap_num_str}]: {message_text}"
                app_state.race_control_log.appendleft(log_entry)
                new_messages_added += 1
            except Exception as e:
                 logger.error(f"Error processing RC message item #{i+1}: {msg} - Error: {e}", exc_info=True)
        else:
             logger.warning(f"Unexpected item type #{i+1} in RaceControlMessages source: {type(msg)}")


def _process_weather_data(data):
    """ Helper function to process WeatherData stream """
    if isinstance(data, dict):
        if 'WeatherData' not in app_state.data_store:
            app_state.data_store['WeatherData'] = {}
        app_state.data_store['WeatherData'].update(data) 
    else:
        logger.warning(f"Unexpected WeatherData format received: {type(data)}")

def _process_timing_app_data(data):
    """ Helper function to process TimingAppData stream data (contains Stint/Tyre info) """
    if not app_state.timing_state: 
        return

    if isinstance(data, dict) and 'Lines' in data and isinstance(data['Lines'], dict):
        for car_num_str, line_data in data['Lines'].items():
            driver_current_state = app_state.timing_state.get(car_num_str)
            if driver_current_state and isinstance(line_data, dict):
                stints_payload = line_data.get('Stints')
                if isinstance(stints_payload, dict) and stints_payload:
                    driver_current_state['StintsData'] = stints_payload
                    driver_current_state['ReliablePitStops'] = max(0, len(stints_payload) - 1)

                    current_compound = driver_current_state.get('TyreCompound', '-')
                    current_age = driver_current_state.get('TyreAge', '?')
                    is_new_tyre = driver_current_state.get('IsNewTyre', False)

                    try:
                        latest_stint_key = sorted(stints_payload.keys(), key=int)[-1]
                        latest_stint_info = stints_payload[latest_stint_key]

                        if isinstance(latest_stint_info, dict):
                            compound_value = latest_stint_info.get('Compound')
                            if isinstance(compound_value, str) and compound_value:
                                current_compound = compound_value.upper()

                            new_status_str = latest_stint_info.get('New')
                            if isinstance(new_status_str, str):
                                is_new_tyre = new_status_str.lower() == 'true'

                            age_determined = False
                            total_laps_value = latest_stint_info.get('TotalLaps')
                            if total_laps_value is not None:
                                try:
                                    current_age = int(total_laps_value)
                                    age_determined = True
                                except (ValueError, TypeError):
                                    logger.warning(f"Driver {car_num_str}: Could not convert TotalLaps '{total_laps_value}' to int for Stint {latest_stint_key}.")

                            if not age_determined:
                                start_laps_value = latest_stint_info.get('StartLaps')
                                num_laps_value = driver_current_state.get('NumberOfLaps')
                                if start_laps_value is not None and num_laps_value is not None:
                                    try:
                                        start_lap = int(start_laps_value)
                                        current_lap_completed = int(num_laps_value)
                                        age_calc = current_lap_completed - start_lap + 1
                                        current_age = age_calc if age_calc >= 0 else '?'
                                    except (ValueError, TypeError) as e:
                                         logger.warning(f"Driver {car_num_str}: Error converting StartLaps/NumberOfLaps for age calc: {e}. Stint: {latest_stint_key}")
                        else:
                            logger.warning(f"Driver {car_num_str}: Data for Stint {latest_stint_key} is not a dict: {type(latest_stint_info)}")
                    except (ValueError, IndexError, KeyError, TypeError) as e:
                         logger.error(f"Driver {car_num_str}: Error processing Stints data in TimingAppData: {e} - Data: {stints_payload}", exc_info=False)

                    driver_current_state['TyreCompound'] = current_compound
                    driver_current_state['TyreAge'] = current_age
                    driver_current_state['IsNewTyre'] = is_new_tyre
    elif data:
         logger.warning(f"Unexpected TimingAppData format received: {type(data)}")


def _process_driver_list(data):
    """ Helper to process DriverList data ONLY from the stream """
    added_count = 0; updated_count = 0; processed_count = 0
    if isinstance(data, dict):
        processed_count = len(data)
        for driver_num_str, driver_info in data.items():
            if not isinstance(driver_info, dict):
                if driver_num_str == "_kf": continue 
                else: logger.warning(f"Skipping invalid driver_info for {driver_num_str} in DriverList: {driver_info}"); continue

            is_new_driver = driver_num_str not in app_state.timing_state
            tla_from_stream = driver_info.get("Tla", "N/A")

            default_best_lap_sector_info = {
                "PersonalBestLapTimeValue": None, 
                "PersonalBestLapTime": {"Value": None, "Lap": None},
                "IsOverallBestLap": False, 
                "PersonalBestSectors": [None, None, None], 
                "IsOverallBestSector": [False, False, False] 
            }
            
            if is_new_driver:
                app_state.timing_state[driver_num_str] = {
                    "RacingNumber": driver_info.get("RacingNumber", driver_num_str), "Tla": tla_from_stream,
                    "FullName": driver_info.get("FullName", "N/A"), "TeamName": driver_info.get("TeamName", "N/A"),
                    "Line": driver_info.get("Line", "-"), "TeamColour": driver_info.get("TeamColour", "FFFFFF"),
                    "FirstName": driver_info.get("FirstName", ""), "LastName": driver_info.get("LastName", ""),
                    "Reference": driver_info.get("Reference", ""), "CountryCode": driver_info.get("CountryCode", ""),
                    "Position": "-", "Time": "-", "GapToLeader": "-", "IntervalToPositionAhead": {"Value": "-"},
                    "LastLapTime": {}, "BestLapTime": {}, "Sectors": {}, "Status": "On Track",
                    "InPit": False, "Retired": False, "Stopped": False, "PitOut": False,
                    "TyreCompound": "-", "TyreAge": "?", "IsNewTyre": False, "StintsData": {},
                    "NumberOfPitStops": 0, "ReliablePitStops": 0, "CarData": {}, "PositionData": {}, "PreviousPositionData": {},
                    **default_best_lap_sector_info 
                }
                app_state.lap_time_history[driver_num_str] = []
                app_state.telemetry_data[driver_num_str] = {} 
                added_count += 1
            else:
                current_driver_state = app_state.timing_state[driver_num_str]
                current_tla = current_driver_state.get("Tla")
                if tla_from_stream != "N/A" and (not current_tla or current_tla == "N/A" or current_tla != tla_from_stream):
                    current_driver_state["Tla"] = tla_from_stream
                for key in ["RacingNumber", "FullName", "TeamName", "Line", "TeamColour", "FirstName", "LastName", "Reference", "CountryCode"]:
                     if key in driver_info and driver_info[key] is not None: current_driver_state[key] = driver_info[key]

                default_timing_values = { "Position": "-", "Time": "-", "GapToLeader": "-", "IntervalToPositionAhead": {"Value": "-"}, "LastLapTime": {}, "BestLapTime": {}, "Sectors": {}, "Status": "On Track", "InPit": False, "Retired": False, "Stopped": False, "PitOut": False, "TyreCompound": "-", "TyreAge": "?", "IsNewTyre": False, "StintsData": {}, "NumberOfPitStops": 0, "ReliablePitStops": 0, "CarData": {}, "PositionData": {}, "PreviousPositionData": {}}
                for key, default_val in default_timing_values.items(): current_driver_state.setdefault(key, default_val)
                for key, default_val in default_best_lap_sector_info.items(): current_driver_state.setdefault(key, default_val) 

                if driver_num_str not in app_state.lap_time_history: app_state.lap_time_history[driver_num_str] = []
                if driver_num_str not in app_state.telemetry_data: app_state.telemetry_data[driver_num_str] = {}
                updated_count += 1
        logger.debug(f"Processed DriverList message ({processed_count} entries). Added: {added_count}, Updated: {updated_count}. Total drivers now: {len(app_state.timing_state)}")
    else:
        logger.warning(f"Unexpected DriverList stream data format: {type(data)}. Cannot process.")


def _process_timing_data(data):
    if not app_state.timing_state: return

    if isinstance(data, dict) and 'Lines' in data and isinstance(data['Lines'], dict):
        for car_num_str, line_data in data['Lines'].items():
            driver_current_state = app_state.timing_state.get(car_num_str)
            if driver_current_state and isinstance(line_data, dict):
                original_last_lap_time_info = driver_current_state.get('LastLapTime', {}).copy()

                # Update general timing fields
                for key in ["Position", "Time", "GapToLeader", "InPit", "Retired", "Stopped", "PitOut", "NumberOfLaps", "NumberOfPitStops"]:
                     if key in line_data: driver_current_state[key] = line_data[key]

                # Update BestLapTime (driver's personal best)
                if "BestLapTime" in line_data:
                    incoming_best_lap_info = line_data["BestLapTime"]
                    if isinstance(incoming_best_lap_info, dict) and incoming_best_lap_info.get("Value"):
                        pb_lap_val_str = incoming_best_lap_info.get("Value")
                        pb_lap_seconds = utils.parse_lap_time_to_seconds(pb_lap_val_str)
                        current_pb_lap_seconds_val = driver_current_state.get("PersonalBestLapTimeValue")

                        if pb_lap_seconds is not None:
                            if current_pb_lap_seconds_val is None or pb_lap_seconds < current_pb_lap_seconds_val:
                                driver_current_state["PersonalBestLapTimeValue"] = pb_lap_seconds
                                driver_current_state["PersonalBestLapTime"] = incoming_best_lap_info.copy()
                                logger.debug(f"Driver {car_num_str} new PB Lap from BestLapTime feed: {pb_lap_val_str}")
                    
                    # Ensure the BestLapTime structure is updated in driver_current_state
                    if "BestLapTime" not in driver_current_state or not isinstance(driver_current_state["BestLapTime"], dict):
                        driver_current_state["BestLapTime"] = {}
                    if isinstance(incoming_best_lap_info, dict):
                        driver_current_state["BestLapTime"].update(incoming_best_lap_info)
                    else: 
                        driver_current_state["BestLapTime"]['Value'] = incoming_best_lap_info
                
                # Update Interval and LastLapTime
                for key in ["IntervalToPositionAhead", "LastLapTime"]: 
                    if key in line_data:
                        incoming_value = line_data[key]
                        if key not in driver_current_state or not isinstance(driver_current_state[key], dict):
                            driver_current_state[key] = {}
                        if isinstance(incoming_value, dict):
                            driver_current_state[key].update(incoming_value)
                        else: 
                            driver_current_state[key]['Value'] = incoming_value

                # --- Sector Processing with Normalization ---
                if "Sectors" in line_data and isinstance(line_data["Sectors"], dict) or \
                   "Sectors" not in line_data: # Process even if "Sectors" key is missing to handle resets
                    
                    # Ensure base structure for sectors exists for the driver
                    if "Sectors" not in driver_current_state or not isinstance(driver_current_state["Sectors"], dict):
                        driver_current_state["Sectors"] = {"0": {"Value": "-"}, "1": {"Value": "-"}, "2": {"Value": "-"}}
                    
                    incoming_sectors_data = line_data.get("Sectors", {}) # Get incoming sectors, or empty dict if none

                    # Normalize and update sectors based on incoming data for the current lap
                    # If a new lap starts, often only S1 comes, then S2, then S3.
                    # We need to ensure that if S1 is new, S2 and S3 reflect being "not set yet" for *this current lap attempt*
                    # This logic is tricky because the feed might not explicitly clear old S2/S3 values when S1 of a new lap posts.
                    # For now, we'll focus on normalizing explicit empty values from the feed.
                    # A more advanced approach might involve checking NumberOfLaps changes to reset sector values.

                    for i in range(3): # Iterate 0, 1, 2 for S1, S2, S3
                        sector_idx_str = str(i)
                        sector_data_from_feed = incoming_sectors_data.get(sector_idx_str) # Data for this sector from current message

                        # Ensure the state has a placeholder for this sector
                        if sector_idx_str not in driver_current_state["Sectors"] or \
                           not isinstance(driver_current_state["Sectors"][sector_idx_str], dict):
                            driver_current_state["Sectors"][sector_idx_str] = {"Value": "-", "PersonalFastest": False, "OverallFastest": False}
                        
                        target_sector_state = driver_current_state["Sectors"][sector_idx_str]

                        if sector_data_from_feed is not None: # If there's any data for this sector in the current message
                            if isinstance(sector_data_from_feed, dict):
                                target_sector_state.update(sector_data_from_feed)
                            else: # If feed sends just a value
                                target_sector_state['Value'] = sector_data_from_feed
                            
                            # Normalize after update: if Value is "" or None, set to "-"
                            current_val = target_sector_state.get("Value")
                            if current_val == "" or current_val is None:
                                target_sector_state["Value"] = "-"
                        # If sector_data_from_feed is None (sector not in current message),
                        # its value in target_sector_state remains from previous state or its default.
                        # If it was an old value and should now be cleared because S1 of a new lap posted,
                        # this simple normalization won't catch it unless feed explicitly clears.

                        # Ensure 'Value' key always exists, defaulting to "-"
                        if "Value" not in target_sector_state:
                            target_sector_state["Value"] = "-"

                        # --- Process this sector for PB/OB (using normalized target_sector_state) ---
                        sector_val_str = target_sector_state.get("Value") # Should be normalized now
                        is_this_sector_update_a_pb = target_sector_state.get("PersonalFastest", False)
                        # is_this_sector_update_overall_fastest = target_sector_state.get("OverallFastest", False) # Example

                        if sector_val_str and sector_val_str != "-":
                            sector_seconds = utils.parse_lap_time_to_seconds(sector_val_str)
                            if sector_seconds is not None:
                                # Personal Best Sector Value Update
                                current_pb_sector_seconds_val = driver_current_state["PersonalBestSectors"][i] # i is 0,1,2
                                if is_this_sector_update_a_pb: # Trust feed flag if present and true
                                     if current_pb_sector_seconds_val is None or sector_seconds < current_pb_sector_seconds_val :
                                        driver_current_state["PersonalBestSectors"][i] = sector_seconds
                                        logger.debug(f"Driver {car_num_str} new PB S{i+1} from feed flag: {sector_val_str}")
                                # Fallback: if numerically better, also consider it a PB for internal tracking
                                # This is useful if feed flags are missing in replays
                                elif current_pb_sector_seconds_val is None or sector_seconds < current_pb_sector_seconds_val:
                                    driver_current_state["PersonalBestSectors"][i] = sector_seconds
                                    logger.debug(f"Driver {car_num_str} new PB S{i+1} by numeric comparison: {sector_val_str}")


                                # Overall Best Sector Value Update
                                overall_best_s_time_from_state_val = app_state.session_bests["OverallBestSectors"][i]["Value"]
                                is_sector_segment_valid_for_ob = True # Basic validity
                                # (Add more sophisticated segment status checks here if data is available)

                                if is_sector_segment_valid_for_ob and \
                                   (overall_best_s_time_from_state_val is None or sector_seconds < overall_best_s_time_from_state_val):
                                    app_state.session_bests["OverallBestSectors"][i] = {"Value": sector_seconds, "DriverNumber": car_num_str}
                                    logger.info(f"New Overall Best S{i+1}: {sector_val_str} ({sector_seconds}s) by {car_num_str}")
                
                # Speeds processing
                if "Speeds" in line_data and isinstance(line_data["Speeds"], dict): 
                     if "Speeds" not in driver_current_state or not isinstance(driver_current_state["Speeds"], dict): driver_current_state["Speeds"] = {}
                     driver_current_state["Speeds"].update(line_data["Speeds"])

                # Status flags
                status_flags = []
                if driver_current_state.get("Retired"): status_flags.append("Retired")
                if driver_current_state.get("InPit"): status_flags.append("In Pit")
                if driver_current_state.get("Stopped"): status_flags.append("Stopped")
                if driver_current_state.get("PitOut"): status_flags.append("Out Lap")
                if status_flags: driver_current_state["Status"] = ", ".join(status_flags)
                elif driver_current_state.get("Position", "-") != "-": driver_current_state["Status"] = "On Track"
                
                # LastLapTime processing (Overall Best Lap and Lap History)
                new_last_lap_time_info = driver_current_state.get('LastLapTime', {})
                new_lap_time_str = new_last_lap_time_info.get('Value')
                
                if new_lap_time_str and new_lap_time_str != original_last_lap_time_info.get('Value'): # If new lap time
                    lap_time_seconds = utils.parse_lap_time_to_seconds(new_lap_time_str)
                    if lap_time_seconds is not None:
                        current_overall_best_lap_seconds_from_state_val = utils.parse_lap_time_to_seconds(app_state.session_bests["OverallBestLapTime"]["Value"])
                        
                        is_lap_valid_for_overall_best = not driver_current_state.get('InPit', False) and \
                                                        not driver_current_state.get('PitOut', False) and \
                                                        not driver_current_state.get('Stopped', False)

                        if is_lap_valid_for_overall_best and \
                           (current_overall_best_lap_seconds_from_state_val is None or lap_time_seconds < current_overall_best_lap_seconds_from_state_val):
                            app_state.session_bests["OverallBestLapTime"] = {"Value": new_lap_time_str, "DriverNumber": car_num_str}
                            logger.info(f"New Overall Best Lap: {new_lap_time_str} ({lap_time_seconds}s) by {car_num_str} (valid lap conditions met)")
                        
                        # Lap History
                        current_completed_laps = driver_current_state.get('NumberOfLaps', 0) # Should be accurate after general field update
                        lap_number_for_this_time = current_completed_laps 
                        last_recorded_lap_num = 0
                        if car_num_str in app_state.lap_time_history and app_state.lap_time_history[car_num_str]:
                            last_recorded_lap_num = app_state.lap_time_history[car_num_str][-1]['lap_number']

                        if lap_number_for_this_time > 0 and lap_number_for_this_time > last_recorded_lap_num:
                            compound_for_lap = driver_current_state.get('TyreCompound', 'UNKNOWN')
                            if compound_for_lap == '-': compound_for_lap = 'UNKNOWN'
                            is_valid_lap_time_for_history = new_last_lap_time_info.get('OverallFastest', False) or \
                                                new_last_lap_time_info.get('PersonalFastest', False) or \
                                                is_lap_valid_for_overall_best # Use the same validity
                            lap_entry = {
                                'lap_number': lap_number_for_this_time,
                                'lap_time_seconds': lap_time_seconds,
                                'compound': compound_for_lap,
                                'is_valid': is_valid_lap_time_for_history
                            }
                            app_state.lap_time_history[car_num_str].append(lap_entry)
                            logger.debug(f"Added Lap {lap_number_for_this_time} for {car_num_str}: {new_lap_time_str} ({lap_time_seconds}s) on {compound_for_lap}, ValidForHistory: {is_valid_lap_time_for_history}")
                            
                            # WHEN A NEW LAP IS COMPLETED, RESET SECTOR VALUES FOR THE *NEXT* LAP TO "-".
                            # This is a more proactive way to clear them.
                            logger.debug(f"New lap {lap_number_for_this_time} completed by {car_num_str}. Resetting displayed sector values to '-' for next lap anticipation.")
                            for i_reset in range(3):
                                s_idx_reset_str = str(i_reset)
                                if s_idx_reset_str in driver_current_state["Sectors"] and isinstance(driver_current_state["Sectors"][s_idx_reset_str], dict):
                                    driver_current_state["Sectors"][s_idx_reset_str]["Value"] = "-"
                                    # Optionally reset PersonalFastest/OverallFastest flags for these pending sectors too
                                    driver_current_state["Sectors"][s_idx_reset_str]["PersonalFastest"] = False
                                    driver_current_state["Sectors"][s_idx_reset_str]["OverallFastest"] = False
                                else: # Ensure structure exists if it was missing
                                    driver_current_state["Sectors"][s_idx_reset_str] = {"Value": "-", "PersonalFastest": False, "OverallFastest": False}


        # --- Post-loop updates for overall best flags on driver states ---
        overall_best_lap_holder = app_state.session_bests["OverallBestLapTime"]["DriverNumber"]
        overall_best_sector_holders = [
            app_state.session_bests["OverallBestSectors"][0]["DriverNumber"],
            app_state.session_bests["OverallBestSectors"][1]["DriverNumber"],
            app_state.session_bests["OverallBestSectors"][2]["DriverNumber"]
        ]

        for car_num_str_check, driver_state_check in app_state.timing_state.items():
            driver_state_check["IsOverallBestLap"] = (overall_best_lap_holder == car_num_str_check)
            
            current_overall_best_sectors_for_driver = [False, False, False] # Ensure it's a list
            # Ensure IsOverallBestSector exists and is a list
            if not isinstance(driver_state_check.get("IsOverallBestSector"), list) or len(driver_state_check.get("IsOverallBestSector")) != 3 :
                 driver_state_check["IsOverallBestSector"] = [False, False, False] # Initialize if incorrect type/length

            for i in range(3):
                driver_state_check["IsOverallBestSector"][i] = (overall_best_sector_holders[i] == car_num_str_check)
            # No, this line was wrong: current_overall_best_sectors_for_driver[i] = (overall_best_sector_holders[i] == car_num_str_check)
            # driver_state_check["IsOverallBestSector"] = current_overall_best_sectors_for_driver # This was assigning a new list, not modifying in place as intended previously.
                                                                                                # Corrected by directly assigning to driver_state_check["IsOverallBestSector"][i]

            
    elif data:
         logger.warning(f"Unexpected TimingData format received: {type(data)}")


def _process_track_status(data):
    """Handles TrackStatus data."""
    if not isinstance(data, dict):
        logger.warning(f"TrackStatus handler received non-dict data: {data}")
        return

    new_status = data.get('Status', app_state.track_status_data.get('Status', 'Unknown'))
    new_message = data.get('Message', app_state.track_status_data.get('Message', ''))

    if app_state.track_status_data.get('Status') != new_status or \
       app_state.track_status_data.get('Message') != new_message:
        app_state.track_status_data['Status'] = new_status
        app_state.track_status_data['Message'] = new_message
        logger.info(f"Track Status Update: Status={new_status}, Message='{new_message}'")


def _process_position_data(data):
    """Handles Position data. Updates current and previous position data."""
    if not app_state.timing_state: return
    if not isinstance(data, dict) or 'Position' not in data:
        logger.warning(f"Position handler received unexpected format: {type(data)}")
        return

    position_entries_list = data.get('Position', [])
    if not isinstance(position_entries_list, list):
        logger.warning(f"Position data 'Position' key is not a list: {type(position_entries_list)}")
        return

    for entry_group in position_entries_list:
        if not isinstance(entry_group, dict): continue
        timestamp_str = entry_group.get('Timestamp')
        if not timestamp_str: continue

        entries_dict = entry_group.get('Entries', {})
        if not isinstance(entries_dict, dict): continue

        for car_number_str, new_pos_info in entries_dict.items():
            if car_number_str not in app_state.timing_state: continue
            if isinstance(new_pos_info, dict):
                current_driver_state = app_state.timing_state[car_number_str]
                if 'PositionData' in current_driver_state and current_driver_state['PositionData']:
                    current_driver_state['PreviousPositionData'] = current_driver_state['PositionData'].copy()

                current_driver_state['PositionData'] = {
                    'X': new_pos_info.get('X'), 'Y': new_pos_info.get('Y'),
                    'Status': new_pos_info.get('Status'), 'Timestamp': timestamp_str
                }

def _process_car_data(data):
    """Handles CarData."""
    if 'timing_state' not in app_state.__dict__ or not app_state.timing_state: 
        logger.debug("CarData received but timing_state not ready, skipping.")
        return

    if not isinstance(data, dict) or 'Entries' not in data:
        logger.warning(f"CarData handler received unexpected format: {data}")
        return

    entries = data.get('Entries', [])
    if not isinstance(entries, list):
        logger.warning(f"CarData 'Entries' is not a list: {entries}"); return

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

             if 'CarData' not in app_state.timing_state[car_number_str]:
                 app_state.timing_state[car_number_str]['CarData'] = {}
             car_data_dict = app_state.timing_state[car_number_str]['CarData']

             for channel_num_str, data_key in config.CHANNEL_MAP.items(): #
                 if channel_num_str in channels:
                     car_data_dict[data_key] = channels[channel_num_str]
             car_data_dict['Utc'] = utc_time 

             driver_timing_state = app_state.timing_state[car_number_str]
             completed_laps = driver_timing_state.get('NumberOfLaps', -1) 
             try:
                 current_lap_num = int(completed_laps) + 1
                 if current_lap_num <= 0: current_lap_num = 1
             except (ValueError, TypeError):
                 logger.warning(f"CarData: Cannot determine lap for Drv {car_number_str}, LapInfo='{completed_laps}'. Skip history.")
                 continue

             if car_number_str not in app_state.telemetry_data: app_state.telemetry_data[car_number_str] = {}
             if current_lap_num not in app_state.telemetry_data[car_number_str]:
                 app_state.telemetry_data[car_number_str][current_lap_num] = {
                     'Timestamps': [], **{key: [] for key in config.CHANNEL_MAP.values()} #
                 }
                 logger.debug(f"Initialized telemetry storage for Drv {car_number_str}, Lap {current_lap_num}")

             lap_telemetry_history = app_state.telemetry_data[car_number_str][current_lap_num]
             lap_telemetry_history['Timestamps'].append(utc_time)
             for channel_num_str, data_key in config.CHANNEL_MAP.items(): #
                 value = channels.get(channel_num_str)
                 if data_key in ['RPM', 'Speed', 'Gear', 'Throttle', 'Brake', 'DRS']: 
                     try: value = int(value) if value is not None else None
                     except (ValueError, TypeError): value = None 
                 lap_telemetry_history[data_key].append(value)


def _process_session_data(data):
    """ Processes SessionData updates (like status)."""
    if not isinstance(data, dict):
        logger.warning(f"SessionData handler received non-dict data: {data}")
        return
    try:
        status_series = data.get('StatusSeries')
        if isinstance(status_series, dict):
             for entry_key, status_info in status_series.items(): 
                 if isinstance(status_info, dict):
                      session_status = status_info.get('SessionStatus')
                      if session_status:
                           app_state.session_details['SessionStatus'] = session_status
                           logger.info(f"Session Status Updated: {session_status}")

    except Exception as e:
        logger.error(f"Error processing SessionData: {e}", exc_info=True)

def _process_session_info(data): 
    """ Processes SessionInfo data. Starts background track data fetch if session changes."""
    if not isinstance(data, dict):
        logger.warning(f"SessionInfo non-dict: {data}"); return
    try:
        meeting_info = data.get('Meeting', {}); circuit_info = meeting_info.get('Circuit', {});
        country_info = meeting_info.get('Country', {})
        if not isinstance(circuit_info, dict): circuit_info = {}
        if not isinstance(meeting_info, dict): meeting_info = {}
        if not isinstance(country_info, dict): country_info = {}

        year_str = None; start_date_str = data.get('StartDate')
        if start_date_str and isinstance(start_date_str, str) and len(start_date_str) >= 4:
             try: int(start_date_str[:4]); year_str = start_date_str[:4]
             except ValueError: logger.warning(f"Invalid year in StartDate: {start_date_str}")

        circuit_key = circuit_info.get('Key')
        new_session_key = f"{year_str}_{circuit_key}" if year_str and circuit_key is not None and str(circuit_key).strip() else None
        old_session_key = app_state.session_details.get('SessionKey')

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

        needs_fetch = False
        if new_session_key:
            logger.info(f"DataProcessing: SessionKey '{new_session_key}' set from SessionInfo.")
            cached_session_key = app_state.track_coordinates_cache.get('session_key')
            if old_session_key != new_session_key or cached_session_key != new_session_key:
                 logger.info(f"DataProcessing: Proactive track fetch needed for {new_session_key} (Old: {old_session_key}, Cached: {cached_session_key})")
                 needs_fetch = True
            else:
                 logger.debug(f"DataProcessing: Cache key {new_session_key} seems current. No fetch triggered by SessionInfo.")
        else:
             logger.warning(f"DataProcessing: Could not construct valid SessionKey from SessionInfo. Clearing track cache.")
             app_state.session_details.pop('SessionKey', None) 
             app_state.track_coordinates_cache = app_state.INITIAL_TRACK_COORDINATES_CACHE.copy() 

        if needs_fetch:
             fetch_thread = threading.Thread(
                  target=utils._background_track_fetch_and_update, #
                  args=(new_session_key, year_str, circuit_key, app_state), 
                  daemon=True
             )
             fetch_thread.start()
             logger.info(f"DataProcessing: Background track fetch thread started for {new_session_key}.")

        session_key_log = app_state.session_details.get('SessionKey', 'N/A')
        logger.info(f"Processed SessionInfo: Meeting='{meeting_info.get('Name', '?')}', Circuit='{circuit_info.get('Name', '?')}', Session='{data.get('Name', '?')}', Stored SessionKey: {session_key_log}")

    except Exception as e: logger.error(f"Error processing SessionInfo: {e}", exc_info=True)


# --- Main Processing Loop ---
def data_processing_loop():
    processed_count = 0
    last_log_time = time.monotonic()
    log_interval_seconds = 15 
    log_interval_items = 500  
    loop_counter = 0

    logger.info("Data processing thread started.")

    while True:
        loop_counter += 1
        if app_state.stop_event.is_set():
            with app_state.app_state_lock: current_app_overall_state = app_state.app_status.get("state")
            logger.debug(f"DataProcessingLoop: stop_event is set. Current app state: {current_app_overall_state}")
            if current_app_overall_state in ["Idle", "Initializing", "Connecting", "Live", "Replaying", "Stopping", "Stopped"]:
                time.sleep(0.75) 
                if not app_state.stop_event.is_set(): 
                    logger.info("DataProcessingLoop: stop_event was cleared. Continuing loop.")
                    continue
                else:
                    logger.info("DataProcessingLoop: stop_event remains set after pause. Exiting data processing.")
                    break
            else: 
                logger.info(f"DataProcessingLoop: stop_event set and app state ('{current_app_overall_state}') suggests stop. Exiting.")
                break

        if loop_counter % 50 == 0:
             try: q_s = app_state.data_queue.qsize()
             except NotImplementedError: q_s = 'N/A'

        current_time = time.monotonic()
        if (current_time - last_log_time > log_interval_seconds) or \
           (processed_count > 0 and processed_count % log_interval_items == 0):
            try:
                qsize = app_state.data_queue.qsize()
                last_log_time = current_time
            except Exception as q_err:
                logger.warning(f"Could not get queue size for periodic log: {q_err}")

        item = None
        try:
            item = app_state.data_queue.get(block=True, timeout=0.2)
            processed_count +=1

            if not isinstance(item, dict) or 'stream' not in item or 'data' not in item:
                logger.warning(f"Skipping queue item with unexpected structure: {type(item)}")
                if hasattr(app_state.data_queue, 'task_done'): app_state.data_queue.task_done()
                continue

            stream_name = item['stream']
            actual_data = item['data']
            timestamp = item.get('timestamp')

            with app_state.app_state_lock:
                app_state.data_store[stream_name] = {"data": actual_data, "timestamp": timestamp}
                try:
                    if stream_name == "Heartbeat": app_state.app_status["last_heartbeat"] = timestamp
                    elif stream_name == "DriverList": _process_driver_list(actual_data)
                    elif stream_name == "TimingData": _process_timing_data(actual_data)
                    elif stream_name == "SessionInfo": _process_session_info(actual_data) 
                    elif stream_name == "SessionData": _process_session_data(actual_data)
                    elif stream_name == "TimingAppData": _process_timing_app_data(actual_data)
                    elif stream_name == "TrackStatus": _process_track_status(actual_data)
                    elif stream_name == "CarData": _process_car_data(actual_data)
                    elif stream_name == "Position": _process_position_data(actual_data)
                    elif stream_name == "WeatherData": _process_weather_data(actual_data)
                    elif stream_name == "RaceControlMessages": _process_race_control(actual_data)
                except Exception as proc_ex:
                    logger.error(f"ERROR processing stream '{stream_name}': {proc_ex}", exc_info=True)

            if hasattr(app_state.data_queue, 'task_done'): app_state.data_queue.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"!!! Unhandled exception in data_processing_loop main try-except !!! Error: {e}", exc_info=True)
            if item is not None and hasattr(app_state.data_queue, 'task_done'):
                try: app_state.data_queue.task_done()
                except: pass
            time.sleep(0.5)

    logger.info(f"Data processing thread finished. Final app_state.stop_event status: {app_state.stop_event.is_set()}")

print("DEBUG: data_processing module loaded (with refined best lap/sector logic)")
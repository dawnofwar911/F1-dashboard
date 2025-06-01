# utils.py
"""
Utility functions for data processing, file handling, and F1 session info.
"""
import logging
import json
import zlib
import base64
import datetime
from datetime import timezone
import re
import sys
from pathlib import Path
import requests
from shapely.geometry import LineString
import numpy as np
import plotly.graph_objects as go 

# Import for F1 Schedule / Data
try:
    import fastf1
    import pandas as pd
except ImportError:
    logging.warning("FastF1/Pandas not found. Session info features may be limited. Install with: pip install fastf1 pandas")
    fastf1 = None
    pd = None

# Import config for constants
import config
import app_state

logger = logging.getLogger("F1App.Utils")
main_logger = logging.getLogger("F1App.Utils") # Consider consolidating loggers if they serve the same purpose

def prepare_position_data_updates(actual_data_payload, current_position_data_snapshot):
    """
    Parses Position data payload and prepares updates for PositionData and PreviousPositionData.
    Does NOT modify app_state directly.
    Args:
        actual_data_payload (dict): The raw 'Position' stream data.
        current_position_data_snapshot (dict): Snapshot of {car_num_str: current_pos_dict} from app_state.
    Returns:
        dict: position_updates = {car_num_str: {'PreviousPositionData': {...}, 'PositionData': {...}}}
    """
    logger_prep = logging.getLogger("F1App.DataPrep.PositionData") # Or use existing logger
    position_updates = {}

    if not isinstance(actual_data_payload, dict) or 'Position' not in actual_data_payload:
        logger_prep.warning(f"prepare_position_data_updates: Unexpected format: {type(actual_data_payload)}")
        return position_updates

    position_entries_list = actual_data_payload.get('Position', [])
    if not isinstance(position_entries_list, list):
        logger_prep.warning(f"prepare_position_data_updates: 'Position' data is not a list.")
        return position_updates

    for entry_group in position_entries_list:
        if not isinstance(entry_group, dict): continue
        timestamp_str = entry_group.get('Timestamp')
        if not timestamp_str: continue

        entries_dict_payload = entry_group.get('Entries', {})
        if not isinstance(entries_dict_payload, dict): continue

        for car_num_str, new_pos_info_payload in entries_dict_payload.items():
            # We only prepare updates for drivers we are already tracking (from DriverList)
            # This snapshot check ensures we don't add new drivers here.
            if car_num_str not in current_position_data_snapshot: 
                # logger_prep.debug(f"prepare_position_data_updates: Driver {car_num_str} not in snapshot, skipping PositionData.")
                continue

            if isinstance(new_pos_info_payload, dict):
                current_pos_data_for_driver = current_position_data_snapshot.get(car_num_str, {})
                
                new_position_data_for_state = {
                    'X': new_pos_info_payload.get('X'), 
                    'Y': new_pos_info_payload.get('Y'),
                    'Status': new_pos_info_payload.get('Status'), 
                    'Timestamp': timestamp_str
                }
                
                position_updates[car_num_str] = {
                    'PreviousPositionData': current_pos_data_for_driver.copy(), # The old current becomes the new previous
                    'PositionData': new_position_data_for_state
                }
    return position_updates

def prepare_car_data_updates(actual_data_payload, timing_state_snapshot_for_laps):
    """
    Parses CarData payload and prepares updates for car data and telemetry.
    Does NOT modify app_state directly.
    Args:
        actual_data_payload (dict): The raw 'CarData' stream data.
        timing_state_snapshot_for_laps (dict): A snapshot of app_state.timing_state 
                                              (or just relevant parts like {'RacingNumber': {'NumberOfLaps': X}}).
    Returns:
        tuple: (car_specific_updates, telemetry_specific_updates)
               car_specific_updates = {car_num_str: {'CarData': {...}}}
               telemetry_specific_updates = {(car_num_str, lap_num): {'Timestamps': [...], 'Speed': [...], ...}}
    """
    logger_prep = logging.getLogger("F1App.DataPrep.CarData") # Or use existing logger
    car_specific_updates = {}
    telemetry_specific_updates = {}

    if not isinstance(actual_data_payload, dict) or 'Entries' not in actual_data_payload:
        logger_prep.warning(f"prepare_car_data_updates: Unexpected format: {actual_data_payload}")
        return car_specific_updates, telemetry_specific_updates

    entries = actual_data_payload.get('Entries', [])
    if not isinstance(entries, list):
        logger_prep.warning(f"prepare_car_data_updates: 'Entries' is not a list.")
        return car_specific_updates, telemetry_specific_updates

    for entry in entries:
        if not isinstance(entry, dict): continue
        utc_time = entry.get('Utc')
        cars_data_from_payload = entry.get('Cars', {})
        if not isinstance(cars_data_from_payload, dict): continue

        for car_number, car_details_payload in cars_data_from_payload.items():
            car_num_str = str(car_number)
            # We need NumberOfLaps to determine current_lap_num for telemetry.
            # This comes from the timing_state_snapshot.
            driver_timing_info = timing_state_snapshot_for_laps.get(car_num_str, {})
            
            if not driver_timing_info: # Skip if driver not in our timing_state snapshot
                # logger_prep.debug(f"prepare_car_data_updates: Driver {car_num_str} not in timing_state_snapshot, skipping CarData.")
                continue
                
            if not isinstance(car_details_payload, dict): continue
            channels_payload = car_details_payload.get('Channels', {})
            if not isinstance(channels_payload, dict): continue

            # Prepare CarData update for app_state.timing_state
            current_car_data_update = {}
            for channel_num_str_cfg, data_key_cfg in config.CHANNEL_MAP.items():
                if channel_num_str_cfg in channels_payload:
                    current_car_data_update[data_key_cfg] = channels_payload[channel_num_str_cfg]
            current_car_data_update['Utc'] = utc_time
            
            if car_num_str not in car_specific_updates:
                car_specific_updates[car_num_str] = {}
            car_specific_updates[car_num_str]['CarData'] = current_car_data_update

            # Prepare Telemetry update for app_state.telemetry_data
            completed_laps = driver_timing_info.get('NumberOfLaps', -1)
            current_lap_num = -1
            try:
                current_lap_num = int(completed_laps) + 1
                if current_lap_num <= 0: current_lap_num = 1
            except (ValueError, TypeError):
                logger_prep.warning(f"prepare_car_data_updates: Cannot determine lap for Drv {car_num_str}, LapInfo='{completed_laps}'. Skipping telemetry history for this entry.")
                continue # Skip telemetry for this car if lap number is invalid

            telemetry_key = (car_num_str, current_lap_num)
            if telemetry_key not in telemetry_specific_updates:
                telemetry_specific_updates[telemetry_key] = {'Timestamps': [], **{key: [] for key in config.CHANNEL_MAP.values()}}
            
            lap_telemetry_update_ref = telemetry_specific_updates[telemetry_key]
            lap_telemetry_update_ref['Timestamps'].append(utc_time)
            for channel_num_str_cfg, data_key_cfg in config.CHANNEL_MAP.items():
                value = channels_payload.get(channel_num_str_cfg)
                if data_key_cfg in ['RPM', 'Speed', 'Gear', 'Throttle', 'Brake', 'DRS']:
                    try: value = int(value) if value is not None else None
                    except (ValueError, TypeError): value = None
                lap_telemetry_update_ref[data_key_cfg].append(value)
                
    return car_specific_updates, telemetry_specific_updates


def prepare_session_info_data(raw_session_info_data, 
                               current_app_state_session_type_lower, 
                               current_app_state_session_key, 
                               current_app_state_cached_track_key):
    """
    Parses raw SessionInfo data, determines changes, and prepares data for app_state update.
    Returns:
        - details_for_app_state (dict): Data to update in app_state.session_details.
        - reset_flags (dict): Flags indicating if certain app_state fields need resetting.
        - fetch_info (dict or None): Info needed to start a fetch thread, or None.
    """
    logger_util = logging.getLogger("F1App.Utils.SessionInfoParser") # Or use existing logger

    if not isinstance(raw_session_info_data, dict):
        logger_util.warning(f"prepare_session_info_data: raw_session_info_data is not a dict: {raw_session_info_data}")
        return {}, {"reset_q_and_practice": False}, None

    # --- Start parsing raw_session_info_data ---
    parsed_details = {}
    meeting_info = raw_session_info_data.get('Meeting', {})
    circuit_info = meeting_info.get('Circuit', {})
    country_info = raw_session_info_data.get('Country', {})

    # Ensure sub-dictionaries are dicts
    if not isinstance(circuit_info, dict): circuit_info = {}
    if not isinstance(meeting_info, dict): meeting_info = {}
    if not isinstance(country_info, dict): country_info = {}

    parsed_details['Type'] = raw_session_info_data.get('Type')
    parsed_details['Name'] = raw_session_info_data.get('Name')
    parsed_details['Meeting'] = meeting_info
    parsed_details['Circuit'] = circuit_info
    parsed_details['Country'] = country_info
    parsed_details['StartDate'] = raw_session_info_data.get('StartDate')
    parsed_details['EndDate'] = raw_session_info_data.get('EndDate')
    parsed_details['Path'] = raw_session_info_data.get('Path')
    # Add any other fields from SessionInfo you store in app_state.session_details

    year_str = None
    start_date_str_val = raw_session_info_data.get('StartDate')
    if start_date_str_val and isinstance(start_date_str_val, str) and len(start_date_str_val) >= 4:
        try:
            year_str = start_date_str_val[:4]
            int(year_str) # Validate it's a number
        except ValueError:
            logger_util.warning(f"Invalid year in StartDate: {start_date_str_val}")
            year_str = None # Invalidate if not a number
    parsed_details['Year'] = year_str

    circuit_key_from_data = circuit_info.get('Key')
    parsed_details['CircuitKey'] = circuit_key_from_data
    
    new_session_key = None
    if year_str and circuit_key_from_data is not None and str(circuit_key_from_data).strip():
        new_session_key = f"{year_str}_{circuit_key_from_data}"
    parsed_details['SessionKey'] = new_session_key

    # Calculate ScheduledDurationSeconds
    scheduled_duration_seconds = None
    s_date_str = parsed_details.get('StartDate')
    e_date_str = parsed_details.get('EndDate')
    if s_date_str and e_date_str:
        start_dt_obj = parse_iso_timestamp_safe(s_date_str) # Ensure this util exists and works
        end_dt_obj = parse_iso_timestamp_safe(e_date_str)
        if start_dt_obj and end_dt_obj and end_dt_obj > start_dt_obj:
            duration_td = end_dt_obj - start_dt_obj
            scheduled_duration_seconds = duration_td.total_seconds()
    parsed_details['ScheduledDurationSeconds'] = scheduled_duration_seconds
    # --- End parsing raw_session_info_data ---

    reset_flags = {"reset_q_and_practice": False}
    new_session_type_lower = str(parsed_details.get("Type", "")).lower()

    if new_session_type_lower != current_app_state_session_type_lower:
        logger_util.debug(f"Session type will change from '{current_app_state_session_type_lower}' to '{new_session_type_lower}'. Flagging for reset.")
        reset_flags["reset_q_and_practice"] = True
    
    # Determine if track fetch is needed
    needs_fetch = False
    fetch_info = None
    if new_session_key:
        if current_app_state_session_key != new_session_key or current_app_state_cached_track_key != new_session_key:
            logger_util.debug(f"Track fetch will be needed for {new_session_key} (Old AppState Key: {current_app_state_session_key}, Cached Track Key: {current_app_state_cached_track_key})")
            needs_fetch = True
    else:
        logger_util.warning("Could not construct valid new SessionKey from SessionInfo. No fetch will occur.")
        reset_flags["clear_track_cache"] = True # Flag to clear track cache if SessionKey becomes invalid

    if needs_fetch:
        fetch_info = {
            "target": _background_track_fetch_and_update, # Keep utils. if defined in utils
            "args": (new_session_key, year_str, circuit_key_from_data, app_state) # app_state passed for thread to use
        }
        
    # Info for updating practice_session_scheduled_duration_seconds
    practice_duration_update = None
    if new_session_type_lower.startswith("practice") and scheduled_duration_seconds is not None:
        practice_duration_update = scheduled_duration_seconds

    return parsed_details, reset_flags, practice_duration_update, fetch_info


def format_seconds_to_time_str(total_seconds):
    if total_seconds < 0:
        total_seconds = 0
    hours, remainder = divmod(int(total_seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes:02d}:{seconds:02d}"


def parse_session_time_to_seconds(time_str):
    if not time_str or time_str == "-":
        return 0
    parts = list(map(int, time_str.split(':')))
    if len(parts) == 3: # HH:MM:SS
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    elif len(parts) == 2: # MM:SS
        return parts[0] * 60 + parts[1]
    elif len(parts) == 1: # SS (less likely for "Remaining")
        return parts[0]
    return 0

def create_empty_figure_with_message(height, uirevision, message, margins):
    """Helper to create a consistent empty figure with a message."""
    return go.Figure(layout={
        'template': 'plotly_dark',
        'height': height,
        'margin': margins,
        'uirevision': uirevision,
        'xaxis': {'visible': False, 'range': [0,1]},
        'yaxis': {'visible': False, 'range': [0,1]},
        'annotations': [{'text': message, 'xref': 'paper', 'yref': 'paper',
                         'showarrow': False, 'font': {'size': 12}}]
    })

def parse_feed_time_to_seconds(time_str: str):
    """
    Parses a time string from the feed (e.g., "00:19:50.716" or "01:23.456") into total seconds.
    Handles formats MM:SS.ms and HH:MM:SS.ms.
    """
    if not time_str or not isinstance(time_str, str):
        return None
    try:
        parts = time_str.split(':')
        if len(parts) == 3:  # HH:MM:SS.ms
            h = int(parts[0])
            m = int(parts[1])
            s_ms_parts = parts[2].split('.')
            s = int(s_ms_parts[0])
            ms = int(s_ms_parts[1]) if len(s_ms_parts) > 1 else 0
            return h * 3600 + m * 60 + s + (ms / 1000.0)
        elif len(parts) == 2:  # MM:SS.ms
            m = int(parts[0])
            s_ms_parts = parts[1].split('.')
            s = int(s_ms_parts[0])
            ms = int(s_ms_parts[1]) if len(s_ms_parts) > 1 else 0
            return m * 60 + s + (ms / 1000.0)
        else: # Try to parse if it's just seconds.milliseconds
            s_ms_parts = time_str.split('.')
            if len(s_ms_parts) <= 2: # Avoids splitting on multiple dots if any
                s = int(s_ms_parts[0])
                ms = int(s_ms_parts[1]) if len(s_ms_parts) > 1 else 0
                return s + (ms / 1000.0)
            return None
    except (ValueError, TypeError, IndexError) as e:
        # logger_utils.warning(f"Could not parse feed time string '{time_str}': {e}") # Optional logging
        return None

def parse_lap_time_to_seconds(time_str: str):
    """
    Parses an F1 lap time string (e.g., "1:23.456" or "58.789") into total seconds.
    Returns float if successful, None otherwise.
    """
    if not time_str or not isinstance(time_str, str) or time_str == '-':
        return None

    match_min_sec_ms = re.match(r'(\d+):(\d{2})\.(\d{3})', time_str)
    if match_min_sec_ms:
        minutes = int(match_min_sec_ms.group(1))
        seconds = int(match_min_sec_ms.group(2))
        milliseconds = int(match_min_sec_ms.group(3))
        return minutes * 60 + seconds + milliseconds / 1000.0

    match_sec_ms = re.match(r'(\d+)\.(\d{3})', time_str)
    if match_sec_ms:
        seconds = int(match_sec_ms.group(1))
        # <<< CORRECTED LINE --- START >>>
        milliseconds = int(match_sec_ms.group(2)) # Was group(3), should be group(2)
        # <<< CORRECTED LINE --- END >>>
        return seconds + milliseconds / 1000.0

    # Fallback for times that might just be seconds (e.g., "58" or "58.0") - less common for F1 feed
    # but could be useful for other contexts or malformed data.
    # This regex allows for optional decimal part.
    match_s_only = re.match(r'(\d+(?:\.\d+)?)', time_str)
    if match_s_only:
        try:
            # This will directly convert "58" to 58.0 or "58.789" to 58.789
            # It's broader than the previous r'(\d+)' which would only match integers
            val = float(match_s_only.group(1))
            return val
        except ValueError:
            logger.warning(f"Could not parse lap time string (fallback float conversion): '{time_str}'")
            return None


    logger.warning(f"Could not parse lap time string: '{time_str}' with known F1 formats.")
    return None

def convert_utc_str_to_epoch_ms(timestamp_str):
    """
    Parses an F1 UTC timestamp string using the existing parse_iso_timestamp_safe
    and returns milliseconds since epoch. Returns None if parsing fails.
    """
    if not timestamp_str or not isinstance(timestamp_str, str):
        return None

    dt_object = parse_iso_timestamp_safe(timestamp_str)

    if dt_object:
        if dt_object.tzinfo is None:
            dt_object = dt_object.replace(tzinfo=timezone.utc)
        else:
            dt_object = dt_object.astimezone(timezone.utc)
        return int(dt_object.timestamp() * 1000)
    return None
    
def find_closest_point_index(track_x_coords, track_y_coords, point_x, point_y):
    """
    Finds the index of the closest point in track_x_coords/track_y_coords to point_x/point_y.
    """
    if not track_x_coords or not track_y_coords:
        return None
    
    track_points = np.array(list(zip(track_x_coords, track_y_coords)))
    target_point = np.array([point_x, point_y])
    
    distances_squared = np.sum((track_points - target_point)**2, axis=1)
    closest_index = np.argmin(distances_squared)
    return closest_index

def _fetch_track_data_for_cache(session_key, year, circuit_key): # Existing parameters
    """
    Fetches track data (outline, corners, marshal posts, marshal sectors) from the MultiViewer API.
    Returns a dict for the cache or None on failure.
    """
    if not year or not circuit_key:
        main_logger.error(
            f"Fetch Helper: Invalid year or circuit key ({year}, {circuit_key})")
        return None

    # --- Fetch data from MultiViewer API (existing logic for track line) ---
    api_url = config.MULTIVIEWER_CIRCUIT_API_URL_TEMPLATE.format(circuit_key=circuit_key, year=year)
    main_logger.info(f"Fetch Helper: API fetch initiated for: {api_url}") # Changed from debug to info
    
    map_api_data = None # Initialize to ensure it's defined
    track_x_coords, track_y_coords, track_linestring_obj, x_range, y_range = [None]*5
    
    try:
        response = requests.get(
            api_url, headers={'User-Agent': config.MULTIVIEWER_API_USER_AGENT}, timeout=config.REQUESTS_TIMEOUT_SECONDS, verify=False)
        response.raise_for_status()
        map_api_data = response.json() # Store the full JSON response
        
        # Process main track line (x, y coordinates)
        temp_x_api = [float(p) for p in map_api_data.get('x', [])]
        temp_y_api = [float(p) for p in map_api_data.get('y', [])]

        if temp_x_api and temp_y_api and len(temp_x_api) == len(temp_y_api) and len(temp_x_api) > 1:
            _api_ls = LineString(zip(temp_x_api, temp_y_api))
            if _api_ls.length > 0:
                track_x_coords, track_y_coords, track_linestring_obj = temp_x_api, temp_y_api, _api_ls
                x_min, x_max = np.min(track_x_coords), np.max(track_x_coords)
                y_min, y_max = np.min(track_y_coords), np.max(track_y_coords)
                pad_x = (x_max - x_min) * 0.05
                pad_y = (y_max - y_min) * 0.05
                x_range = [x_min - pad_x, x_max + pad_x]
                y_range = [y_min - pad_y, y_max + pad_y]
                main_logger.info(
                    f"Fetch Helper: API SUCCESS for {session_key}. Main track line loaded.")
            else:
                main_logger.warning(
                    f"Fetch Helper: API {session_key} provided zero-length main track line.")
        else:
            main_logger.warning(
                f"Fetch Helper: API {session_key} no valid x/y for main track line.")
    except Exception as e_api:
        main_logger.error(
            f"Fetch Helper: API FAILED for {session_key} (main track data): {e_api}", exc_info=True) # Changed to True for more detail
        return None # If main track line fails, critical failure

    # --- Initialize variables for additional static data from the SAME API response ---
    corners_data_processed = None
    marshal_lights_data_processed = None
    marshal_sector_points_raw = None # Will hold the direct 'marshalSectors' list from API
    marshal_sector_segments_calculated = None

    # --- Process additional data if map_api_data was successfully fetched ---
    if map_api_data:
        # Process Corners
        if 'corners' in map_api_data and isinstance(map_api_data['corners'], list):
            corners_data_processed = []
            for corner in map_api_data['corners']:
                if isinstance(corner, dict) and 'number' in corner and 'trackPosition' in corner:
                    track_pos = corner['trackPosition']
                    if isinstance(track_pos, dict): # Ensure trackPosition is a dict
                        corners_data_processed.append({
                            'number': corner['number'],
                            'x': track_pos.get('x'),
                            'y': track_pos.get('y')
                        })
            logger.debug(f"Processed {len(corners_data_processed)} corners from API response.")
        else:
            logger.warning(f"No 'corners' data or invalid format in API response for {session_key}.")
            
        # Process Marshal Lights (for static markers)
        if 'marshalLights' in map_api_data and isinstance(map_api_data['marshalLights'], list):
            marshal_lights_data_processed = []
            for light in map_api_data['marshalLights']:
                if isinstance(light, dict) and 'number' in light and 'trackPosition' in light:
                    track_pos = light['trackPosition']
                    if isinstance(track_pos, dict): # Ensure trackPosition is a dict
                        marshal_lights_data_processed.append({
                            'number': light['number'],
                            'x': track_pos.get('x'),
                            'y': track_pos.get('y')
                        })
            logger.debug(f"Processed {len(marshal_lights_data_processed)} marshal lights from API response.")
        else:
            logger.warning(f"No 'marshalLights' data or invalid format in API response for {session_key}.")

        # Store raw Marshal Sector points and calculate segments
        if 'marshalSectors' in map_api_data and isinstance(map_api_data['marshalSectors'], list):
            marshal_sector_points_raw = map_api_data['marshalSectors'] # Store the raw list
            logger.debug(f"Found {len(marshal_sector_points_raw)} raw marshal sector points in API response.")

            if track_x_coords and track_y_coords and marshal_sector_points_raw:
                marshal_sector_segments_calculated = {}
                sorted_marshal_sectors_from_api = sorted(marshal_sector_points_raw, key=lambda s: s.get('number', float('inf')))
                
                sector_start_indices = {}
                for sector_info in sorted_marshal_sectors_from_api:
                    s_num = sector_info.get('number')
                    s_pos_dict = sector_info.get('trackPosition')
                    if s_num is not None and isinstance(s_pos_dict, dict):
                        s_x = s_pos_dict.get('x')
                        s_y = s_pos_dict.get('y')
                        if s_x is not None and s_y is not None:
                            closest_idx = find_closest_point_index(track_x_coords, track_y_coords, s_x, s_y)
                            if closest_idx is not None:
                                sector_start_indices[s_num] = closest_idx
                            else:
                                logger.warning(f"Could not find closest track point for marshal sector {s_num} point ({s_x}, {s_y}).")
                        else:
                            logger.warning(f"Marshal sector {s_num} missing x or y in trackPosition.")
                    else:
                        logger.warning(f"Invalid marshal sector entry or missing number/trackPosition: {sector_info}")
                
                sorted_sector_indices_list = sorted(sector_start_indices.items())

                if len(sorted_sector_indices_list) > 0 and track_x_coords: # Ensure track_x_coords is not None
                    if len(sorted_sector_indices_list) == 1:
                        s_num, s_idx = sorted_sector_indices_list[0]
                        # Single sector covers the whole track for highlighting purposes
                        marshal_sector_segments_calculated[s_num] = (0, len(track_x_coords) - 1)
                    else: # Multiple sectors
                        for i in range(len(sorted_sector_indices_list)):
                            current_sector_num, current_start_idx = sorted_sector_indices_list[i]
                            
                            if i + 1 < len(sorted_sector_indices_list):
                                _, next_start_idx = sorted_sector_indices_list[i+1]
                                # Ensure segment has at least one point; end index is exclusive for slicing if start=end
                                end_idx = max(current_start_idx, next_start_idx -1) 
                            else:
                                # Last sector: runs from its start to the end of the track line
                                end_idx = len(track_x_coords) - 1
                            
                            if current_start_idx <= end_idx: # Ensure valid range
                                marshal_sector_segments_calculated[current_sector_num] = (current_start_idx, end_idx)
                            else: # Should ideally not happen if sorted correctly and indices are distinct
                                marshal_sector_segments_calculated[current_sector_num] = (current_start_idx, current_start_idx) # Fallback to single point
                                logger.warning(f"Marshal sector {current_sector_num} resulted in start_idx ({current_start_idx}) > end_idx ({end_idx}). Defined as single point.")
                
                logger.info(f"Calculated {len(marshal_sector_segments_calculated)} marshal sector segments.")
                if not marshal_sector_segments_calculated and marshal_sector_points_raw:
                    logger.warning("Could not calculate marshal sector segments despite having raw points from API. Check indices and track data.")
        else:
            logger.warning(f"No 'marshalSectors' data or invalid format in API response for {session_key}.")
    else:
        main_logger.warning(f"Fetch Helper: map_api_data is None for {session_key}, cannot process corners/marshal info.")


    # --- Prepare data for app_state.track_coordinates_cache ---
    cache_update_data = {
        'session_key': session_key, 
        'x': track_x_coords, 
        'y': track_y_coords,
        'linestring': track_linestring_obj,
        'range_x': x_range, 
        'range_y': y_range,
        'corners_data': corners_data_processed,
        'marshal_lights_data': marshal_lights_data_processed,
        'marshal_sector_points': marshal_sector_points_raw,
        'marshal_sector_segments': marshal_sector_segments_calculated,
        'rotation': map_api_data.get('rotation') if map_api_data and isinstance(map_api_data, dict) else None
    }
    
    ls_type = type(cache_update_data.get('linestring')).__name__
    num_corners = len(corners_data_processed or [])
    num_lights = len(marshal_lights_data_processed or [])
    num_segments = len(marshal_sector_segments_calculated or [])

    main_logger.info( # Changed to info for better visibility of successful load
        f"Fetch Helper: Returning data for {session_key}. LineString: {ls_type}, X points: {len(track_x_coords or [])}, "
        f"Corners: {num_corners}, Lights: {num_lights}, Segments: {num_segments}")
    return cache_update_data

def _background_track_fetch_and_update(session_key, year, circuit_key, app_state_module): # Renamed app_state to app_state_module
    """Runs fetch in background and updates cache under lock."""
    fetched_data = _fetch_track_data_for_cache(session_key, year, circuit_key)
    if fetched_data:
        with app_state_module.app_state_lock: # Use app_state_module
            current_session_in_state = app_state_module.session_details.get( # Use app_state_module
                'SessionKey')
            if current_session_in_state == session_key:
                main_logger.info(
                    f"Background Fetch: Updating cache for {session_key}.")
                app_state_module.track_coordinates_cache = fetched_data # Use app_state_module
                ls_type = type(app_state_module.track_coordinates_cache.get( # Use app_state_module
                    'linestring')).__name__
                main_logger.debug(
                    f"Background Fetch: Cache updated. Linestring Type={ls_type}")
            else:
                main_logger.warning(
                    f"Background Fetch: Session changed ({current_session_in_state}) while fetching for {session_key}. Discarding fetched data.")
    else:
        main_logger.error(
            f"Background Fetch: Fetch helper failed for {session_key}. Cache not updated.")

def sanitize_filename(name):
    """Removes/replaces characters unsuitable for filenames."""
    if not name: return "Unknown"
    name = str(name).strip()
    name = re.sub(r'[\\/:*?"<>|\s\-\:\.,\(\)]+', '_', name)
    name = re.sub(r'[^\w_]+', '', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    return name if name else "InvalidName"

def _decode_and_decompress(encoded_data):
    """Decodes base64 encoded and zlib decompressed data (message payload)."""
    if not encoded_data or not isinstance(encoded_data, str):
        return None

    try:
        missing_padding = len(encoded_data) % 4
        if missing_padding:
            encoded_data += '=' * (4 - missing_padding)
        decoded_data = base64.b64decode(encoded_data)
        decompressed_data = zlib.decompress(decoded_data, -zlib.MAX_WBITS)
        return json.loads(decompressed_data.decode('utf-8'))
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error after decompression: {e}. Data sample: {decompressed_data[:100]}...", exc_info=False)
        return None
    except Exception as e:
        logger.error(f"Decode/Decompress error: {e}. Data: {str(encoded_data)[:50]}...", exc_info=False)
        return None

def parse_iso_timestamp_safe(timestamp_str, line_num_for_log="?"):
    """
    Safely parses an ISO timestamp string, replacing 'Z', padding/truncating
    microseconds to EXACTLY 6 digits, and handling potential errors.
    Returns a datetime object or None.
    """
    if not timestamp_str or not isinstance(timestamp_str, str):
        return None

    cleaned_ts = timestamp_str
    timestamp_to_parse = cleaned_ts

    try:
        cleaned_ts = timestamp_str.replace('Z', '+00:00')
        timestamp_to_parse = cleaned_ts

        if '.' in cleaned_ts:
            parts = cleaned_ts.split('.', 1)
            integer_part = parts[0]
            fractional_part_full = parts[1]
            offset_part = ''

            # Correctly find the start of the timezone offset if present after fractional seconds
            plus_offset_match = re.search(r'\+(\d{2}:\d{2})$', fractional_part_full)
            minus_offset_match = re.search(r'-(\d{2}:\d{2})$', fractional_part_full)

            if plus_offset_match:
                offset_part = '+' + plus_offset_match.group(1)
                fractional_part = fractional_part_full[:plus_offset_match.start()]
            elif minus_offset_match:
                offset_part = '-' + minus_offset_match.group(1)
                fractional_part = fractional_part_full[:minus_offset_match.start()]
            else: # No explicit offset found after fractional part
                fractional_part = fractional_part_full
                if '+00:00' not in timestamp_to_parse and not offset_part : # Avoid double adding +00:00 if already there from Z replace
                     # Check if the original string ended with Z, implying UTC
                     if not timestamp_str.endswith('Z') and not re.search(r'[+\-]\d{2}:\d{2}$', timestamp_str):
                         # If no offset and not Z, it's ambiguous, but F1 feed usually implies UTC or has Z
                         # For safety, if no offset info at all, we might assume UTC based on context or log.
                         # Here, if Z was replaced, +00:00 is already part of timestamp_to_parse.
                         # If no Z and no offset, we might need to decide on a default or log warning.
                         # Let's assume if no offset is found AND timestamp_to_parse doesn't have one from 'Z' replace,
                         # it might be a local time or needs UTC assumption.
                         # The original `fromisoformat` would handle it if it's a valid ISO without offset.
                         pass # Let fromisoformat handle cases without explicit offset if it can

            # Ensure fractional part is exactly 6 digits
            if fractional_part: # Only pad/truncate if fractional part exists
                fractional_part_padded = f"{fractional_part:<06s}"[:6]
                timestamp_to_parse = f"{integer_part}.{fractional_part_padded}{offset_part}"
            else: # No fractional part
                timestamp_to_parse = f"{integer_part}{offset_part}"


        parsed_dt = datetime.datetime.fromisoformat(timestamp_to_parse)
        # Ensure timezone is set to UTC if it's naive or already UTC
        if parsed_dt.tzinfo is None or parsed_dt.tzinfo == datetime.timezone.utc:
             return parsed_dt.replace(tzinfo=timezone.utc)
        else: # If it has other timezone info, convert to UTC
             return parsed_dt.astimezone(timezone.utc)

    except ValueError as e:
        logger.warning(f"Timestamp format error line {line_num_for_log}: Original='{timestamp_str}', ParsedAttempt='{timestamp_to_parse}'. Err: {e}")
        # Attempt to parse without microseconds if the format is an issue there
        try:
            if '.' in timestamp_to_parse:
                base_ts_no_ms = timestamp_to_parse.split('.')[0]
                offset_if_any = timestamp_to_parse.split('.')[-1] # Get part after '.'
                # find offset again if it was with ms
                plus_offset_match = re.search(r'\+(\d{2}:\d{2})$', offset_if_any)
                minus_offset_match = re.search(r'-(\d{2}:\d{2})$', offset_if_any)
                final_ts_no_ms = base_ts_no_ms
                if plus_offset_match: final_ts_no_ms += '+' + plus_offset_match.group(1)
                elif minus_offset_match: final_ts_no_ms += '-' + minus_offset_match.group(1)
                elif timestamp_str.endswith('Z'): final_ts_no_ms += "+00:00"

                parsed_dt_no_ms = datetime.datetime.fromisoformat(final_ts_no_ms)
                logger.debug(f"Successfully parsed timestamp '{timestamp_str}' without microseconds after initial failure.")
                if parsed_dt_no_ms.tzinfo is None or parsed_dt_no_ms.tzinfo == datetime.timezone.utc:
                    return parsed_dt_no_ms.replace(tzinfo=timezone.utc)
                else:
                    return parsed_dt_no_ms.astimezone(timezone.utc)
        except ValueError:
            pass # If this also fails, the original None will be returned
        return None
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error parsing timestamp line {line_num_for_log}: Original='{timestamp_str}', ParsedAttempt='{timestamp_to_parse}'. Err: {e}", exc_info=True)
        return None


def get_current_or_next_session_info():
    """
    Uses FastF1 to find the currently ongoing session (if started recently)
    OR the next upcoming session.
    Returns event_name (str), session_name (str) or None, None.
    """
    if fastf1 is None or pd is None:
        logger.error("FastF1 or Pandas not available for session info.")
        return None, None

    try:
        try:
            # Use constant from config.py for cache_dir
            cache_dir = config.FASTF1_CACHE_DIR.resolve() #
            cache_dir.mkdir(parents=True, exist_ok=True)
            fastf1.Cache.enable_cache(cache_dir)
            logger.debug(f"FastF1 Cache enabled at: {cache_dir}")
        except Exception as cache_err:
            logger.warning(f"Could not configure FastF1 cache: {cache_err}")


        year = datetime.datetime.now().year
        logger.debug(f"FastF1: Fetching schedule for {year}...")
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        now = pd.Timestamp.now(tz='UTC')
        logger.debug(f"FastF1: Current UTC time: {now}")

        last_past_session = {'date': pd.Timestamp.min.tz_localize('UTC'), 'event_name': None, 'session_name': None}
        next_future_session = {'date': pd.Timestamp.max.tz_localize('UTC'), 'event_name': None, 'session_name': None}

        for index, event in schedule.iterrows():
            for i in range(1, 6): # Check up to Session5
                session_date_col = f'Session{i}DateUtc'
                session_name_col = f'Session{i}'

                if session_date_col in event and pd.notna(event[session_date_col]):
                    session_date = event[session_date_col]
                    # Ensure session_date is a Timestamp object
                    if not isinstance(session_date, pd.Timestamp):
                         try: session_date = pd.Timestamp(session_date)
                         except: logger.warning(f"Could not parse date: {event[session_date_col]} for {event.get('EventName')} Session {i}"); continue
                    
                    # Ensure session_date is timezone-aware (UTC)
                    if session_date.tzinfo is None:
                         session_date = session_date.tz_localize('UTC')
                    else: # If it has a timezone, convert to UTC for consistent comparison
                         session_date = session_date.tz_convert('UTC')


                    if session_date > now: # Future session
                        if session_date < next_future_session['date']:
                            next_future_session.update({
                                'date': session_date,
                                'event_name': event.get('EventName'),
                                'session_name': event.get(session_name_col)
                            })
                    elif session_date <= now: # Past or current session start time
                        if session_date > last_past_session['date']:
                            last_past_session.update({
                                'date': session_date,
                                'event_name': event.get('EventName'),
                                'session_name': event.get(session_name_col)
                            })

        # Consider a session "ongoing" if its official start time was within the last X hours
        ongoing_window = pd.Timedelta(hours=config.FASTF1_ONGOING_SESSION_WINDOW_HOURS) # Use from config

        if last_past_session.get('event_name') and (now - last_past_session.get('date', now)) <= ongoing_window:
            logger.debug(f"FastF1: Using ongoing session: {last_past_session['event_name']} - {last_past_session['session_name']}")
            return last_past_session['event_name'], last_past_session['session_name']
        elif next_future_session.get('event_name'):
            logger.debug(f"FastF1: Using next future session: {next_future_session['event_name']} - {next_future_session['session_name']}")
            return next_future_session['event_name'], next_future_session['session_name']
        else:
            logger.warning("FastF1: Could not determine current or next session from available schedule data.")
            return None, None

    except Exception as e:
        logger.error(f"FastF1 Error getting session info: {e}", exc_info=True)
        return None, None

def get_nested_state(d, *keys, default=None):
    """Safely accesses nested dictionary keys."""
    val = d
    for key in keys:
        if isinstance(val, dict):
            val = val.get(key)
        elif isinstance(val, list): # Handle list indexing if key is int-like
             try:
                  key_int = int(key)
                  if 0 <= key_int < len(val):
                       val = val[key_int]
                  else: # Index out of bounds
                       return default
             except (ValueError, TypeError): # Key not convertible to int for list
                  return default
        else: # Not a dict or list, cannot go deeper
            return default
        if val is None: # If .get() returned None or list index was None
             return default
    return val

def pos_sort_key(item):
    """Sort key function for DataTable position column."""
    pos_str = item.get('Pos', '999') # Default to a high number for sorting
    if isinstance(pos_str, (int, float)): return pos_str
    if isinstance(pos_str, str) and pos_str.isdigit():
        try: return int(pos_str)
        except ValueError: return 999 # Should not happen if isdigit() is true but good practice
    return 999 # For non-numeric positions like 'NC', '-', or if it's unexpectedly not a string/number

def generate_driver_options(timing_state_dict):
    """Generates list of options for driver dropdowns from timing_state."""
    options = []
    logger.debug(f"Generating driver options from timing_state keys: {list(timing_state_dict.keys())}")

    if not timing_state_dict or not isinstance(timing_state_dict, dict):
        logger.warning("generate_driver_options received empty or invalid timing_state.")
        # Use constant from config.py
        return config.DROPDOWN_NO_DRIVERS_OPTIONS #

    driver_list_for_sorting = []
    for driver_num, driver_data in timing_state_dict.items():
        if isinstance(driver_data, dict):
             # Ensure RacingNumber is treated as a string for labels, default to driver_num if missing
             racing_number_label = str(driver_data.get('RacingNumber', driver_num))
             driver_list_for_sorting.append({
                 'value': driver_num, # This should be the car_num_str key from timing_state
                 'number': racing_number_label,
                 'tla': driver_data.get('Tla', '???'),
                 'name': driver_data.get('FullName', 'Unknown Driver')
             })

    # Sort by racing number (as integer if possible)
    def sort_key(item):
        try: return int(item.get('number', 999))
        except (ValueError, TypeError): return 999 # Fallback for non-integer numbers

    sorted_drivers = sorted(driver_list_for_sorting, key=sort_key)

    for driver in sorted_drivers:
        label = f"{driver['tla']} (#{driver['number']}) - {driver['name']}"
        options.append({'label': label, 'value': driver['value']})

    if not options:
        # Use constant from config.py
         return config.DROPDOWN_NO_DRIVERS_PROCESSED_OPTIONS #

    return options

class RecordDataFilter(logging.Filter):
    """Logging filter to control recording based on app_state."""
    def filter(self, record):
        # Dynamically import app_state here to avoid circular dependencies at module load time
        # This is generally safe for filters as they are called after module setup.
        try:
            import app_state as current_app_state # Use an alias to avoid conflict if app_state is also a parameter
            return current_app_state.is_saving_active
        except ImportError:
            # Fallback if app_state cannot be imported (should not happen in normal operation)
            return False


print("DEBUG: utils module loaded")
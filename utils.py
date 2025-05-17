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

logger = logging.getLogger("F1App.Utils")
main_logger = logging.getLogger("F1App.Utils") # Consider consolidating loggers if they serve the same purpose

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

def _fetch_track_data_for_cache(session_key, year, circuit_key):
    """Fetches track data from API. Returns a dict for the cache or None on failure."""
    if not year or not circuit_key:
        main_logger.error(
            f"Fetch Helper: Invalid year or circuit key ({year}, {circuit_key})")
        return None

    # Use constant from config.py
    api_url = config.MULTIVIEWER_CIRCUIT_API_URL_TEMPLATE.format(circuit_key=circuit_key, year=year) #
    main_logger.info(f"Fetch Helper: API fetch initiated for: {api_url}")
    track_x_coords, track_y_coords, track_linestring_obj, x_range, y_range = [
        None]*5
    try:
        response = requests.get(
            api_url, headers={'User-Agent': config.MULTIVIEWER_API_USER_AGENT}, timeout=config.REQUESTS_TIMEOUT_SECONDS, verify=False) #
        response.raise_for_status()
        map_api_data = response.json()
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
                    f"Fetch Helper: API SUCCESS for {session_key}.")
            else:
                main_logger.warning(
                    f"Fetch Helper: API {session_key} provided zero-length track.")
        else:
            main_logger.warning(
                f"Fetch Helper: API {session_key} no valid x/y.")
    except Exception as e_api:
        main_logger.error(
            f"Fetch Helper: API FAILED for {session_key}: {e_api}", exc_info=False)

    cache_update_data = {
        'session_key': session_key, 'x': track_x_coords, 'y': track_y_coords,
        'linestring': track_linestring_obj, 'range_x': x_range, 'range_y': y_range
    }
    ls_type = type(cache_update_data.get('linestring')).__name__
    main_logger.debug(
        f"Fetch Helper: Returning data. Linestring Type={ls_type}, X is None: {track_x_coords is None}")
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
                logger.info(f"Successfully parsed timestamp '{timestamp_str}' without microseconds after initial failure.")
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
            logger.info(f"FastF1: Using ongoing session: {last_past_session['event_name']} - {last_past_session['session_name']}")
            return last_past_session['event_name'], last_past_session['session_name']
        elif next_future_session.get('event_name'):
            logger.info(f"FastF1: Using next future session: {next_future_session['event_name']} - {next_future_session['session_name']}")
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


print("DEBUG: utils module loaded (with create_empty_figure helper, config usage, and corrected parse_lap_time_to_seconds)")
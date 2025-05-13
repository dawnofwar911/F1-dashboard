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

# Import for F1 Schedule / Data
try:
    import fastf1
    import pandas as pd
except ImportError:
    # Log error but allow app to potentially continue without FastF1 features
    logging.warning("FastF1/Pandas not found. Session info features may be limited. Install with: pip install fastf1 pandas")
    fastf1 = None
    pd = None

logger = logging.getLogger("F1App.Utils")
main_logger = logging.getLogger("F1App.Utils")

def parse_lap_time_to_seconds(time_str: str) -> float | None:
    """
    Parses an F1 lap time string (e.g., "1:23.456" or "58.789") into total seconds.
    Returns float if successful, None otherwise.
    """
    if not time_str or not isinstance(time_str, str) or time_str == '-':
        return None

    # Check for minute:second.millisecond format
    match_min_sec_ms = re.match(r'(\d+):(\d{2})\.(\d{3})', time_str)
    if match_min_sec_ms:
        minutes = int(match_min_sec_ms.group(1))
        seconds = int(match_min_sec_ms.group(2))
        milliseconds = int(match_min_sec_ms.group(3))
        return minutes * 60 + seconds + milliseconds / 1000.0

    # Check for second.millisecond format
    match_sec_ms = re.match(r'(\d+)\.(\d{3})', time_str)
    if match_sec_ms:
        seconds = int(match_sec_ms.group(1))
        milliseconds = int(match_sec_ms.group(2))
        return seconds + milliseconds / 1000.0
    
    # Check for just seconds (less common for lap times, but possible)
    match_s = re.match(r'(\d+)', time_str)
    if match_s:
        try:
            return float(time_str) # Could be an int or float already
        except ValueError:
            pass # Fall through if not a simple float

    logger.warning(f"Could not parse lap time string: '{time_str}'")
    return None

def convert_utc_str_to_epoch_ms(timestamp_str):
    """
    Parses an F1 UTC timestamp string using the existing parse_iso_timestamp_safe
    and returns milliseconds since epoch. Returns None if parsing fails.
    """
    if not timestamp_str or not isinstance(timestamp_str, str):
        # logger.debug(f"convert_utc_str_to_epoch_ms: Invalid input - {timestamp_str}")
        return None
    
    # Assuming parse_iso_timestamp_safe is robust and handles various F1 TS formats
    dt_object = parse_iso_timestamp_safe(timestamp_str) 
    
    if dt_object:
        # Ensure it's UTC before getting timestamp
        if dt_object.tzinfo is None: # If naive, assume it's UTC as per F1 data context
            dt_object = dt_object.replace(tzinfo=timezone.utc)
        else: # If timezone-aware, convert to UTC
            dt_object = dt_object.astimezone(timezone.utc)
        return int(dt_object.timestamp() * 1000)
    
    # logger.warning(f"convert_utc_str_to_epoch_ms: Failed to parse '{timestamp_str}' using parse_iso_timestamp_safe.")
    return None

def _fetch_track_data_for_cache(session_key, year, circuit_key):
    """Fetches track data from API. Returns a dict for the cache or None on failure."""
    # Ensure year and circuit_key are usable strings for the URL
    if not year or not circuit_key:
        main_logger.error(
            f"Fetch Helper: Invalid year or circuit key ({year}, {circuit_key})")
        return None  # Return None to indicate failure

    api_url = f"https://api.multiviewer.app/api/v1/circuits/{circuit_key}/{year}"
    main_logger.info(f"Fetch Helper: API fetch initiated for: {api_url}")
    track_x_coords, track_y_coords, track_linestring_obj, x_range, y_range = [
        None]*5
    try:
        response = requests.get(
            api_url, headers={'User-Agent': 'F1-Dash/0.4'}, timeout=15)
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
        # Keep log concise
        main_logger.error(
            f"Fetch Helper: API FAILED for {session_key}: {e_api}", exc_info=False)

    # Return results in a dictionary matching cache structure
    cache_update_data = {
        'session_key': session_key, 'x': track_x_coords, 'y': track_y_coords,
        'linestring': track_linestring_obj, 'range_x': x_range, 'range_y': y_range
    }
    # Log what we are returning
    ls_type = type(cache_update_data.get('linestring')).__name__
    main_logger.debug(
        f"Fetch Helper: Returning data. Linestring Type={ls_type}, X is None: {track_x_coords is None}")
    return cache_update_data

# --- Target function for the background fetch thread ---
def _background_track_fetch_and_update(session_key, year, circuit_key, app_state):
    """Runs fetch in background and updates cache under lock."""
    fetched_data = _fetch_track_data_for_cache(session_key, year, circuit_key)
    # Only update cache if fetch returned data (even if partial/None values)
    if fetched_data:
        with app_state.app_state_lock:  # Acquire lock ONLY for cache update
            # Check if session key hasn't changed AGAIN since fetch started
            current_session_in_state = app_state.session_details.get(
                'SessionKey')
            if current_session_in_state == session_key:
                main_logger.info(
                    f"Background Fetch: Updating cache for {session_key}.")
                app_state.track_coordinates_cache = fetched_data
                ls_type = type(app_state.track_coordinates_cache.get(
                    'linestring')).__name__
                main_logger.debug(
                    f"Background Fetch: Cache updated. Linestring Type={ls_type}")
            else:
                main_logger.warning(
                    f"Background Fetch: Session changed ({current_session_in_state}) while fetching for {session_key}. Discarding fetched data.")
    else:
        main_logger.error(
            f"Background Fetch: Fetch helper failed for {session_key}. Cache not updated.")

# --- Filename Sanitization ---
def sanitize_filename(name):
    """Removes/replaces characters unsuitable for filenames."""
    if not name: return "Unknown"
    name = str(name).strip()
    # Replace spaces and various invalid characters with underscores
    name = re.sub(r'[\\/:*?"<>|\s\-\:\.,\(\)]+', '_', name)
    # Remove any remaining non-alphanumeric or non-underscore characters
    name = re.sub(r'[^\w_]+', '', name)
    # Consolidate multiple underscores
    name = re.sub(r'_+', '_', name)
    # Remove leading/trailing underscores
    name = name.strip('_')
    return name if name else "InvalidName"

# --- Data Decoding ---
def _decode_and_decompress(encoded_data):
    """Decodes base64 encoded and zlib decompressed data (message payload)."""
    if not encoded_data or not isinstance(encoded_data, str):
        # logger.warning(f"decode_and_decompress received non-string or empty data: type {type(encoded_data)}")
        return None

    try:
        # Add padding if necessary
        missing_padding = len(encoded_data) % 4
        if missing_padding:
            encoded_data += '=' * (4 - missing_padding)
        decoded_data = base64.b64decode(encoded_data)
        # Use -zlib.MAX_WBITS for raw deflate data
        decompressed_data = zlib.decompress(decoded_data, -zlib.MAX_WBITS)
        return json.loads(decompressed_data.decode('utf-8'))
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error after decompression: {e}. Data sample: {decompressed_data[:100]}...", exc_info=False)
        return None
    except Exception as e:
        # Log less verbosely, include only first part of encoded data
        logger.error(f"Decode/Decompress error: {e}. Data: {str(encoded_data)[:50]}...", exc_info=False)
        return None

# --- Timestamp Parsing ---
def parse_iso_timestamp_safe(timestamp_str, line_num_for_log="?"):
    """
    Safely parses an ISO timestamp string, replacing 'Z', padding/truncating
    microseconds to EXACTLY 6 digits, and handling potential errors.
    Returns a datetime object or None.
    """
    if not timestamp_str or not isinstance(timestamp_str, str):
        return None

    cleaned_ts = timestamp_str # Initialize with original
    timestamp_to_parse = cleaned_ts # Initialize

    try:
        # Always replace 'Z' first
        cleaned_ts = timestamp_str.replace('Z', '+00:00')
        timestamp_to_parse = cleaned_ts  # Default if no fractional part

        if '.' in cleaned_ts:
            parts = cleaned_ts.split('.', 1)
            integer_part = parts[0]
            fractional_part_full = parts[1]
            offset_part = ''

            # Split fractional part from timezone offset
            if '+' in fractional_part_full:
                frac_parts = fractional_part_full.split('+', 1)
                fractional_part = frac_parts[0]
                offset_part = '+' + frac_parts[1]
            elif '-' in fractional_part_full:
                frac_parts = fractional_part_full.split('-', 1)
                fractional_part = frac_parts[0]
                offset_part = '-' + frac_parts[1]
            else: # Assume UTC offset if Z was replaced or no offset present
                fractional_part = fractional_part_full
                # If original didn't end with Z and had no offset, this might be wrong
                # but fromisoformat needs an offset. Assume UTC if missing.
                if '+00:00' not in timestamp_to_parse:
                     offset_part = '+00:00' # Assume UTC if Z wasn't there and no offset


            # Pad/truncate fractional part to exactly 6 digits
            fractional_part_padded = f"{fractional_part:<06s}"[:6]

            # Reassemble the string
            timestamp_to_parse = f"{integer_part}.{fractional_part_padded}{offset_part}"

        # Attempt parsing the potentially modified string
        parsed_dt = datetime.datetime.fromisoformat(timestamp_to_parse)
        # Ensure timezone is UTC if offset was +00:00
        if parsed_dt.tzinfo == datetime.timezone.utc or parsed_dt.tzinfo is None:
             return parsed_dt.replace(tzinfo=timezone.utc) # Standardize to UTC object
        else:
             return parsed_dt # Return with original offset if not UTC

    except ValueError as e:
        logger.warning(f"Timestamp format error line {line_num_for_log}: Original='{timestamp_str}', ParsedAttempt='{timestamp_to_parse}'. Err: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing timestamp line {line_num_for_log}: Original='{timestamp_str}'. Err: {e}", exc_info=True)
        return None


# --- FastF1 Session Info ---
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
        # Cache setup for FastF1 (optional but recommended)
        # Ensure cache dir exists, handle potential errors
        try:
            cache_dir = Path("./ff1_cache").resolve() # Example cache location
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
            for i in range(1, 6): # Session1 to Session5
                session_date_col = f'Session{i}DateUtc'
                session_name_col = f'Session{i}'

                if session_date_col in event and pd.notna(event[session_date_col]):
                    session_date = event[session_date_col]
                    # Ensure tz-aware
                    if not isinstance(session_date, pd.Timestamp):
                         try: session_date = pd.Timestamp(session_date)
                         except: logger.warning(f"Could not parse date: {event[session_date_col]}"); continue

                    if session_date.tzinfo is None:
                         session_date = session_date.tz_localize('UTC')

                    if session_date > now: # Future
                        if session_date < next_future_session['date']:
                            next_future_session.update({
                                'date': session_date,
                                'event_name': event.get('EventName'),
                                'session_name': event.get(session_name_col)
                            })
                    elif session_date <= now: # Past or Current start time
                        if session_date > last_past_session['date']:
                            last_past_session.update({
                                'date': session_date,
                                'event_name': event.get('EventName'),
                                'session_name': event.get(session_name_col)
                            })

        # Decision Logic
        ongoing_window = pd.Timedelta(hours=3) # Consider session ongoing if started within 3 hours
        if last_past_session.get('event_name') and (now - last_past_session.get('date', now)) <= ongoing_window:
            logger.info(f"FastF1: Using ongoing session: {last_past_session['event_name']} - {last_past_session['session_name']}")
            return last_past_session['event_name'], last_past_session['session_name']
        elif next_future_session.get('event_name'):
            logger.info(f"FastF1: Using next future session: {next_future_session['event_name']} - {next_future_session['session_name']}")
            return next_future_session['event_name'], next_future_session['session_name']
        else:
            logger.warning("FastF1: Could not determine current or next session.")
            return None, None

    except Exception as e:
        logger.error(f"FastF1 Error getting session info: {e}", exc_info=True)
        return None, None

# --- Nested Dictionary Access ---
def get_nested_state(d, *keys, default=None):
    """Safely accesses nested dictionary keys."""
    val = d
    for key in keys:
        if isinstance(val, dict):
            val = val.get(key)
        elif isinstance(val, list): # Allow integer index access for lists
             try:
                  key_int = int(key)
                  if 0 <= key_int < len(val):
                       val = val[key_int]
                  else:
                       return default # Index out of bounds
             except (ValueError, TypeError):
                  return default # Key wasn't a valid integer for list index
        else:
            # Not a dict or list, cannot go deeper
            return default
        if val is None: # Stop if None is encountered at any level
             return default
    return val


# --- Data Table Sorting ---
def pos_sort_key(item):
    """Sort key function for DataTable position column."""
    pos_str = item.get('Pos', '999')
    if isinstance(pos_str, (int, float)): return pos_str
    if isinstance(pos_str, str) and pos_str.isdigit():
        try: return int(pos_str)
        except ValueError: return 999
    return 999 # Place non-numeric (OUT, "", etc.) at the end
    
def generate_driver_options(timing_state_dict): # Changed argument name for clarity
    """Generates list of options for driver dropdowns from timing_state."""
    options = []
    logger.debug(f"Generating driver options from timing_state keys: {list(timing_state_dict.keys())}")

    if not timing_state_dict or not isinstance(timing_state_dict, dict):
        logger.warning("generate_driver_options received empty or invalid timing_state.")
        return [{'label': 'No drivers available', 'value': '', 'disabled': True}]

    driver_list_for_sorting = []
    # Extract necessary info for sorting and display first
    for driver_num, driver_data in timing_state_dict.items():
        if isinstance(driver_data, dict): # Basic check
             driver_list_for_sorting.append({
                 'value': driver_num, # The key is the value for the dropdown
                 'number': driver_data.get('RacingNumber', 'N/A'),
                 'tla': driver_data.get('Tla', '???'),
                 'name': driver_data.get('FullName', 'Unknown Driver')
             })

    # Sort by racing number numerically
    def sort_key(item):
        try: return int(item.get('number', 999))
        except (ValueError, TypeError): return 999

    sorted_drivers = sorted(driver_list_for_sorting, key=sort_key)

    # Create options list
    for driver in sorted_drivers:
        label = f"{driver['tla']} (#{driver['number']}) - {driver['name']}"
        options.append({'label': label, 'value': driver['value']})

    if not options: # Handle case where processing failed or no valid drivers found
         return [{'label': 'No drivers processed', 'value': '', 'disabled': True}]

    return options


# --- Logging Filter ---
class RecordDataFilter(logging.Filter):
    """Logging filter to control recording based on app_state."""
    def filter(self, record):
        # Assuming app_state is imported and accessible
        try:
            import app_state # Import here to avoid circular dependencies at module level
            # Only allow the record to pass if recording is active
            return app_state.is_saving_active
        except ImportError:
            return False # Cannot record if app_state isn't available


print("DEBUG: utils module loaded")
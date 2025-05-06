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
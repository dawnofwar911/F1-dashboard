# utils.py
"""
Utility functions for data decoding, timestamp parsing, filename sanitization, etc.
"""

import base64
import json
import zlib
import re
import datetime
from datetime import timezone
import logging

# Get a logger for utility functions if needed, or pass logger from calling module
util_logger = logging.getLogger("F1App.utils") # Example: child logger


def _decode_and_decompress(encoded_data):
    """Decodes base64 encoded and zlib decompressed data (message payload)."""
    if encoded_data and isinstance(encoded_data, str):
        try:
            missing_padding = len(encoded_data) % 4
            if missing_padding:
                encoded_data += '=' * (4 - missing_padding)
            decoded_data = base64.b64decode(encoded_data)
            decompressed_data = zlib.decompress(decoded_data, -zlib.MAX_WBITS) # Raw deflate
            return json.loads(decompressed_data.decode('utf-8'))
        except json.JSONDecodeError as e:
            # Use logger if available, otherwise print
            log_msg = f"JSON decode error after decompression: {e}. Data sample: {decoded_data[:100]}..."
            try: util_logger.error(log_msg, exc_info=False)
            except: print(f"ERROR: {log_msg}")
            return None
        except Exception as e:
            log_msg = f"Decode/Decompress error: {e}. Data: {str(encoded_data)[:50]}..."
            try: util_logger.error(log_msg, exc_info=False)
            except: print(f"ERROR: {log_msg}")
            return None
    # If input wasn't a string or was empty
    log_msg = f"decode_and_decompress received non-string or empty data: type {type(encoded_data)}"
    try: util_logger.warning(log_msg)
    except: print(f"WARNING: {log_msg}")
    return None

def sanitize_filename(name):
    """Removes/replaces characters unsuitable for filenames."""
    if not name: return "Unknown"
    name = str(name).strip()
    name = re.sub(r'[\\/:*?"<>|\s\-\:\.,\(\)]+', '_', name) # Replace invalid chars/spaces with underscore
    name = re.sub(r'[^\w_]+', '', name) # Remove remaining non-alphanumeric/underscore
    name = re.sub(r'_+', '_', name) # Consolidate multiple underscores
    name = name.strip('_') # Remove leading/trailing underscores
    return name if name else "InvalidName"

def parse_iso_timestamp_safe(timestamp_str, line_num_for_log="?"):
    """
    Safely parses an ISO timestamp string, replacing 'Z', padding/truncating
    microseconds to EXACTLY 6 digits, and handling potential errors.
    Returns a datetime object or None.
    """
    if not timestamp_str or not isinstance(timestamp_str, str):
        return None
    try:
        cleaned_ts = timestamp_str.replace('Z', '+00:00')
        timestamp_to_parse = cleaned_ts
        if '.' in cleaned_ts:
            parts = cleaned_ts.split('.', 1)
            integer_part = parts[0]
            fractional_part_full = parts[1]
            offset_part = ''
            if '+' in fractional_part_full:
                frac_parts = fractional_part_full.split('+', 1)
                fractional_part = frac_parts[0]; offset_part = '+' + frac_parts[1]
            elif '-' in fractional_part_full:
                frac_parts = fractional_part_full.split('-', 1)
                fractional_part = frac_parts[0]; offset_part = '-' + frac_parts[1]
            else:
                fractional_part = fractional_part_full

            # Force 6 microsecond digits
            fractional_part_padded = f"{fractional_part:<06s}"[:6]
            timestamp_to_parse = f"{integer_part}.{fractional_part_padded}{offset_part}"

        return datetime.datetime.fromisoformat(timestamp_to_parse)
    except ValueError as e:
        log_msg = f"Timestamp format error line {line_num_for_log}: Original='{timestamp_str}', FinalParsedAttempt='{timestamp_to_parse}'. Err: {e}"
        try: util_logger.warning(log_msg)
        except: print(f"WARNING: {log_msg}")
        return None
    except Exception as e:
        log_msg = f"Unexpected error parsing timestamp line {line_num_for_log}: Original='{timestamp_str}'. Err: {e}"
        try: util_logger.error(log_msg, exc_info=True)
        except: print(f"ERROR: {log_msg}")
        return None

def get_nested_state(d, *keys, default=None):
    """Safely accesses nested dictionary keys."""
    val = d
    for key in keys:
        if isinstance(val, dict):
            val = val.get(key)
        else:
            return default
    return val if val is not None else default

def pos_sort_key(item):
    """Sort key function for DataTable position column."""
    pos_str = item.get('Pos', '999') # Default to large number if missing
    if isinstance(pos_str, (int, float)):
        return pos_str
    if isinstance(pos_str, str) and pos_str.isdigit():
        try:
            return int(pos_str)
        except ValueError:
            return 999 # Should not happen if isdigit() is true
    return 999 # Place non-numeric positions (OUT, "", etc.) at the end

print("DEBUG: utils module loaded")

# Add other utilities here if needed, e.g., rotate_coords
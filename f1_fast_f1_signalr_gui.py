import logging
import json
import zlib
import base64
import datetime
from datetime import timezone
import time
import threading
import urllib.parse
import sys
import os
import queue
import collections
import requests
import app_state
import numpy as np
import plotly.graph_objects as go # Make sure this is imported at the top
import uuid
import re

# Import for F1 Schedule / Data
try:
    import fastf1
    import pandas as pd
except ImportError:
    print("Please install fastf1 and pandas: pip install fastf1 pandas")
    sys.exit(1)

# Import from synchronous signalrcore library
from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.transport.websockets.connection import ConnectionState # Assuming ConnectionState enum is still valid for comparison if needed elsewhere
from signalrcore.hub.errors import HubConnectionError, HubError, UnAuthorizedHubError
from signalrcore.hub.base_hub_connection import BaseHubConnection
from signalrcore.protocol.json_hub_protocol import JsonHubProtocol

# Added for Dash GUI
try:
    import dash
    # Import dash_table along with other components
    from dash import dcc, html, Dash, dash_table
    from dash.dependencies import Input, Output, State
    import dash_bootstrap_components as dbc # For better styling and layout
except ImportError:
    print("Please install Dash, dash-bootstrap-components and Pandas: pip install dash dash-bootstrap-components pandas")
    sys.exit(1)

# --- Configuration ---
NEGOTIATE_URL_BASE = "https://livetiming.formula1.com/signalr"
WEBSOCKET_URL_BASE = "wss://livetiming.formula1.com/signalr"
HUB_NAME = "Streaming"
STREAMS_TO_SUBSCRIBE = ["Heartbeat", "CarData.z", "Position.z", "ExtrapolatedClock",
    "TimingAppData", "TimingData", "TimingStats", "TrackStatus",
    "SessionData", "DriverList", "RaceControlMessages", "SessionInfo"]
DATA_FILENAME_TEMPLATE = "f1_signalr_data_{timestamp}.data.txt"
DATABASE_FILENAME_TEMPLATE = "f1_signalr_data_{timestamp}.db"
REPLAY_DIR = "replays" # <-- Define the directory for replay files
# --- Near top of f1_fast_f1_signalr_gui_v14.py ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # Gets the directory of the script
TARGET_SAVE_DIRECTORY = os.path.join(SCRIPT_DIR, "replays")
DEFAULT_REPLAY_FILENAME = "2023-yas-marina-quali.data.txt" # Default replay file

# --- Logging Setup ---
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_level = logging.DEBUG # Change to logging.DEBUG for more detail if needed

# Main application logger
main_logger = logging.getLogger("F1App")
main_logger.setLevel(log_level)
main_handler = logging.StreamHandler(sys.stdout)
main_handler.setFormatter(log_formatter)
if not main_logger.hasHandlers(): main_logger.addHandler(main_handler)

# SignalR Core library logger
signalr_logger = logging.getLogger("signalrcore")
signalr_logger.setLevel(log_level)
signalr_handler = logging.StreamHandler(sys.stdout)
signalr_handler.setFormatter(log_formatter)
if not signalr_logger.hasHandlers(): signalr_logger.addHandler(signalr_handler)

# --- Global Variables & Threading Utilities ---
hub_connection = None
connection_thread = None
replay_thread = None
processing_thread = None
stop_event = threading.Event()
app = Dash(__name__, external_stylesheets=[dbc.themes.SLATE], suppress_callback_exceptions=True)
data_queue = queue.Queue()
data_store = {}
timing_state = {} # Holds persistent timing state per driver
track_status_data = {} # To store TrackStatus info (Status, Message)
session_details = {} # To store SessionInfo/SessionData details
race_control_log = collections.deque(maxlen=50)
track_coordinates_cache = {'x': None, 'y': None, 'range_x': None, 'range_y': None, 'rotation': None, 'corner_x': None, 'corner_y': None, 'session_key': None} # Expanded cache

# --- F1 Helper Functions ---
# FILE: f1_fast_f1_signalr_gui_v14.py
# (Make sure imports for logging, fastf1, pandas, datetime are present)

def get_current_or_next_session_info():
    """
    Uses FastF1 to find the currently ongoing session (if started recently)
    OR the next upcoming session.
    Returns event_name, session_name or None, None.
    """
    main_logger = logging.getLogger("F1App")
    if fastf1 is None or pd is None:
        main_logger.error("FastF1 or Pandas not available for session info.")
        return None, None

    try:
        year = datetime.datetime.now().year
        main_logger.debug(f"FastF1: Fetching schedule for {year} to find current/next session...")
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        now = pd.Timestamp.now(tz='UTC') # Current time is tz-aware UTC
        main_logger.debug(f"FastF1: Current UTC time: {now}")

        # --- Initialize trackers ---
        # Track the latest session that started *before* or *at* now
        last_past_session = {'date': pd.Timestamp.min.tz_localize('UTC'), 'event_name': None, 'session_name': None}
        # Track the earliest session that starts *after* now
        next_future_session = {'date': pd.Timestamp.max.tz_localize('UTC'), 'event_name': None, 'session_name': None}

        # --- Iterate through schedule ---
        for index, event in schedule.iterrows():
            for i in range(1, 6): # Check Session1 to Session5
                session_date_col = f'Session{i}DateUtc'
                session_name_col = f'Session{i}'

                if session_date_col in event and pd.notna(event[session_date_col]) and isinstance(event[session_date_col], pd.Timestamp):
                    session_date = event[session_date_col]

                    # Defensive Check: Ensure tz-aware
                    if session_date.tzinfo is None:
                         main_logger.warning(f"FastF1 returned tz-naive date for {event.get('EventName','?')}-{session_name_col}. Localizing to UTC.")
                         session_date = session_date.tz_localize('UTC')

                    # --- Categorize session ---
                    if session_date > now: # Future Session
                        if session_date < next_future_session['date']:
                            next_future_session['date'] = session_date
                            next_future_session['event_name'] = event['EventName']
                            next_future_session['session_name'] = event[session_name_col]
                    elif session_date <= now: # Past or Current Session Start Time
                        if session_date > last_past_session['date']:
                            last_past_session['date'] = session_date
                            last_past_session['event_name'] = event['EventName']
                            last_past_session['session_name'] = event[session_name_col]

        # --- Decision Logic ---
        # Define a window to consider a past session "ongoing" (e.g., 3 hours)
        ongoing_window = pd.Timedelta(hours=3)

        # Check if a past session exists and started recently enough to likely be ongoing
        if last_past_session['event_name'] and (now - last_past_session['date']) <= ongoing_window:
            main_logger.info(f"FastF1: Using potentially ongoing session: {last_past_session['event_name']} - {last_past_session['session_name']} (started {last_past_session['date']})")
            return last_past_session['event_name'], last_past_session['session_name']

        # Otherwise, if no ongoing session detected, check if a future session was found
        elif next_future_session['event_name']:
             main_logger.info(f"FastF1: Using next future session: {next_future_session['event_name']} - {next_future_session['session_name']} starting at {next_future_session['date']}")
             return next_future_session['event_name'], next_future_session['session_name']

        # Otherwise, no relevant session found
        else:
            main_logger.warning("FastF1: Could not determine current or next session (maybe end of season?).")
            return None, None

    except Exception as e:
        main_logger.error(f"FastF1 Error getting current/next session: {e}", exc_info=True)
        return None, None # Return None on error to allow fallback

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
# --- END HELPER FUNCTIONS ---
# --- Data Handling ---

def ensure_replay_dir_exists():
    """Creates the replay directory if it doesn't exist."""
    # Use the REPLAY_DIR constant defined above
    if not os.path.exists(REPLAY_DIR):
        try:
            os.makedirs(REPLAY_DIR)
            main_logger.info(f"Created replay directory: {REPLAY_DIR}")
        except OSError as e:
            main_logger.error(f"Failed to create replay directory '{REPLAY_DIR}': {e}")

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

def get_replay_files(directory):
    """
    Scans the specified directory for files suitable for replay
    (e.g., ending in .data.txt or .session.jsonl).

    Args:
        directory (str): The path to the directory to scan.

    Returns:
        list: A list of dictionaries suitable for dcc.Dropdown options
              (e.g., [{'label': 'file1.txt', 'value': 'file1.txt'}, ...]).
              Returns an empty list if the directory doesn't exist or is empty.
    """
    if not os.path.isdir(directory):
        main_logger.warning(f"Replay directory '{directory}' not found or is not a directory.")
        return []

    files = []
    try:
        for filename in os.listdir(directory):
            # Adjust the condition based on expected replay file extensions
            if filename.endswith((".data.txt", ".session.jsonl")) and \
               os.path.isfile(os.path.join(directory, filename)):
                files.append({'label': filename, 'value': filename})
        main_logger.info(f"Found {len(files)} replay files in '{directory}'.")
    except OSError as e:
        main_logger.error(f"Error scanning replay directory '{directory}': {e}")
        return [] # Return empty list on error
    return files

# Replace your _decode_and_decompress function
def _decode_and_decompress(encoded_data):
    """Decodes base64 encoded and zlib decompressed data (message payload)."""
    if encoded_data and isinstance(encoded_data, str):
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
            main_logger.error(f"JSON decode error after decompression: {e}. Data sample: {decoded_data[:100]}...", exc_info=False)
            return None
        except Exception as e:
            main_logger.error(f"Decode/Decompress error: {e}. Data: {str(encoded_data)[:50]}...", exc_info=False)
            return None
    # If input wasn't a string or was empty, return None or original? Let's return None.
    main_logger.warning(f"decode_and_decompress received non-string or empty data: type {type(encoded_data)}")
    return None

def run_connection_manual_neg(target_url, headers_for_ws):
    """Target function for connection thread using pre-negotiated URL."""
    global hub_connection # Allow modification of the global reference

    hub_connection = None # Ensure clean slate

    try:
        main_logger.info("Connection thread: Initializing HubConnection (manual neg)...")
        # --- Build Hub Connection with skip_negotiation=True ---
        hub_connection = (
            HubConnectionBuilder()
            .with_url(target_url, options={
                "verify_ssl": True,
                "headers": headers_for_ws, # Pass ALL required headers from main thread
                "skip_negotiation": True   # MUST skip internal negotiation
                })
            .with_hub_protocol(JsonHubProtocol()) # Keep explicit protocol
            .configure_logging(logging.DEBUG) # Use logger defined elsewhere
            # Add reconnect logic here if/when needed
            .build()
        )
        # --- End Builder ---

        # --- CHECK Type (Keep for now) ---
        if hub_connection:
            main_logger.info(f"CHECK (Manual Neg Build): Type returned by build(): {type(hub_connection)}")
            main_logger.info(f"CHECK (Manual Neg Build): Hub has 'send' attribute? {hasattr(hub_connection, 'send')}")
            if not hasattr(hub_connection, 'send'):
                 raise HubConnectionError("Built object missing '.send()' method!")
        else:
            main_logger.error("CHECK (Manual Neg Build): Hub build returned None!")
            raise HubConnectionError("Builder returned None")
        # --- END CHECK ---

        # Register handlers (Make sure function names match your definitions)
        hub_connection.on_open(handle_connect) # Use your open handler name
        hub_connection.on_close(handle_disconnect) # Use your close handler name
        hub_connection.on_error(handle_error) # Use your error handler name

        FEED_TARGET_NAME = "feed"
        hub_connection.on(FEED_TARGET_NAME, on_message)
        main_logger.info(f"Connection thread: Handler 'on_message' registered for target '{FEED_TARGET_NAME}'.")
        # Log the final list of handlers stored on the connection object
        app_handlers = getattr(hub_connection, 'handlers', [])
        main_logger.info(f"Connection thread: Handlers successfully registered: {app_handlers!r}")

        # Update app state before starting
        with app_state.app_state_lock:
             app_state.app_status.update({"state": "Connecting", "connection": "Socket Connecting"})

        main_logger.info("Connection thread: Starting connection (manual neg)...")
        hub_connection.start() # Start the connection

        main_logger.info("Connection thread: Hub connection started. Waiting for stop_event...")
        stop_event.wait() # Wait until stopped externally or by callbacks
        main_logger.info("Connection thread: Stop event received.")

    except Exception as e:
        main_logger.error(f"Connection thread error (manual neg): {e}", exc_info=True)
        with app_state.app_state_lock:
            app_state.app_status.update({"state": "Error", "connection": f"Thread Error: {type(e).__name__}"})
        if not stop_event.is_set(): stop_event.set() # Ensure stop event is set on error

    finally:
        main_logger.info("Connection thread finishing (manual neg).")
        temp_hub = hub_connection # Use local copy for cleanup
        if temp_hub:
             try:
                 main_logger.info("Attempting final hub stop...")
                 temp_hub.stop()
                 main_logger.info("Hub stopped (finally).")
             except Exception as e:
                 main_logger.error(f"Err stopping hub (finally): {e}", exc_info=True)

        # Use helper functions for cleanup (assuming they exist)
        close_live_file()

        with app_state.app_state_lock:
             if app_state.app_status["state"] not in ["Stopped", "Error", "Playback Complete"]:
                 app_state.app_status.update({"state": "Stopped", "connection": "Disconnected / Thread End"})
        if not stop_event.is_set():
             main_logger.warning("Setting stop event during cleanup (manual neg).")
             stop_event.set()
        globals()['hub_connection'] = None # Clear global reference
        main_logger.info("Conn thread cleanup finished (manual neg).")

def on_message(args):
    """Handles 'feed' targeted messages received from the SignalR hub connection."""
    global data_queue, main_logger # Ensure needed globals are declared

    # Changed initial log to DEBUG level as it can be very frequent
    main_logger.debug(f"APP HANDLER (on_message) called with args type: {type(args)}") # Corrected logging

    try:
        # --- Start of CORRECTED logic ---
        # Library passes message.arguments directly, which should be a list for 'feed'.
        if not isinstance(args, list):
            main_logger.warning(f"  APP HANDLER received unexpected args format (not a list): {type(args)} - Content: {args!r}")
            return # Cannot process non-list arguments for 'feed'

        # Process the arguments list directly (no loop needed here)
        if len(args) >= 2:
            stream_name_raw = args[0]
            data_content = args[1]
            # Extract optional timestamp if present (assuming it's the 3rd arg)
            timestamp_for_queue = args[2] if len(args) > 2 else None
            if timestamp_for_queue is None:
                # Fallback timestamp if not provided in message arguments
                timestamp_for_queue = datetime.datetime.now(timezone.utc).isoformat() + 'Z'
                main_logger.debug(f"  Using fallback timestamp for stream '{stream_name_raw}'")

            stream_name = stream_name_raw
            actual_data = data_content

            # Check for compressed data indicated by '.z' suffix
            if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                stream_name = stream_name_raw[:-2] # Remove suffix
                actual_data = _decode_and_decompress(data_content) # Call helper function
                if actual_data is None:
                    # Log warning, data will be skipped below
                    main_logger.warning(f"    Failed to decode/decompress data for stream '{stream_name_raw}'. Skipping queue put.")
                # else: # Optional success log
                #    main_logger.debug(f"    Successfully decoded/decompressed '{stream_name_raw}'")


            # Ensure we have data (might be None if decompression failed) and queue exists before putting
            if actual_data is not None:
                try:
                    # Ensure data_queue is accessible (check global scope, initialization)
                    if 'data_queue' not in globals():
                         main_logger.error("    FATAL: data_queue is not defined in global scope for on_message!")
                         return # Exit if queue doesn't exist

                    queue_item = {"stream": stream_name, "data": actual_data, "timestamp": timestamp_for_queue}
                    # Use non-blocking put with timeout to prevent handler blocking if queue is full
                    try:
                        data_queue.put(queue_item, block=True, timeout=0.1) # Timeout after 100ms
                    except queue.Full:
                         main_logger.warning(f"    Data queue full! Discarding '{stream_name}' message after timeout.")
                         return # Exit this handler call if queue is full

                    # Log after successful put
                    try: data_size = len(str(actual_data))
                    except: data_size = "N/A" # Handle potential errors converting complex data to string
                    main_logger.debug(f"    Put '{stream_name}' onto data_queue (approx size: {data_size}).")

                # Catch potential errors during the queue put operation itself (less likely with timeout)
                except Exception as queue_ex:
                    main_logger.error(f"    Error putting message onto data_queue: {queue_ex}", exc_info=True)
            else:
                # This case handles when actual_data is None (e.g., decompression failed)
                # Changed to debug level as it might happen often for non-data messages if helper isn't robust
                main_logger.debug(f"    Skipping queue put for stream '{stream_name}' due to None data.")

        else:
            # Log if the 'feed' target doesn't have the expected number of arguments
            main_logger.warning(f"  Invocation target 'feed' received with unexpected arguments structure (expected >= 2): {args!r}")
        # --- End of CORRECTED logic ---

    except Exception as e:
        # Catch errors during the processing of the args list itself
        main_logger.error(f"APP HANDLER (on_message) outer error processing arguments: {e}", exc_info=True)



def handle_message(message_data):
    """
    Handles specific incoming parsed SignalR message data types (R, List, {}).
    Puts structured items {"stream":..., "data":..., "timestamp":...} onto the data_queue.
    NOTE: Does NOT handle {"M": [...]} blocks - on_message handles those directly.
    """
    global data_queue # Make sure queue is accessible

    if isinstance(message_data, dict) and "R" in message_data:
        # ... (Keep the logic for handling "R" blocks from your v12.py/response #211) ...
        # Example:
        snapshot_data = message_data.get("R", {})
        if isinstance(snapshot_data, dict):
            snapshot_ts = snapshot_data.get("Heartbeat", {}).get("Utc") or (datetime.datetime.now(timezone.utc).isoformat() + 'Z')
            processed_count = 0
            for stream_name_raw, stream_data in snapshot_data.items():
                stream_name = stream_name_raw; actual_data = stream_data
                if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                    stream_name = stream_name_raw[:-2]
                    actual_data = _decode_and_decompress(stream_data)
                    if actual_data is None: continue
                if actual_data is not None:
                    # Check if stream is in STREAMS_TO_SUBSCRIBE if necessary
                    data_queue.put({"stream": stream_name, "data": actual_data, "timestamp": snapshot_ts})
                    processed_count += 1
            main_logger.info(f"Queued {processed_count} streams from snapshot (R) block via handle_message.")
        else: main_logger.warning(f"Snapshot block 'R' non-dict: {type(snapshot_data)}")


    elif isinstance(message_data, list) and len(message_data) >= 2:
        # ... (Keep the logic for handling direct lists from your v12.py/response #211) ...
        # Example:
        stream_name_raw = message_data[0]; data_content = message_data[1]
        timestamp_for_queue = message_data[2] if len(message_data) > 2 else (datetime.datetime.now(timezone.utc).isoformat() + 'Z')
        stream_name = stream_name_raw; actual_data = data_content
        if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
            stream_name = stream_name_raw[:-2]
            actual_data = _decode_and_decompress(data_content)
            if actual_data is None: return # Skip
        if actual_data is not None:
             data_queue.put({"stream": stream_name, "data": actual_data, "timestamp": timestamp_for_queue})


    elif isinstance(message_data, dict) and not message_data: # Heartbeat {}
        # Update heartbeat state directly, no need to queue unless processor needs it
        with app_state.app_state_lock:
            app_state.app_status["last_heartbeat"] = datetime.datetime.now(timezone.utc).isoformat()
    # NOTE: handle_message no longer needs to check for 'C' or 'M' if called correctly by on_message
    # else:
    #    main_logger.warning(f"handle_message received unexpected format: {type(message_data)}")



# Replace your parse_iso_timestamp_safe function with this version

def parse_iso_timestamp_safe(timestamp_str, line_num_for_log="?"):
    """
    Safely parses an ISO timestamp string, replacing 'Z', padding/truncating
    microseconds to EXACTLY 6 digits, and handling potential errors.
    Returns a datetime object or None.
    """
    if not timestamp_str or not isinstance(timestamp_str, str):
        return None

    try:
        # Always replace 'Z' first - fromisoformat might be stricter with offsets
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
            elif '-' in fractional_part_full:  # Handle just in case
                frac_parts = fractional_part_full.split('-', 1)
                fractional_part = frac_parts[0]
                offset_part = '-' + frac_parts[1]
            else:  # Should have offset now, but handle if not
                fractional_part = fractional_part_full

            # --- >>> FORCE 6 MICROSECOND DIGITS <<< ---
            # Pad with trailing zeros if less than 6, truncate if more than 6
            fractional_part_padded = f"{fractional_part:<06s}"[:6]
            # --- >>> END FORCE <<< ---

            # Reassemble the string with exactly 6 microsecond digits
            timestamp_to_parse = f"{integer_part}.{fractional_part_padded}{offset_part}"
            # Log only if modified significantly (e.g., truncated or padded)
            #if timestamp_to_parse != cleaned_ts:
                # main_logger.debug(
                    #f"Line {line_num_for_log}: Modified timestamp for parsing. Original='{timestamp_str}', ParsedAs='{timestamp_to_parse}'")

        # Attempt parsing the potentially modified string
        return datetime.datetime.fromisoformat(timestamp_to_parse)

    except ValueError as e:
        # Log the final string we tried to parse
        main_logger.warning(
            f"Timestamp format error line {line_num_for_log}: Original='{timestamp_str}', FinalParsedAttempt='{timestamp_to_parse}'. Err: {e}")
        return None
    except Exception as e:  # Catch any other unexpected error during processing
        main_logger.error(
            f"Unexpected error parsing timestamp line {line_num_for_log}: Original='{timestamp_str}'. Err: {e}", exc_info=True)
        return None

# Replace the existing _process_race_control function

# Replace _process_race_control again with this version

def _process_race_control(data):
    """ Helper function to process RaceControlMessages stream """
    global race_control_log

    messages_to_process = []
    if isinstance(data, dict) and 'Messages' in data:
        messages_payload = data.get('Messages')
        if isinstance(messages_payload, list):
             # main_logger.debug(f"RC Handler: Processing Messages as LIST (Count: {len(messages_payload)})")
             messages_to_process = messages_payload
        elif isinstance(messages_payload, dict):
             # main_logger.debug(f"RC Handler: Processing Messages as DICT (Count: {len(messages_payload)})")
             messages_to_process = messages_payload.values() # Get the values (message dicts)
        else:
             main_logger.warning(f"RaceControlMessages 'Messages' field was not a list or dict: {type(messages_payload)}")
             return
    elif data:
         main_logger.warning(f"Unexpected RaceControlMessages format received: {type(data)}. Expected dict with 'Messages'.")
         return
    else: # No data or empty payload
        return

    new_messages_added = 0
    for i, msg in enumerate(messages_to_process): # Add index for clarity
        if isinstance(msg, dict):
            try:
                timestamp = msg.get('Utc', 'Timestamp?')
                lap = msg.get('Lap', '-')
                message_text = msg.get('Message', '')
                # ... (extract other fields: category, flag etc.) ...

                time_str = "Timestamp?"
                if isinstance(timestamp, str) and 'T' in timestamp:
                     try: time_str = timestamp.split('T')[1].split('.')[0]
                     except: time_str = timestamp

                log_entry = f"[{time_str} L{lap}]: {message_text}" # Simplified for logging clarity
                # main_logger.debug(f"RC Handler: Formatted entry {i+1}: '{log_entry}'") # Log BEFORE append

                # Prepend to the deque
                race_control_log.appendleft(log_entry)
                new_messages_added += 1
                # Log AFTER successful append
                # main_logger.debug(f"RC Handler: Appended entry {i+1}. Deque size now: {len(race_control_log)}")

            except Exception as e:
                main_logger.error(f"Error processing RC message item #{i+1}: {msg} - Error: {e}", exc_info=True)
                # Continue to next message in list/dict even if one fails
        else:
             main_logger.warning(f"Unexpected item type #{i+1} in RaceControlMessages source: {type(msg)}")

    # if new_messages_added > 0:
    #    main_logger.info(f"Finished processing RC payload, added {new_messages_added} message(s).")

def _process_weather_data(data):
    """ Helper function to process WeatherData stream """
    global data_store # Use data_store for less frequently updated info
    if isinstance(data, dict):
        # WeatherData stream payload seems to be the dict itself
        # Update the 'WeatherData' entry in data_store directly
        # No need to nest under ['data'] like we did for SessionData previously
        if 'WeatherData' not in data_store: data_store['WeatherData'] = {}
        data_store['WeatherData'].update(data) # Update with received keys/values
        # main_logger.debug(f"Updated WeatherData: {data}")
    else:
        main_logger.warning(f"Unexpected WeatherData format received: {type(data)}")

def _process_timing_app_data(data):
    """ Helper function to process TimingAppData stream data (contains Stint/Tyre info) """
    global timing_state # Access the global state
    if not timing_state:
        return # Cannot process without initialized timing_state

    if isinstance(data, dict) and 'Lines' in data and isinstance(data['Lines'], dict):
        for car_num_str, line_data in data['Lines'].items():
            driver_current_state = timing_state.get(car_num_str)
            if driver_current_state and isinstance(line_data, dict):

                 current_compound = driver_current_state.get('TyreCompound', '-')
                 current_age = driver_current_state.get('TyreAge', '?') # Keep previous age as default before processing

                 stints_data = line_data.get('Stints')
                 if isinstance(stints_data, dict) and stints_data:
                     try:
                         latest_stint_key = sorted(stints_data.keys(), key=int)[-1]
                         latest_stint_info = stints_data[latest_stint_key]

                         if isinstance(latest_stint_info, dict):
                             # --- Compound Processing ---
                             compound_value = latest_stint_info.get('Compound')
                             if isinstance(compound_value, str):
                                 current_compound = compound_value.upper()
                             # else: keep previous/default compound

                             # --- Age Processing with Debug Logging ---
                             age_determined = False # Flag to see if we set age in this block

                             # Check for TotalLaps first
                             total_laps_value = latest_stint_info.get('TotalLaps')
                             # main_logger.debug(f"Driver {car_num_str}, Stint {latest_stint_key}: Checking 'TotalLaps'. Found: {total_laps_value} (Type: {type(total_laps_value)})")
                             if total_laps_value is not None:
                                 try:
                                     # Attempt conversion just in case it's a string sometimes
                                     current_age = int(total_laps_value)
                                     # main_logger.debug(f"Driver {car_num_str}: Using age from TotalLaps: {current_age}")
                                     age_determined = True
                                 except (ValueError, TypeError):
                                      main_logger.warning(f"Driver {car_num_str}: Could not convert TotalLaps '{total_laps_value}' to int.")
                                      # Keep existing current_age (which might be previous value or '?')

                             # If TotalLaps didn't yield age, try calculating from StartLaps
                             if not age_determined:
                                 start_laps_value = latest_stint_info.get('StartLaps')
                                 num_laps_value = driver_current_state.get('NumberOfLaps') # Get completed laps from state
                                 # main_logger.debug(f"Driver {car_num_str}, Stint {latest_stint_key}: 'TotalLaps' not used. Checking 'StartLaps': {start_laps_value}, State 'NumberOfLaps': {num_laps_value}")

                                 if start_laps_value is not None and num_laps_value is not None:
                                     try:
                                         start_lap = int(start_laps_value)
                                         current_lap_completed = int(num_laps_value)
                                         # Age = laps completed *on this tyre set* + 1?
                                         age_calc = current_lap_completed - start_lap + 1
                                         current_age = age_calc if age_calc >= 0 else '?'
                                         # main_logger.debug(f"Driver {car_num_str}: Calculated age {current_age} (Completed={current_lap_completed}, Start={start_lap})")
                                         age_determined = True
                                     except (ValueError, TypeError) as e:
                                          main_logger.warning(f"Driver {car_num_str}: Error converting StartLaps/NumberOfLaps for age calculation: {e}")
                                          # Keep existing current_age

                             # If age still wasn't determined by TotalLaps or calculation
                             #if not age_determined:
                                  # main_logger.debug(f"Driver {car_num_str}: Could not determine age from Stint {latest_stint_key} info in this message. Keeping previous/default.")
                                  # current_age retains its value from start of block (previous state or '?')
                             # --- End Age Processing ---

                         else: # latest_stint_info was not a dict
                             main_logger.warning(f"Driver {car_num_str}: Data for Stint {latest_stint_key} is not a dictionary: {type(latest_stint_info)}")

                     except (ValueError, IndexError, KeyError, TypeError) as e:
                          main_logger.error(f"Driver {car_num_str}: Error processing Stints data in TimingAppData: {e} - Data was: {stints_data}", exc_info=False) # Hide traceback for cleaner logs unless needed

                 # Update the state with the final values for this processing cycle
                 driver_current_state['TyreCompound'] = current_compound
                 driver_current_state['TyreAge'] = current_age
                 # Log final state for this driver after processing this specific message
                 # main_logger.debug(f"Driver {car_num_str} state post-TimingAppData: Compound='{current_compound}', Age='{current_age}'")

            elif not driver_current_state:
                 pass # Silently skip if driver not found
    elif data:
         main_logger.warning(f"Unexpected TimingAppData format received: {type(data)}")

def _process_driver_list(data):
    """ Helper to process DriverList data ONLY from the stream """
    global timing_state # No driver_tla_map needed here
    added_count = 0
    updated_count = 0
    processed_count = 0
    if isinstance(data, dict):
        processed_count = len(data)
        for driver_num_str, driver_info in data.items():
            if not isinstance(driver_info, dict):
                if driver_num_str == "_kf":
                    continue
                else:
                    main_logger.warning(f"Skipping invalid driver_info for {driver_num_str} in DriverList: {driver_info}")
                
                continue

            is_new_driver = driver_num_str not in timing_state

            # --- TLA Logic (Stream ONLY) ---
            # Get TLA ONLY from the current stream message, default to "N/A" if missing
            tla_from_stream = driver_info.get("Tla", "N/A")
            # --- END TLA Logic ---

            if is_new_driver:
                timing_state[driver_num_str] = {
                    "RacingNumber": driver_info.get("RacingNumber", driver_num_str),
                    "Tla": tla_from_stream, # Use TLA from stream or "N/A"
                    "FullName": driver_info.get("FullName", "N/A"),
                    "TeamName": driver_info.get("TeamName", "N/A"),
                    "Line": driver_info.get("Line", "-"), # Store Line if available
                    "TeamColour": driver_info.get("TeamColour", "FFFFFF"), # Store Colour if available
                    "FirstName": driver_info.get("FirstName", ""),
                    "LastName": driver_info.get("LastName", ""),
                    "Reference": driver_info.get("Reference", ""),
                    "CountryCode": driver_info.get("CountryCode", ""),
                    # Initialize timing fields
                    "Position": "-", "Time": "-", "GapToLeader": "-",
                    "IntervalToPositionAhead": {"Value": "-"}, "LastLapTime": {},
                    "BestLapTime": {}, "Sectors": {}, "Status": "On Track",
                    "InPit": False, "Retired": False, "Stopped": False, "PitOut": False
                }
                added_count += 1
            else:
                # Update existing driver
                current_driver_state = timing_state[driver_num_str]

                # Update TLA only if stream provides a non-"N/A" value
                # AND the current value is "N/A" or missing (don't overwrite a good TLA with "N/A")
                current_tla = current_driver_state.get("Tla")
                if tla_from_stream != "N/A" and (not current_tla or current_tla == "N/A"):
                     current_driver_state["Tla"] = tla_from_stream
                # Or if the stream provides a different, non-"N/A" TLA than what's stored
                elif tla_from_stream != "N/A" and current_tla != tla_from_stream:
                     current_driver_state["Tla"] = tla_from_stream

                # Update other descriptive fields if present in the stream message
                for key in ["RacingNumber", "FullName", "TeamName", "Line", "TeamColour", "FirstName", "LastName", "Reference", "CountryCode"]:
                    if key in driver_info and driver_info[key] is not None:
                        current_driver_state[key] = driver_info[key]

                # Ensure essential default timing keys exist (use simple setdefault for dicts too here)
                default_timing_values = { "Position": "-", "Time": "-", "GapToLeader": "-", "IntervalToPositionAhead": {"Value": "-"}, "LastLapTime": {}, "BestLapTime": {}, "Sectors": {}, "Status": "On Track", "InPit": False, "Retired": False, "Stopped": False, "PitOut": False }
                for key, default_val in default_timing_values.items():
                    current_driver_state.setdefault(key, default_val)
                updated_count += 1

        main_logger.debug(f"Processed DriverList message ({processed_count} entries). Added: {added_count}, Updated: {updated_count}. Total drivers now: {len(timing_state)}")
    else:
        main_logger.warning(f"Unexpected DriverList stream data format: {type(data)}. Cannot process.")

def _process_timing_data(data):
    """ Helper function to process TimingData stream data """
    global timing_state
    if not timing_state:
        # main_logger.debug("TimingData received before DriverList processed, skipping.")
        return # Cannot process without initialized timing_state

    if isinstance(data, dict) and 'Lines' in data and isinstance(data['Lines'], dict):
        for car_num_str, line_data in data['Lines'].items():
            driver_current_state = timing_state.get(car_num_str)
            # Process only if driver exists in state and line_data is a dict
            if driver_current_state and isinstance(line_data, dict):
                 # Update direct fields
                 # --- >>> Log the incoming line_data for this driver (Careful, can be verbose!) <<< ---
                 # Temporarily uncomment this to see ALL data for a specific driver if needed
                # if car_num_str == '1': # Example: Log only for driver #1
#                    main_logger.debug(f"TimingData line_data for Driver {car_num_str}: {line_data}")
                 # --- >>> END Log incoming line_data <<< ---
                 for key in ["Position", "Time", "GapToLeader", "InPit", "Retired", "Stopped", "PitOut", "NumberOfLaps", "NumberOfPitStops"]: # Added Laps/Stops
                     if key in line_data: driver_current_state[key] = line_data[key]

                 # Update nested fields (Lap Times, Interval)
                 for key in ["IntervalToPositionAhead", "LastLapTime", "BestLapTime"]:
                     if key in line_data:
                         incoming_value = line_data[key]
                         # Ensure target dict exists
                         if key not in driver_current_state or not isinstance(driver_current_state[key], dict):
                             driver_current_state[key] = {}
                         # Merge if incoming is dict, otherwise store in sub-key
                         if isinstance(incoming_value, dict):
                             driver_current_state[key].update(incoming_value)
                         else:
                             sub_key = 'Value' if key == "IntervalToPositionAhead" else 'Time'
                             driver_current_state[key][sub_key] = incoming_value
                             # main_logger.debug(f"Stored non-dict {key} value '{incoming_value}' into ['{sub_key}'] for {car_num_str}")

                 # Update Sectors
                 if "Sectors" in line_data and isinstance(line_data["Sectors"], dict):
                     if "Sectors" not in driver_current_state or not isinstance(driver_current_state["Sectors"], dict):
                         driver_current_state["Sectors"] = {}
                     for sector_idx, sector_data in line_data["Sectors"].items():
                         if sector_idx not in driver_current_state["Sectors"] or not isinstance(driver_current_state["Sectors"][sector_idx], dict):
                              driver_current_state["Sectors"][sector_idx] = {}
                         # Merge if dict, else assume 'Time' (though stream seems to use 'Value' here too?)
                         if isinstance(sector_data, dict):
                             driver_current_state["Sectors"][sector_idx].update(sector_data)
                         else:
                              driver_current_state["Sectors"][sector_idx]['Value'] = sector_data # Changed sub-key to 'Value' consistent with display logic
                              # main_logger.debug(f"Stored non-dict Sector {sector_idx} value '{sector_data}' into ['Value'] for {car_num_str}")

                 # Update Speeds (Optional, add to table if needed)
                 if "Speeds" in line_data and isinstance(line_data["Speeds"], dict):
                      if "Speeds" not in driver_current_state or not isinstance(driver_current_state["Speeds"], dict):
                          driver_current_state["Speeds"] = {}
                      driver_current_state["Speeds"].update(line_data["Speeds"])

                 # Update overall status
                 status_flags = []
                 if driver_current_state.get("Retired"): status_flags.append("Retired")
                 if driver_current_state.get("InPit"): status_flags.append("In Pit")
                 if driver_current_state.get("Stopped"): status_flags.append("Stopped")
                 if driver_current_state.get("PitOut"): status_flags.append("Out Lap")
                 if status_flags:
                      driver_current_state["Status"] = ", ".join(status_flags)
                 elif driver_current_state.get("Position", "-") != "-": # If has position and no flags, assume On Track
                      driver_current_state["Status"] = "On Track"
                 # else: keep existing status ("On Track" default from DriverList or previous)

        #    elif not driver_current_state:
                # main_logger.debug(f"TimingData for driver {car_num_str} received, but driver not yet in timing_state. Data skipped.")
    elif data: # Log if TimingData is not the expected dict structure but not None/empty
         main_logger.warning(f"Unexpected TimingData format received: {type(data)}")

def _process_track_status(data):
    """Handles TrackStatus data. MUST be called within app_state.app_state_lock."""
    global track_status_data # Access the global variable

    if not isinstance(data, dict):
        main_logger.warning(f"TrackStatus handler received non-dict data: {data}")
        return

    # Example data: {'Status': '1', 'Message': 'AllClear'}
    new_status = data.get('Status', track_status_data.get('Status', 'Unknown')) # Keep old if missing
    new_message = data.get('Message', track_status_data.get('Message', '')) # Keep old if missing

    # Check if status has actually changed to avoid unnecessary logging/updates
    if track_status_data.get('Status') != new_status or track_status_data.get('Message') != new_message:
        track_status_data['Status'] = new_status
        track_status_data['Message'] = new_message
        main_logger.info(f"Track Status Update: Status={new_status}, Message='{new_message}'")
        # TODO: Add logic here if you need to trigger immediate UI updates based on status change

def _process_position_data(data):
    """Handles Position data. MUST be called within app_state.app_state_lock."""
    global timing_state

    if 'timing_state' not in globals():
         main_logger.error("Global 'timing_state' not found for Position processing.")
         return

    # Expected structure: {'Position': [ {'Timestamp': '...', 'Entries': {'<CarNum>': {'X': ..., 'Y': ..., 'Z': ..., 'Status': ...}}} ]}
    if not isinstance(data, dict) or 'Position' not in data:
        main_logger.warning(f"Position handler received unexpected format: {data}")
        return

    position_entries = data.get('Position', [])
    if not isinstance(position_entries, list):
         main_logger.warning(f"Position data 'Position' key is not a list: {position_entries}")
         return

    for entry_group in position_entries:
        if not isinstance(entry_group, dict): continue
        timestamp = entry_group.get('Timestamp')
        entries = entry_group.get('Entries', {})
        if not isinstance(entries, dict): continue

        for car_number_str, pos_info in entries.items():
            if car_number_str not in timing_state:
                continue # Skip if driver isn't known

            if isinstance(pos_info, dict):
                x_pos = pos_info.get('X')
                y_pos = pos_info.get('Y')
                status = pos_info.get('Status') # e.g., "OnTrack"

                # Ensure 'PositionData' sub-dictionary exists
                if 'PositionData' not in timing_state[car_number_str]:
                    timing_state[car_number_str]['PositionData'] = {}

                # Store the latest position data
                pos_data_dict = timing_state[car_number_str]['PositionData']
                if x_pos is not None: pos_data_dict['X'] = x_pos
                if y_pos is not None: pos_data_dict['Y'] = y_pos
                if status is not None: pos_data_dict['Status'] = status
                if timestamp is not None: pos_data_dict['Timestamp'] = timestamp
                # main_logger.debug(f"Updated Position for {car_number_str}: X={x_pos}, Y={y_pos}")

def _process_car_data(data):
    """Handles CarData. MUST be called within app_state.app_state_lock."""
    global timing_state # Use timing_state as the main driver data store

    if 'timing_state' not in globals():
         main_logger.error("Global 'timing_state' not found for CarData processing.")
         return
         
    channel_map = {
        '0': 'RPM',       # Channel 0 seems to be RPM here
        '2': 'Speed',     # Channel 2 seems to be Speed (km/h) here
        '3': 'Gear',      # Channel 3 is Gear
        '4': 'Throttle',  # Channel 4 is Throttle (%)
        '5': 'Brake',     # Channel 5 is Brake (binary?)
        '45': 'DRS'       # Channel 45 is DRS status
    }
    
    # Expected structure from _decode_and_decompress:
    # {'Entries': [ {'Utc': 'timestamp', 'Cars': {'<CarNum>': {'Channels': { '0': Speed, ...}}}} ]}
    if not isinstance(data, dict) or 'Entries' not in data:
        main_logger.warning(f"CarData handler received unexpected format: {data}")
        return

    entries = data.get('Entries', [])
    if not isinstance(entries, list):
         main_logger.warning(f"CarData 'Entries' is not a list: {entries}")
         return

    for entry in entries:
        if not isinstance(entry, dict): continue # Skip non-dicts

        utc_time = entry.get('Utc')
        cars_data = entry.get('Cars', {})
        if not isinstance(cars_data, dict): continue # Skip non-dicts

        for car_number, car_details in cars_data.items():
            car_number_str = str(car_number) # Ensure string key

            # --- Check against timing_state ---
            if car_number_str not in timing_state:
                # Driver not found in timing_state, maybe hasn't appeared in DriverList/TimingData yet
                # Log less frequently if this becomes noisy
                # main_logger.debug(f"Received CarData for driver {car_number_str} not yet in timing_state.")
                continue # Skip processing CarData for this driver for now

            if not isinstance(car_details, dict): continue
            channels = car_details.get('Channels', {})
            if not isinstance(channels, dict): continue

            # Ensure 'CarData' sub-dictionary exists
            if 'CarData' not in timing_state[car_number_str]:
                timing_state[car_number_str]['CarData'] = {}

            car_data_dict = timing_state[car_number_str]['CarData']

            # --- Use the channel_map to populate car_data_dict ---
            for channel_num_str, data_key in channel_map.items():
                if channel_num_str in channels:
                    car_data_dict[data_key] = channels[channel_num_str]
                # else: # Optional: Handle missing channels if needed
                #     car_data_dict[data_key] = None # Or some default

            car_data_dict['Utc'] = utc_time # Store timestamp separately

            # Optional: Log specific updates only if needed for debugging
            # main_logger.debug(f"Updated CarData for {car_number_str} using map: {car_data_dict}")

def _process_session_data(data):
    """ Processes SessionData updates (like status).
        MUST be called within app_state.app_state_lock.
    """
    global session_details # Access the global dictionary

    if not isinstance(data, dict):
        main_logger.warning(f"SessionData handler received non-dict data: {data}")
        return

    try:
        # Example: Extract Status updates from StatusSeries if present
        status_series = data.get('StatusSeries')
        if isinstance(status_series, dict):
            # StatusSeries contains numbered entries, often just one per message
            for entry_key, status_info in status_series.items():
                if isinstance(status_info, dict):
                    session_status = status_info.get('SessionStatus')
                    if session_status:
                         session_details['SessionStatus'] = session_status # Store the latest status
                         main_logger.info(f"Session Status Updated: {session_status}")

        # Add logic here to extract other fields from SessionData if needed
        # e.g., AirTemp, TrackTemp, Humidity, Pressure, WindSpeed etc. might appear here sometimes
        # Example: session_details['AirTemp'] = data.get('AirTemp')

        # main_logger.debug(f"Processed SessionData. Current details: {session_details}")

    except Exception as e:
        main_logger.error(f"Error processing SessionData: {e}", exc_info=True)

def _process_session_info(data):
    """ Processes SessionInfo data and stores it in the global session_details dict.
        MUST be called within app_state.app_state_lock.
    """
    global session_details # Access the global dictionary

    if not isinstance(data, dict):
        main_logger.warning(f"SessionInfo handler received non-dict data: {data}")
        return

    try:
        # --- Corrected Extraction Logic ---
        meeting_info = data.get('Meeting', {}) # Get the Meeting dict first
        if not isinstance(meeting_info, dict): # Ensure it's a dict before proceeding
             meeting_info = {}
             main_logger.warning("SessionInfo 'Meeting' key was not a dictionary.")

        # Get Circuit and Country FROM the meeting_info dictionary
        circuit_info = meeting_info.get('Circuit', {})
        country_info = meeting_info.get('Country', {})
        # --- End Corrected Extraction Logic ---

        # Update the global dictionary
        session_details['Meeting'] = meeting_info # Store the whole Meeting dict
        session_details['Circuit'] = circuit_info if isinstance(circuit_info, dict) else {} # Store Circuit dict found within Meeting
        session_details['Country'] = country_info if isinstance(country_info, dict) else {} # Store Country dict found within Meeting

        # Extract Year from StartDate
        year = None
        start_date_str = data.get('StartDate')
        if start_date_str and isinstance(start_date_str, str) and len(start_date_str) >= 4:
            try:
                year = int(start_date_str[:4])
            except ValueError:
                main_logger.warning(f"Could not parse year from StartDate: {start_date_str}")
                
                
        # Update the global dictionary
        session_details['Meeting'] = meeting_info
        session_details['Circuit'] = circuit_info
        session_details['Country'] = country_info
        session_details['Name'] = data.get('Name')
        session_details['Type'] = data.get('Type')
        session_details['StartDate'] = start_date_str
        session_details['EndDate'] = data.get('EndDate')
        session_details['GmtOffset'] = data.get('GmtOffset')
        session_details['Path'] = data.get('Path')
        session_details['Year'] = year # <<< Store Year
        session_details['CircuitKey'] = circuit_info.get('Key') # <<< Store Circuit Key

        # Log extracted info for confirmation
        meeting_name = session_details.get('Meeting', {}).get('Name', 'N/A')
        session_name = session_details.get('Name', 'N/A')
        circuit_name_log = session_details.get('Circuit', {}).get('ShortName', 'N/A')
        circuit_key_log = session_details.get('CircuitKey', 'N/A')
        year_log = session_details.get('Year', 'N/A')
        main_logger.info(f"Processed SessionInfo: Y:{year_log} {meeting_name} - {session_name} (Circuit: {circuit_name_log}, Key: {circuit_key_log})")
        # main_logger.debug(f"Full SessionInfo details stored: {session_details}")

    except Exception as e:
        main_logger.error(f"Error processing SessionInfo data: {e}", exc_info=True)

def data_processing_loop():
    global data_store, db_cursor, timing_state # No fastf1 map needed
    processed_count = 0
    last_log_time = time.monotonic() # Track time for periodic logging
    log_interval_seconds = 15 # How often (in seconds) to log queue size
    log_interval_items = 500  # How many items between logging queue size
    # max_process = 100
    loop_counter = 0 # Add counter

    while not stop_event.is_set():
            loop_counter += 1
            if loop_counter % 50 == 0: # Log every 50 iterations (approx 10 seconds)
                 main_logger.debug(f"Data processing loop is running (Iteration {loop_counter})...")
            # --- >>> END ADDED LOG <<< ---
            current_time = time.monotonic()
            # Log based on time interval OR item count interval
            # Note: qsize() has some overhead, so don't call it extremely frequently in a tight loop
            if (current_time - last_log_time > log_interval_seconds) or \
               (processed_count > 0 and processed_count % log_interval_items == 0):
                 try:
                     qsize = data_queue.qsize() # Get current queue size
                     main_logger.debug(f"Data processing loop status: Processed={processed_count}, Queue Size={qsize}")
                     last_log_time = current_time # Reset timer after logging based on time
                 except Exception as q_err:
                     # Handle potential errors getting qsize (less likely but possible)
                     main_logger.warning(f"Could not get queue size: {q_err}")
        # processed_count += 1
            item = None # Initialize item to None for this iteration
            try:
                item = data_queue.get(block=True, timeout=0.2)
                
                # --- START ADDED LOGGING ---
                #main_logger.debug(f"Processing Queue Item: {item.get('stream', 'UnknownStream')}")
                # --- END ADDED LOGGING ---
                # --- Expect item = {"stream": stream_name, "data": data, "timestamp": timestamp} ---
                if not isinstance(item, dict) or 'stream' not in item or 'data' not in item:
                    main_logger.warning(f"Skipping queue item with unexpected structure: {type(item)}")
                    if item is not None: data_queue.task_done()
                    continue

                stream_name = item['stream']
                actual_data = item['data']
                timestamp = item.get('timestamp') # Use .get for safety, might be None from some sources
    
                # --- Process individual streams (Update state) ---
                with app_state.app_state_lock: # Lock for timing_state and data_store updates
                    data_store[stream_name] = {"data": actual_data, "timestamp": timestamp}
                    
                    #main_logger.debug(f"  Calling processor for: {stream_name}")
                
    
                    # Specific stream handlers
                    try:
                        if stream_name == "Heartbeat":
                           app_state.app_status["last_heartbeat"] = timestamp
                        elif stream_name == "DriverList":
                            _process_driver_list(actual_data) # Existing handler
                        elif stream_name == "TimingData":
                            _process_timing_data(actual_data) # Existing handler
                        elif stream_name == "SessionInfo":
                            _process_session_info(actual_data) # Existing handler
                        # --- ADDED Stream Handlers ---
                        elif stream_name == "SessionData":
                             _process_session_data(actual_data) # Call the new handler
                        # --- END ADDED ---
                        elif stream_name == "TimingAppData":
                            _process_timing_app_data(actual_data) # Updates timing_state with tyre info
                        elif stream_name == "TrackStatus":
                            _process_track_status(actual_data) # Call new handler
                        elif stream_name == "CarData":
                            _process_car_data(actual_data) # Call new handler
                        elif stream_name == "Position":
                            _process_position_data(actual_data) # Call the position handler
                        # --- END ADDED Stream Handlers ---
                        # Add other handlers here (WeatherData, TimingStats, etc.)
                        elif stream_name == "WeatherData":
                             _process_weather_data(actual_data) # call the weather handler
                        elif stream_name == "RaceControlMessages":
                            _process_race_control(actual_data)
                        else:
                             # Optional: Log if you want to know about unhandled streams that made it this far
                            main_logger.debug(f"No specific handler for stream: {stream_name}") 
                    except Exception as proc_ex:
                        # --- START ADDED LOGGING ---
                        # Log errors specific to the processing function
                        main_logger.error(f"  ERROR processing stream '{stream_name}': {proc_ex}", exc_info=True)
                        # --- END ADDED LOGGING ---
    
                data_queue.task_done() # Mark the original queue item as done
    
            except queue.Empty:
                continue
                    #break # No more items
            except Exception as e: # <<< Catch ALL exceptions within the while loop's iteration
                main_logger.error(f"!!! Unhandled exception in data_processing_loop !!! Error: {e}", exc_info=True)
                if item is not None:
                    try:
                        data_queue.task_done()
                    except: pass # Ignore errors during error cleanup
                time.sleep(0.5) # Avoid busy-looping on continuous errors

    main_logger.info("Data processing thread finished cleanly (stop_event set).") # Changed log message

# --- SignalR Connection Handling ---
def build_connection_url(negotiate_url, hub_name):
    main_logger.info(f"Negotiating connection: {negotiate_url}/negotiate")
    try:
        params={"clientProtocol": "1.5", "connectionData": json.dumps([{"name": hub_name}])}
        response = requests.get(f"{negotiate_url}/negotiate", params=params, timeout=10)
        response.raise_for_status()
        negotiate_data = response.json()
        connection_token = negotiate_data.get('ConnectionToken');
        if not connection_token:
            raise ValueError("No ConnectionToken")
        ws_params = {"transport": "webSockets", "clientProtocol": "1.5", "connectionToken": connection_token, "connectionData": json.dumps([{"name": hub_name}])}
        full_ws_url = f"{WEBSOCKET_URL_BASE}/connect?{urllib.parse.urlencode(ws_params)}"
        main_logger.info("Negotiation OK.")
        return full_ws_url
    except requests.exceptions.Timeout:
        main_logger.error("Negotiation timeout.")
    except requests.exceptions.RequestException as e:
        main_logger.error(f"Negotiation HTTP fail: {e}", exc_info=True)
    except Exception as e:
        main_logger.error(f"Negotiation error: {e}", exc_info=True)
    return None

def handle_connect():
    """Callback executed when the hub connection is successfully opened."""
    global hub_connection, streams_to_subscribe # Make sure these are accessible

    connection_id = "N/A" # SignalR Core doesn't expose easily like older versions
    main_logger.info(f"****** Connection Opened! ****** Connection ID: {connection_id}")

    with app_state.app_state_lock:
        # Check if we are already Live to prevent issues if handle_connect is called unexpectedly
        if app_state.app_status["state"] == "Live":
             main_logger.warning("handle_connect called but unexpected state: Live. Proceeding with subscribe.")
             # Consider just returning if this state shouldn't happen
             # return

        app_state.app_status.update({"state": "Live", "connection": "Socket Connected - Subscribing"})

    if hub_connection:
        try:
            main_logger.info(f"Attempting to subscribe using **RAW JSON**: {STREAMS_TO_SUBSCRIBE}")

            # --- START REPLACEMENT for subscription ---
            # invocation_id = hub_connection.send("Subscribe", [streams_to_subscribe]) # OLD WAY
            # main_logger.info(f"Subscription request sent via .send(). Invocation ID: {invocation_id}") # OLD WAY

            # Construct the old-style JSON message
            # Generate a simple invocation ID (e.g., incrementing integer or random)
            invocation_counter = str(uuid.uuid4())[:8] # Example: use part of a UUID
            subscribe_message = {
                "H": "Streaming", # Hub name
                "M": "Subscribe", # Method name
                "A": [STREAMS_TO_SUBSCRIBE], # Arguments: Must be List[List[str]]
                "I": invocation_counter # Invocation ID (client-generated counter)
            }
            json_string = json.dumps(subscribe_message)

            # Send using the new raw method
            hub_connection.send_raw_json(json_string)
            main_logger.info(f"Subscription request sent via **send_raw_json()**. Invocation ID ('I'): {invocation_counter}")
            # --- END REPLACEMENT ---

        except Exception as e:
            main_logger.error(f"Error sending subscription in handle_connect: {e}", exc_info=True)
            # Update state or handle error appropriately
            with app_state.app_state_lock:
                app_state.app_status.update({"state": "Error", "connection": f"Subscription Send Error: {type(e).__name__}"})
            # Consider stopping connection here?
            # stop_connection()
    else:
        main_logger.error("handle_connect called but hub_connection is None!")
        with app_state.app_state_lock:
            app_state.app_status.update({"state": "Error", "connection": "Hub object missing"})

def handle_disconnect(): # Was on_close
    main_logger.warning("Connection closed.")
    with app_state.app_state_lock:
         # Only update state if we weren't already stopping/stopped/error
         if app_state.app_status["state"] not in ["Stopping", "Stopped", "Error", "Playback Complete"]:
              app_state.app_status.update({"connection": "Closed Unexpectedly", "state": "Stopped"})
    if not stop_event.is_set():
        main_logger.info("Setting stop_event due to disconnect.")
        stop_event.set()

def handle_error(error): # Was on_error
    # Avoid logging expected closure errors if possible
    if "WebSocket connection is already closed" in str(error):
         main_logger.info(f"Ignoring expected SignalR error on close: {error}")
         return
    main_logger.error(f"Connection error received: {error}")
    with app_state.app_state_lock:
        if app_state.app_status["state"] not in ["Error", "Stopping", "Stopped"]:
             app_state.app_status.update({"connection": f"SignalR Error: {type(error).__name__}", "state": "Error"})
    if not stop_event.is_set():
        main_logger.info("Setting stop_event due to SignalR error.")
        stop_event.set()

def init_live_file():
    """
    Initializes the live data file, attempting to name it using
    FastF1 info, falling back to a default name.
    Returns True on success, False on failure.
    """
    global main_logger # Assuming main_logger is defined globally
    # Note: db_conn, db_cursor, db_filename seem unrelated to the live .data.txt file
    # based on the original function content, they might be for a separate DB?
    # Removing them from global declaration here unless they are truly needed for this file.

    filepath = None # Define for use in exception logging
    try:
        # --- Get session info using FastF1 ---
        main_logger.info("Attempting to get next session info via FastF1 for filename...")
        event_name, session_name = get_current_or_next_session_info() # Call helper function

        # Use FastF1 info if available, otherwise fallback
        if event_name and session_name:
             event_part = sanitize_filename(event_name)
             session_part = sanitize_filename(session_name)
             filename_prefix = f"{event_part}_{session_part}"
             main_logger.info(f"Using filename prefix from FastF1: {filename_prefix}")
        else:
             main_logger.warning("Using fallback filename prefix 'f1_live_data'.")
             filename_prefix = "f1_live_data" # Fallback name
        # --- End FastF1 part ---

        # Ensure the target save directory exists
        os.makedirs(TARGET_SAVE_DIRECTORY, exist_ok=True)
        main_logger.debug(f"Ensured save directory exists: {TARGET_SAVE_DIRECTORY}")

        # Generate the final filename using the determined prefix and timestamp
        timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        data_filename = f"{filename_prefix}_{timestamp_str}.data.txt" # Use prefix
        filepath = os.path.join(TARGET_SAVE_DIRECTORY, data_filename) # Use os.path.join

        main_logger.info(f"Initializing live data file: {filepath}")
        # Use app_state lock for thread safety when modifying shared state
        with app_state.app_state_lock:
            # Close previous file if open
            if app_state.live_data_file and not app_state.live_data_file.closed:
                 main_logger.warning("Closing previously open live data file in init_live_file.")
                 try:
                     app_state.live_data_file.close()
                 except Exception as close_err:
                     main_logger.error(f"Error closing previous live file: {close_err}")
                 app_state.live_data_file = None # Ensure it's None after closing attempt

            # Open new file (ensure 'app_state.live_data_file' is the correct global variable)
            # buffering=1 means line buffered
            app_state.live_data_file = open(filepath, "w", buffering=1, encoding='utf-8')
            app_state.is_saving_active = True # Set saving flag

        main_logger.info(f"Live data file initialized: {filepath}")
        return True # Indicate success

    except IOError as e: # Catch file system errors specifically
       main_logger.error(f"IOError initializing live file '{filepath}': {e}", exc_info=True)
       with app_state.app_state_lock:
           app_state.live_data_file = None
           app_state.is_saving_active = False
       return False # Indicate failure
    except Exception as e: # Catch other potential errors (e.g., during FastF1 call if not handled internally)
       main_logger.error(f"Unexpected error during init_live_file (path='{filepath}'): {e}", exc_info=True)
       with app_state.app_state_lock:
           app_state.live_data_file = None
           app_state.is_saving_active = False
       return False # Indicate failure

def close_live_file():
    # ... (logic to close file stored in app_state.live_data_file) ...
     with app_state.app_state_lock:
          if app_state.live_data_file and not app_state.live_data_file.closed:
              try:
                  file_name = app_state.live_data_file.name
                  main_logger.info(f"Closing live file: {file_name}")
                  app_state.live_data_file.close()
              except Exception as e:
                   main_logger.error(f"Err closing live file: {e}")
          app_state.live_data_file = None
          app_state.is_saving_active = False # Stop saving when file closed

def stop_connection():
    global hub_connection, connection_thread, stop_event
    main_logger.info("Stop connection requested.")
    with app_state.app_state_lock:
        current_state = app_state.app_status["state"]
        thread_running = connection_thread and connection_thread.is_alive()
    if current_state not in ["Connecting", "Live", "Stopping"] and not thread_running:
         main_logger.warning(f"Stop conn called, state={current_state}, thread_active={thread_running}. No active conn.")
         if not stop_event.is_set():
             stop_event.set()
         with app_state.app_state_lock:
             if current_state in ["Connecting", "Live"]:
                 app_state.app_status.update({"state": "Stopped", "connection": "Disconnected (Force Stop)"})
         return
    with app_state.app_state_lock:
        if current_state == "Stopping":
            main_logger.info("Stop already in progress.")
            return
        app_state.app_status.update({"state": "Stopping", "connection": "Disconnecting"})
    stop_event.set()
    main_logger.debug("Stop event set.")
    temp_hub = hub_connection
    # *** CORRECTED ATTRIBUTE CHECK HERE ***
    if temp_hub and temp_hub.transport and getattr(temp_hub.transport, 'connection_alive', False):
         main_logger.info("Attempting immediate hub stop...")
         try:
             temp_hub.stop()
         except Exception as e:
             main_logger.error(f"Error during immediate stop: {e}", exc_info=True)
    local_conn_thread = globals().get('connection_thread')
    if local_conn_thread and local_conn_thread.is_alive():
        main_logger.info("Waiting for connection thread join...")
        local_conn_thread.join(timeout=10)
        if local_conn_thread.is_alive():
            main_logger.warning("Connection thread did not join cleanly.")
        else:
            main_logger.info("Connection thread joined.")
    with app_state.app_state_lock:
        if app_state.app_status["state"] == "Stopping":
            app_state.app_status.update({"state": "Stopped", "connection": "Disconnected"})
        app_state.app_status["subscribed_streams"] = []
    globals()['hub_connection'] = None
    globals()['connection_thread'] = None
    main_logger.info("Stop connection sequence complete.")

def replay_from_file(data_file_path, replay_speed=1.0):
    global replay_thread, stop_event, timing_state
    if replay_thread and replay_thread.is_alive():
        main_logger.warning("Replay thread running.")
        return
    stop_event.clear()
    main_logger.debug("Stop event cleared (replay).")
    if not os.path.exists(data_file_path):
        main_logger.error(f"File not found: {data_file_path}")
        with app_state.app_state_lock:
            app_state.app_status.update({"state": "Error", "connection": f"File Not Found: {os.path.basename(data_file_path)}"})
        return
    with app_state.app_state_lock:
        app_state.app_status.update({"state": "Initializing", "connection": f"Preparing Replay: {os.path.basename(data_file_path)}", "subscribed_streams": [], "last_heartbeat": None})
        data_store.clear()
        timing_state.clear() # Clear persistent state
        
    # --- MODIFICATION START: Disable DB Init for Replay ---
    main_logger.info("Replay mode: Closing active DB connection (if any) and skipping init.")
    with app_state.app_state_lock:
        app_state.app_status.update({"state": "Replaying", "connection": f"File: {os.path.basename(data_file_path)}", "subscribed_streams": ["Replay"]})
        
        def replay(): # The actual replay loop
            main_logger.info(f"Starting main replay loop: {data_file_path}, speed {replay_speed}")
            last_timestamp_for_delay = None # Use a separate variable for delay calculation timestamp
            lines_processed = 0; lines_skipped = 0; first_message_processed = False
            try:
                with open(data_file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        if stop_event.is_set():
                            main_logger.info("Replay stopped by signal.")
                            break
    
                        line = line.strip()
                        if not line: continue
    
                        start_time_line = time.monotonic()
                        timestamp_for_this_line = None # Initialize timestamp for this line/message block
                        should_apply_delay = False # Flag to control delay logic

                        try:
                            raw_message = json.loads(line)
                            default_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat() + 'Z'
# Default timestamp
    
                            # --- MODIFICATION START: Handle different message structures ---
                            if isinstance(raw_message, dict) and "R" in raw_message:
                                # main_logger.debug(f"Line {line_num}: Identified Snapshot (R) message. Calling handle_message directly.")
                                handle_message(raw_message) # handle_message unpacks and queues
                                lines_processed += 1 # Count the snapshot block as one processed line for stats
                                # DO NOT apply delay for the whole R block based on its internal timestamp here
                                should_apply_delay = False # Delay is handled by individual messages queued by handle_message
                                # We don't get a single 'timestamp' for the 'R' block to use for delay here
    
                            elif isinstance(raw_message, dict) and "M" in raw_message and isinstance(raw_message["M"], list):
                                 # Standard message block {"M": [...]}
                                 # main_logger.debug(f"Line {line_num}: Identified Standard (M) message block.")
                                 messages_in_line = []
                                 for msg_container in raw_message["M"]:
                                      if isinstance(msg_container, dict) and msg_container.get("M") == "feed":
                                          msg_args = msg_container.get("A")
                                          if isinstance(msg_args, list) and len(msg_args) >= 2:
                                               messages_in_line.append(msg_args)
    
                                 if not messages_in_line: lines_skipped += 1; continue

                                 for message_parts in messages_in_line:
                                     if stop_event.is_set(): break
                                     stream_name_raw=message_parts[0]; data_content=message_parts[1]
                                     # GET timestamp for this specific message part
                                     timestamp_for_this_line = message_parts[2] if len(message_parts) > 2 else default_timestamp

    
                                     # Decompression and queuing logic (moved from process_data_queue)
                                     stream_name = stream_name_raw
                                     actual_data = data_content
                                     if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                                          stream_name = stream_name_raw[:-2]
                                          actual_data = _decode_and_decompress(data_content)
                                          if actual_data is None:
                                              main_logger.warning(f"Decompress failed for stream {stream_name_raw} in M block line {line_num}. Skipping.")
                                              continue
    
                                     if actual_data is not None:
                                         data_queue.put({"stream": stream_name, "data": actual_data, "timestamp": timestamp_for_this_line})
                                         lines_processed += 1
                                         should_apply_delay = True # Apply delay after processing this M block
                                     else: lines_skipped += 1; # Log already happens
    
                                 if stop_event.is_set(): break

    
                            elif isinstance(raw_message, list) and len(raw_message) >= 2:
                                 # Direct message list ["StreamName", {...}, "Timestamp"]
                                 # main_logger.debug(f"Line {line_num}: Identified Direct stream list message.")
                                 message_parts = raw_message # The whole list is the message parts
                                 if stop_event.is_set(): break
                                 stream_name_raw = message_parts[0]
                                 data_content = message_parts[1]
                                 # GET timestamp for this direct message
                                 timestamp_for_this_line = message_parts[2] if len(message_parts) > 2 else default_timestamp
    
                                 # Decompression and queuing logic
                                 stream_name = stream_name_raw
                                 actual_data = data_content
                                 if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                                      stream_name = stream_name_raw[:-2]
                                      actual_data = _decode_and_decompress(data_content)
                                      if actual_data is None:
                                          main_logger.warning(f"Decompress failed for direct stream {stream_name_raw} line {line_num}. Skipping.")
                                          continue
    
                                 if actual_data is not None:
                                     data_queue.put({"stream": stream_name, "data": actual_data, "timestamp": timestamp_for_this_line})
                                     lines_processed += 1
                                     should_apply_delay = True # Apply delay for direct messages
                                 else: lines_skipped += 1; # Log already happens
    
                            else: # Unrecognized JSON structure
                                lines_skipped += 1
                                #if line_num > 3: main_logger.debug(f"Line {line_num}: Skipping unrecognized JSON top-level structure: {type(raw_message).__name__}")
                                continue # Skip delay logic
                            # --- MODIFICATION END ---
    
    
                            # --- Delay logic (Apply only if flag is set) ---
                            # --- MODIFIED Delay logic (Uses last A[2] timestamp) ---
                            delay_applied = False
                            # Use the timestamp extracted from the LAST message part processed in this block
                            timestamp_to_use_for_current_block = timestamp_for_this_line
                
                            if should_apply_delay and timestamp_to_use_for_current_block:
                                current_ts_dt = None; prev_ts_dt = None
                
                                if not first_message_processed:
                                    current_ts_dt = parse_iso_timestamp_safe(timestamp_to_use_for_current_block, line_num)
                                    first_message_processed = True
                                elif last_timestamp_for_delay: # This holds the A[2] string from the previous relevant block
                                    current_ts_dt = parse_iso_timestamp_safe(timestamp_to_use_for_current_block, line_num)
                                    prev_ts_dt = parse_iso_timestamp_safe(last_timestamp_for_delay, f"{line_num-1}(prev)")
                
                                    if current_ts_dt and prev_ts_dt:
                                        try:
                                            time_diff_seconds = (current_ts_dt - prev_ts_dt).total_seconds()
                                            if time_diff_seconds > 0:
                                                target_delay = time_diff_seconds / replay_speed if replay_speed > 0 else time_diff_seconds
                                                processing_time = time.monotonic() - start_time_line
                                                actual_delay = max(0, target_delay - processing_time)
                                                max_physical_delay = 2.0
                                                actual_sleep = min(actual_delay, max_physical_delay)
                                                if actual_sleep > 0.001:
                                                    time.sleep(actual_sleep)
                                                    delay_applied = True
                                            elif time_diff_seconds < 0:
                                                 #main_logger.debug(f"Timestamp (A[2]) backwards line {line_num}: {timestamp_to_use_for_current_block} vs {last_timestamp_for_delay}")
                                                 time.sleep(0.001 / replay_speed if replay_speed > 0 else 0.001)
                                                 delay_applied = True
                                        except Exception as calc_err:
                                             main_logger.error(f"Error during delay calculation/sleep line {line_num}: {calc_err}", exc_info=True)
                
                                # Fallback fixed delay if needed
                                if not delay_applied and first_message_processed:
                                     time.sleep(0.005 / replay_speed if replay_speed > 0 else 0.005)
                
                                # Update last_timestamp_for_delay with the CURRENT block's last A[2] timestamp string
                                last_timestamp_for_delay = timestamp_to_use_for_current_block
                                start_time_line = time.monotonic()
                
                            elif not delay_applied: # If no delay applied (e.g., R block, no timestamp, etc.)
                                start_time_line = time.monotonic() # Still need to reset timer
                            # --- End Modified Delay Logic ---
    
                        except json.JSONDecodeError as e:
                             lines_skipped += 1
                             if line_num > 3: # Ignore first few non-JSON lines
                                  main_logger.warning(f"Invalid JSON line {line_num}: {e} - Line: {line[:100]}...")
                        except Exception as e:
                             lines_skipped += 1
                             ts_for_error = timestamp_for_this_line if timestamp_for_this_line else "N/A"
                             main_logger.error(f"Error processing line {line_num} (Timestamp: {ts_for_error}): {e} - Line: {line[:100]}...", exc_info=True)

    
                # --- Replay finished ---
                if not stop_event.is_set(): main_logger.info(f"Replay finished. Proc: {lines_processed}, Skip: {lines_skipped}"); # ... update status ...
                else: main_logger.info(f"Replay stopped. Proc: {lines_processed}, Skip: {lines_skipped}"); # ... update status .....
	
	        # --- Exception handling & cleanup ---
            except FileNotFoundError:
                main_logger.error(f"Replay file not found: {data_file_path}")
                with app_state.app_state_lock:
                    app_state.app_status.update({"state": "Error", "connection": "Replay File Error"})
            except Exception as e:
                main_logger.error(f"Error during playback: {e}", exc_info=True)
                with app_state.app_state_lock:
                    app_state.app_status.update({"state": "Error", "connection": "Replay Runtime Error"})
            finally:
                 main_logger.info("Replay thread cleanup...")
                 close_live_file()
                 with app_state.app_state_lock:
                     current_state = app_state.app_status["state"]
                     if current_state not in ["Error", "Stopped", "Playback Complete"]:
                        if stop_event.is_set():
                              app_state.app_status.update({"state": "Stopped", "connection": "Disconnected"})
                        else:
                              app_state.app_status.update({"state": "Error", "connection": "Thread End Unexpectedly"})
                 main_logger.info("Replay thread cleanup finished.")
	             
    globals()['replay_thread'] = threading.Thread(target=replay, name="ReplayThread", daemon=True)
    replay_thread.start()


def stop_replay():
    global replay_thread, stop_event
    main_logger.info("Stop replay requested.")
    with app_state.app_state_lock: current_state = app_state.app_status["state"]; thread_running = replay_thread and replay_thread.is_alive()
    if not thread_running and current_state != "Replaying":
        main_logger.warning(f"Stop replay called, state={current_state}, thread_active={thread_running}.")
        with app_state.app_state_lock:
            if current_state == "Replaying": app_state.app_status.update({"state": "Stopped", "connection": "Disconnected (Force Stop)"})
        return
    with app_state.app_state_lock:
        if current_state == "Stopping": main_logger.info("Stop already in progress."); return
        app_state.app_status.update({"state": "Stopping", "connection": "Stopping Replay"})
    stop_event.set(); main_logger.debug("Stop event set for replay.")
    local_replay_thread = globals().get('replay_thread')
    if local_replay_thread and local_replay_thread.is_alive():
        main_logger.info("Waiting for replay thread join..."); local_replay_thread.join(timeout=5)
        if local_replay_thread.is_alive(): main_logger.warning("Replay thread did not join cleanly.")
        else: main_logger.info("Replay thread joined.")
    with app_state.app_state_lock:
        if app_state.app_status["state"] == "Stopping": app_state.app_status.update({"state": "Stopped", "connection": "Disconnected"})
    globals()['replay_thread'] = None
    main_logger.info("Stop replay sequence complete.")


ensure_replay_dir_exists()

replay_file_options = get_replay_files(REPLAY_DIR)
# --- Dash GUI Setup ---
app = Dash(__name__, external_stylesheets=[dbc.themes.SLATE], suppress_callback_exceptions=True)
timing_table_columns = [
    # --- Existing Columns ---
    {"name": "Car", "id": "Car"}, # Keep your logic for TLA/Number
    {"name": "Pos", "id": "Pos"},
    {"name": "Tyre", "id": "Tyre"},
    {"name": "Time", "id": "Time"}, # Assuming 'Time' is the gap/time field you want
    {"name": "Interval", "id": "Interval"},
    {"name": "Gap", "id": "Gap"}, # Assuming 'Gap' is GapToLeader
    {"name": "Last Lap", "id": "Last Lap"},
    {"name": "Best Lap", "id": "Best Lap"},
    {"name": "S1", "id": "S1"},
    {"name": "S2", "id": "S2"},
    {"name": "S3", "id": "S3"},
    {"name": "Status", "id": "Status"},
    # --- ADDED: CarData Columns ---
    {'name': 'Speed', 'id': 'Speed', 'type': 'numeric'},
    {'name': 'Gear', 'id': 'Gear', 'type': 'numeric'},
    {'name': 'RPM', 'id': 'RPM', 'type': 'numeric'},
    {'name': 'DRS', 'id': 'DRS'},
    # --- END ADDED ---
]
app.layout = dbc.Container([
    dbc.Row(dbc.Col(html.H1("F1 Live Timing SignalR Viewer"), width=12), className="mb-3"),
    # --- ADDED: Session Details Row ---
    dbc.Row([
        dbc.Col(html.Div(id='session-info-display'), width=12) # Display across full width initially
    ], className="mb-3", id='session-details-row'), # Give the row an ID too if needed
    # --- END ADDED ---
    dbc.Row([
    dbc.Col(html.Div(id='status-display'), width="auto"), 
    dbc.Col(html.Div(id='heartbeat-display'), width="auto", style={'marginLeft': '20px'}),
    dbc.Col(html.Div(id='track-status-display', children="Track: Unknown"), width="auto", style={'marginLeft': '20px'}),
    # --- END ADDED ---
    ], className="mb-3"),
    dbc.Row([
    # Start/Stop Buttons remain in their own columns
    dbc.Col(dbc.Button("Start Live Feed", id="start-button", color="success", className="me-1"), width="auto"),
    dbc.Col(dbc.Button("Stop Feed / Replay", id="stop-button", color="danger", className="me-1"), width="auto"),

    # --- Replay Controls Column (consolidated) ---
    dbc.Col([
        # This Div now allows wrapping
        html.Div([
            dcc.Dropdown(
                id='replay-file-dropdown',
                options=replay_file_options,
                placeholder="Select replay file...",
                style={
                    'minWidth': '300px',
                    'flexGrow': '1',
                    'color': '#333'
                    # Consider adding a bottom margin for when it wraps
                    # 'marginBottom': '5px'
                },
                className="me-2"
            ),
            html.Button(
                'Refresh List',
                id='refresh-replay-list-button',
                n_clicks=0,
                style={
                    'height': '38px',
                    'minWidth': '110px'
                    # Consider adding a bottom margin for when it wraps
                    # 'marginBottom': '5px'
                },
                className="me-2"
            ),
            dbc.Input(
                id="replay-speed-input", placeholder="Speed", type="number",
                min=0.1, step=0.1, value=1.0, debounce=True,
                style={
                    'width': '90px'
                    # Consider adding a bottom margin for when it wraps
                    # 'marginBottom': '5px'
                },
                className="me-2"
            ),
            dbc.Button(
                "Start Replay",
                id="replay-button",
                color="primary",
                className="me-1"
                 # Consider adding a bottom margin for when it wraps
                 # style={'marginBottom': '5px'}
            ),
        ], style={
            'display': 'flex',
            'alignItems': 'center',
            'width': '100%',
            'flexWrap': 'wrap' # <<< ADDED THIS LINE TO ALLOW WRAPPING
            })
    ], width=True)
    # --- End Replay Controls Column ---

], className="mb-3 align-items-center"),
    dbc.Row([dbc.Col([html.H3("Latest Data (Non-Timing)"), html.Div(id='live-data-display', style={'maxHeight': '300px', 'overflowY': 'auto', 'border': '1px solid grey', 'padding': '10px', 'marginBottom': '10px'}), html.H3("Timing Data Details"), html.Div(id='timing-data-table', children=[html.P(id='timing-data-timestamp', children="Waiting for data..."), dash_table.DataTable(id='timing-data-actual-table', columns=timing_table_columns, data=[], fixed_rows={'headers': True}, style_table={'height': '400px', 'overflowY': 'auto', 'overflowX': 'auto'}, style_cell={'minWidth': '50px', 'width': '80px', 'maxWidth': '120px','overflow': 'hidden','textOverflow': 'ellipsis','textAlign': 'left','padding': '5px','backgroundColor': 'rgb(50, 50, 50)','color': 'white'}, style_header={'backgroundColor': 'rgb(30, 30, 30)','fontWeight': 'bold','border': '1px solid grey'}, style_data={'borderBottom': '1px solid grey'}, style_data_conditional=[{'if': {'row_index': 'odd'},'backgroundColor': 'rgb(60, 60, 60)'},{'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "SOFT"'},
            'backgroundColor': '#FF3333', 'color': 'black', 'fontWeight': 'bold'}, # Red for Soft
        {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "MEDIUM"'},
            'backgroundColor': '#FFF333', 'color': 'black', 'fontWeight': 'bold'}, # Yellow for Medium
        {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "HARD"'},
            'backgroundColor': '#FFFFFF', 'color': 'black', 'fontWeight': 'bold'}, # White for Hard
        {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "INTERMEDIATE"'},
            'backgroundColor': '#33FF33', 'color': 'black', 'fontWeight': 'bold'}, # Green for Intermediate
        {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "WET"'},
            'backgroundColor': '#3333FF', 'color': 'white', 'fontWeight': 'bold'}, # Blue for Wet
        {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} = "-"'}, # Default/Unknown
            'backgroundColor': 'inherit', 'color': 'grey'}, # Grey out if unknown ('inherit' uses row bg)
], tooltip_duration=None)])], width=12)]),
    dbc.Row([
        dbc.Col([
            html.H3("Race Control Messages"),
            dcc.Textarea(
                id='race-control-log-display',
                value='Waiting for Race Control messages...',
                style={'width': '100%', 'height': '200px', 'backgroundColor': '#333', 'color': '#DDD', 'border': '1px solid grey', 'fontFamily': 'monospace'},
                readOnly=True,
                # rows=10 # Alternative height control
            )
        ], width=12)
    ], className="mb-3"), # Add margin-bottom
    # --- ADDED: Track Map Row ---
    dbc.Row([
        dbc.Col(dcc.Graph(id='track-map-graph', style={'height': '60vh'})) # Adjust height as needed
    ], className="mt-3"), # Add margin-top
    # --- END ADDED ---
    dcc.Interval(id='interval-component', interval=500, n_intervals=0),
], fluid=True)

# --- Dash Callbacks ---
@app.callback(
    Output('status-display', 'children'),
    Output('heartbeat-display', 'children'),
    Output('track-status-display', 'children'), # <<< ADDED Output
    # Output('session-info-display', 'children'), # <<< ADDED Output
    Output('live-data-display', 'children'),
    Output('timing-data-actual-table', 'data'),
    Output('timing-data-timestamp', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_output(n):

    status_text = "State: Unknown"
    heartbeat_text = "Last HB: N/A"
    track_display_text = "Track: Unknown"
    # session_info_display_children = "Session Details: Waiting..." # <<< Initialize
    other_data_display = []
    table_data = []
    timing_timestamp_text = "Waiting for TimingData..."

    with app_state.app_state_lock: # <<< Move lock acquisition earlier
        # Status, Heartbeat, Track Status (already implicitly covered)
        status_text = f"State: {app_state.app_status['state']} | Conn: {app_state.app_status['connection']}"
        heartbeat_text = f"Last HB: {app_state.app_status['last_heartbeat'] or 'N/A'}"
        track_status_code = track_status_data.get('Status', '0')
        track_status_map = { # Define map inside or outside callback
             '1': "Track Clear", '2': "Yellow Flag", '3': "Flag",
             '4': "SC Deployed", '5': "Red Flag", '6': "VSC Deployed", '7': "VSC Ending"
        }
        track_display_text = f"Track: {track_status_map.get(track_status_code, f'Unknown ({track_status_code})')}"

        # Other Data Display (Reads data_store)
        sorted_streams = sorted([s for s in data_store.keys() if s not in ['TimingData', 'DriverList', 'Position.z', 'CarData.z']])
        for stream in sorted_streams:
            value = data_store.get(stream, {}) # Use .get for safety
            data_str = json.dumps(value.get('data', 'N/A'), indent=2)
            timestamp_str = value.get('timestamp', 'N/A')
            if len(data_str) > 500:
                data_str = data_str[:500] + "\n... (data truncated)"
            other_data_display.append(html.Details([
                html.Summary(f"{stream} ({timestamp_str})"),
                html.Pre(data_str, style={'marginLeft': '15px', 'maxHeight': '200px', 'overflowY': 'auto'})
            ], style={'marginBottom': '5px'}, open=(stream=="SessionInfo")))

        # Timing Table Timestamp (Reads data_store)
        if 'TimingData' in data_store:
            timing_timestamp_text = f"Timing Timestamp: {data_store['TimingData'].get('timestamp', 'N/A')}"
        elif not timing_state: # Read timing_state existence check inside lock
            timing_timestamp_text = "Waiting for DriverList..."

        # Generate Timing Table Data (Reads timing_state) <<< Now inside lock
        if timing_state:
            processed_table_data = []
            sorted_driver_numbers = sorted(timing_state.keys(), key=lambda x: int(x) if x.isdigit() else float('inf'))

            for car_num in sorted_driver_numbers:
                # Safely get driver state, might be modified by other thread
                driver_state = timing_state.get(car_num)
                if not driver_state: continue # Skip if driver was removed mid-read

                tyre_compound = driver_state.get('TyreCompound', '-')
                tyre_age = driver_state.get('TyreAge', '?')
                tyre_display = f"{tyre_compound} ({tyre_age}L)" if tyre_compound != '-' else '-'
                tla = driver_state.get("Tla")
                car_display_value = tla if tla and tla != "N/A" else car_num
                car_data = driver_state.get('CarData', {})

                row = {
                    'Car': car_display_value,
                    'Pos': driver_state.get('Position', '-'),
                    "Tyre": tyre_display,
                    'Time': driver_state.get('Time', '-'),
                    'Gap': driver_state.get('GapToLeader', '-'),
                    'Interval': get_nested_state(driver_state, 'IntervalToPositionAhead', 'Value', default='-'),
                    'Last Lap': get_nested_state(driver_state, 'LastLapTime', 'Value', default='-'),
                    'Best Lap': get_nested_state(driver_state, 'BestLapTime', 'Value', default='-'),
                    'S1': get_nested_state(driver_state, 'Sectors', '0', 'Value', default='-'),
                    'S2': get_nested_state(driver_state, 'Sectors', '1', 'Value', default='-'),
                    'S3': get_nested_state(driver_state, 'Sectors', '2', 'Value', default='-'),
                    'Status': driver_state.get('Status', 'N/A'),
                    'Speed': car_data.get('Speed', '-'),
                    'Gear': car_data.get('Gear', '-'),
                    'RPM': car_data.get('RPM', '-'),
                    'DRS': {8: "Eligible",10: "On",12: "On", 14: "ON"}.get(car_data.get('DRS'), 'Off'),
                }
                processed_table_data.append(row)
            processed_table_data.sort(key=pos_sort_key)
            table_data = processed_table_data
        # --- End Timing Table Data Processing (inside lock) ---

    # Return all outputs
    return (status_text, heartbeat_text, track_display_text,
            other_data_display, table_data, timing_timestamp_text)

@app.callback( Output('start-button', 'disabled'), Output('stop-button', 'disabled'), Output('replay-button', 'disabled'), Output('replay-file-input', 'disabled'), Output('replay-speed-input', 'disabled'), Input('interval-component', 'n_intervals'))
def update_button_states(n):
    with app_state.app_state_lock: state = app_state.app_status['state']
    is_idle = state in ["Idle", "Stopped", "Error", "Playback Complete"]; is_running = state in ["Connecting", "Live", "Replaying", "Initializing"]; is_stopping = state == "Stopping"
    start_disabled = is_running or is_stopping; replay_disabled = is_running or is_stopping; stop_disabled = is_idle; input_disabled = is_running or is_stopping
    return start_disabled, stop_disabled, replay_disabled, input_disabled, input_disabled

@app.callback(
    Output('replay-file-dropdown', 'options'),
    Input('refresh-replay-list-button', 'n_clicks'),
    prevent_initial_call=True
)
def refresh_replay_files(n_clicks):
    if n_clicks > 0:
        main_logger.info("Refreshing replay file list...")
        new_options = get_replay_files(REPLAY_DIR)
        return new_options
    return dash.no_update

@app.callback(
    Output('race-control-log-display', 'value'),
    Input('interval-component', 'n_intervals')
)
def update_race_control_log(n):
    log_snapshot = []
    try:
        # Acquire lock when reading the deque
        with app_state.app_state_lock: # <<< Add lock here
            log_snapshot = list(race_control_log) # Create snapshot inside lock

        # Process the snapshot outside the lock
        display_text = "\n".join(reversed(log_snapshot))

        if not display_text and n > 0: return "No messages received yet."
        elif not display_text: return "Waiting for Race Control messages..."
        return display_text

    except Exception as e:
        main_logger.error(f"Error in update_race_control_log: {e}", exc_info=True)
        return "Error updating Race Control log."

@app.callback(
        Output('session-info-display', 'children'), # Target the specific Div for session info
        Input('interval-component', 'n_intervals') # Triggered by the same interval
    )

def update_session_info_display(n):
        # Default values
        session_info_parts = ["Session Info: Waiting..."]
        weather_elements = []

        try:
            with app_state.app_state_lock:
                # --- Read SessionInfo Data ---
                session_info_store_entry = data_store.get('SessionInfo', {})
                session_details = {}
                if isinstance(session_info_store_entry, dict):
                     if 'data' in session_info_store_entry and isinstance(session_info_store_entry['data'], dict):
                          session_details = session_info_store_entry['data']
                     else:
                          session_details = session_info_store_entry

                # --- Read Weather Data ---
                weather_data = data_store.get('WeatherData', {})
                if not isinstance(weather_data, dict): weather_data = {}

                # --- Format Session Details ---
                meeting_name = session_details.get('Meeting', {}).get('Name', 'Unknown Meeting')

                session_name = session_details.get('Name', 'Unknown Session')

                circuit_name = session_details.get('Circuit', {}).get('ShortName', 'Unknown Circuit') # Or 'OfficialName'

                start_time_str = session_details.get('StartDate') # Already a string? Format if needed

                country_name = session_details.get('Country', {}).get('Name', '')

                session_info_parts = []
                if circuit_name != 'Unknown Circuit': session_info_parts.append(f"{circuit_name}")
                if country_name: session_info_parts.append(f"({country_name})")
                session_info_parts.append(f"Event: {meeting_name}")
                session_info_parts.append(f"Session: {session_name}")
                if start_time_str: session_info_parts.append(f"Starts: {start_time_str}") # Format this date/time nicer?
                session_info_str = " | ".join(session_info_parts) if                         session_info_parts else "Session: N/A"

                # --- Format Weather Details (using CORRECT PascalCase keys) ---
                air_temp = weather_data.get('AirTemp')         # Corrected key
                track_temp = weather_data.get('TrackTemp')       # Corrected key
                humidity = weather_data.get('Humidity')       # Corrected key
                pressure = weather_data.get('Pressure')       # Corrected key
                wind_speed = weather_data.get('WindSpeed')     # Corrected key
                wind_dir = weather_data.get('WindDirection') # Corrected key
                rainfall = weather_data.get('Rainfall')       # Corrected key

                if air_temp is not None: weather_elements.append(f"Air: {air_temp}°C")
                if track_temp is not None: weather_elements.append(f"Track: {track_temp}°C")
                if humidity is not None: weather_elements.append(f"Hum: {humidity}%")
                if pressure is not None: weather_elements.append(f"Press: {pressure} hPa") # Changed unit assumption
                if wind_speed is not None:
                     wind_str = f"Wind: {wind_speed} m/s" # Changed unit assumption (often m/s)
                     if wind_dir is not None: wind_str += f" ({wind_dir}°)"
                     weather_elements.append(wind_str)
                if rainfall is not None and str(rainfall) == '1': # Check for '1' string (or adjust if it's 0/1 int)
                     weather_elements.append("RAIN")

                weather_string = " | ".join(weather_elements) if weather_elements else "Weather: N/A"

                # --- Combine Output ---
                combined_info = dbc.Row([
                     dbc.Col(session_info_str, width="auto", style={'paddingRight': '15px'}),
                     dbc.Col(weather_string, width="auto")
                ], justify="start", className="ms-1")

                return combined_info

        except Exception as e:
            main_logger.error(f"Error in update_session_info_display callback: {e}", exc_info=True)
            return "Error loading session info..."


@app.callback(Output('status-display', 'children', allow_duplicate=True), Input('start-button', 'n_clicks'), prevent_initial_call=True)
def start_live_callback(n_clicks):
    global connection_thread # Manage the global thread variable

    # Check if already running
    with app_state.app_state_lock:
         current_state = app_state.app_status["state"]
         if current_state in ["Connecting", "Live", "Replaying"]:
             main_logger.warning(f"Start Live clicked but already active (State: {current_state}).")
             # Return current status without starting again
             return f"Status: {current_state}", app_state.app_status.get("connection", "N/A")

    main_logger.info(f"Start Live clicked (n={n_clicks}). Initiating connection sequence...")

    # --- Manual Negotiation ---
    negotiate_cookie = None
    connection_token = None
    websocket_url = None
    ws_headers = None

    try:
        # Set initial state
        with app_state.app_state_lock:
             app_state.app_status.update({"state": "Initializing", "connection": "Negotiating..."})
             # Reset stop event for new connection attempt
             stop_event.clear()

        # Perform Negotiation
        connection_data = json.dumps([{"name": HUB_NAME}])
        params = { "connectionData": connection_data, "clientProtocol": "1.5" }
        negotiate_url_full = f"{NEGOTIATE_URL_BASE}/negotiate?{urllib.parse.urlencode(params)}"
        main_logger.info(f"Negotiating connection: {negotiate_url_full}")
        negotiate_headers = {"User-Agent": "Python Requests"}
        response = requests.get(negotiate_url_full, headers=negotiate_headers, verify=True, timeout=15)
        main_logger.info(f"Negotiate status: {response.status_code}")
        response.raise_for_status()

        # Extract Cookie
        if response.cookies:
             cookie_list = [f"{c.name}={c.value}" for c in response.cookies]
             negotiate_cookie = "; ".join(cookie_list)
             main_logger.info(f"Got negotiation cookie(s): {negotiate_cookie}")
        else:
             main_logger.warning("No cookie found in negotiate response.")

        # Extract Token
        neg_data = response.json()
        if "ConnectionToken" in neg_data:
            connection_token = neg_data["ConnectionToken"]
            main_logger.info("Got connection token.")
        else:
            raise HubConnectionError("Negotiation response missing ConnectionToken.")

        # Build WebSocket URL & Headers (Match F1 Docs + Cookie logic)
        ws_params = {
            "clientProtocol": "1.5",
            "transport": "webSockets",
            "connectionToken": connection_token,
            "connectionData": connection_data
        }
        websocket_url = f"{WEBSOCKET_URL_BASE}/connect?{urllib.parse.urlencode(ws_params)}"
        main_logger.info(f"Constructed WebSocket URL: {websocket_url}")

        # Headers required by F1 endpoint + Cookie for builder options
        ws_headers = {
            "User-Agent": "BestHTTP",
            "Accept-Encoding": "gzip, identity"
        }
        if negotiate_cookie:
             ws_headers["Cookie"] = negotiate_cookie # Will be passed via builder options
        main_logger.info(f"Will use WebSocket headers in builder options: {ws_headers}")

    except requests.exceptions.RequestException as req_ex:
        main_logger.error(f"HTTP Negotiation failed: {req_ex}", exc_info=True)
        with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Negotiation Failed"})
        return "Status: Error", "Negotiation Failed!" # Update status outputs
    except Exception as e:
        main_logger.error(f"Error during negotiation/setup: {e}", exc_info=True)
        with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Setup Error"})
        return "Status: Error", "Setup Error!" # Update status outputs

    # --- Start Connection Thread (Only if Negotiation Succeeded) ---
    if websocket_url and ws_headers:
        # Initialize DB/File
        # Ensure these functions correctly update app_state.db_conn / app_state.live_data_file
        if not init_live_file(): # Assuming uses app_state.data_filename or similar
             # ... error handling ...
             close_live_file()
             return "Status: Error", "File Error!"

        # Start the thread
        main_logger.info("Starting connection thread (manual neg)...")
        connection_thread = threading.Thread(
            target=run_connection_manual_neg,
            args=(websocket_url, ws_headers), # Pass URL and Headers
            name="SignalRConnectionThread",
            daemon=True)
        connection_thread.start()
        main_logger.info("Connection thread started.")
        # Return status updates for UI
        return "Status: Connecting", "Connecting..."
    else:
         main_logger.error("Cannot start connection thread: URL or Headers missing.")
         with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": "Internal Setup Error"})
         return "Status: Error", "Internal Error!"


@app.callback(Output('status-display', 'children', allow_duplicate=True), Input('stop-button', 'n_clicks'), prevent_initial_call=True)
def handle_stop_button(n_clicks):
    if n_clicks is None or n_clicks == 0: return dash.no_update
    main_logger.info(f"Stop clicked (n={n_clicks}).")
    triggered_stop = False; current_state_on_click = "Unknown"
    with app_state.app_state_lock: state = app_state.app_status['state']; current_state_on_click = state
    if state in ["Replaying", "Initializing"]: stop_replay(); triggered_stop = True
    elif state in ["Live", "Connecting"]: stop_connection(); triggered_stop = True
    elif state == "Stopping": main_logger.info("Stop clicked while stopping.")
    else: main_logger.warning(f"Stop clicked in state {state}. Set stop event.");
    if not stop_event.is_set(): stop_event.set()
    with app_state.app_state_lock: new_state = app_state.app_status['state']; new_conn = app_state.app_status['connection']
    if triggered_stop: return f"Stop req from '{current_state_on_click}'. New: {new_state} | {new_conn}"
    else: return f"Stop ignored ('{current_state_on_click}'). Current: {new_state} | {new_conn}"

@app.callback(Output('status-display', 'children', allow_duplicate=True), Input('replay-button', 'n_clicks'), State('replay-file-dropdown', 'value'), State('replay-speed-input', 'value'), prevent_initial_call=True)
def handle_replay_button(n_clicks, selected_file, speed_val):
    if n_clicks is None or n_clicks == 0: return dash.no_update
    if not selected_file:
        main_logger.warning("Replay button clicked but no file selected from dropdown.")
        # Optionally provide user feedback via another output or return existing status
        with app_state.app_state_lock:
             # Return current status without attempting replay
             return f"State: {app_state.app_status['state']} | Conn: {app_state.app_status['connection']} (No replay file selected)"
        # return dash.no_update # Alternative: do nothing visibly

    main_logger.info(f"Replay clicked (n={n_clicks}) for file: {selected_file}.")
    try: speed = float(speed_val) if speed_val is not None else 1.0; speed = max(0, speed)
    except: main_logger.warning(f"Invalid speed '{speed_val}', default 1.0"); speed = 1.0
    replay_file_path = os.path.join(REPLAY_DIR, selected_file)
    main_logger.info(f"Attempting replay for: {replay_file_path}, speed: {speed}")

    if not os.path.exists(replay_file_path):
        main_logger.error(f"Replay file not found: {replay_file_path}")
        with app_state.app_state_lock:
            # Update status display to show the error
            app_state.app_status.update({"state": "Error", "connection": f"File Not Found"})
            return f"State: Error | Conn: File Not Found: {selected_file}"

    # --- Call replay function (Keep existing logic, but use constructed path) ---
    replay_from_file(replay_file_path, speed) # Use the constructed full path

    # --- Return updated status (Keep existing logic) ---
    with app_state.app_state_lock:
        return f"State: {app_state.app_status['state']} | Conn: {app_state.app_status['connection']}"
    
@app.callback(
    Output('track-map-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_track_map(n):
    global timing_state, session_details, track_coordinates_cache # Keep necessary globals

    # --- Step 1: Determine Session Key and Check Cache Match (Read-only, lock optional but safer) ---
    current_session_key = None
    needs_api_fetch = False
    with app_state.app_state_lock:
        year = session_details.get('Year')
        circuit_key = session_details.get('CircuitKey')
        current_session_key = f"{year}_{circuit_key}" if year and circuit_key else None
        cached_session_key = track_coordinates_cache.get('session_key')

    if current_session_key and cached_session_key != current_session_key:
        needs_api_fetch = True
        main_logger.debug(f"Map Check: Cache miss/new session. Need API fetch for {current_session_key}.")
    elif not current_session_key:
        main_logger.debug("Map Check: No session key.")
        # Clear cache if session becomes invalid later? (Handled in Step 3)
    else:
        main_logger.debug(f"Map Check: Cache hit for {current_session_key}.")


    # --- Step 2: Fetch API data if needed (OUTSIDE lock) ---
    api_data = None # Holds extracted data from API if successful
    if needs_api_fetch:
        api_url = f"https://api.multiviewer.app/api/v1/circuits/{circuit_key}/{year}"
        main_logger.info(f"Attempting API fetch: {api_url}")
        try:
            headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' }
            response = requests.get(api_url, headers=headers, timeout=10)
            response.raise_for_status()
            map_api_data = response.json()

            # Extract data into a temporary dictionary on success
            extracted_data = {}
            temp_track_x = map_api_data.get('x'); temp_track_y = map_api_data.get('y')
            if isinstance(temp_track_x, list) and isinstance(temp_track_y, list):
                 extracted_data['x'] = [float(p) for p in temp_track_x]
                 extracted_data['y'] = [float(p) for p in temp_track_y]
            # Extract corners (add robustness)
            corners_raw = map_api_data.get('corners')
            if isinstance(corners_raw, list):
                 corner_x_temp, corner_y_temp = [], []
                 for corner in corners_raw:
                      if isinstance(corner, dict):
                           pos = corner.get('trackPosition', {})
                           cx, cy = pos.get('x'), pos.get('y')
                           if cx is not None and cy is not None:
                                try:
                                     corner_x_temp.append(float(cx)); corner_y_temp.append(float(cy))
                                except (ValueError, TypeError): pass # Ignore bad corner coords
                 if corner_x_temp:
                      extracted_data['corner_x'] = corner_x_temp; extracted_data['corner_y'] = corner_y_temp
            # Extract rotation
            try: extracted_data['rotation'] = float(map_api_data.get('rotation', 0.0))
            except (ValueError, TypeError): extracted_data['rotation'] = 0.0

            # Calculate ranges only if coordinates are valid
            local_x_range, local_y_range = None, None
            if 'x' in extracted_data and 'y' in extracted_data:
                try:
                    x_min, x_max = np.min(extracted_data['x']), np.max(extracted_data['x'])
                    y_min, y_max = np.min(extracted_data['y']), np.max(extracted_data['y'])
                    padding_x = (x_max - x_min) * 0.05; padding_y = (y_max - y_min) * 0.05
                    local_x_range = [x_min - padding_x, x_max + padding_x]
                    local_y_range = [y_min - padding_y, y_max + padding_y]
                except Exception as range_err: main_logger.error(f"Error calculating range: {range_err}")
            extracted_data['range_x'] = local_x_range; extracted_data['range_y'] = local_y_range

            api_data = extracted_data # Assign successful extraction to api_data
            main_logger.info(f"API fetch SUCCESS for {current_session_key}")

        except Exception as e:
            main_logger.error(f"API fetch FAILED for {current_session_key}: {e}", exc_info=False)
            api_data = None # Ensure it's None on failure

    # --- Step 3: Acquire lock ONCE to update cache & read ALL data for plot ---
    track_x, track_y, x_range, y_range = None, None, None, None
    drivers_x_raw, drivers_y_raw, drivers_text, drivers_color, drivers_opacity = [], [], [], [], []
    plot_session_key = None # Store the key used for this plot's data

    with app_state.app_state_lock:
        # A. Update cache if fetch was attempted
        if needs_api_fetch:
            if api_data: # API call succeeded and data extracted
                track_coordinates_cache = {**api_data, 'session_key': current_session_key} # Combine extracted data and key
                main_logger.debug("Cache updated with NEW API data.")
            else: # API call failed or extraction failed
                # Update key but clear coordinates/ranges to prevent using stale data
                track_coordinates_cache = {
                    'x': None, 'y': None, 'corner_x': None, 'corner_y': None,
                    'range_x': None, 'range_y': None, 'rotation': 0,
                    'session_key': current_session_key
                }
                main_logger.debug("Cache updated for FAILED API fetch (cleared coords).")

        # B. Read data for plotting from current state (cache and timing_state)
        plot_session_key = track_coordinates_cache.get('session_key') # Use the key actually in cache now
        if plot_session_key == current_session_key: # Check key match again *after* potential update
            track_x = track_coordinates_cache.get('x')
            track_y = track_coordinates_cache.get('y')
            x_range = track_coordinates_cache.get('range_x')
            y_range = track_coordinates_cache.get('range_y')
            # rotation_angle = track_coordinates_cache.get('rotation', 0) # Read if needed

        # Read car positions (always read latest state)
        for car_num, driver_state in timing_state.items():
            pos_data = driver_state.get('PositionData')
            status_string = driver_state.get('Status', '').lower()
            is_off_main_track = ('pit' in status_string or 'retired' in status_string or 'out' in status_string)
            if pos_data and 'X' in pos_data and 'Y' in pos_data:
                 try: # Add try-except for coordinate conversion
                     x_coord = float(pos_data['X'])
                     y_coord = float(pos_data['Y'])
                     drivers_x_raw.append(x_coord)
                     drivers_y_raw.append(y_coord)
                     drivers_text.append("" if is_off_main_track else driver_state.get('Tla', car_num))
                     drivers_color.append(f"#{driver_state.get('TeamColour', 'FFFFFF')}")
                     drivers_opacity.append(0.15 if is_off_main_track else 1.0) # Keep dim opacity for now
                 except (ValueError, TypeError) as coord_err:
                      main_logger.warning(f"Could not convert position data for car {car_num}: X={pos_data['X']}, Y={pos_data['Y']} - Error: {coord_err}")

    # --- Step 4: Create Plotly Figure (Outside lock) ---
    figure_data = []
    main_logger.debug(f"Plotting: Key='{plot_session_key}', Has track={track_x is not None}, Has cars={len(drivers_x_raw)>0}, x_range={x_range is not None}")

    try:
        # Add Track Outline Trace
        if track_x is not None and track_y is not None:
            figure_data.append(go.Scatter(x=track_x, y=track_y, mode='lines', line=dict(color='grey', width=2), name='Track', showlegend=False, hoverinfo='none'))

        # Add Car Trace
        if drivers_x_raw: # Check if we actually have car data to plot
             figure_data.append(go.Scatter(
                 x=drivers_x_raw, y=drivers_y_raw, mode='markers+text',
                 marker=dict(size=10, color=drivers_color, line=dict(width=1, color='Black'), opacity=drivers_opacity),
                 text=drivers_text, textposition='middle right', name='Cars', hoverinfo='text', showlegend=False,
                 textfont=dict(size=9, color='White')
             ))

        # Define Layout
        layout = go.Layout(
            xaxis=dict(range=x_range, showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(range=y_range, showgrid=False, zeroline=False, showticklabels=False, scaleanchor="x", scaleratio=1),
            showlegend=False, margin=dict(l=5, r=5, t=5, b=5),
            uirevision=plot_session_key, # Use the key consistent with the data being plotted
            plot_bgcolor='rgb(50,50,50)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color='white')
        )

        # Create Final Figure
        if not figure_data and not (track_x is not None and track_y is not None): # Check if we have neither track nor cars
             empty_title = "Waiting for Session Info..." if not current_session_key else "Fetching/Waiting for Track Data..."
             final_figure = go.Figure(data=[], layout=go.Layout(title=empty_title, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='white')))
        else: # We have at least track data or car data (or both)
             final_figure = go.Figure(data=figure_data, layout=layout)

    except Exception as fig_err:
         main_logger.error(f"!!! Error creating Plotly figure: {fig_err}", exc_info=True)
         error_layout = go.Layout(title=f"Error creating map: {fig_err}", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='red'))
         final_figure = go.Figure(data=[], layout=error_layout)

    main_logger.debug(f"--- update_track_map Tick {n} End ---")
    return final_figure

# --- Main Execution Logic ---
if __name__ == '__main__':
    main_logger.info("Application starting...")
    stop_event.clear()
    try:
        dash_logger = logging.getLogger('werkzeug')
        dash_logger.setLevel(logging.ERROR)
        #dash_logger.disabled
        main_logger.info("Set Werkzeug log level to ERROR.")
    except Exception as e_werkzeug:
        main_logger.error(f"Failed to configure Werkzeug logger: {e_werkzeug}")
    try:
        # Make sure 'app' is your Dash app object
        if app and hasattr(app, 'logger'):
             app.logger.disabled = True
             main_logger.info("Attempted to disable Flask app logger.")
        else:
             main_logger.warning("Flask app object 'app' not found or has no logger attribute here.")
    except Exception as e_flask:
        main_logger.error(f"Failed to disable Flask app logger: {e_flask}")
    # --- END ADDED ---
    def run_dash():
        main_logger.info("Dash thread started.")
        try:
            app.run(debug=False, host='0.0.0.0', port=8050, use_reloader=False)
            main_logger.info("Dash server stopped.")
        except SystemExit:
             main_logger.info("Dash server exited.")
        except Exception as e:
             main_logger.error(f"Dash server failed: {e}", exc_info=True)
        finally:
             main_logger.info("Dash thread finishing.")
             if not stop_event.is_set():
                 main_logger.info("Dash thread set stop event.")
                 stop_event.set()
    dash_thread = threading.Thread(target=run_dash, name="DashThread", daemon=True)
    dash_thread.start()
    main_logger.info("Dash server starting on http://localhost:8050")
    
    # --- ADDED: Start Data Processing Thread ---
    stop_event.clear()
    processing_thread = threading.Thread(target=data_processing_loop, name="DataProcessingThread", daemon=True)
    processing_thread.start()
    main_logger.info("Data processing thread started.")
    # --- END ADDED ---
    
    try:
        while not stop_event.is_set():
            with app_state.app_state_lock:
                current_state=app_state.app_status["state"]
                conn_thread_obj=connection_thread
                replay_thread_obj=replay_thread
                proc_thread_obj=processing_thread
            conn_active = conn_thread_obj and conn_thread_obj.is_alive()
            replay_active = replay_thread_obj and replay_thread_obj.is_alive()
            proc_active = proc_thread_obj and proc_thread_obj.is_alive()
            # Check thread states
            if current_state in ["Connecting", "Live"] and not conn_active:
                 main_logger.warning("Conn thread died. Stopping.")
                 if not stop_event.is_set():
                     stop_event.set()
                 with app_state.app_state_lock:
                     if app_state.app_status["state"] not in ["Error", "Stopped"]:
                         app_state.app_status.update({"state": "Error", "connection": "Conn Thread Died"})
                 break

            if current_state == "Replaying" and not replay_active:
                 main_logger.warning("Replay thread died.")
                 if globals().get('replay_thread') is replay_thread_obj:
                     globals()['replay_thread'] = None
                 with app_state.app_state_lock:
                     if app_state.app_status["state"] == "Replaying":
                          app_state.app_status["state"] = "Stopped" # Or Error
                          
            # --- ADDED: Check Processing Thread ---
            if not proc_active:
                 # Should this stop the app? Depends on if it can restart...
                 # For now, just log it and maybe stop everything if it dies unexpectedly
                 main_logger.error("FATAL: Data Processing thread died! Stopping application.")
                 if not stop_event.is_set(): stop_event.set()
                 break # Exit main loop
            # --- END ADDED ---

            if not dash_thread.is_alive():
                 main_logger.warning("Dash thread died. Stopping.")
                 if not stop_event.is_set():
                     stop_event.set()
                 break

            time.sleep(1) # Check periodically

        main_logger.info("Main loop exiting.")
    except (KeyboardInterrupt, SystemExit):
        main_logger.info("Exit signal received. Shutting down...")
        if not stop_event.is_set():
             stop_event.set() # Ensure stop event is set for cleanup
    except Exception as e:
        main_logger.error(f"Main loop error: {e}", exc_info=True)
        if not stop_event.is_set():
             stop_event.set() # Ensure stop event is set on unexpected error
    finally:
        main_logger.info("Final cleanup...")
        if not stop_event.is_set():
            main_logger.info("Setting stop event (cleanup).")
            stop_event.set()
        with app_state.app_state_lock:
            current_state = app_state.app_status["state"]
            conn_thread_obj = connection_thread
            replay_thread_obj = replay_thread
            dash_thread_obj = dash_thread
        if current_state in ["Live", "Connecting", "Stopping"] or (conn_thread_obj and conn_thread_obj.is_alive()):
            main_logger.info("Cleanup: Stopping connection...")
            stop_connection()
        if current_state in ["Replaying", "Stopping"] or (replay_thread_obj and replay_thread_obj.is_alive()):
            main_logger.info("Cleanup: Stopping replay...")
            stop_replay()
            close_live_file()
        if processing_thread and processing_thread.is_alive():
            main_logger.info("Waiting for Data Processing thread...")
            processing_thread.join(timeout=3) # Wait max 3 seconds
            if processing_thread.is_alive():
                main_logger.warning("Data Processing thread did not exit cleanly.")
        #if dash_thread and dash_thread.is_alive():
#            main_logger.info("Waiting for Dash thread...")
#            dash_thread.join(timeout=3)
#            if dash_thread.is_alive():
#                main_logger.warning("Dash thread didn't exit cleanly.")
        local_live_data_file = globals().get('app_state.live_data_file') # Use local var for check
        if local_live_data_file and not local_live_data_file.closed:
             main_logger.warning("Live file open cleanup. Closing.")
             try:
                 local_live_data_file.close()
             except Exception as e:
                 main_logger.error(f"Err closing live file: {e}")
             globals()['app_state.live_data_file'] = None # Clear global reference
        main_logger.info("Shutdown complete.")
        print("\n --- App Exited --- \n")
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
import sqlite3
import numpy as np
import plotly.graph_objects as go # Make sure this is imported at the top

# Use standard requests library for sync negotiation
import requests

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
STREAMS_TO_SUBSCRIBE = [
    "Heartbeat", "CarData.z", "Position.z", "ExtrapolatedClock",
    "TimingAppData", "TimingData", "TimingStats", "TrackStatus",
    "SessionData", "DriverList", "RaceControlMessages", "SessionInfo"
]
DATA_FILENAME_TEMPLATE = "f1_signalr_data_{timestamp}.data.txt"
DATABASE_FILENAME_TEMPLATE = "f1_signalr_data_{timestamp}.db"
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
stop_event = threading.Event()
data_queue = queue.Queue()
data_store = {}
timing_state = {} # Holds persistent timing state per driver
track_status_data = {} # To store TrackStatus info (Status, Message)
session_details = {} # To store SessionInfo/SessionData details
track_coordinates_cache = {'x': None, 'y': None, 'range_x': None, 'range_y': None, 'rotation': None, 'corner_x': None, 'corner_y': None, 'session_key': None} # Expanded cache
app_status = {"state": "Idle", "connection": "Disconnected", "subscribed_streams": [], "last_heartbeat": None}
app_state_lock = threading.Lock()
live_data_file = None
db_conn = None
db_cursor = None
db_lock = threading.Lock()

# --- F1 Helper Functions ---
def get_latest_f1_session(session_type='Race'):
    main_logger.info(f"Fetching latest F1 session info for type: {session_type}")
    try:
        fastf1.Cache.enable_cache('fastf1_cache')
        schedule = fastf1.get_event_schedule(datetime.datetime.now().year, include_testing=False)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        past_events = schedule[schedule['EventDate'] <= now_utc]
        if past_events.empty:
            main_logger.warning("No past events found.")
            return None
        latest_event = past_events.iloc[-1]
        session = latest_event.get_session(session_type)
        main_logger.info(f"Latest session: {session.event['EventName']} - {session.name}")
        return session
    except Exception as e:
        main_logger.error(f"Error fetching F1 session: {e}", exc_info=True)
        return None

# --- Data Handling ---
def _decode_and_decompress(message):
    """Decodes and decompresses SignalR messages ending in .z"""
    if message and isinstance(message, str): # No need to check for .z here, handle_message does that
        try:
            # The data might already be stripped of '.z' by handle_message
            encoded_data = message
            # Add padding if necessary
            missing_padding = len(encoded_data) % 4
            if missing_padding:
                encoded_data += '=' * (4 - missing_padding)
            decoded_data = base64.b64decode(encoded_data)
            # Use -zlib.MAX_WBITS for raw deflate data
            return json.loads(zlib.decompress(decoded_data, -zlib.MAX_WBITS))
        except json.JSONDecodeError as e:
             main_logger.error(f"JSON decode error after decompression: {e}. Data sample: {decoded_data[:100]}...", exc_info=False)
             return None # Return None on JSON error
        except Exception as e:
            # Catch other potential errors like incorrect padding, zlib errors
            main_logger.error(f"Decode/Decompress error: {e}. Data: {message[:50]}...", exc_info=False) # Log only start of data
            return None # Return None on error
    # Return non-string messages as-is (though handle_message primarily sends strings needing decode)
    return message

def handle_message(message_data):
    """
    Handles incoming SignalR messages.
    Processes standard lists ["StreamName", Data, Timestamp]
    and unpacks the initial snapshot dictionary {"R": {StreamName: Data, ...}}.
    Puts individual streams onto the data_queue.
    """
    main_logger.debug(f"Received message type: {type(message_data)}")

    # --- Case 1: Initial Snapshot Dictionary {"R": {...}} ---
    if isinstance(message_data, dict) and "R" in message_data:
        main_logger.info("Processing initial snapshot message (R: block)...")
        snapshot_data = message_data.get("R", {})
        if not isinstance(snapshot_data, dict):
            main_logger.warning(f"Snapshot block 'R' contained non-dict data: {type(snapshot_data)}")
            return

        # Try to get a consistent timestamp for all streams in this snapshot
        # Often a Heartbeat is included, use its timestamp if possible
        snapshot_ts = snapshot_data.get("Heartbeat", {}).get("Utc")
        if not snapshot_ts: # Fallback to current time
             snapshot_ts = datetime.datetime.utcnow().isoformat() + 'Z'
             main_logger.debug("Using current time as timestamp for snapshot block.")
        else:
             main_logger.debug(f"Using Heartbeat timestamp for snapshot block: {snapshot_ts}")


        # Iterate through streams within the 'R' block
        known_streams = set(STREAMS_TO_SUBSCRIBE) # Keep this set
        for stream_name_raw, stream_data in snapshot_data.items():
            stream_name = stream_name_raw
            actual_data = stream_data
            stream_name_no_z = stream_name_raw # Name without .z for queueing

            # Decompress if needed (handle keys like "CarData.z")
            if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                stream_name_no_z = stream_name_raw[:-2]
                actual_data = _decode_and_decompress(stream_data)
                if actual_data is None:
                    main_logger.warning(f"Failed to decompress {stream_name_raw} from snapshot block.")
                    continue # Skip this stream if decompression fails

            # Check if the key looks like a stream we care about
            # --- CORRECTED CHECK ---
            # Check if EITHER the raw name (e.g., Position.z) OR the name without .z (e.g., Position)
            # is in the list of streams we originally subscribed to.
            # Also ensure the data wasn't None after potential decompression.
            if actual_data is not None and (stream_name_raw in known_streams or stream_name_no_z in known_streams):
                 main_logger.debug(f"Queueing stream from snapshot: {stream_name_no_z}")
                 # Use the name WITHOUT .z when putting onto the queue
                 data_queue.put({"stream": stream_name_no_z, "data": actual_data, "timestamp": snapshot_ts})
            # --- END CORRECTED CHECK ---
            # Don't need the else block logging ignored keys anymore if the check is correct
            # else:
            #      main_logger.debug(f"Ignoring non-stream key '{stream_name_raw}' within snapshot block.")

    # --- Case 2: Standard Message List ["StreamName", Data, Timestamp] ---
    elif isinstance(message_data, list) and len(message_data) >= 2:
        stream_name_raw = message_data[0]
        data = message_data[1]
        # Use provided timestamp if available, else generate one
        timestamp = message_data[2] if len(message_data) > 2 else datetime.datetime.utcnow().isoformat() + 'Z'

        stream_name = stream_name_raw
        actual_data = data
        # Decompress if needed
        if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
            stream_name = stream_name_raw[:-2]
            actual_data = _decode_and_decompress(data)
            if actual_data is None:
                main_logger.warning(f"Decompress failed for stream {stream_name_raw}. Skipping.")
                return # Skip if decompression fails

        if actual_data is not None:
             main_logger.debug(f"Queueing standard stream: {stream_name}")
             data_queue.put({"stream": stream_name, "data": actual_data, "timestamp": timestamp})
        else:
             main_logger.warning(f"Data was None for standard stream {stream_name}. Skipping.")

    # --- Case 3: Other formats (e.g., keep-alive messages, unexpected structures) ---
    else:
        # Ignore keep-alive messages (often empty dicts {}) or log unexpected formats
        if isinstance(message_data, dict) and not message_data:
            main_logger.debug("Ignoring empty keep-alive message.")
        else:
            main_logger.warning(f"Received unexpected message format: {type(message_data)} - {str(message_data)[:100]}")

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

        main_logger.info(f"Processed DriverList message ({processed_count} entries). Added: {added_count}, Updated: {updated_count}. Total drivers now: {len(timing_state)}")
    else:
        main_logger.warning(f"Unexpected DriverList stream data format: {type(data)}. Cannot process.")

def _process_timing_data(data):
    """ Helper function to process TimingData stream data """
    global timing_state
    if not timing_state:
        main_logger.debug("TimingData received before DriverList processed, skipping.")
        return # Cannot process without initialized timing_state

    if isinstance(data, dict) and 'Lines' in data and isinstance(data['Lines'], dict):
        for car_num_str, line_data in data['Lines'].items():
            driver_current_state = timing_state.get(car_num_str)
            # Process only if driver exists in state and line_data is a dict
            if driver_current_state and isinstance(line_data, dict):
                 # Update direct fields
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
                             main_logger.debug(f"Stored non-dict {key} value '{incoming_value}' into ['{sub_key}'] for {car_num_str}")

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
                              main_logger.debug(f"Stored non-dict Sector {sector_idx} value '{sector_data}' into ['Value'] for {car_num_str}")

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
                 if driver_current_state.get("PitOut"): status_flags.append("Pit Out")
                 if status_flags:
                      driver_current_state["Status"] = ", ".join(status_flags)
                 elif driver_current_state.get("Position", "-") != "-": # If has position and no flags, assume On Track
                      driver_current_state["Status"] = "On Track"
                 # else: keep existing status ("On Track" default from DriverList or previous)

            elif not driver_current_state:
                main_logger.debug(f"TimingData for driver {car_num_str} received, but driver not yet in timing_state. Data skipped.")
    elif data: # Log if TimingData is not the expected dict structure but not None/empty
         main_logger.warning(f"Unexpected TimingData format received: {type(data)}")

def _process_track_status(data):
    """Handles TrackStatus data. MUST be called within app_state_lock."""
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
    """Handles Position data. MUST be called within app_state_lock."""
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
                main_logger.debug(f"Updated Position for {car_number_str}: X={x_pos}, Y={y_pos}")

def _process_car_data(data):
    """Handles CarData. MUST be called within app_state_lock."""
    global timing_state # Use timing_state as the main driver data store

    if 'timing_state' not in globals():
         main_logger.error("Global 'timing_state' not found for CarData processing.")
         return
    
    # --- Define Channel Mapping for THIS data source ---
    # Based on observation of 2023 Yas Marina replay data (Nov 25th 2023)
    # !!! This map might need changing for LIVE feeds or other replays !!!
    channel_map = {
        '0': 'RPM',       # Channel 0 seems to be RPM here
        '2': 'Speed',     # Channel 2 seems to be Speed (km/h) here
        '3': 'Gear',      # Channel 3 is Gear
        '4': 'Throttle',  # Channel 4 is Throttle (%)
        '5': 'Brake',     # Channel 5 is Brake (binary?)
        '45': 'DRS'       # Channel 45 is DRS status
    }
    # --- You might need a different map for live data, e.g.: ---
    # channel_map_live = {
    #     '0': 'Speed', '2': 'RPM', '3': 'Gear', '4': 'Throttle', '5': 'Brake', '45': 'DRS'
    # }
    # ---
    
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
        MUST be called within app_state_lock.
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

        main_logger.debug(f"Processed SessionData. Current details: {session_details}")

    except Exception as e:
        main_logger.error(f"Error processing SessionData: {e}", exc_info=True)

def _process_session_info(data):
    """ Processes SessionInfo data and stores it in the global session_details dict.
        MUST be called within app_state_lock.
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
        main_logger.debug(f"Full SessionInfo details stored: {session_details}")

    except Exception as e:
        main_logger.error(f"Error processing SessionInfo data: {e}", exc_info=True)

# Helper function for rotation (define globally or ensure accessible)
def rotate_coords(x, y, angle_deg):
    """Rotates points (x, y) by angle_deg degrees."""
    angle_rad = np.radians(angle_deg)
    x = np.array(x)
    y = np.array(y)
    x_rotated = x * np.cos(angle_rad) - y * np.sin(angle_rad)
    y_rotated = x * np.sin(angle_rad) + y * np.cos(angle_rad)
    return x_rotated, y_rotated

def process_data_queue():
    global data_store, app_status, live_data_file, db_cursor, timing_state # No fastf1 map needed
    processed_count = 0
    max_process = 100

    while not data_queue.empty() and processed_count < max_process:
        processed_count += 1
        item = None # Initialize item to None for this iteration
        try:
            item = data_queue.get_nowait()
            # Expect item = {"stream": stream_name, "data": data, "timestamp": timestamp}
            # OR item = {"stream": None, "data": {"R":{...}}, "timestamp": timestamp} for snapshot

            timestamp = item['timestamp']
            top_level_data = item['data']
            top_level_stream_name = item['stream']
            streams_to_process_this_item = {} # Initialize dictionary for this item

            # --- Check for initial snapshot structure 'R' ---
            if isinstance(top_level_data, dict) and 'R' in top_level_data:
                 main_logger.info("Processing initial snapshot message block (R:)...")
                 initial_snapshot_data = top_level_data.get('R', {})
                 if isinstance(initial_snapshot_data, dict):
                     # Extract streams from within the 'R' dictionary
                     for stream_name_key, stream_data_value in initial_snapshot_data.items():
                          streams_to_process_this_item[stream_name_key] = stream_data_value # Store raw data
                 else:
                     main_logger.warning(f"Snapshot block 'R' contained non-dict data: {type(initial_snapshot_data)}")
                     data_queue.task_done() # Mark done even if skipped
                     continue

            # --- Check for normal message structure 'M' ---
            elif isinstance(top_level_data, dict) and 'M' in top_level_data:
                 main_logger.debug("Processing standard message block (M:)...")
                 if isinstance(top_level_data['M'], list):
                     for msg_container in top_level_data['M']:
                          if isinstance(msg_container, dict) and msg_container.get("M") == "feed":
                              msg_args = msg_container.get("A")
                              if isinstance(msg_args, list) and len(msg_args) >= 2:
                                   stream_name = msg_args[0]
                                   stream_data = msg_args[1]
                                   # Allow individual message timestamp override if present
                                   if len(msg_args) > 2: timestamp = msg_args[2]
                                   streams_to_process_this_item[stream_name] = stream_data
            # --- Handle case where the queued item IS the direct stream data ---
            elif top_level_stream_name:
                 main_logger.debug(f"Processing direct stream message for: {top_level_stream_name}")
                 streams_to_process_this_item[top_level_stream_name] = top_level_data

            else:
                 # Unexpected structure
                 main_logger.warning(f"Skipping queue item with unexpected structure: stream={top_level_stream_name}, data_type={type(top_level_data)}")
                 data_queue.task_done() # Mark done even if skipped
                 continue

            # --- If no streams were extracted, skip further processing for this item ---
            if not streams_to_process_this_item:
                main_logger.debug("No processable streams found in the queue item.")
                data_queue.task_done()
                continue

            # --- Loop through streams found in the message ---
            processed_data_for_saving = {} # Store processed/decompressed data for saving
            for stream_name_raw, stream_data in streams_to_process_this_item.items():
                stream_name = stream_name_raw
                actual_data = stream_data

                # Decompress if needed
                if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                     stream_name = stream_name_raw[:-2]
                     actual_data = _decode_and_decompress(stream_data)
                     if actual_data is None:
                          main_logger.warning(f"Failed to decompress {stream_name_raw}.")
                          continue # Skip processing this specific stream

                processed_data_for_saving[stream_name] = actual_data # Store for saving later

                # --- Process individual streams (Update state) ---
                with app_state_lock: # Lock for timing_state and data_store updates
                    data_store[stream_name] = {"data": actual_data, "timestamp": timestamp}

                    # Specific stream handlers
                    if stream_name == "Heartbeat":
                        app_status["last_heartbeat"] = timestamp
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
                    elif stream_name == "TrackStatus":
                         _process_track_status(actual_data) # Call new handler
                    elif stream_name == "CarData":
                         _process_car_data(actual_data) # Call new handler
                    elif stream_name == "Position":
                        _process_position_data(actual_data) # Call the position handler
                    # --- END ADDED Stream Handlers ---
                    # Add other handlers here (WeatherData, TimingStats, etc.)
                    # elif stream_name == "WeatherData":
                    #    _process_weather_data(actual_data) # Need to create this function
                    else:
                        # Optional: Log if you want to know about unhandled streams that made it this far
                        main_logger.debug(f"No specific handler for stream: {stream_name}")

            # --- Database/File saving (Done AFTER processing all streams in the item) ---
            for stream_name_saved, data_saved in processed_data_for_saving.items():
                  save_to_database(stream_name_saved, data_saved, timestamp)

            if live_data_file and not live_data_file.closed:
                try:
                    # Save the original item structure to the live file
                    live_data_file.write(json.dumps(item) + "\n")
                except TypeError as e: main_logger.error(f"JSON Error (live file): {e}", exc_info=False)
                except Exception as e: main_logger.error(f"Live file write error: {e}", exc_info=True)

            data_queue.task_done() # Mark the original queue item as done

        except queue.Empty:
            break # No more items
        except Exception as e:
            main_logger.error(f"Outer queue processing error for item {item}: {e}", exc_info=True)
            try: data_queue.task_done() # Mark done even on error
            except ValueError: pass
            except AttributeError: pass # Handle cases where item might be None if error occurred early

# --- Ensure Helper functions exist and are correct ---
# _process_driver_list(data) - Use version from previous response (stream only TLA)
# _process_timing_data(data) - Use version from previous response
# _process_session_info(data) - Placeholder or implement as needed
# _decode_and_decompress(message) - Should exist from original code

# --- Database Functions ---
def init_database(filename):
    global db_conn, db_cursor
    main_logger.info(f"Initializing database: {filename}")
    try:
        with db_lock:
            if db_conn:
                try:
                    db_conn.close()
                    main_logger.debug("Closed existing DB conn.")
                except Exception as e:
                     main_logger.error(f"Err closing old DB: {e}", exc_info=True)
            db_conn = sqlite3.connect(filename, check_same_thread=False, timeout=10)
            db_cursor = db_conn.cursor()
            db_cursor.execute("PRAGMA journal_mode=WAL;")
            db_cursor.execute('CREATE TABLE IF NOT EXISTS signalr_data (id INTEGER PRIMARY KEY AUTOINCREMENT, stream_name TEXT NOT NULL, data TEXT NOT NULL, timestamp TEXT NOT NULL, received_at DATETIME DEFAULT CURRENT_TIMESTAMP)')
            db_cursor.execute('CREATE INDEX IF NOT EXISTS idx_stream_timestamp ON signalr_data (stream_name, timestamp)')
            db_conn.commit()
        main_logger.info(f"DB initialized: {filename}")
        return True
    except sqlite3.Error as e:
        main_logger.error(f"DB init error: {e}", exc_info=True)
        db_conn = None; db_cursor = None
        return False

def save_to_database(stream_name, data, timestamp):
    # Check connection and cursor existence *before* trying to lock/save
    if not db_conn or not db_cursor:
        # Log only if it seems like it *should* be open (e.g., during live feed/replay)
        with app_state_lock:
            app_state = app_status["state"]
        if app_state in ["Live", "Replaying", "Connecting", "Initializing"]:
             main_logger.warning(f"DB save skipped: db_conn ({db_conn is not None}) or db_cursor ({db_cursor is not None}) is None during active state '{app_state}'.")
        return # Exit function if no valid connection/cursor

    try:
        # Acquire lock *before* accessing shared resources (db_cursor)
        with db_lock:
            # Double-check cursor *inside* the lock, as it might have been closed between the outer check and acquiring the lock
            if not db_cursor:
                main_logger.warning("DB save skipped: db_cursor became None before save operation inside lock.")
                return

            data_json = None
            try:
                data_json = json.dumps(data)
            except TypeError as e:
                main_logger.error(f"JSON Error (DB): {e}. Sample: {str(data)[:100]}", exc_info=False)
                data_json = json.dumps({"error": "Not serializable"})

            try:
                # Perform execute and commit together inside the try/except block
                db_cursor.execute('INSERT INTO signalr_data (stream_name, data, timestamp) VALUES (?, ?, ?)', (stream_name, data_json, timestamp))
                db_conn.commit()
            except sqlite3.Error as e:
                 main_logger.error(f"DB insert/commit error: {e}", exc_info=True)
                 try:
                     # Attempt rollback only if an error occurred during execute/commit
                     if db_conn: # Check if connection still exists before rollback
                         db_conn.rollback()
                         main_logger.info("DB rolled back due to error.")
                 except Exception as rb_e:
                     main_logger.error(f"DB rollback error: {rb_e}")

    # Catch potential errors acquiring lock or other unexpected issues outside the inner try/except
    except Exception as e:
         main_logger.error(f"Outer DB save error: {e}", exc_info=True)

def close_database():
    global db_conn, db_cursor
    with db_lock:
        if db_conn:
            db_name = str(db_conn)
            main_logger.info(f"Closing DB: {db_name}")
            try:
                 if db_cursor:
                     db_cursor.close()
                 db_conn.close()
                 main_logger.info(f"DB closed: {db_name}")
            except Exception as e:
                 main_logger.error(f"Err closing DB {db_name}: {e}", exc_info=True)
            finally:
                 db_conn = None
                 db_cursor = None

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

def on_error(error):
    if "already closed" in str(error).lower():
        main_logger.warning(f"Ignoring SignalR error (closed): {error}")
        return
    main_logger.error(f"SignalR err cb: {error} ({type(error).__name__})")
    with app_state_lock:
        if app_status["state"] not in ["Error", "Stopping", "Stopped"]:
            app_status.update({"connection": f"SignalR Error: {type(error).__name__}", "state": "Error"})

def on_close():
    main_logger.warning("SignalR close cb.")
    with app_state_lock:
        if app_status["state"] not in ["Stopping", "Stopped", "Error", "Playback Complete"]:
             main_logger.warning("Conn closed unexpectedly.")
             app_status.update({"connection": "Closed Unexpectedly", "state": "Stopped"})

def on_open():
    main_logger.info("SignalR open cb.")
    with app_state_lock:
        if app_status["state"] == "Connecting":
            app_status.update({"connection": "Connected, Subscribing", "state": "Live"})
        else:
            main_logger.warning(f"on_open unexpected state: {app_status['state']}.")
            app_status["connection"] = "Connected (out of sync?)"
    temp_hub = hub_connection
    if temp_hub:
        try:
            main_logger.info(f"Subscribing to: {STREAMS_TO_SUBSCRIBE}")
            temp_hub.invoke("Subscribe", STREAMS_TO_SUBSCRIBE)
            with app_state_lock:
                app_status["subscribed_streams"] = STREAMS_TO_SUBSCRIBE
                if app_status["state"] == "Live":
                    app_status["connection"] = "Live / Subscribed"
            main_logger.info("Subscribed OK.")
        except (HubConnectionError, HubError) as e:
            main_logger.error(f"Sub error: {e}", exc_info=True)
            with app_state_lock:
                app_status.update({"connection": f"Sub Error: {type(e).__name__}", "state": "Error"})
            if not stop_event.is_set():
                stop_event.set()
        except Exception as e:
            main_logger.error(f"Unexpected sub error: {e}", exc_info=True)
            with app_state_lock:
                app_status.update({"connection": f"Unexpected Sub Error: {type(e).__name__}", "state": "Error"})
            if not stop_event.is_set():
                stop_event.set()
    else:
        main_logger.error("on_open: hub_connection None.")
        with app_state_lock:
            app_status.update({"connection": "Sub Failed (No Hub)", "state": "Error"})
        if not stop_event.is_set():
            stop_event.set()

def start_signalr_connection():
    global live_data_file
    global hub_connection, connection_thread, live_data_file, db_conn, db_cursor, stop_event, timing_state
    if connection_thread and connection_thread.is_alive():
        main_logger.warning("Conn thread running.")
        return
    stop_event.clear()
    main_logger.debug("Stop event cleared (start conn).")
    with app_state_lock:
        app_status.update({"state": "Initializing", "connection": "Negotiating", "subscribed_streams": [], "last_heartbeat": None})
        data_store.clear()
        timing_state.clear()
    connection_url = build_connection_url(NEGOTIATE_URL_BASE, HUB_NAME)
    if not connection_url:
        main_logger.error("Build URL fail.")
        with app_state_lock:
            app_status.update({"state": "Error", "connection": "Negotiation Failed"})
        return
    timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    data_filename = DATA_FILENAME_TEMPLATE.format(timestamp=timestamp_str)
    db_filename = DATABASE_FILENAME_TEMPLATE.format(timestamp=timestamp_str)
    if not init_database(db_filename):
        main_logger.error("DB init fail.")
        with app_state_lock:
            app_status.update({"state": "Error", "connection": "DB Init Failed"})
        return
    try:
        if live_data_file and not live_data_file.closed:
            main_logger.warning("Closing old live file.")
            live_data_file.close()
        live_data_file = open(data_filename, 'w', encoding='utf-8')
        main_logger.info(f"Live data file: {data_filename}")
    except IOError as e:
        main_logger.error(f"Live file open error: {e}", exc_info=True)
        live_data_file = None
        close_database()
        with app_state_lock:
            app_status.update({"state": "Error", "connection": "File Open Failed"})
        return
    hub_connection = None
    try:
        hub_connection = (HubConnectionBuilder().with_url(connection_url, options={"verify_ssl": True, "skip_negotiation": True})
                          .configure_logging(logging.INFO, handler=signalr_handler).build())
        main_logger.info("Hub built.")
    except Exception as e:
        main_logger.error(f"Hub build error: {e}", exc_info=True)
        close_database()
        if live_data_file and not live_data_file.closed:
            try:
                live_data_file.close()
            except Exception as file_e:
                 main_logger.error(f"Error closing live data file during hub build cleanup: {file_e}")
        live_data_file = None
        with app_state_lock:
            app_status.update({"state": "Error", "connection": "Hub Build Failed"})
        return
    hub_connection.on_open(on_open)
    hub_connection.on_close(on_close)
    hub_connection.on_error(on_error)
    hub_connection.on("feed", handle_message)
    with app_state_lock:
        app_status.update({"state": "Connecting", "connection": "Socket Connecting"})

    def connect():
        # Moved global declaration here
        global live_data_file
        try:
            if not hub_connection:
                main_logger.error("Hub None (thread).")
                raise HubConnectionError("Hub None.")
            main_logger.info("Starting connect (thread)...")
            hub_connection.start()
            main_logger.info("Connect initiated.")
            # *** CORRECTED ATTRIBUTE NAME HERE ***
            while hub_connection and hub_connection.transport and hub_connection.transport.connection_alive and not stop_event.is_set():
                time.sleep(0.5)
            main_logger.info("Conn loop finished.")
        except UnAuthorizedHubError as e:
            main_logger.error(f"Auth error: {e}", exc_info=True)
            with app_state_lock:
                app_status.update({"connection": "Auth Failed", "state": "Error"})
        except HubConnectionError as e:
            main_logger.error(f"Hub connect error: {e}", exc_info=True)
            with app_state_lock:
                 if app_status["state"] not in ["Stopping", "Stopped"]:
                     app_status.update({"connection": f"Conn Error: {type(e).__name__}", "state": "Error"})
        except Exception as e:
            # This will now catch the AttributeError if connection_alive doesn't exist either
            main_logger.error(f"Unexpected conn thread error: {e}", exc_info=True)
            with app_state_lock:
                if app_status["state"] not in ["Stopping", "Stopped"]:
                    app_status.update({"connection": f"Unexpected Error: {type(e).__name__}", "state": "Error"})
        finally:
            main_logger.info("Conn thread cleanup...")
            temp_hub = hub_connection # Use local copy

            # Stop hub connection if transport exists and connection is alive
            # *** CORRECTED ATTRIBUTE NAME HERE ***
            if temp_hub and temp_hub.transport and getattr(temp_hub.transport, 'connection_alive', False): # Safely check attribute
                try:
                    main_logger.info("Final hub stop...")
                    temp_hub.stop()
                    main_logger.info("Hub stopped (finally).")
                except Exception as e:
                    main_logger.error(f"Err stopping hub (finally): {e}", exc_info=True)

            # Close file ('global live_data_file' declared at top of function)
            if live_data_file and not live_data_file.closed:
                main_logger.info(f"Closing live file: {live_data_file.name}")
                try:
                    live_data_file.close()
                except Exception as e:
                     main_logger.error(f"Err closing live file: {e}")
                live_data_file = None # Reset global variable

            close_database() # Close DB

            # Update status
            with app_state_lock:
                 if app_status["state"] not in ["Stopped", "Error", "Playback Complete"]:
                     if stop_event.is_set() and app_status["state"] != "Stopping":
                         app_status.update({"state": "Stopped", "connection": "Disconnected"})
                     elif app_status["state"] != "Error":
                         app_status.update({"state": "Stopped", "connection": "Disconnected / Thread End"})
            # Signal main thread if needed
            if not stop_event.is_set():
                main_logger.info("Conn thread setting stop event.")
                stop_event.set()
            # Clear global hub reference
            globals()['hub_connection'] = None
            main_logger.info("Conn thread cleanup finished.")

    connection_thread = threading.Thread(target=connect, name="SignalRConnectionThread", daemon=True)
    connection_thread.start()
    main_logger.info("Conn thread started.")

def stop_connection():
    global hub_connection, connection_thread, stop_event
    main_logger.info("Stop connection requested.")
    with app_state_lock:
        current_state = app_status["state"]
        thread_running = connection_thread and connection_thread.is_alive()
    if current_state not in ["Connecting", "Live", "Stopping"] and not thread_running:
         main_logger.warning(f"Stop conn called, state={current_state}, thread_active={thread_running}. No active conn.")
         if not stop_event.is_set():
             stop_event.set()
         with app_state_lock:
             if current_state in ["Connecting", "Live"]:
                 app_status.update({"state": "Stopped", "connection": "Disconnected (Force Stop)"})
         return
    with app_state_lock:
        if current_state == "Stopping":
            main_logger.info("Stop already in progress.")
            return
        app_status.update({"state": "Stopping", "connection": "Disconnecting"})
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
    with app_state_lock:
        if app_status["state"] == "Stopping":
            app_status.update({"state": "Stopped", "connection": "Disconnected"})
        app_status["subscribed_streams"] = []
    globals()['hub_connection'] = None
    globals()['connection_thread'] = None
    main_logger.info("Stop connection sequence complete.")

def replay_from_file(data_file_path, replay_speed=1.0):
    global replay_thread, db_conn, db_cursor, stop_event, timing_state
    if replay_thread and replay_thread.is_alive():
        main_logger.warning("Replay thread running.")
        return
    stop_event.clear()
    main_logger.debug("Stop event cleared (replay).")
    if not os.path.exists(data_file_path):
        main_logger.error(f"File not found: {data_file_path}")
        with app_state_lock:
            app_status.update({"state": "Error", "connection": f"File Not Found: {os.path.basename(data_file_path)}"})
        return
    with app_state_lock:
        app_status.update({"state": "Initializing", "connection": f"Preparing Replay: {os.path.basename(data_file_path)}", "subscribed_streams": [], "last_heartbeat": None})
        data_store.clear()
        timing_state.clear() # Clear persistent state

    db_filename = os.path.splitext(data_file_path)[0] + ".db"
    if not init_database(db_filename):
        main_logger.error("Replay DB init fail.")
        with app_state_lock:
            app_status.update({"state": "Error", "connection": "Replay DB Init Failed"})
        return
    with app_state_lock:
        app_status.update({"state": "Replaying", "connection": f"File: {os.path.basename(data_file_path)}", "subscribed_streams": ["Replay"]})
        
        def replay(): # The actual replay loop
            main_logger.info(f"Starting main replay loop: {data_file_path}, speed {replay_speed}")
            last_timestamp_for_delay = None # Use a separate variable for delay calculation timestamp
            start_time = time.monotonic() # Overall start time, maybe not needed? 
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
                                main_logger.debug(f"Line {line_num}: Identified Snapshot (R) message. Calling handle_message directly.")
                                handle_message(raw_message) # handle_message unpacks and queues
                                lines_processed += 1 # Count the snapshot block as one processed line for stats
                                # DO NOT apply delay for the whole R block based on its internal timestamp here
                                should_apply_delay = False # Delay is handled by individual messages queued by handle_message
                                # We don't get a single 'timestamp' for the 'R' block to use for delay here
    
                            elif isinstance(raw_message, dict) and "M" in raw_message and isinstance(raw_message["M"], list):
                                 # Standard message block {"M": [...]}
                                 main_logger.debug(f"Line {line_num}: Identified Standard (M) message block.")
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
                                 main_logger.debug(f"Line {line_num}: Identified Direct stream list message.")
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
                                if line_num > 3: main_logger.warning(f"Line {line_num}: Skipping unrecognized JSON top-level structure: {type(raw_message).__name__}")
                                continue # Skip delay logic
                            # --- MODIFICATION END ---
    
    
                            # --- Delay logic (Apply only if flag is set) ---
                            if should_apply_delay and timestamp_for_this_line: # Check if timestamp was actually set
                                if replay_speed > 0:
                                    if not first_message_processed: time.sleep(0.01 / replay_speed if replay_speed > 0 else 0.001); first_message_processed = True
                                    elif last_timestamp_for_delay: # Use the dedicated delay timestamp
                                        try:
                                            # Use timestamp_for_this_line which was set above for M or List messages
                                            current_ts_dt = datetime.datetime.fromisoformat(timestamp_for_this_line.replace('Z', '+00:00'))
                                            prev_ts_dt = datetime.datetime.fromisoformat(last_timestamp_for_delay.replace('Z', '+00:00'))
                                            time_diff_seconds = (current_ts_dt - prev_ts_dt).total_seconds()
                                            if time_diff_seconds > 0:
                                                target_delay = time_diff_seconds / replay_speed; processing_time = time.monotonic() - start_time_line
                                                actual_delay = max(0, target_delay - processing_time); time.sleep(actual_delay)
                                            elif time_diff_seconds < 0: main_logger.warning(f"Timestamp backwards line {line_num}: {timestamp_for_this_line} vs {last_timestamp_for_delay}"); time.sleep(0.001 / replay_speed if replay_speed > 0 else 0.001)
                                        except Exception as ts_e: main_logger.warning(f"Timestamp parse/delay error line {line_num}: '{timestamp_for_this_line}'. Err: {ts_e}. Fixed delay."); time.sleep(0.01 / replay_speed if replay_speed > 0 else 0.001)
                                    else: time.sleep(0.01 / replay_speed if replay_speed > 0 else 0.001)
                                # Update the timestamp used for the *next* delay calculation
                                last_timestamp_for_delay = timestamp_for_this_line
                                start_time_line = time.monotonic() # Reset line processing timer
    
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
	            with app_state_lock:
	                app_status.update({"state": "Error", "connection": "Replay File Error"})
            except Exception as e:
	            main_logger.error(f"Error during playback: {e}", exc_info=True)
	            with app_state_lock:
	                app_status.update({"state": "Error", "connection": "Replay Runtime Error"})
            finally:
	             main_logger.info("Replay thread cleanup...")
	             close_database()
	             with app_state_lock:
	                 current_state = app_status["state"]
	                 if current_state not in ["Error", "Stopped", "Playback Complete"]:
	                     if stop_event.is_set():
	                          app_status.update({"state": "Stopped", "connection": "Disconnected"})
	                     else:
	                          app_status.update({"state": "Error", "connection": "Thread End Unexpectedly"})
	             main_logger.info("Replay thread cleanup finished.")
	             
    globals()['replay_thread'] = threading.Thread(target=replay, name="ReplayThread", daemon=True)
    replay_thread.start()


def stop_replay():
    global replay_thread, stop_event
    main_logger.info("Stop replay requested.")
    with app_state_lock: current_state = app_status["state"]; thread_running = replay_thread and replay_thread.is_alive()
    if not thread_running and current_state != "Replaying":
        main_logger.warning(f"Stop replay called, state={current_state}, thread_active={thread_running}.")
        with app_state_lock:
            if current_state == "Replaying": app_status.update({"state": "Stopped", "connection": "Disconnected (Force Stop)"})
        return
    with app_state_lock:
        if current_state == "Stopping": main_logger.info("Stop already in progress."); return
        app_status.update({"state": "Stopping", "connection": "Stopping Replay"})
    stop_event.set(); main_logger.debug("Stop event set for replay.")
    local_replay_thread = globals().get('replay_thread')
    if local_replay_thread and local_replay_thread.is_alive():
        main_logger.info("Waiting for replay thread join..."); local_replay_thread.join(timeout=5)
        if local_replay_thread.is_alive(): main_logger.warning("Replay thread did not join cleanly.")
        else: main_logger.info("Replay thread joined.")
    with app_state_lock:
        if app_status["state"] == "Stopping": app_status.update({"state": "Stopped", "connection": "Disconnected"})
    globals()['replay_thread'] = None
    main_logger.info("Stop replay sequence complete.")


# --- Dash GUI Setup ---
app = Dash(__name__, external_stylesheets=[dbc.themes.SLATE], suppress_callback_exceptions=True)
timing_table_columns = [
    # --- Existing Columns ---
    {"name": "Car", "id": "Car"}, # Keep your logic for TLA/Number
    {"name": "Pos", "id": "Pos"},
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
    dbc.Row([dbc.Col(dbc.Button("Start Live Feed", id="start-button", color="success", className="me-1"), width="auto"), dbc.Col(dbc.Button("Stop Feed / Replay", id="stop-button", color="danger", className="me-1"), width="auto"), dbc.Col(dbc.Input(id="replay-file-input", placeholder="Enter .data file path or blank", type="text", value=DEFAULT_REPLAY_FILENAME, debounce=True), width=4), dbc.Col(dbc.Input(id="replay-speed-input", placeholder="Speed", type="number", min=0, step=0.1, value=1.0, debounce=True), width="auto"), dbc.Col(dbc.Button("Start Replay", id="replay-button", color="primary", className="me-1"), width="auto")], className="mb-3 align-items-center"),
    dbc.Row([dbc.Col([html.H3("Latest Data (Non-Timing)"), html.Div(id='live-data-display', style={'maxHeight': '300px', 'overflowY': 'auto', 'border': '1px solid grey', 'padding': '10px', 'marginBottom': '10px'}), html.H3("Timing Data Details"), html.Div(id='timing-data-table', children=[html.P(id='timing-data-timestamp', children="Waiting for data..."), dash_table.DataTable(id='timing-data-actual-table', columns=timing_table_columns, data=[], fixed_rows={'headers': True}, style_table={'height': '400px', 'overflowY': 'auto', 'overflowX': 'auto'}, style_cell={'minWidth': '50px', 'width': '80px', 'maxWidth': '120px','overflow': 'hidden','textOverflow': 'ellipsis','textAlign': 'left','padding': '5px','backgroundColor': 'rgb(50, 50, 50)','color': 'white'}, style_header={'backgroundColor': 'rgb(30, 30, 30)','fontWeight': 'bold','border': '1px solid grey'}, style_data={'borderBottom': '1px solid grey'}, style_data_conditional=[{'if': {'row_index': 'odd'},'backgroundColor': 'rgb(60, 60, 60)'}], tooltip_duration=None)])], width=12)]),
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
    Output('session-info-display', 'children'), # <<< ADDED Output
    Output('live-data-display', 'children'),
    Output('timing-data-actual-table', 'data'),
    Output('timing-data-timestamp', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_output(n):
    process_data_queue() # Process any pending data first

    status_text = "State: Unknown"
    heartbeat_text = "Last HB: N/A"
    track_display_text = "Track: Unknown"
    session_info_display_children = "Session Details: Waiting..." # <<< Initialize
    other_data_display = []
    table_data = []
    timing_timestamp_text = "Waiting for TimingData..."

    with app_state_lock:
        # Update status and heartbeat displays
        status_text = f"State: {app_status['state']} | Conn: {app_status['connection']}"
        heartbeat_text = f"Last HB: {app_status['last_heartbeat'] or 'N/A'}"
        
        # --- ADDED: Get and Format Track Status ---
        track_status_code = track_status_data.get('Status', '0')
        track_status_message = track_status_data.get('Message', '')
        track_status_map = {
            '1': "AllClear", '2': "Yellow", '3': "SC Retiring?", # Adjust map as needed
            '4': "SC Deployed", '5': "Red Flag", '6': "VSC Ending", '7': "VSC Deployed"
        }
        track_display_text = f"Track: {track_status_map.get(track_status_code, f'Unknown ({track_status_code})')}"
        # Optionally add message if different from standard mapping
        # if track_status_message and track_status_message != track_status_map.get(track_status_code):
        #      track_display_text += f" ({track_status_message})"
        # --- END ADDED ---
        
        # --- ADDED: Get and Format Session Details ---
        # Extract details safely using .get()
        meeting_name = session_details.get('Meeting', {}).get('Name', 'Unknown Meeting')
        session_name = session_details.get('Name', 'Unknown Session')
        circuit_name = session_details.get('Circuit', {}).get('ShortName', 'Unknown Circuit') # Or 'OfficialName'
        start_time_str = session_details.get('StartDate') # Already a string? Format if needed
        country_name = session_details.get('Country', {}).get('Name', '')

        session_info_parts = [f"Circuit: {circuit_name}"]
        if country_name: session_info_parts.append(f"({country_name})")
        session_info_parts.append(f"Event: {meeting_name}")
        session_info_parts.append(f"Session: {session_name}")
        if start_time_str: session_info_parts.append(f"Starts: {start_time_str}") # Format this date/time nicer?

        session_info_display_children = " | ".join(session_info_parts)
        # --- END ADDED ---

        # Display other stream data (collapsed by default except SessionInfo)
        sorted_streams = sorted([s for s in data_store.keys() if s not in ['TimingData', 'DriverList', 'Position.z', 'CarData.z']]) # Exclude high-frequency streams
        for stream in sorted_streams:
             value = data_store[stream]
             # Limit displayed data size for performance
             data_str = json.dumps(value.get('data', 'N/A'), indent=2)
             timestamp_str = value.get('timestamp', 'N/A')
             if len(data_str) > 500: # Limit display size
                 data_str = data_str[:500] + "\n... (data truncated)"
             other_data_display.append(html.Details([
                 html.Summary(f"{stream} ({timestamp_str})"),
                 html.Pre(data_str, style={'marginLeft': '15px', 'maxHeight': '200px', 'overflowY': 'auto'})
             ], style={'marginBottom': '5px'}, open=(stream=="SessionInfo"))) # Open SessionInfo by default

        # Update timing table timestamp
        if 'TimingData' in data_store:
            timing_timestamp_text = f"Timing Timestamp: {data_store['TimingData'].get('timestamp', 'N/A')}"
        elif not timing_state:
             timing_timestamp_text = "Waiting for DriverList..."

        # --- Generate Timing Table Data ---
        if timing_state:
            processed_table_data = []
            # Sort drivers by car number initially for consistent order before position sort
            sorted_driver_numbers = sorted(timing_state.keys(), key=lambda x: int(x) if x.isdigit() else float('inf'))

            # Inside update_output function, within the loop:

            for car_num in sorted_driver_numbers:
                driver_state = timing_state[car_num]

                # Helper function to safely get nested dictionary values (remains the same)
                def get_nested_state(d, *keys, default=None):
                     val = d
                     for key in keys:
                         if isinstance(val, dict):
                             val = val.get(key)
                         else:
                             return default
                     return val if val is not None else default

                # --- MODIFICATION START ---
                # Logic to determine what to display in the 'Car' column
                tla = driver_state.get("Tla") # Get the stored TLA value (might be "N/A" or None)
                car_display_value = car_num   # Default to the car number string

                # Use the TLA *only if* it exists and is not the string "N/A"
                if tla and tla != "N/A":
                    car_display_value = tla
                    
                # --- ADDED: Get CarData ---
                car_data = driver_state.get('CarData', {}) # Get sub-dict, default to {}
                # --- END ADDED ---

                # Create the table row data
                row = {
                    'Car': car_display_value, # Use the determined display value
                    'Pos': driver_state.get('Position', '-'),
                    'Time': driver_state.get('Time', '-'),
                    'Gap': driver_state.get('GapToLeader', '-'),
                    'Interval': get_nested_state(driver_state, 'IntervalToPositionAhead', 'Value', default='-'),
                    'Last Lap': get_nested_state(driver_state, 'LastLapTime', 'Value', default='-'),
                    'Best Lap': get_nested_state(driver_state, 'BestLapTime', 'Value', default='-'),
                    'S1': get_nested_state(driver_state, 'Sectors', '0', 'Value', default='-'),
                    'S2': get_nested_state(driver_state, 'Sectors', '1', 'Value', default='-'),
                    'S3': get_nested_state(driver_state, 'Sectors', '2', 'Value', default='-'),
                    'Status': driver_state.get('Status', 'N/A'),
                # --- ADDED: CarData values to row ---
                    'Speed': car_data.get('Speed', '-'),
                    'Gear': car_data.get('Gear', '-'),
                    'RPM': car_data.get('RPM', '-'),
                    'DRS': {8: "Eligible",10: "On",12: "On", 14: "ON"}.get(car_data.get('DRS'), 'Off'), # Expanded DRS map slightly, adjust as needed
                    # --- END ADDED ---
                # --- END UPDATED DRS Mapping ---
                }
                # --- MODIFICATION END ---
                processed_table_data.append(row)

            # Sort final table data by position (handle non-numeric positions)
            def pos_sort_key(item):
                pos_str = item.get('Pos', '999') # Default to 999 if missing
                if isinstance(pos_str, (int, float)):
                    return pos_str # Already numeric
                if isinstance(pos_str, str) and pos_str.isdigit():
                     try:
                         return int(pos_str)
                     except ValueError:
                          return 999 # Should not happen if isdigit() is true, but safety
                return 999 # Place non-numeric positions (OUT, "", etc.) at the end

            processed_table_data.sort(key=pos_sort_key)
            table_data = processed_table_data
        # --- End Timing Table Data ---

    # --- Return all outputs in correct order ---
    return (status_text, heartbeat_text, track_display_text,
            session_info_display_children, # <<< Added session info
            other_data_display, table_data, timing_timestamp_text)

@app.callback( Output('start-button', 'disabled'), Output('stop-button', 'disabled'), Output('replay-button', 'disabled'), Output('replay-file-input', 'disabled'), Output('replay-speed-input', 'disabled'), Input('interval-component', 'n_intervals'))
def update_button_states(n):
    with app_state_lock: state = app_status['state']
    is_idle = state in ["Idle", "Stopped", "Error", "Playback Complete"]; is_running = state in ["Connecting", "Live", "Replaying", "Initializing"]; is_stopping = state == "Stopping"
    start_disabled = is_running or is_stopping; replay_disabled = is_running or is_stopping; stop_disabled = is_idle; input_disabled = is_running or is_stopping
    return start_disabled, stop_disabled, replay_disabled, input_disabled, input_disabled

@app.callback(Output('status-display', 'children', allow_duplicate=True), Input('start-button', 'n_clicks'), prevent_initial_call=True)
def handle_start_button(n_clicks):
    if n_clicks is None or n_clicks == 0: return dash.no_update
    main_logger.info(f"Start Live clicked (n={n_clicks}).")
    start_signalr_connection()
    with app_state_lock: return f"State: {app_status['state']} | Conn: {app_status['connection']}"

@app.callback(Output('status-display', 'children', allow_duplicate=True), Input('stop-button', 'n_clicks'), prevent_initial_call=True)
def handle_stop_button(n_clicks):
    if n_clicks is None or n_clicks == 0: return dash.no_update
    main_logger.info(f"Stop clicked (n={n_clicks}).")
    triggered_stop = False; current_state_on_click = "Unknown"
    with app_state_lock: state = app_status['state']; current_state_on_click = state
    if state in ["Replaying", "Initializing"]: stop_replay(); triggered_stop = True
    elif state in ["Live", "Connecting"]: stop_connection(); triggered_stop = True
    elif state == "Stopping": main_logger.info("Stop clicked while stopping.")
    else: main_logger.warning(f"Stop clicked in state {state}. Set stop event.");
    if not stop_event.is_set(): stop_event.set()
    with app_state_lock: new_state = app_status['state']; new_conn = app_status['connection']
    if triggered_stop: return f"Stop req from '{current_state_on_click}'. New: {new_state} | {new_conn}"
    else: return f"Stop ignored ('{current_state_on_click}'). Current: {new_state} | {new_conn}"

@app.callback(Output('status-display', 'children', allow_duplicate=True), Input('replay-button', 'n_clicks'), State('replay-file-input', 'value'), State('replay-speed-input', 'value'), prevent_initial_call=True)
def handle_replay_button(n_clicks, file_path, speed_val):
    if n_clicks is None or n_clicks == 0: return dash.no_update
    main_logger.info(f"Replay clicked (n={n_clicks}).");
    try: speed = float(speed_val) if speed_val is not None else 1.0; speed = max(0, speed)
    except: main_logger.warning(f"Invalid speed '{speed_val}', default 1.0"); speed = 1.0
    file_path_cleaned = file_path.strip() if isinstance(file_path, str) else DEFAULT_REPLAY_FILENAME
    file_path_to_use = file_path_cleaned if file_path_cleaned else DEFAULT_REPLAY_FILENAME
    main_logger.info(f"Replay file: {file_path_to_use}, speed: {speed}")
    replay_from_file(file_path_to_use, speed)
    with app_state_lock: return f"State: {app_status['state']} | Conn: {app_status['connection']}"
    
@app.callback(
    Output('track-map-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_track_map(n):
    global timing_state, session_details, track_coordinates_cache

    figure_data = []
    drivers_x_raw = []
    drivers_y_raw = []
    drivers_text = []
    drivers_color = []
    drivers_opacity = [] # <<< ADDED list for opacity

    track_x, track_y = None, None
    corner_x, corner_y = None, None
    x_range, y_range = None, None
    rotation_angle = 0
    current_session_key = None

    # --- Load track data: Check cache or fetch from API ---
    with app_state_lock:
        # 1. Identify Current Session Year and Circuit Key
        year = session_details.get('Year')
        circuit_key = session_details.get('CircuitKey')

        if year and circuit_key:
            current_session_key = f"{year}_{circuit_key}"
        else:
            # Not enough info yet to load map data
            main_logger.debug("Map update skipped: Year or CircuitKey missing from session_details.")
            current_session_key = None

        # 2. Check Cache / Fetch from API
        if current_session_key and track_coordinates_cache.get('session_key') == current_session_key:
            # Use cached data
            track_x = track_coordinates_cache.get('x')
            track_y = track_coordinates_cache.get('y')
            corner_x = track_coordinates_cache.get('corner_x')
            corner_y = track_coordinates_cache.get('corner_y')
            x_range = track_coordinates_cache.get('range_x')
            y_range = track_coordinates_cache.get('range_y')
            rotation_angle = track_coordinates_cache.get('rotation', 0)
            main_logger.debug(f"Using cached track map for {current_session_key}")
        elif current_session_key:
            # Cache miss or new session, try fetching from API
            api_url = f"https://api.multiviewer.app/api/v1/circuits/{circuit_key}/{year}"
            main_logger.info(f"Attempting to fetch track map from API: {api_url}")
            try:
                # --- ADD User-Agent Header ---
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                response = requests.get(api_url, headers=headers, timeout=10) # Add headers=headers
                # --- End Add Header ---
                response.raise_for_status() # Check for 4xx/5xx errors AFTER getting response
                map_api_data = response.json()

                # --- Extract data from API response ---
                # --- TEMPORARY LOGS - Inspect API Response ---
                main_logger.info(f"--- MultiViewer API Response Received (Keys) ---")
                main_logger.info(f"Top-level Keys: {list(map_api_data.keys())}")
                # Log specific parts to verify structure/existence:
                main_logger.info(f"Rotation Key ('rotation'): {map_api_data.get('rotation')}")
                main_logger.info(f"Corners Key ('corners') Type: {type(map_api_data.get('corners'))}")
                main_logger.info(f"Track Key ('track') Type: {type(map_api_data.get('track'))}")
                main_logger.info(f"Boundary Keys ('xMin', 'xMax', 'yMin', 'yMax'): "
                                 f"{map_api_data.get('xMin')}, {map_api_data.get('xMax')}, "
                                 f"{map_api_data.get('yMin')}, {map_api_data.get('yMax')}")
                # You could even log the whole thing if it's not too large initially:
                main_logger.info(f"Full API Response (snippet): {json.dumps(map_api_data, indent=2)[:1000]}")
                # --- END TEMPORARY LOGS ---

                                # --- CORRECTED Data Extraction ---
                temp_track_x = map_api_data.get('x') # Get list from 'x' key
                temp_track_y = map_api_data.get('y') # Get list from 'y' key

                if isinstance(temp_track_x, list) and isinstance(temp_track_y, list) and len(temp_track_x) == len(temp_track_y):
                    # Convert to numbers (API might return strings sometimes)
                    try:
                        track_x = [float(p) for p in temp_track_x]
                        track_y = [float(p) for p in temp_track_y]
                    except (ValueError, TypeError) as conv_err:
                         main_logger.error(f"Could not convert track coordinates to float: {conv_err}")
                         track_x, track_y = None, None # Reset on error
                else:
                    main_logger.warning("Could not extract valid 'x' and 'y' lists from API response.")
                    track_x, track_y = None, None

                # Extract corner coordinates (seems OK based on API response)
                corners_raw = map_api_data.get('corners')
                if isinstance(corners_raw, list):
                    corner_x_temp, corner_y_temp = [], []
                    for corner in corners_raw:
                        if isinstance(corner, dict):
                             pos = corner.get('trackPosition', {})
                             cx = pos.get('x')
                             cy = pos.get('y')
                             if cx is not None and cy is not None:
                                 try:
                                     corner_x_temp.append(float(cx))
                                     corner_y_temp.append(float(cy))
                                 except (ValueError, TypeError):
                                     pass # Ignore corners if conversion fails
                    if corner_x_temp:
                        corner_x = corner_x_temp
                        corner_y = corner_y_temp
                else: main_logger.warning("Could not extract 'corners' data from API response.")

                # Extract rotation (seems OK)
                rotation_angle = float(map_api_data.get('rotation', 0.0))

                # --- CORRECTED Range Calculation (from extracted track_x/y) ---
                if track_x and track_y: # Calculate only if we got valid track coordinates
                    try:
                        x_min, x_max = np.min(track_x), np.max(track_x)
                        y_min, y_max = np.min(track_y), np.max(track_y)
                        padding_x = (x_max - x_min) * 0.05
                        padding_y = (y_max - y_min) * 0.05
                        x_range = [x_min - padding_x, x_max + padding_x]
                        y_range = [y_min - padding_y, y_max + padding_y]
                    except Exception as range_err:
                         main_logger.error(f"Error calculating ranges from track data: {range_err}")
                         x_range, y_range = None, None # Reset on error
                else:
                     main_logger.warning("Cannot calculate ranges because track_x or track_y is missing/invalid.")
                     x_range, y_range = None, None
                # --- END CORRECTED Range Calculation ---

                # Store successfully extracted/calculated data in cache
                track_coordinates_cache = {
                    'x': track_x, 'y': track_y,
                    'corner_x': corner_x, 'corner_y': corner_y,
                    'range_x': x_range, 'range_y': y_range,
                    'rotation': rotation_angle,
                    'session_key': current_session_key
                }
                main_logger.info(f"Successfully processed and cached track map for {current_session_key}")

            except requests.exceptions.RequestException as e:
                main_logger.error(f"API request failed for {api_url}: {e}")
                track_coordinates_cache = {'session_key': None} # Clear cache on error
            except json.JSONDecodeError as e:
                 main_logger.error(f"Failed to parse JSON response from {api_url}: {e}")
                 track_coordinates_cache = {'session_key': None}
            except Exception as e:
                main_logger.error(f"Error processing API data for {api_url}: {e}", exc_info=True)
                track_coordinates_cache = {'session_key': None}

        else:
             # No session key yet, or attempt failed, keep cache cleared
             if track_coordinates_cache.get('session_key') is not None:
                   main_logger.info("Clearing track map cache.")
                   track_coordinates_cache = {'session_key': None}

        # 3. Get RAW Car Positions (No rotation yet) - (Keep existing logic)
        for car_num, driver_state in timing_state.items():
             # ... (extract raw x, y, text, color) ...
             pos_data = driver_state.get('PositionData')
             status_string = driver_state.get('Status', '').lower(); is_off_main_track = ('pit' in status_string or 'retired' in status_string)
             if pos_data and 'X' in pos_data and 'Y' in pos_data:
                  drivers_x_raw.append(pos_data['X'])
                  drivers_y_raw.append(pos_data['Y'])
                  if is_off_main_track:
                       drivers_text.append("") # Append empty string if in pit
                  else:
                       drivers_text.append(driver_state.get('Tla', car_num)) # Append TLA otherwise
                  team_color = driver_state.get('TeamColour', 'FFFFFF')
                  drivers_color.append(f'#{team_color}')
                  # --- Set Opacity based on Status ---
                  drivers_opacity.append(0.0 if is_off_main_track else 1.0) # <<< Make pitting cars semi-transparent
                  # ---


    # --- Create Plotly Figure (Outside Lock) ---
    # 4. Add Track Outline Trace (if available)
    if track_x is not None and track_y is not None:
        figure_data.append(go.Scatter(
            x=track_x, y=track_y, mode='lines', # Use the extracted x, y lists
            line=dict(color='grey', width=2),
            name='Track', showlegend=False, hoverinfo='none'
        ))
    # Optional: Add corner plotting logic here if needed, using corner_x, corner_y

    # 5. Rotate Live Car Positions and Add Trace
    if drivers_x_raw:
         # Ensure raw coords are numbers before rotating
         # --- TEMPORARY LOGS ---
         main_logger.info(f"Applying rotation angle: {rotation_angle}") # Log angle used
         main_logger.info(f"Raw X[:5]: {drivers_x_raw[:5]}")
         main_logger.info(f"Raw Y[:5]: {drivers_y_raw[:5]}")
         # --- END TEMP LOGS ---
         #try:
#              drivers_x_np = np.array([float(x) for x in drivers_x_raw])
#              drivers_y_np = np.array([float(y) for y in drivers_y_raw])
#             # Add try-except around rotation and plotting
#              drivers_x_rotated, drivers_y_rotated = rotate_coords(np.array(drivers_x_raw), np.array(drivers_y_raw), rotation_angle)
#              # --- TEMPORARY LOGS ---
#              main_logger.info(f"Rotated X[:5]: {drivers_x_rotated[:5]}")
#              main_logger.info(f"Rotated Y[:5]: {drivers_y_rotated[:5]}")
#              # --- END TEMP LOGS ---
    
         try:
                  figure_data.append(go.Scatter(
                  x=drivers_x_raw, y=drivers_y_raw, # Use rotated coordinates
              mode='markers+text',
              marker=dict(size=10, color=drivers_color, line=dict(width=1, color='Black'),opacity=drivers_opacity),
              text=drivers_text, textposition='middle right',
              name='Cars', hoverinfo='text', showlegend=False,
              textfont=dict(size=9, color='White')
              ))
         except Exception as plot_err:
              main_logger.error(f"Error plotting cars: {plot_err}", exc_info=True)


    # 6. Define Layout (Use calculated ranges)
    layout = go.Layout(
        xaxis=dict(range=x_range, showgrid=False, zeroline=False, showticklabels=False), # Use calculated range
        yaxis=dict(range=y_range, showgrid=False, zeroline=False, showticklabels=False, scaleanchor="x", scaleratio=1), # Use calculated range
        showlegend=False,
        margin=dict(l=5, r=5, t=5, b=5),
        uirevision=current_session_key or n,
        plot_bgcolor='rgb(50,50,50)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white')
    )

    # Handle empty figure case
    if not figure_data:
         empty_title = "Waiting for Session Info..." if not current_session_key else "Waiting for Track/Position Data..."
         return go.Figure(data=[], layout=go.Layout(title=empty_title, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='white')))
    else:
         return go.Figure(data=figure_data, layout=layout)

# --- Main Execution Logic ---
if __name__ == '__main__':
    main_logger.info("Application starting...")
    stop_event.clear()
    def run_dash():
        global app
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
    try:
        while not stop_event.is_set():
            with app_state_lock:
                current_state=app_status["state"]
                conn_thread_obj=connection_thread
                replay_thread_obj=replay_thread
            conn_active = conn_thread_obj and conn_thread_obj.is_alive()
            replay_active = replay_thread_obj and replay_thread_obj.is_alive()

            # Check thread states
            if current_state in ["Connecting", "Live"] and not conn_active:
                 main_logger.warning("Conn thread died. Stopping.")
                 if not stop_event.is_set():
                     stop_event.set()
                 with app_state_lock:
                     if app_status["state"] not in ["Error", "Stopped"]:
                         app_status.update({"state": "Error", "connection": "Conn Thread Died"})
                 break

            if current_state == "Replaying" and not replay_active:
                 main_logger.warning("Replay thread died.")
                 if globals().get('replay_thread') is replay_thread_obj:
                     globals()['replay_thread'] = None

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
        with app_state_lock:
            current_state = app_status["state"]
            conn_thread_obj = connection_thread
            replay_thread_obj = replay_thread
        if current_state in ["Live", "Connecting", "Stopping"] or (conn_thread_obj and conn_thread_obj.is_alive()):
            main_logger.info("Cleanup: Stopping connection...")
            stop_connection()
        if current_state in ["Replaying", "Stopping"] or (replay_thread_obj and replay_thread_obj.is_alive()):
            main_logger.info("Cleanup: Stopping replay...")
            stop_replay()
        if dash_thread and dash_thread.is_alive():
            main_logger.info("Waiting for Dash thread...")
            dash_thread.join(timeout=2)
            if dash_thread.is_alive():
                main_logger.warning("Dash thread didn't exit cleanly.")
        close_database()
        local_live_data_file = globals().get('live_data_file') # Use local var for check
        if local_live_data_file and not local_live_data_file.closed:
             main_logger.warning("Live file open cleanup. Closing.")
             try:
                 local_live_data_file.close()
             except Exception as e:
                 main_logger.error(f"Err closing live file: {e}")
             globals()['live_data_file'] = None # Clear global reference
        main_logger.info("Shutdown complete.")
        print("\n --- App Exited --- \n")
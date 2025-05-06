# config.py
"""
Configuration constants for the F1 Telemetry Dashboard application.
"""

import os
from pathlib import Path

# --- SignalR Connection ---
NEGOTIATE_URL_BASE = "https://livetiming.formula1.com/signalr"
WEBSOCKET_URL_BASE = "wss://livetiming.formula1.com/signalr"
HUB_NAME = "Streaming"
STREAMS_TO_SUBSCRIBE = ["Heartbeat", "CarData.z", "Position.z", "ExtrapolatedClock",
                        "TimingAppData", "TimingData", "TimingStats", "TrackStatus",
                        "SessionData", "DriverList", "RaceControlMessages", "SessionInfo",
                        "LapCount"] # Added LapCount based on type defs, confirm if needed

# --- File Paths ---
# Consider making REPLAY_DIR absolute or relative to main script location
_SCRIPT_DIR = Path(__file__).parent.resolve()
# Instead, get script dir in main.py and pass it or define paths relative to expected run location
REPLAY_DIR_NAME = "replays"
REPLAY_DIR = _SCRIPT_DIR / REPLAY_DIR_NAME
# TARGET_SAVE_DIRECTORY could be the same as REPLAY_DIR or different if needed
TARGET_SAVE_DIRECTORY = REPLAY_DIR # Directory for saving live data files
DEFAULT_REPLAY_FILENAME = "2023-yas-marina-quali.data.txt" # Default replay file suggestion
DASH_DEBUG_MODE = False
DASH_HOST = "0.0.0.0"
DASH_PORT = 8050

# --- Filename Templates ---
DATA_FILENAME_TEMPLATE = "f1_signalr_data_{timestamp}.data.txt" # Template for live data logs (Not currently used?)
# DATABASE_FILENAME_TEMPLATE = "f1_signalr_data_{timestamp}.db" # If DB functionality added later

# --- Telemetry Channel Mapping ---
# IMPORTANT WARNING: This map might need changing for live data or replays from different eras.
# Based on observation of 2023 Yas Marina replay data.
CHANNEL_MAP = {
    '0': 'RPM',       # Channel 0 seems to be RPM here
    '2': 'Speed',     # Channel 2 seems to be Speed (km/h) here
    '3': 'Gear',      # Channel 3 is Gear
    '4': 'Throttle',  # Channel 4 is Throttle (%)
    '5': 'Brake',     # Channel 5 is Brake (binary 0/1 or boolean?)
    '45': 'DRS'       # Channel 45 is DRS status (Needs mapping to useful values like 'Off', 'Eligible', 'On')
}

# --- Timing Table Column Definitions ---
# Moved here for central configuration
TIMING_TABLE_COLUMNS = [
    {"name": "Car", "id": "Car"},
    {"name": "Pos", "id": "Pos"},
    {"name": "Tyre", "id": "Tyre"},
    {"name": "Time", "id": "Time"},
    {"name": "Interval", "id": "Interval"},
    {"name": "Gap", "id": "Gap"},
    {"name": "Last Lap", "id": "Last Lap"},
    {"name": "Best Lap", "id": "Best Lap"},
    {"name": "S1", "id": "S1"},
    {"name": "S2", "id": "S2"},
    {"name": "S3", "id": "S3"},
    {"name": "Status", "id": "Status"},
    {'name': 'Speed', 'id': 'Speed', 'type': 'numeric'},
    {'name': 'Gear', 'id': 'Gear', 'type': 'numeric'},
    {'name': 'RPM', 'id': 'RPM', 'type': 'numeric'},
    {'name': 'DRS', 'id': 'DRS'},
]

# --- Other Config ---
# Example: Replay speed could be moved here if desired
# DEFAULT_REPLAY_SPEED = 1.0

print("DEBUG: config module loaded")
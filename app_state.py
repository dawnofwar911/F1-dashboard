
# app_state.py
"""
Module to hold shared application state variables and the main lock.
"""

import threading
import queue
import collections

# --- Core Application State ---
app_status = {
    "state": "Idle",  # e.g., Idle, Initializing, Connecting, Live, Replaying, Stopping, Stopped, Error
    "connection": "Disconnected", # e.g., Disconnected, Negotiating, Socket Connecting, Connected, Error, Closed Unexpectedly
    "subscribed_streams": [],
    "last_heartbeat": None,
    "current_replay_file": None, # Add field to track replay file
}
app_state_lock = threading.Lock() # Main lock for protecting shared data
stop_event = threading.Event()    # Event to signal threads to stop

# --- Data Queue ---
# Queue for passing raw/decoded messages from SignalR/Replay to processing loop
data_queue = queue.Queue()

# --- Data Storage ---
# Stores the latest message for each stream (used for non-timing display)
data_store = {}

# Holds the consolidated, persistent state per driver (Timing, Tyres, Telemetry, Position)
# Keyed by driver number (string)
timing_state = {}
# Example structure for timing_state[driver_num_str]:
# {
#     "RacingNumber": "1", "Tla": "VER", ...,
#     "PositionData": {"X": ..., "Y": ..., "Status": ..., "Timestamp": "..."},
#     "PreviousPositionData": {"X": ..., "Y": ..., "Timestamp": "..."}, # <<< NEW FIELD
#     "CarData": {"RPM": ..., "Speed": ..., "Utc": "..."}
#     # Other timing info (laps, sectors, etc.)
# }

# Specific state stores easily accessible by callbacks
track_status_data = {} # Holds latest TrackStatus (Status, Message)
session_details = {}   # Holds latest SessionInfo/SessionData (Meeting, Circuit, Name, Year, Status, etc.)
race_control_log = collections.deque(maxlen=50) # Holds recent RaceControlMessages strings
track_coordinates_cache = { # Holds track layout from API
    'x': None, 'y': None, 'range_x': None, 'range_y': None,
    'rotation': None, 'corner_x': None, 'corner_y': None, 'session_key': None
}
# Historical Telemetry Storage
# Structure: { driver_num: { lap_num: {'Timestamps': [], 'RPM': [], 'Speed': [], ...} } }
telemetry_data = {}
driver_info = {}
initial_replay_speed = 1.0
replay_speed = 1.0
# --- Live Recording State ---
live_data_file = None       # File handle for live recording
is_saving_active = False    # Flag indicating if recording is enabled and file is open
record_live_data = False # Default
current_recording_filename = None

# --- Note ---
# Thread objects (connection_thread, replay_thread, processing_thread, dash_thread) and
# the SignalR hub_connection object are typically managed in main.py where they are created/started.

print("DEBUG: app_state module loaded with shared variables")
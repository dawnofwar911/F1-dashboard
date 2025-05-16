
# app_state.py
"""
Module to hold shared application state variables and the main lock.
"""

import threading
import queue
import collections
import logging

# --- Core Application State ---
INITIAL_APP_STATUS = {
    "state": "Idle",
    "connection": "Disconnected",
    "subscribed_streams": [],
    "last_heartbeat": None,
    "current_replay_file": None,
}

app_status = INITIAL_APP_STATUS.copy() # Initialize with a copy
app_state_lock = threading.Lock()
stop_event = threading.Event()

# --- Data Queue ---
# Queue for passing raw/decoded messages from SignalR/Replay to processing loop
data_queue = queue.Queue()

# --- Data Storage ---
INITIAL_DATA_STORE = {}
data_store = INITIAL_DATA_STORE.copy()

INITIAL_TIMING_STATE = {}
timing_state = INITIAL_TIMING_STATE.copy()

INITIAL_LAP_TIME_HISTORY = {}
lap_time_history = INITIAL_LAP_TIME_HISTORY.copy()

INITIAL_TRACK_STATUS_DATA = {}
track_status_data = INITIAL_TRACK_STATUS_DATA.copy()

INITIAL_SESSION_DETAILS = {}
session_details = INITIAL_SESSION_DETAILS.copy()

INITIAL_RACE_CONTROL_LOG_MAXLEN = 50 # Store maxlen for re-creation
race_control_log = collections.deque(maxlen=INITIAL_RACE_CONTROL_LOG_MAXLEN)

INITIAL_TRACK_COORDINATES_CACHE = {
    'x': None, 'y': None, 'range_x': None, 'range_y': None,
    'rotation': None, 'corner_x': None, 'corner_y': None, 'session_key': None
}
track_coordinates_cache = INITIAL_TRACK_COORDINATES_CACHE.copy()

INITIAL_TELEMETRY_DATA = {}
telemetry_data = INITIAL_TELEMETRY_DATA.copy()

INITIAL_DRIVER_INFO = {}
driver_info = INITIAL_DRIVER_INFO.copy()

initial_replay_speed = 1.0 # This might not need resetting, or reset to a config default
replay_speed = 1.0         # Same as above

# --- Live Recording State ---
# These are generally managed by init_live_file and close_live_file,
# but ensuring they are reset is good practice.
live_data_file = None
is_saving_active = False
record_live_data = False # Default
current_recording_filename = None

logger = logging.getLogger("F1App.AppState") # Logger for this module

def reset_to_default_state():
    """
    Resets all relevant application state variables to their initial default values.
    This function should be called AFTER stopping any active connections or replays.
    It acquires the app_state_lock.
    """
    logger.info("Resetting application state to default...")
    with app_state_lock:
        global app_status, data_store, timing_state, lap_time_history, track_status_data
        global session_details, race_control_log, track_coordinates_cache, telemetry_data, driver_info
        global live_data_file, is_saving_active, current_recording_filename # record_live_data is a user setting

        app_status = INITIAL_APP_STATUS.copy()
        data_store = INITIAL_DATA_STORE.copy()
        timing_state = INITIAL_TIMING_STATE.copy()
        lap_time_history = INITIAL_LAP_TIME_HISTORY.copy()
        track_status_data = INITIAL_TRACK_STATUS_DATA.copy()
        session_details = INITIAL_SESSION_DETAILS.copy()
        
        # Re-initialize deque for race_control_log
        race_control_log.clear() # Clear existing items
        # If you need to change maxlen, you'd reassign:
        # race_control_log = collections.deque(maxlen=INITIAL_RACE_CONTROL_LOG_MAXLEN)

        track_coordinates_cache = INITIAL_TRACK_COORDINATES_CACHE.copy()
        telemetry_data = INITIAL_TELEMETRY_DATA.copy()
        driver_info = INITIAL_DRIVER_INFO.copy()

        # Clear data queue
        while not data_queue.empty():
            try:
                data_queue.get_nowait()
            except queue.Empty:
                break
        logger.debug("Data queue cleared.")

        # Reset recording state variables (though close_live_file should also do this)
        if live_data_file and not live_data_file.closed:
            try:
                logger.warning("Found an open live_data_file during reset. Attempting to close.")
                live_data_file.close()
            except Exception as e:
                logger.error(f"Error closing live_data_file during reset: {e}")
        live_data_file = None
        is_saving_active = False
        current_recording_filename = None
        # app_state.record_live_data is a user preference, typically not reset here unless intended

        # stop_event is generally managed by the consuming threads (start/stop actions)
        # If you want to ensure it's set during a reset, you can do:
        # if not stop_event.is_set():
        #     logger.debug("Setting stop_event during reset (if not already set).")
        #     stop_event.set()
        # However, usually stop_connection/stop_replay would have already set this.
        # Clearing it should happen when a *new* action (Connect/Replay) is initiated.

        logger.info("Application state has been reset to defaults.")

# --- Note ---
# Thread objects are managed in main.py

print("DEBUG: app_state module loaded with shared variables and reset function")
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

INITIAL_SESSION_DETAILS = {
    # ... other existing initial details ...
    'ScheduledDurationSeconds': None, # Will be calculated from SessionInfo's Start/End Dates
    'PreviousSessionStatus': None,    # For Q session resume logic
}
session_details = INITIAL_SESSION_DETAILS.copy()

INITIAL_RACE_CONTROL_LOG_MAXLEN = 50 # Store maxlen for re-creation
race_control_log = collections.deque(maxlen=INITIAL_RACE_CONTROL_LOG_MAXLEN)

INITIAL_TEAM_RADIO_MESSAGES_MAXLEN = 20 # Store latest 50 messages
team_radio_messages = collections.deque(maxlen=INITIAL_TEAM_RADIO_MESSAGES_MAXLEN)

INITIAL_TRACK_COORDINATES_CACHE = {
    'x': None, 'y': None, 'range_x': None, 'range_y': None,
    'rotation': None, 'corner_x': None, 'corner_y': None, 'session_key': None
}
track_coordinates_cache = INITIAL_TRACK_COORDINATES_CACHE.copy()

INITIAL_TELEMETRY_DATA = {}
telemetry_data = INITIAL_TELEMETRY_DATA.copy()

INITIAL_DRIVER_INFO = {} # Though this seems unused, keeping for consistency if planned
driver_info = INITIAL_DRIVER_INFO.copy()

initial_replay_speed = 1.0 # This might not need resetting, or reset to a config default
replay_speed = 1.0         # Same as above

# --- Live Recording State ---
live_data_file = None
is_saving_active = False
record_live_data = False # Default
current_recording_filename = None

INITIAL_EXTRAPOLATED_CLOCK_INFO = {
    "Utc": None,
    "Remaining": "00:00:00",  # Default value
    "Extrapolating": False,
    "Timestamp": None  # To store when we received the data
}
extrapolated_clock_info = INITIAL_EXTRAPOLATED_CLOCK_INFO.copy()

INITIAL_QUALIFYING_SEGMENT_STATE = {
    "old_segment": None,
    "current_segment": None,  # e.g., "Q1", "Q2", "Q3", "SQ1", "SQ2", "SQ3", "Between Segments", "Ended"
    "official_segment_remaining_seconds": 0, # Time from ExtrapolatedClock when segment started/synced
    "last_official_time_capture_utc": None,  # datetime object (wall clock UTC)
    "last_capture_replay_speed": 1.0,        # Replay speed at time of capture
    "just_resumed_flag": False,
    "session_status_at_capture": None        # e.g. "Started", "Running"
}
qualifying_segment_state = INITIAL_QUALIFYING_SEGMENT_STATE.copy()

# --- Practice Session Timing ---
practice_session_actual_start_utc = None
practice_session_scheduled_duration_seconds = None # e.g., 3600 for a 60-minute session

# --- Replay Feed Pacing ---
current_processed_feed_timestamp_utc_dt = None # datetime object of the latest processed message
session_start_feed_timestamp_utc_dt = None     # datetime of the first key message for current session/segment clock start in replay
current_segment_scheduled_duration_seconds = None # Duration of the current timed segment for replay

# --- Session Best Times ---
INITIAL_SESSION_BESTS = {
    "OverallBestLapTime": {"Value": None, "DriverNumber": None},
    "OverallBestSectors": [
        {"Value": None, "DriverNumber": None}, # Sector 1
        {"Value": None, "DriverNumber": None}, # Sector 2
        {"Value": None, "DriverNumber": None}  # Sector 3
    ]
}
session_bests = INITIAL_SESSION_BESTS.copy()

last_known_total_laps = None

INITIAL_LAST_KNOWN_OVERALL_WEATHER_CONDITION = "default"
INITIAL_LAST_KNOWN_WEATHER_CARD_COLOR = "light"
INITIAL_LAST_KNOWN_WEATHER_CARD_INVERSE = False
INITIAL_LAST_KNOWN_MAIN_WEATHER_ICON_KEY = "default" # Store the key for the icon
INITIAL_LAST_KNOWN_AIR_TEMP = None
INITIAL_LAST_KNOWN_TRACK_TEMP = None
INITIAL_LAST_KNOWN_HUMIDITY = None
INITIAL_LAST_KNOWN_PRESSURE = None
INITIAL_LAST_KNOWN_WIND_SPEED = None
INITIAL_LAST_KNOWN_WIND_DIRECTION = None
INITIAL_LAST_KNOWN_RAINFALL_VAL = None # For the "RAIN" text persistence

last_known_overall_weather_condition = INITIAL_LAST_KNOWN_OVERALL_WEATHER_CONDITION
last_known_weather_card_color = INITIAL_LAST_KNOWN_WEATHER_CARD_COLOR
last_known_weather_card_inverse = INITIAL_LAST_KNOWN_WEATHER_CARD_INVERSE
last_known_main_weather_icon_key = INITIAL_LAST_KNOWN_MAIN_WEATHER_ICON_KEY
last_known_air_temp = INITIAL_LAST_KNOWN_AIR_TEMP
last_known_track_temp = INITIAL_LAST_KNOWN_TRACK_TEMP
last_known_humidity = INITIAL_LAST_KNOWN_HUMIDITY
last_known_pressure = INITIAL_LAST_KNOWN_PRESSURE
last_known_wind_speed = INITIAL_LAST_KNOWN_WIND_SPEED
last_known_wind_direction = INITIAL_LAST_KNOWN_WIND_DIRECTION
last_known_rainfall_val = INITIAL_LAST_KNOWN_RAINFALL_VAL

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
        global live_data_file, is_saving_active, current_recording_filename
        global session_bests
        global extrapolated_clock_info
        global last_known_total_laps 
        global last_known_overall_weather_condition, last_known_weather_card_color
        global last_known_weather_card_inverse, last_known_main_weather_icon_key
        global last_known_air_temp, last_known_track_temp, last_known_humidity
        global last_known_pressure, last_known_wind_speed, last_known_wind_direction
        global last_known_rainfall_val
        global current_processed_feed_timestamp_utc_dt, session_start_feed_timestamp_utc_dt
        global current_segment_scheduled_duration_seconds
        global qualifying_segment_state
        global practice_session_actual_start_utc

        app_status = INITIAL_APP_STATUS.copy()
        data_store = INITIAL_DATA_STORE.copy()
        timing_state = INITIAL_TIMING_STATE.copy()
        lap_time_history = INITIAL_LAP_TIME_HISTORY.copy()
        track_status_data = INITIAL_TRACK_STATUS_DATA.copy()
        session_details = INITIAL_SESSION_DETAILS.copy()
        extrapolated_clock_info = INITIAL_EXTRAPOLATED_CLOCK_INFO.copy() # Reset it
        qualifying_segment_state = INITIAL_QUALIFYING_SEGMENT_STATE.copy() # Reset new state
        
        race_control_log.clear()
        team_radio_messages.clear()

        track_coordinates_cache = INITIAL_TRACK_COORDINATES_CACHE.copy()
        telemetry_data = INITIAL_TELEMETRY_DATA.copy()
        driver_info = INITIAL_DRIVER_INFO.copy() # <<< ENSURE THIS IS RESET IF USED

        session_bests = INITIAL_SESSION_BESTS.copy() # <<< ADDED RESET
        last_known_total_laps = None
        
        last_known_overall_weather_condition = INITIAL_LAST_KNOWN_OVERALL_WEATHER_CONDITION
        last_known_weather_card_color = INITIAL_LAST_KNOWN_WEATHER_CARD_COLOR
        last_known_weather_card_inverse = INITIAL_LAST_KNOWN_WEATHER_CARD_INVERSE
        last_known_main_weather_icon_key = INITIAL_LAST_KNOWN_MAIN_WEATHER_ICON_KEY
        last_known_air_temp = INITIAL_LAST_KNOWN_AIR_TEMP
        last_known_track_temp = INITIAL_LAST_KNOWN_TRACK_TEMP
        last_known_humidity = INITIAL_LAST_KNOWN_HUMIDITY
        last_known_pressure = INITIAL_LAST_KNOWN_PRESSURE
        last_known_wind_speed = INITIAL_LAST_KNOWN_WIND_SPEED
        last_known_wind_direction = INITIAL_LAST_KNOWN_WIND_DIRECTION
        last_known_rainfall_val = INITIAL_LAST_KNOWN_RAINFALL_VAL
        
        current_processed_feed_timestamp_utc_dt = None
        session_start_feed_timestamp_utc_dt = None
        current_segment_scheduled_duration_seconds = None
    
        practice_session_actual_start_utc = None #
        while not data_queue.empty():
            try:
                data_queue.get_nowait()
            except queue.Empty:
                break
        logger.debug("Data queue cleared.")

        if live_data_file and not live_data_file.closed:
            try:
                logger.warning("Found an open live_data_file during reset. Attempting to close.")
                live_data_file.close()
            except Exception as e:
                logger.error(f"Error closing live_data_file during reset: {e}")
        live_data_file = None
        is_saving_active = False
        current_recording_filename = None
        
        logger.info("Application state has been reset to defaults.")

print("DEBUG: app_state module loaded")
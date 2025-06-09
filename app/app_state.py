# app_state.py
"""
Module to hold shared application state variables, now per-session.
"""
import threading
import queue  # For queue.Queue
import collections  # For collections.deque
import logging
from copy import deepcopy
import uuid
import flask  # Required for accessing Flask's session object
from typing import Dict, Optional, Set, Deque, List, Any  # Import necessary types

# Logger for this module
logger = logging.getLogger("F1App.AppState")

# --- Constants for Initializing Session State ---
# (INITIAL_SESSION_* constants remain the same as in Response #13)
INITIAL_SESSION_APP_STATUS: Dict[str, Any] = {  # Added type hint
    "state": "Idle",
    "connection": "Disconnected",
    "subscribed_streams": [],
    "last_heartbeat": None,
    "current_replay_file": None,
    "auto_connected_session_identifier": None,
    "auto_connected_session_end_detected_utc": None,
}

INITIAL_SESSION_DATA_STORE: Dict = {}  # Example type hint
INITIAL_SESSION_TIMING_STATE: Dict = {}
INITIAL_SESSION_LAP_TIME_HISTORY: Dict = {}
INITIAL_SESSION_TRACK_STATUS_DATA: Dict = {}
INITIAL_SESSION_SESSION_DETAILS: Dict[str, Any] = {
    'ScheduledDurationSeconds': None, 'PreviousSessionStatus': None, 'Year': None,
    'CircuitKey': None, 'CircuitName': "", 'EventName': "", 'SessionName': "",
    'SessionKey': None, 'Path': "", 'Type': "", 'SessionStartTimeUTC': None,
    'SessionStatus': 'Unknown', 'Meeting': {}, 'ArchiveStatus': {}, 'Key': None,
    'Number': None, 'Name': "", 'GmtOffset': None, 'Location': ""
}
INITIAL_SESSION_EXTRAPOLATED_CLOCK_INFO: Dict[str, Any] = {
    "Utc": None, "Remaining": "00:00:00", "Extrapolating": False, "Timestamp": None
}
INITIAL_SESSION_QUALIFYING_SEGMENT_STATE: Dict[str, Any] = {
    "old_segment": None, "current_segment": None, "official_segment_remaining_seconds": 0,
    "last_official_time_capture_utc": None, "last_capture_replay_speed": 1.0,
    "just_resumed_flag": False, "session_status_at_capture": None
}
INITIAL_SESSION_SESSION_BESTS: Dict[str, Any] = {
    "OverallBestLapTime": {"Value": None, "DriverNumber": None},
    "OverallBestSectors": [{"Value": None, "DriverNumber": None} for _ in range(3)]
}
INITIAL_SESSION_TRACK_COORDINATES_CACHE: Dict[str, Any] = {
    'x': None, 'y': None, 'range_x': None, 'range_y': None, 'rotation': None,
    'session_key': None, 'corners_data': None, 'marshal_lights_data': None,
    'marshal_sector_points': None, 'marshal_sector_segments': None
}
INITIAL_RACE_CONTROL_LOG_MAXLEN: int = 50
INITIAL_TEAM_RADIO_MESSAGES_MAXLEN: int = 20
INITIAL_ACTIVE_YELLOW_SECTORS: Set[Any] = set()  # Example type hint
INITIAL_TELEMETRY_DATA: Dict = {}
INITIAL_DRIVER_STINT_DATA: Dict = {}
INITIAL_DRIVER_INFO: Dict = {}


# --- Per-Session State Class ---
class SessionState:
    def __init__(self, session_id: str):
        self.session_id: str = session_id
        self.lock: threading.RLock = threading.RLock()

        self.app_status: Dict[str, Any] = deepcopy(INITIAL_SESSION_APP_STATUS)
        self.stop_event: threading.Event = threading.Event()
        # type: ignore[type-arg] # If using older queue version
        self.data_queue: queue.Queue = queue.Queue()
        self.data_store: Dict[str, Any] = deepcopy(INITIAL_SESSION_DATA_STORE)
        self.timing_state: Dict[str, Any] = deepcopy(
            INITIAL_SESSION_TIMING_STATE)
        self.lap_time_history: Dict[str, Any] = deepcopy(
            INITIAL_SESSION_LAP_TIME_HISTORY)
        self.track_status_data: Dict[str, Any] = deepcopy(
            INITIAL_SESSION_TRACK_STATUS_DATA)
        self.session_details: Dict[str, Any] = deepcopy(
            INITIAL_SESSION_SESSION_DETAILS)
        self.race_control_log: Deque[str] = collections.deque(
            maxlen=INITIAL_RACE_CONTROL_LOG_MAXLEN)
        self.team_radio_messages: Deque[Dict[str, Any]] = collections.deque(
            maxlen=INITIAL_TEAM_RADIO_MESSAGES_MAXLEN)  # Assuming dicts
        self.track_coordinates_cache: Dict[str, Any] = deepcopy(
            INITIAL_SESSION_TRACK_COORDINATES_CACHE)
        self.active_yellow_sectors: Set[Any] = deepcopy(
            INITIAL_ACTIVE_YELLOW_SECTORS)
        self.telemetry_data: Dict[str, Any] = deepcopy(INITIAL_TELEMETRY_DATA)
        self.driver_stint_data: Dict[str, Any] = deepcopy(
            INITIAL_DRIVER_STINT_DATA)
        self.driver_info: Dict[str, Any] = deepcopy(INITIAL_DRIVER_INFO)
        self.replay_speed: float = 1.0

        # Assuming it's a file-like object, replace Any with actual type
        self.live_data_file: Optional[Any] = None
        self.is_saving_active: bool = False
        self.record_live_data: bool = False
        self.current_recording_filename: Optional[str] = None  # CORRECTED

        self.extrapolated_clock_info: Dict[str, Any] = deepcopy(
            INITIAL_SESSION_EXTRAPOLATED_CLOCK_INFO)
        self.qualifying_segment_state: Dict[str, Any] = deepcopy(
            INITIAL_SESSION_QUALIFYING_SEGMENT_STATE)
        # Replace Any with datetime if that's the type
        self.practice_session_actual_start_utc: Optional[Any] = None
        self.practice_session_scheduled_duration_seconds: Optional[int] = None
        # Replace Any with datetime
        self.current_processed_feed_timestamp_utc_dt: Optional[Any] = None
        # Replace Any with datetime
        self.session_start_feed_timestamp_utc_dt: Optional[Any] = None
        self.current_segment_scheduled_duration_seconds: Optional[int] = None
        self.session_bests: Dict[str, Any] = deepcopy(
            INITIAL_SESSION_SESSION_BESTS)
        self.last_known_total_laps: Optional[int] = None

        self.last_known_overall_weather_condition: str = "default"
        self.last_known_weather_card_color: str = "light"
        self.last_known_weather_card_inverse: bool = False
        self.last_known_main_weather_icon_key: str = "default"
        self.last_known_air_temp: Optional[float] = None
        self.last_known_track_temp: Optional[float] = None
        self.last_known_humidity: Optional[float] = None
        self.last_known_pressure: Optional[float] = None
        self.last_known_wind_speed: Optional[float] = None
        self.last_known_wind_direction: Optional[int] = None  # Or str
        self.last_known_rainfall_val: Optional[str] = None  # Or int

        # Assuming driver number as str
        self.selected_driver_for_map_and_lap_chart: Optional[str] = None

        self.connection_thread: Optional[threading.Thread] = None  # CORRECTED
        # Replace Any with actual HubConnection type if available
        self.hub_connection: Optional[Any] = None
        self.replay_thread: Optional[threading.Thread] = None  # CORRECTED
        # CORRECTED
        self.data_processing_thread: Optional[threading.Thread] = None

        self.auto_connect_enabled: bool = False
        # CORRECTED
        self.auto_connect_thread: Optional[threading.Thread] = None
        self.track_data_fetch_thread: Optional[threading.Thread] = None # ADD THIS LINE

        logger.info(
            f"Initialized new SessionState for session_id: {self.session_id}")

    def reset_state_variables(self):
        # (Implementation of reset_state_variables as in Response #13)
        # Ensure all attributes are reset according to their types defined above
        with self.lock:
            self.app_status = deepcopy(INITIAL_SESSION_APP_STATUS)
            while not self.data_queue.empty():
                try:
                    self.data_queue.get_nowait()
                except queue.Empty:
                    break
            self.data_store = deepcopy(INITIAL_SESSION_DATA_STORE)
            self.timing_state = deepcopy(INITIAL_SESSION_TIMING_STATE)
            self.lap_time_history = deepcopy(INITIAL_SESSION_LAP_TIME_HISTORY)
            self.track_status_data = deepcopy(
                INITIAL_SESSION_TRACK_STATUS_DATA)
            self.session_details = deepcopy(INITIAL_SESSION_SESSION_DETAILS)
            self.race_control_log.clear()
            self.team_radio_messages.clear()
            self.track_coordinates_cache = deepcopy(
                INITIAL_SESSION_TRACK_COORDINATES_CACHE)
            self.active_yellow_sectors = deepcopy(
                INITIAL_ACTIVE_YELLOW_SECTORS)
            self.telemetry_data = deepcopy(INITIAL_TELEMETRY_DATA)
            self.driver_stint_data = deepcopy(INITIAL_DRIVER_STINT_DATA)
            self.driver_info = deepcopy(INITIAL_DRIVER_INFO)
            self.replay_speed = 1.0
            if self.live_data_file and not self.live_data_file.closed:
                try:
                    logger.warning(
                        f"Session {self.session_id}: Found an open live_data_file during reset. Attempting to close.")
                    self.live_data_file.close()
                except Exception as e:
                    logger.error(
                        f"Session {self.session_id}: Error closing live_data_file during reset: {e}")
            self.live_data_file = None
            self.is_saving_active = False
            self.record_live_data = False
            self.current_recording_filename = None
            self.extrapolated_clock_info = deepcopy(
                INITIAL_SESSION_EXTRAPOLATED_CLOCK_INFO)
            self.qualifying_segment_state = deepcopy(
                INITIAL_SESSION_QUALIFYING_SEGMENT_STATE)
            self.practice_session_actual_start_utc = None
            self.practice_session_scheduled_duration_seconds = None
            self.current_processed_feed_timestamp_utc_dt = None
            self.session_start_feed_timestamp_utc_dt = None
            self.current_segment_scheduled_duration_seconds = None
            self.session_bests = deepcopy(INITIAL_SESSION_SESSION_BESTS)
            self.last_known_total_laps = None
            self.last_known_overall_weather_condition = "default"
            self.last_known_weather_card_color = "light"
            self.last_known_weather_card_inverse = False
            self.last_known_main_weather_icon_key = "default"
            self.last_known_air_temp = None
            self.last_known_track_temp = None
            self.last_known_humidity = None
            self.last_known_pressure = None
            self.last_known_wind_speed = None
            self.last_known_wind_direction = None
            self.last_known_rainfall_val = None
            self.selected_driver_for_map_and_lap_chart = None
            self.auto_connect_thread = None
            self.track_data_fetch_thread = None # ADD THIS LINE
            logger.info(
                f"Session {self.session_id}: State variables have been reset to defaults.")


# --- Global Session Management ---
SESSIONS_STORE: Dict[str, SessionState] = {}  # Added type hint
SESSIONS_STORE_LOCK: threading.Lock = threading.Lock()


def get_current_session_id() -> Optional[str]:  # CORRECTED
    """
    Retrieves the user_app_session_id from Flask's session context.
    Creates and stores a new ID in Flask's session if one is not present.
    Returns the session ID string, or None if not in a request context.
    """
    try:
        if not flask.has_request_context():
            logger.debug(
                "get_current_session_id called outside of a Flask request context.")
            return None

        session_id = flask.session.get('user_app_session_id')
        if not session_id:
            session_id = str(uuid.uuid4())
            flask.session['user_app_session_id'] = session_id
            logger.info(
                f"Generated new Flask session ID and stored: {session_id}")
        return session_id
    except RuntimeError as e:
        logger.error(
            f"Error accessing Flask session context in get_current_session_id: {e}")
        return None


# CORRECTED
def get_session_state(session_id: Optional[str] = None) -> Optional[SessionState]:
    """
    Gets the state object for a given session_id. If session_id is None, uses current Flask session.
    Returns None if the session_id cannot be determined or the state object is not found.
    """
    resolved_session_id = session_id or get_current_session_id()
    if not resolved_session_id:
        return None
    with SESSIONS_STORE_LOCK:
        return SESSIONS_STORE.get(resolved_session_id)


# CORRECTED
def get_or_create_session_state(session_id: Optional[str] = None) -> Optional[SessionState]:
    """
    Gets or creates the state object for a session_id.
    If session_id is None, uses current Flask session (creating the Flask session ID if necessary).
    Returns the SessionState object, or None if a session_id cannot be established.
    """
    resolved_session_id = session_id or get_current_session_id()
    if not resolved_session_id:
        logger.error(
            "get_or_create_session_state: Critical - Could not establish a session_id.")
        return None

    with SESSIONS_STORE_LOCK:
        if resolved_session_id not in SESSIONS_STORE:
            logger.info(
                f"Session_id '{resolved_session_id}' not in SESSIONS_STORE. Creating new SessionState.")
            SESSIONS_STORE[resolved_session_id] = SessionState(
                resolved_session_id)
        return SESSIONS_STORE[resolved_session_id]


def remove_session_state(session_id: str):
    # (Implementation as in Response #13)
    if not session_id:
        logger.warning(
            "Attempted to remove session state with an empty session_id.")
        return
    with SESSIONS_STORE_LOCK:
        if session_id in SESSIONS_STORE:
            logger.info(
                f"Removing SessionState object from SESSIONS_STORE for session_id: {session_id}")
            del SESSIONS_STORE[session_id]
        else:
            logger.warning(
                f"Attempted to remove non-existent session_id from SESSIONS_STORE: {session_id}")


print("DEBUG: app_state.py (multi-session structure with corrected type hints) loaded.")

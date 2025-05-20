# config.py
"""
Configuration constants for the F1 Telemetry Dashboard application.
"""

import os
from pathlib import Path

# --- Core Application & Server ---
DASH_DEBUG_MODE = False
DASH_HOST = "0.0.0.0"
DASH_PORT = 8050


# --- File Paths ---
_SCRIPT_DIR = Path(__file__).parent.resolve()
REPLAY_DIR_NAME = "replays"
REPLAY_DIR = _SCRIPT_DIR / REPLAY_DIR_NAME
TARGET_SAVE_DIRECTORY = REPLAY_DIR # Directory for saving live data files
DEFAULT_REPLAY_FILENAME = "2023-yas-marina-quali.data.txt" # Default replay file suggestion
FASTF1_CACHE_DIR = _SCRIPT_DIR / "ff1_cache" # Cache directory for FastF1

# --- Filename Templates ---
DATA_FILENAME_TEMPLATE = "f1_signalr_data_{timestamp}.data.txt" # Not currently used directly for replay saving
LIVE_DATA_FILENAME_FALLBACK_PREFIX = "F1LiveData"
# DATABASE_FILENAME_TEMPLATE = "f1_signalr_data_{timestamp}.db" # If DB functionality added later

# --- SignalR Connection ---
NEGOTIATE_URL_BASE = "https://livetiming.formula1.com/signalr"
WEBSOCKET_URL_BASE = "wss://livetiming.formula1.com/signalr"
HUB_NAME = "Streaming"
STREAMS_TO_SUBSCRIBE = ["Heartbeat",
        "CarData.z",
        "Position.z",
        "ExtrapolatedClock",
        "TopThree",
        "RcmSeries",
        "TimingStats",
        "TimingAppData",
        "WeatherData",
        "TrackStatus",
        "SessionStatus",
        "DriverList",
        "RaceControlMessages",
        "SessionInfo",
        "SessionData",
        "LapCount",
        "TimingData",
        "TeamRadio",
        "PitLaneTimeCollection",
        "ChampionshipPrediction"]
SIGNALR_CLIENT_PROTOCOL = "1.5"
REQUESTS_TIMEOUT_SECONDS = 15
USER_AGENT_NEGOTIATE = "Python SignalRClient"
USER_AGENT_WEBSOCKET = "BestHTTP" # Match F1 expectations

# --- Telemetry Channel Mapping ---
CHANNEL_MAP = {
    '0': 'RPM',
    '2': 'Speed',
    '3': 'Gear',
    '4': 'Throttle',
    '5': 'Brake',
    '45': 'DRS'
}

# --- Timing Table Column Definitions ---
TIMING_TABLE_COLUMNS_CONFIG = [
    {'name': 'Pos', 'id': 'Pos'},
    {'name': 'No.', 'id': 'No.'},
    {'name': 'Driver', 'id': 'Car'},      # 'Car' is the ID for TLA
    {'name': 'Tyre', 'id': 'Tyre'},
    # New combined column for Interval and Gap
    {'name': 'Int / Gap', 'id': 'IntervalGap', 'presentation': 'markdown'}, # ADD THIS
    {'name': 'Last Lap', 'id': 'Last Lap'},
    {'name': 'Best Lap', 'id': 'Best Lap'},
    {'name': 'S1', 'id': 'S1'},
    {'name': 'S2', 'id': 'S2'},
    {'name': 'S3', 'id': 'S3'},
    {'name': 'Pits', 'id': 'Pits'},        # Will be filtered out if not Race/Sprint
    {'name': 'Status', 'id': 'Status'},
    # The individual 'Interval' and 'Gap' columns have been removed.
]

# --- UI Constants: Text & Messages ---
# General
APP_TITLE = "F1 Timing Dashboard"
STATUS_INITIALIZING = "Status: Initializing..."
TEXT_WAITING_FOR_DATA = "Waiting for data..."
TEXT_LOADING_DATA = "Loading data..."
TEXT_ERROR_UPDATING = "Error updating!" # Generic update error
TEXT_ERROR_LOADING = "Error loading!"   # Generic loading error

# Connection Status
TEXT_CONN_STATUS_DEFAULT = "State: ? | Conn: ?"
TEXT_CONN_STATUS_ERROR_UPDATE = "Error updating status!"

# Session Info
TEXT_SESSION_INFO_AWAITING = "Session Info: Awaiting data..."
TEXT_SESSION_INFO_ERROR = "Error: Session Info"
TEXT_WEATHER_AWAITING = "Weather: Loading..." # Used in layout.py
TEXT_WEATHER_ERROR = "Error: Weather"
TEXT_WEATHER_UNAVAILABLE = "Weather data unavailable"
TEXT_WEATHER_CONDITION_GENERIC = "{condition} conditions" # For placeholder like "Cloudy conditions"
FASTF1_ONGOING_SESSION_WINDOW_HOURS = 3

# Track Status
TEXT_TRACK_STATUS_DEFAULT_LABEL = "CLEAR" # Used in layout.py

# Lap Counter
TEXT_LAP_COUNTER_DEFAULT = "Lap: -/-"
TEXT_LAP_COUNTER_AWAITING = "Lap: Awaiting Data..."

# Pit Data
TEXT_PIT_OUT_DISPLAY = "Pit Out"

# Session Types
SESSION_TYPE_RACE = "Race"
SESSION_TYPE_SPRINT = "Sprint" # Assuming "Sprint" is the exact string used for Sprint sessions. Adjust if necessary.
SESSION_TYPE_QUALI = "Qualifying"
SESSION_TYPE_PRAFTICE = "Practice"
 

# Race Control
TEXT_RC_WAITING = "Waiting for Race Control messages..."
TEXT_RC_ERROR = "Error loading RC log."

# Replay Control
TEXT_REPLAY_SELECT_FILE = "Select replay file..."
TEXT_REPLAY_ALREADY_RUNNING = "Replay already in progress. Please stop the current replay first."
TEXT_REPLAY_FILE_NOT_FOUND_ERROR_PREFIX = "Replay file not found or not a file: "
TEXT_REPLAY_CLEARING_STATE = "Replay mode: Clearing previous state..."
TEXT_REPLAY_STATE_CLEARED = "Replay mode: Previous state cleared."
TEXT_REPLAY_ERROR_FILE_NOT_FOUND_STATUS = "File Not Found"
TEXT_REPLAY_ERROR_THREAD_START_FAILED_STATUS = "Replay Thread Failed Start"
TEXT_REPLAY_LOG_PREFIX_FALLBACK = "F1LiveData"
# Replay Thread Statuses
REPLAY_STATUS_RUNNING = "Running"
REPLAY_STATUS_STOPPED = "Stopped"
REPLAY_STATUS_COMPLETE = "Complete"
REPLAY_STATUS_ERROR_FILE_NOT_FOUND = "Error - File Not Found"
REPLAY_STATUS_ERROR_RUNTIME = "Error - Runtime"
REPLAY_STATUS_CONNECTION_REPLAY_ENDED = "Disconnected (Replay Ended)"
REPLAY_STATUS_CONNECTION_REPLAY_STOPPED = "Disconnected (Replay Stopped)"

# SignalR Client
TEXT_SIGNALR_NEGOTIATION_TIMEOUT = "Negotiation timeout."
TEXT_SIGNALR_NEGOTIATION_HTTP_FAIL_PREFIX = "Negotiation HTTP fail: "
TEXT_SIGNALR_NEGOTIATION_ERROR_PREFIX = "Negotiation error: "
TEXT_SIGNALR_BUILD_HUB_FAILED = "Failed to build valid HubConnection object."
TEXT_SIGNALR_SUBSCRIPTION_ERROR_STATUS = "Subscription Error"
TEXT_SIGNALR_HUB_OBJECT_MISSING_STATUS = "Hub object missing"
TEXT_SIGNALR_CLOSED_UNEXPECTEDLY_STATUS = "Closed Unexpectedly"
TEXT_SIGNALR_ERROR_STATUS_PREFIX = "SignalR Error: "
TEXT_SIGNALR_THREAD_ERROR_STATUS_PREFIX = "Thread Error: "
TEXT_SIGNALR_SOCKET_CONNECTING_STATUS = "Socket Connecting"
TEXT_SIGNALR_SOCKET_CONNECTED_SUBSCRIBING_STATUS = "Socket Connected - Subscribing"
TEXT_SIGNALR_CONNECTED_SUBSCRIBED_STATUS = "Connected & Subscribed"
TEXT_SIGNALR_DISCONNECTING_STATUS = "Disconnecting"
TEXT_SIGNALR_DISCONNECTED_STATUS = "Disconnected"
TEXT_SIGNALR_DISCONNECTED_THREAD_END_STATUS = "Disconnected / Thread End"


# Driver/Telemetry Display
TEXT_DRIVER_SELECT = "Select a driver."
TEXT_DRIVER_SELECT_LAP = "Select driver & lap"
TEXT_DRIVER_NO_LAP_DATA_PREFIX = "No lap data for " # Driver TLA/num will be appended
TEXT_DRIVER_SELECT_A_LAP_PREFIX = "Select a lap for " # Driver TLA/num will be appended
TEXT_TELEMETRY_ERROR = "Error loading telemetry."
TEXT_TELEMETRY_NO_PLOT_DATA_FOR_LAP_PREFIX = "No plot data for Lap " # Lap num will be appended
TEXT_NO_DRIVERS_AVAILABLE = "No drivers available"
TEXT_ERROR_LOADING_DRIVERS = "Error loading drivers"
TEXT_NO_DRIVERS_PROCESSED = "No drivers processed"


# Track Map
TEXT_TRACK_MAP_LOADING = "Loading track data..."
TEXT_TRACK_MAP_LOADING_FOR_SESSION_PREFIX = "Track data loading for " # Session ID will be appended
TEXT_TRACK_MAP_DATA_WILL_LOAD = "Track data will load when session is active."

# Lap Progression Chart
TEXT_LAP_PROG_SELECT_DRIVERS = "Select drivers for lap progression"
TEXT_LAP_PROG_NO_DATA = "No lap data for selected driver(s)."
TEXT_LAP_CHART_SELECT_DRIVERS_PLACEHOLDER = "Select drivers for lap chart..."


# Dropdown Defaults
DROPDOWN_NO_LAPS_OPTIONS = [{'label': 'No Laps', 'value': ''}]
DROPDOWN_NO_DRIVERS_OPTIONS = [{'label': TEXT_NO_DRIVERS_AVAILABLE, 'value': '', 'disabled': True}]
DROPDOWN_ERROR_LOADING_DRIVERS_OPTIONS = [{'label': TEXT_ERROR_LOADING_DRIVERS, 'value': '', 'disabled': True}]
DROPDOWN_NO_DRIVERS_PROCESSED_OPTIONS = [{'label': TEXT_NO_DRIVERS_PROCESSED, 'value': '', 'disabled': True}]

# --- UI Constants: Layout & Styling ---
# Graph/Plot uirevision constants
INITIAL_TRACK_MAP_UIREVISION = 'track_map_main_layout_v2' # Added v2 to ensure change if old one cached
INITIAL_TELEMETRY_UIREVISION = 'telemetry_main_layout_v2'
INITIAL_LAP_PROG_UIREVISION = 'lap_prog_main_layout_v2'

# Wrapper Heights (in pixels)
TRACK_MAP_WRAPPER_HEIGHT = 360
TELEMETRY_WRAPPER_HEIGHT = 320
LAP_PROG_WRAPPER_HEIGHT = 320
DRIVER_DETAILS_HEIGHT = 80 # For the driver details text box under telemetry

# Plot Margins (Plotly margin dict format)
TRACK_MAP_MARGINS = {'l': 2, 'r': 2, 't': 2, 'b': 2}
TELEMETRY_MARGINS_EMPTY = {'l': 30, 'r': 5, 't': 10, 'b': 20}
TELEMETRY_MARGINS_DATA = {'l': 35, 'r': 10, 't': 30, 'b': 30}
LAP_PROG_MARGINS_EMPTY = {'l': 35, 'r': 5, 't': 20, 'b': 30}
LAP_PROG_MARGINS_DATA = {'l': 40, 'r': 10, 't': 30, 'b': 40}

# Track Status Styling
TRACK_STATUS_STYLES = {
    '1': {"label": "CLEAR", "card_color": "success", "text_color": "white"},
    '2': {"label": "YELLOW", "card_color": "warning", "text_color": "black"},
    '3': {"label": "SC DEPLOYED?", "card_color": "warning", "text_color": "black"}, # SC Expected / Deployed
    '4': {"label": "SAFETY CAR", "card_color": "warning", "text_color": "black"},
    '5': {"label": "RED FLAG", "card_color": "danger", "text_color": "white"},
    '6': {"label": "VSC DEPLOYED", "card_color": "info", "text_color": "white"},
    '7': {"label": "VSC ENDING", "card_color": "info", "text_color": "white"},
    'DEFAULT': {"label": "UNKNOWN", "card_color": "secondary", "text_color": "white"}
}

# Weather Icon Mapping
WEATHER_ICON_MAP = {
    "sunny": "☀️", "cloudy": "☁️", "overcast": "🌥️",
    "rain": "🌧️", "drizzle": "🌦️", "windy": "💨",
    "default": "🌡️"
}

# --- Logging Configuration ---
LOG_FORMAT_DEFAULT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FORMAT_RAW_MESSAGE = '%(message)s' # For replay file saving
LOG_REPLAY_FILE_HEADER_TS_FORMAT = "%Y%m%d_%H%M%S%Z"
LOG_REPLAY_FILE_START_MSG_PREFIX = "# Recording Started: "
LOG_REPLAY_FILE_SESSION_INFO_PREFIX = "# Session Info (from FastF1 at start): "
LOG_REPLAY_FILE_STOP_MSG_PREFIX = "\n# Recording Stopped: "


# --- API URLs (other than SignalR) ---
MULTIVIEWER_CIRCUIT_API_URL_TEMPLATE = "https://api.multiviewer.app/api/v1/circuits/{circuit_key}/{year}"
MULTIVIEWER_API_USER_AGENT = 'F1-Dash/0.5' # Increment version or make more dynamic

print("DEBUG: config module loaded")
# app/settings.py
"""
User-configurable settings for the F1 Telemetry Dashboard application.
"""

import os
from pathlib import Path

# --- User Settings ---
USE_MPH = False
HIDE_RETIRED_DRIVERS = False
KPH_TO_MPH_FACTOR = 0.621371

# --- Admin and Session Management ---
# Password for the admin settings panel, set via environment variable
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', None)

# How long a session can be inactive before being removed (in hours)
SESSION_TIMEOUT_HOURS = 12

# How often the garbage collector runs to check for stale sessions (in minutes)
SESSION_CLEANUP_INTERVAL_MINUTES = 60

# --- Core Application & Server ---
DASH_ENV = os.environ.get('DASH_ENV', 'development')
IS_PRODUCTION = DASH_ENV == 'production'
DASH_DEBUG_MODE = not IS_PRODUCTION
DASH_HOST = os.environ.get('DASH_HOST', '0.0.0.0')
DASH_PORT = int(os.environ.get('DASH_PORT', 8050))

# --- File Paths ---
_SCRIPT_DIR = Path(__file__).parent.resolve()
REPLAY_DIR = Path(os.environ.get('REPLAY_DIR', _SCRIPT_DIR / 'replays'))
TARGET_SAVE_DIRECTORY = Path(os.environ.get('TARGET_SAVE_DIRECTORY', REPLAY_DIR))
FASTF1_CACHE_DIR = Path(os.environ.get('FASTF1_CACHE_DIR', _SCRIPT_DIR / 'ff1_cache'))
SETTINGS_FILE_PATH = TARGET_SAVE_DIRECTORY / 'settings.json'

# --- Default Global Settings ---
# These are the settings that will be used if the settings.json file is missing
# or if a new setting is added to the app.
DEFAULT_GLOBAL_SETTINGS = {
    'record_live_sessions': False,
    # Add new settings here with their default values
}

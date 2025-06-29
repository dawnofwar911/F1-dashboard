# config.py
"""
Configuration constants for the F1 Telemetry Dashboard application.
"""

import logging

# --- Logging Configuration ---
LOG_FORMAT_DEFAULT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FORMAT_RAW_MESSAGE = '%(message)s' # For replay file saving
LOG_REPLAY_FILE_HEADER_TS_FORMAT = "%Y%m%d_%H%M%S%Z"
LOG_REPLAY_FILE_START_MSG_PREFIX = "# Recording Started: "
LOG_REPLAY_FILE_SESSION_INFO_PREFIX = "# Session Info (from FastF1 at start): "
LOG_REPLAY_FILE_STOP_MSG_PREFIX = "\n# Recording Stopped: "

# Callback Logging Level (e.g., logging.INFO, logging.DEBUG, logging.WARNING)
CALLBACK_LOG_LEVEL = logging.INFO


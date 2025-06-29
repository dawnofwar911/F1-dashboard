# app/api.py
"""
API-related URLs and keys for the F1 Telemetry Dashboard application.
"""

import os

# --- API and Network ---
NEGOTIATE_URL_BASE = os.environ.get('NEGOTIATE_URL_BASE', 'https://livetiming.formula1.com/signalr')
WEBSOCKET_URL_BASE = os.environ.get('WEBSOCKET_URL_BASE', 'wss://livetiming.formula1.com/signalr')
REQUESTS_TIMEOUT_SECONDS = int(os.environ.get('REQUESTS_TIMEOUT_SECONDS', 15))
F1_LIVETIMING_BASE_URL = os.environ.get('F1_LIVETIMING_BASE_URL', 'livetiming.formula1.com')
MULTIVIEWER_CIRCUIT_API_URL_TEMPLATE = os.environ.get('MULTIVIEWER_CIRCUIT_API_URL_TEMPLATE', "https://api.multiviewer.app/api/v1/circuits/{circuit_key}/{year}")

# --- SignalR Connection ---
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
USER_AGENT_NEGOTIATE = "Python SignalRClient"
USER_AGENT_WEBSOCKET = "BestHTTP" # Match F1 expectations

# --- API URLs (other than SignalR) ---
MULTIVIEWER_API_USER_AGENT = 'F1-Dash/0.5' # Increment version or make more dynamic

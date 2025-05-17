# signalr_client.py
"""
Manages the SignalR connection, negotiation, message handling, and related threads.
"""

import logging
import threading
import requests
import json
import uuid
import urllib.parse
import time # Only needed if adding delays/sleeps
import datetime # For on_message timestamp fallback

# SignalR Core imports
from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.protocol.json_hub_protocol import JsonHubProtocol
from signalrcore.hub.errors import HubConnectionError, HubError
import queue # For queue.Full exception in on_message

# Local imports
import app_state
import config # <<< UPDATED: For constants
import utils
import replay # Needed for close_live_file

# --- Module-level variables ---
hub_connection = None
connection_thread = None

main_logger = logging.getLogger("F1App.SignalR")

# --- Connection Functions ---

def build_connection_url(negotiate_url_base_arg, hub_name_arg): # Args renamed to avoid confusion with config
    """Performs negotiation and builds the WebSocket connection URL."""
    main_logger.info(f"Negotiating connection: {negotiate_url_base_arg}/negotiate")
    try:
        connection_data = json.dumps([{"name": hub_name_arg}])
        params = {"clientProtocol": config.SIGNALR_CLIENT_PROTOCOL, "connectionData": connection_data} # Use constant
        negotiate_url_full = f"{negotiate_url_base_arg}/negotiate?{urllib.parse.urlencode(params)}"

        # Use constant for User-Agent and timeout
        negotiate_headers = {"User-Agent": config.USER_AGENT_NEGOTIATE}
        response = requests.get(negotiate_url_full, headers=negotiate_headers, verify=True, timeout=config.REQUESTS_TIMEOUT_SECONDS)
        main_logger.info(f"Negotiate status: {response.status_code}")
        response.raise_for_status()

        negotiate_cookie = '; '.join([f'{c.name}={c.value}' for c in response.cookies])
        if negotiate_cookie: main_logger.info(f"Got negotiation cookie(s): {negotiate_cookie}")
        else: main_logger.warning("No negotiation cookie found.")

        neg_data = response.json()
        connection_token = neg_data.get("ConnectionToken")
        if not connection_token: raise HubConnectionError("Negotiation response missing ConnectionToken.")
        main_logger.info("Got connection token.")

        ws_params = {
            "clientProtocol": config.SIGNALR_CLIENT_PROTOCOL, # Use constant
            "transport": "webSockets",
            "connectionToken": connection_token,
            "connectionData": connection_data
        }
        # Use constant for base URL
        websocket_url = f"{config.WEBSOCKET_URL_BASE}/connect?{urllib.parse.urlencode(ws_params)}"
        main_logger.info(f"Constructed WebSocket URL: {websocket_url}")

        # Use constant for User-Agent
        ws_headers = {
             "User-Agent": config.USER_AGENT_WEBSOCKET,
             "Accept-Encoding": "gzip, identity"
        }
        if negotiate_cookie: ws_headers["Cookie"] = negotiate_cookie

        main_logger.info(f"Negotiation OK. WS URL and Headers prepared.")
        return websocket_url, ws_headers

    except requests.exceptions.Timeout:
        main_logger.error(config.TEXT_SIGNALR_NEGOTIATION_TIMEOUT) # Use constant
    except requests.exceptions.RequestException as e:
        main_logger.error(config.TEXT_SIGNALR_NEGOTIATION_HTTP_FAIL_PREFIX + str(e), exc_info=False) # Use constant
    except Exception as e:
        main_logger.error(config.TEXT_SIGNALR_NEGOTIATION_ERROR_PREFIX + str(e), exc_info=True) # Use constant
    return None, None

def run_connection_manual_neg(target_url, headers_for_ws):
    """Target function for connection thread using pre-negotiated URL."""
    global hub_connection

    hub_connection = None
    try:
        main_logger.info("Connection thread: Initializing HubConnection...")
        hub_connection = (
            HubConnectionBuilder()
            .with_url(target_url, options={
                "verify_ssl": True,
                "headers": headers_for_ws,
                "skip_negotiation": True
                })
            .with_hub_protocol(JsonHubProtocol())
            .configure_logging(logging.DEBUG) # Uses F1App.SignalR logger
            .build()
        )

        if not hub_connection or not hasattr(hub_connection, 'send'):
            raise HubConnectionError(config.TEXT_SIGNALR_BUILD_HUB_FAILED) # Use constant

        hub_connection.on_open(handle_connect)
        hub_connection.on_close(handle_disconnect)
        hub_connection.on_error(handle_error)
        hub_connection.on("feed", on_message) # "feed" is the message target from F1
        main_logger.info("Connection thread: HubConnection handlers registered.")

        with app_state.app_state_lock:
             app_state.app_status.update({"state": "Connecting", "connection": config.TEXT_SIGNALR_SOCKET_CONNECTING_STATUS}) # Use constant

        main_logger.info("Connection thread: Starting hub_connection.start()...")
        hub_connection.start()
        main_logger.info("Connection thread: Hub connection started. Waiting for stop_event...")
        app_state.stop_event.wait()
        main_logger.info("Connection thread: Stop event received.")

    except Exception as e:
        main_logger.error(f"Connection thread error: {e}", exc_info=True)
        with app_state.app_state_lock:
             if app_state.app_status["state"] not in ["Stopping", "Stopped"]:
                  # Use constant
                  app_state.app_status.update({"state": "Error", "connection": config.TEXT_SIGNALR_THREAD_ERROR_STATUS_PREFIX + type(e).__name__})
        if not app_state.stop_event.is_set(): app_state.stop_event.set()

    finally:
        main_logger.info("Connection thread finishing.")
        temp_hub = hub_connection
        if temp_hub:
            try:
                main_logger.info("Attempting final hub stop in thread finally block...")
                temp_hub.stop()
                main_logger.info("Hub stopped (thread finally).")
            except Exception as e_stop:
                main_logger.error(f"Error stopping hub in thread finally: {e_stop}")

        with app_state.app_state_lock:
            if app_state.app_status["state"] not in ["Stopped", "Error"]:
                # Use constant
                app_state.app_status.update({"state": "Stopped", "connection": config.TEXT_SIGNALR_DISCONNECTED_THREAD_END_STATUS})
        hub_connection = None
        main_logger.info("Connection thread cleanup finished.")


def on_message(args):
    """Handles 'feed' targeted messages received. Decodes if needed, puts on queue."""
    main_logger.debug(f"SignalR on_message called with args type: {type(args)}")

    try:
        if not isinstance(args, list):
            main_logger.warning(f"on_message received unexpected args format: {type(args)} - Content: {args!r}")
            return

        if len(args) >= 2:
            stream_name_raw = args[0]
            data_content = args[1]
            timestamp_for_queue = args[2] if len(args) > 2 else None
            if timestamp_for_queue is None:
                timestamp_for_queue = datetime.datetime.now(datetime.timezone.utc).isoformat() + 'Z' # Corrected timezone usage

            stream_name = stream_name_raw
            actual_data = data_content

            if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                stream_name = stream_name_raw[:-2]
                actual_data = utils._decode_and_decompress(data_content)
                if actual_data is None:
                    main_logger.warning(f"Failed to decode/decompress data for stream '{stream_name_raw}'. Skipping.")
                    return

            if actual_data is not None:
                try:
                    queue_item = {"stream": stream_name, "data": actual_data, "timestamp": timestamp_for_queue}
                    app_state.data_queue.put(queue_item, block=True, timeout=0.1)
                except queue.Full: # Corrected exception import
                    main_logger.warning(f"Data queue full! Discarding '{stream_name}' message.")
                except Exception as queue_ex:
                    main_logger.error(f"Error putting message onto data_queue: {queue_ex}", exc_info=True)
            else:
                main_logger.debug(f"Skipping queue put for stream '{stream_name}' due to None data.")
        else:
            main_logger.warning(f"'feed' received with unexpected arguments structure: {args!r}")

    except Exception as e:
        main_logger.error(f"Error in on_message handler: {e}", exc_info=True)


def handle_connect():
    """Callback executed when the hub connection is successfully opened."""
    global hub_connection
    main_logger.info(f"****** SignalR Connection Opened! ******")

    with app_state.app_state_lock:
        if app_state.app_status["state"] == "Live":
             main_logger.warning("handle_connect called but state already Live.")
             return
        # Use constant
        app_state.app_status.update({"state": "Live", "connection": config.TEXT_SIGNALR_SOCKET_CONNECTED_SUBSCRIBING_STATUS})

    if hub_connection:
        try:
            main_logger.info(f"Attempting to subscribe to streams: {config.STREAMS_TO_SUBSCRIBE}")
            invocation_counter = str(uuid.uuid4())[:8]
            subscribe_message = {
                "H": config.HUB_NAME,
                "M": "Subscribe",
                "A": [config.STREAMS_TO_SUBSCRIBE],
                "I": invocation_counter
            }
            json_string = json.dumps(subscribe_message)
            hub_connection.send_raw_json(json_string) # Assumes send_raw_json is a method of the library
            main_logger.info(f"Subscription request sent. Invocation ID: {invocation_counter}")
            with app_state.app_state_lock:
                app_state.app_status["subscribed_streams"] = config.STREAMS_TO_SUBSCRIBE
                app_state.app_status["connection"] = config.TEXT_SIGNALR_CONNECTED_SUBSCRIBED_STATUS # Use constant

        except Exception as e:
            main_logger.error(f"Error sending subscription: {e}", exc_info=True)
            with app_state.app_state_lock:
                app_state.app_status.update({"state": "Error", "connection": config.TEXT_SIGNALR_SUBSCRIPTION_ERROR_STATUS}) # Use constant
    else:
        main_logger.error("handle_connect called but hub_connection is None!")
        with app_state.app_state_lock:
             app_state.app_status.update({"state": "Error", "connection": config.TEXT_SIGNALR_HUB_OBJECT_MISSING_STATUS}) # Use constant


def handle_disconnect():
    """Callback executed when the hub connection is closed."""
    main_logger.warning("SignalR Connection Closed.")
    with app_state.app_state_lock:
        if app_state.app_status["state"] not in ["Stopping", "Stopped", "Error", "Playback Complete"]:
             # Use constant
             app_state.app_status.update({"connection": config.TEXT_SIGNALR_CLOSED_UNEXPECTEDLY_STATUS, "state": "Stopped"})
             app_state.app_status["subscribed_streams"] = []
    if not app_state.stop_event.is_set():
        main_logger.info("Setting stop_event due to unexpected disconnect.")
        app_state.stop_event.set()


def handle_error(error):
    """Callback executed on hub connection error."""
    err_str = str(error)
    if "WebSocket connection is already closed" in err_str:
        main_logger.info(f"Ignoring expected SignalR error on close: {err_str}")
        return
    main_logger.error(f"SignalR Connection Error received: {error}")
    with app_state.app_state_lock:
        if app_state.app_status["state"] not in ["Error", "Stopping", "Stopped"]:
            # Use constant
            app_state.app_status.update({"connection": config.TEXT_SIGNALR_ERROR_STATUS_PREFIX + type(error).__name__, "state": "Error"})
    if not app_state.stop_event.is_set():
        main_logger.info("Setting stop_event due to SignalR error.")
        app_state.stop_event.set()

def stop_connection():
    """Stops the SignalR connection and cleans up the thread."""
    global hub_connection, connection_thread
    main_logger.info("Stop SignalR connection requested.")

    with app_state.app_state_lock:
        current_state = app_state.app_status["state"]
        thread_running = connection_thread and connection_thread.is_alive()

        if current_state not in ["Connecting", "Live", "Stopping"] and not thread_running:
            main_logger.warning(f"Stop connection called, but not active. State={current_state}")
            if not app_state.stop_event.is_set(): app_state.stop_event.set()
            return

        if current_state == "Stopping":
            main_logger.info("Stop connection already in progress.")
            return
        # Use constant
        app_state.app_status.update({"state": "Stopping", "connection": config.TEXT_SIGNALR_DISCONNECTING_STATUS})

    app_state.stop_event.set()
    main_logger.debug("Stop event set for connection thread.")

    temp_hub = hub_connection
    if temp_hub:
        main_logger.info("Attempting immediate hub stop call...")
        try:
            temp_hub.stop()
        except Exception as e:
            main_logger.error(f"Error during immediate hub.stop(): {e}")

    local_thread = connection_thread
    if local_thread and local_thread.is_alive():
        main_logger.info("Waiting for connection thread join...")
        local_thread.join(timeout=10) # Consider making timeout a config const
        if local_thread.is_alive(): main_logger.warning("Connection thread did not join cleanly.")
        else: main_logger.info("Connection thread joined.")

    with app_state.app_state_lock:
        if app_state.app_status["state"] == "Stopping":
            # Use constant
            app_state.app_status.update({"state": "Stopped", "connection": config.TEXT_SIGNALR_DISCONNECTED_STATUS})
        app_state.app_status["subscribed_streams"] = []

    hub_connection = None
    if connection_thread is local_thread:
        connection_thread = None

    main_logger.info("Calling close_live_file from stop_connection.")
    replay.close_live_file()

    main_logger.info("Stop connection sequence complete.")

print("DEBUG: signalr_client module loaded (with config constant usage)")
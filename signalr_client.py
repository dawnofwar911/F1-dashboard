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

# SignalR Core imports
from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.protocol.json_hub_protocol import JsonHubProtocol
from signalrcore.hub.errors import HubConnectionError, HubError
# from signalrcore.transport.websockets.connection import ConnectionState # Not directly used here now

# Local imports
import app_state
import config
import utils
import replay # Needed for close_live_file

# --- Module-level variables ---
hub_connection = None
connection_thread = None

# Get logger
main_logger = logging.getLogger("F1App.SignalR")

# --- Connection Functions ---

def build_connection_url(negotiate_url, hub_name):
    """Performs negotiation and builds the WebSocket connection URL."""
    main_logger.info(f"Negotiating connection: {negotiate_url}/negotiate")
    try:
        connection_data = json.dumps([{"name": hub_name}]) # Use hub_name argument
        params = {"clientProtocol": "1.5", "connectionData": connection_data}
        # Use config constants
        negotiate_url_full = f"{config.NEGOTIATE_URL_BASE}/negotiate?{urllib.parse.urlencode(params)}"

        negotiate_headers = {"User-Agent": "Python SignalRClient"} # Simple UA for negotiate
        response = requests.get(negotiate_url_full, headers=negotiate_headers, verify=True, timeout=15)
        main_logger.info(f"Negotiate status: {response.status_code}")
        response.raise_for_status()

        # Extract cookie and token
        negotiate_cookie = '; '.join([f'{c.name}={c.value}' for c in response.cookies])
        if negotiate_cookie: main_logger.info(f"Got negotiation cookie(s): {negotiate_cookie}")
        else: main_logger.warning("No negotiation cookie found.")

        neg_data = response.json()
        connection_token = neg_data.get("ConnectionToken")
        if not connection_token: raise HubConnectionError("Negotiation response missing ConnectionToken.")
        main_logger.info("Got connection token.")

        # Build WebSocket URL
        ws_params = {
            "clientProtocol": "1.5",
            "transport": "webSockets",
            "connectionToken": connection_token,
            "connectionData": connection_data
        }
        # Use config constant
        websocket_url = f"{config.WEBSOCKET_URL_BASE}/connect?{urllib.parse.urlencode(ws_params)}"
        main_logger.info(f"Constructed WebSocket URL: {websocket_url}")

        # Prepare headers for WS connection (passed via options)
        ws_headers = {
             "User-Agent": "BestHTTP", # Match F1 expectations
             "Accept-Encoding": "gzip, identity"
        }
        if negotiate_cookie: ws_headers["Cookie"] = negotiate_cookie

        main_logger.info(f"Negotiation OK. WS URL and Headers prepared.")
        return websocket_url, ws_headers # Return URL and headers

    except requests.exceptions.Timeout:
        main_logger.error("Negotiation timeout.")
    except requests.exceptions.RequestException as e:
        main_logger.error(f"Negotiation HTTP fail: {e}", exc_info=False) # Less verbose log
    except Exception as e:
        main_logger.error(f"Negotiation error: {e}", exc_info=True)
    return None, None # Return None tuple on failure

def run_connection_manual_neg(target_url, headers_for_ws):
    """Target function for connection thread using pre-negotiated URL."""
    global hub_connection # Modify module-level variable

    hub_connection = None # Ensure clean slate before build attempt
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
            .configure_logging(logging.DEBUG) # Use root logger configured elsewhere
            .build()
        )

        if not hub_connection or not hasattr(hub_connection, 'send'):
            raise HubConnectionError("Failed to build valid HubConnection object.")

        # Register handlers
        hub_connection.on_open(handle_connect)
        hub_connection.on_close(handle_disconnect)
        hub_connection.on_error(handle_error)
        hub_connection.on(config.HUB_NAME, on_message) # Use config.HUB_NAME? No, "feed" is the target
        hub_connection.on("feed", on_message)
        main_logger.info("Connection thread: HubConnection handlers registered.")

        with app_state.app_state_lock:
             app_state.app_status.update({"state": "Connecting", "connection": "Socket Connecting"})

        main_logger.info("Connection thread: Starting hub_connection.start()...")
        hub_connection.start()
        main_logger.info("Connection thread: Hub connection started. Waiting for stop_event...")
        app_state.stop_event.wait() # Wait using app_state event
        main_logger.info("Connection thread: Stop event received.")

    except Exception as e:
        main_logger.error(f"Connection thread error: {e}", exc_info=True)
        with app_state.app_state_lock:
             # Check current state before overwriting with error
             if app_state.app_status["state"] not in ["Stopping", "Stopped"]:
                  app_state.app_status.update({"state": "Error", "connection": f"Thread Error: {type(e).__name__}"})
        if not app_state.stop_event.is_set(): app_state.stop_event.set()

    finally:
        main_logger.info("Connection thread finishing.")
        temp_hub = hub_connection # Local copy
        if temp_hub:
            try:
                main_logger.info("Attempting final hub stop in thread finally block...")
                temp_hub.stop()
                main_logger.info("Hub stopped (thread finally).")
            except Exception as e_stop:
                main_logger.error(f"Error stopping hub in thread finally: {e_stop}")

        # Don't call close_live_file here - let stop_connection handle it
        with app_state.app_state_lock:
            if app_state.app_status["state"] not in ["Stopped", "Error"]: # Avoid overwriting Error state if already set
                app_state.app_status.update({"state": "Stopped", "connection": "Disconnected / Thread End"})
        # Don't clear stop event here, main loop manages overall shutdown
        hub_connection = None # Clear module-level reference
        main_logger.info("Connection thread cleanup finished.")


def on_message(args):
    """Handles 'feed' targeted messages received. Decodes if needed, puts on queue."""
    # global data_queue -> use app_state.data_queue
    # global main_logger -> use logger defined above

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
                timestamp_for_queue = datetime.datetime.now(timezone.utc).isoformat() + 'Z'

            stream_name = stream_name_raw
            actual_data = data_content

            if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                stream_name = stream_name_raw[:-2]
                actual_data = utils._decode_and_decompress(data_content) # Use utils
                if actual_data is None:
                    main_logger.warning(f"Failed to decode/decompress data for stream '{stream_name_raw}'. Skipping.")
                    return # Skip if decode fails

            # Put structured item onto queue
            if actual_data is not None:
                try:
                    queue_item = {"stream": stream_name, "data": actual_data, "timestamp": timestamp_for_queue}
                    # Use app_state.data_queue
                    app_state.data_queue.put(queue_item, block=True, timeout=0.1)
                    # main_logger.debug(f"Put '{stream_name}' onto data_queue.") # Can be very verbose
                except queue.Full:
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
    global hub_connection # Access module-level variable
    main_logger.info(f"****** SignalR Connection Opened! ******")

    with app_state.app_state_lock:
        if app_state.app_status["state"] == "Live":
             main_logger.warning("handle_connect called but state already Live.")
             # Decide if resubscribing is needed or safe here? Probably not.
             return # Avoid double-subscribe if state somehow wrong
        app_state.app_status.update({"state": "Live", "connection": "Socket Connected - Subscribing"})

    if hub_connection:
        try:
            main_logger.info(f"Attempting to subscribe to streams: {config.STREAMS_TO_SUBSCRIBE}") # Use config
            invocation_counter = str(uuid.uuid4())[:8]
            subscribe_message = {
                "H": config.HUB_NAME, # Use config
                "M": "Subscribe",
                "A": [config.STREAMS_TO_SUBSCRIBE], # Use config
                "I": invocation_counter
            }
            json_string = json.dumps(subscribe_message)
            hub_connection.send_raw_json(json_string) # Assumes hub_connection has this method
            main_logger.info(f"Subscription request sent. Invocation ID: {invocation_counter}")
            # Update state to reflect subscription attempt
            with app_state.app_state_lock:
                app_state.app_status["subscribed_streams"] = config.STREAMS_TO_SUBSCRIBE
                app_state.app_status["connection"] = "Connected & Subscribed" # More specific status

        except Exception as e:
            main_logger.error(f"Error sending subscription: {e}", exc_info=True)
            with app_state.app_state_lock:
                app_state.app_status.update({"state": "Error", "connection": f"Subscription Error"})
            # Consider calling stop_connection() here if subscribe fails?
            # stop_connection()
    else:
        main_logger.error("handle_connect called but hub_connection is None!")
        with app_state.app_state_lock:
             app_state.app_status.update({"state": "Error", "connection": "Hub object missing"})


def handle_disconnect():
    """Callback executed when the hub connection is closed."""
    main_logger.warning("SignalR Connection Closed.")
    with app_state.app_state_lock:
        if app_state.app_status["state"] not in ["Stopping", "Stopped", "Error", "Playback Complete"]:
             app_state.app_status.update({"connection": "Closed Unexpectedly", "state": "Stopped"})
             # Clear streams only if closed unexpectedly, might be needed during stopping sequence otherwise
             app_state.app_status["subscribed_streams"] = []
    if not app_state.stop_event.is_set():
        main_logger.info("Setting stop_event due to unexpected disconnect.")
        app_state.stop_event.set() # Ensure main loop knows


def handle_error(error):
    """Callback executed on hub connection error."""
    # Avoid logging expected closure errors if possible
    err_str = str(error)
    if "WebSocket connection is already closed" in err_str:
        main_logger.info(f"Ignoring expected SignalR error on close: {err_str}")
        return
    main_logger.error(f"SignalR Connection Error received: {error}")
    with app_state.app_state_lock:
        if app_state.app_status["state"] not in ["Error", "Stopping", "Stopped"]:
            app_state.app_status.update({"connection": f"SignalR Error: {type(error).__name__}", "state": "Error"})
    if not app_state.stop_event.is_set():
        main_logger.info("Setting stop_event due to SignalR error.")
        app_state.stop_event.set() # Ensure main loop knows

def stop_connection():
    """Stops the SignalR connection and cleans up the thread."""
    global hub_connection, connection_thread # Use module-level variables
    main_logger.info("Stop SignalR connection requested.")

    with app_state.app_state_lock:
        current_state = app_state.app_status["state"]
        thread_running = connection_thread and connection_thread.is_alive()

        # Prevent stopping if already stopped/idle and thread isn't running
        if current_state not in ["Connecting", "Live", "Stopping"] and not thread_running:
            main_logger.warning(f"Stop connection called, but not active. State={current_state}")
            if not app_state.stop_event.is_set(): app_state.stop_event.set() # Ensure event is set anyway
            return

        # Prevent double stops
        if current_state == "Stopping":
            main_logger.info("Stop connection already in progress.")
            return
        # Set state to Stopping
        app_state.app_status.update({"state": "Stopping", "connection": "Disconnecting"})

    # Signal thread to stop
    app_state.stop_event.set()
    main_logger.debug("Stop event set for connection thread.")

    # Attempt immediate hub stop (best effort)
    temp_hub = hub_connection
    if temp_hub: # Removed check for transport.connection_alive as it might not be reliable
        main_logger.info("Attempting immediate hub stop call...")
        try:
            temp_hub.stop() # Let the library handle internal state checks
        except Exception as e:
            main_logger.error(f"Error during immediate hub.stop(): {e}")

    # Wait for thread to exit
    local_thread = connection_thread
    if local_thread and local_thread.is_alive():
        main_logger.info("Waiting for connection thread join...")
        local_thread.join(timeout=10) # Wait up to 10 seconds
        if local_thread.is_alive(): main_logger.warning("Connection thread did not join cleanly.")
        else: main_logger.info("Connection thread joined.")

    # Clean up state after thread confirmed stopped or timed out
    with app_state.app_state_lock:
        if app_state.app_status["state"] == "Stopping": # Only update if still in stopping phase
            app_state.app_status.update({"state": "Stopped", "connection": "Disconnected"})
        app_state.app_status["subscribed_streams"] = []

    # Clean up module-level variables
    hub_connection = None
    if connection_thread is local_thread: # Avoid race condition if stop called twice
        connection_thread = None

    # Close associated live file/log handler
    main_logger.info("Calling close_live_file from stop_connection.")
    replay.close_live_file() # Call the function now in replay module

    main_logger.info("Stop connection sequence complete.")

print("DEBUG: signalr_client module loaded")
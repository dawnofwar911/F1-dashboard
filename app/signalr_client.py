# signalr_client.py
"""
Manages SignalR connections, negotiation, message handling, and related threads
on a per-session basis.
"""

import logging
import threading
import requests
import json
import uuid
import urllib.parse
import time
import datetime

# SignalR Core imports
from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.protocol.json_hub_protocol import JsonHubProtocol
from signalrcore.hub.errors import HubConnectionError, HubError
import queue  # For queue.Full exception

# Local imports
# app_state will be passed as an argument (session_state) to functions
# import app_state
import config
import utils
# import replay # For replay.close_live_file_session - will be a conceptual call

# Module-level loggers (can still be used, but messages should include session context)
main_logger = logging.getLogger("F1App.SignalR")  # General SignalR operations
# module_logger = logging.getLogger("F1App.SignalR") # This seems redundant if main_logger is used


# --- Connection Functions ---

def build_connection_url(negotiate_url_base_arg: str, hub_name_arg: str):
    """
    Performs negotiation and builds the WebSocket connection URL.
    This function remains largely the same as it's a pre-connection step
    and doesn't depend on an active session_state.
    """
    main_logger.info(
        f"Negotiating connection: {negotiate_url_base_arg}/negotiate for hub {hub_name_arg}")
    try:
        connection_data = json.dumps([{"name": hub_name_arg}])
        params = {"clientProtocol": config.SIGNALR_CLIENT_PROTOCOL,
                  "connectionData": connection_data}
        negotiate_url_full = f"{negotiate_url_base_arg}/negotiate?{urllib.parse.urlencode(params)}"

        negotiate_headers = {"User-Agent": config.USER_AGENT_NEGOTIATE}
        response = requests.get(negotiate_url_full, headers=negotiate_headers,
                                verify=True, timeout=config.REQUESTS_TIMEOUT_SECONDS)
        main_logger.info(f"Negotiate status: {response.status_code}")
        response.raise_for_status()

        negotiate_cookie_parts = [
            f'{c.name}={c.value}' for c in response.cookies]
        negotiate_cookie = '; '.join(
            negotiate_cookie_parts) if negotiate_cookie_parts else None
        if negotiate_cookie:
            # Log truncated cookie
            main_logger.info(
                f"Got negotiation cookie(s): {negotiate_cookie[:100]}...")
        else:
            main_logger.warning("No negotiation cookie found.")

        neg_data = response.json()
        connection_token = neg_data.get("ConnectionToken")
        if not connection_token:
            raise HubConnectionError(
                "Negotiation response missing ConnectionToken.")
        main_logger.info("Got connection token.")

        ws_params = {
            "clientProtocol": config.SIGNALR_CLIENT_PROTOCOL,
            "transport": "webSockets",
            "connectionToken": connection_token,
            "connectionData": connection_data
        }
        websocket_url = f"{config.WEBSOCKET_URL_BASE}/connect?{urllib.parse.urlencode(ws_params)}"
        # Log truncated URL
        main_logger.info(
            f"Constructed WebSocket URL: {websocket_url[:150]}...")

        ws_headers = {
            "User-Agent": config.USER_AGENT_WEBSOCKET,
            "Accept-Encoding": "gzip, identity"  # Common encoding
        }
        if negotiate_cookie:
            ws_headers["Cookie"] = negotiate_cookie

        main_logger.info("Negotiation OK. WS URL and Headers prepared.")
        return websocket_url, ws_headers

    except requests.exceptions.Timeout:
        main_logger.error(config.TEXT_SIGNALR_NEGOTIATION_TIMEOUT)
    except requests.exceptions.RequestException as e:
        main_logger.error(
            config.TEXT_SIGNALR_NEGOTIATION_HTTP_FAIL_PREFIX + str(e), exc_info=False)
    except Exception as e:
        main_logger.error(
            config.TEXT_SIGNALR_NEGOTIATION_ERROR_PREFIX + str(e), exc_info=True)
    return None, None


def run_connection_session(session_state: 'app_state.SessionState', target_url: str, headers_for_ws: dict):
    """
    Target function for a session-specific connection thread.
    Manages the HubConnection lifecycle for the given session.
    """
    sess_id = session_state.session_id[:8]  # Short ID for logging
    logger_s = logging.getLogger(
        f"F1App.SignalR.Sess_{sess_id}")  # Session-specific logger

    # Hub connection is now part of session_state
    # session_state.hub_connection = None # Should be initialized to None before thread starts

    try:
        logger_s.info("Connection thread: Initializing HubConnection...")
        hub_connection_builder = (
            HubConnectionBuilder()
            .with_url(target_url, options={
                "verify_ssl": True,
                "headers": headers_for_ws,
                "skip_negotiation": True
            })
            .with_hub_protocol(JsonHubProtocol())
            # Library's internal logger config
            .configure_logging(logging.WARNING)
        )
        # Store the hub connection in the session state
        with session_state.lock:
            session_state.hub_connection = hub_connection_builder.build()

        # Configure SignalRCoreClient logger (library's logger)
        library_logger_name = "SignalRCoreClient"
        signalrcore_lib_logger = logging.getLogger(library_logger_name)
        if signalrcore_lib_logger.hasHandlers():
            # logger_s.info(f"'{library_logger_name}' logger (pre-clear) has handlers: {signalrcore_lib_logger.handlers}")
            signalrcore_lib_logger.handlers.clear()
            # logger_s.info(f"Cleared existing handlers from '{library_logger_name}' logger.")
        signalrcore_lib_logger.setLevel(logging.WARNING)
        signalrcore_lib_logger.propagate = True  # Let root handler manage output
        # logger_s.info(f"'{library_logger_name}' logger configured.")

        # Changed from send to send_raw_json based on usage in handle_connect
        if not session_state.hub_connection or not hasattr(session_state.hub_connection, 'send_raw_json'):
            raise HubConnectionError(config.TEXT_SIGNALR_BUILD_HUB_FAILED)

        # Register session-aware handlers using lambdas or functools.partial
        session_state.hub_connection.on_open(
            lambda: handle_connect_session(session_state))
        session_state.hub_connection.on_close(
            lambda: handle_disconnect_session(session_state))
        session_state.hub_connection.on_error(
            lambda error: handle_error_session(session_state, error))
        session_state.hub_connection.on(
            "feed", lambda args: on_message_session(session_state, args))
        logger_s.info("HubConnection handlers registered.")

        with session_state.lock:
            session_state.app_status.update(
                {"state": "Connecting", "connection": config.TEXT_SIGNALR_SOCKET_CONNECTING_STATUS})

        logger_s.info("Starting hub_connection.start()...")
        # This is a blocking call until connection stops or fails
        session_state.hub_connection.start()
        logger_s.info(
            "Hub connection started. Waiting for session_state.stop_event...")

        # The thread will effectively block here if hub_connection.start() blocks.
        # If start() is non-blocking and connection runs in background threads of the lib,
        # then this wait is correct. SignalRCore usually handles its own threads for the connection.
        session_state.stop_event.wait()  # Wait for external signal to stop
        logger_s.info("Stop event received by connection thread.")

    except Exception as e:
        logger_s.error(f"Connection thread error: {e}", exc_info=True)
        with session_state.lock:
            if session_state.app_status["state"] not in ["Stopping", "Stopped"]:
                session_state.app_status.update(
                    {"state": "Error", "connection": config.TEXT_SIGNALR_THREAD_ERROR_STATUS_PREFIX + type(e).__name__})
        if not session_state.stop_event.is_set():
            session_state.stop_event.set()  # Ensure other parts of this session know to stop

    finally:
        logger_s.info("Connection thread finishing and cleaning up.")
        # temp_hub is local to this session_state
        with session_state.lock:
            temp_hub = session_state.hub_connection

        if temp_hub:
            try:
                logger_s.info(
                    "Attempting final hub stop in thread's finally block...")
                temp_hub.stop()  # This should be called to clean up the connection
                logger_s.info("Hub stopped (thread finally).")
            except Exception as e_stop:
                logger_s.error(
                    f"Error stopping hub in thread finally: {e_stop}")

        with session_state.lock:
            # Added Playback Complete
            if session_state.app_status["state"] not in ["Stopped", "Error", "Playback Complete"]:
                session_state.app_status.update(
                    {"state": "Stopped", "connection": config.TEXT_SIGNALR_DISCONNECTED_THREAD_END_STATUS})
            session_state.hub_connection = None  # Clear from session state
            # This thread is ending, so clear its handle
            session_state.connection_thread = None
        logger_s.info("Connection thread cleanup finished for session.")


def on_message_session(session_state: 'app_state.SessionState', args: list):
    """Handles 'feed' targeted messages received for a session. Decodes if needed, puts on session's queue."""
    sess_id = session_state.session_id[:8]
    logger_s_msg = logging.getLogger(
        f"F1App.SignalR.Msg_{sess_id}")  # Logger for messages
    # logger_s_msg.debug(f"on_message_session called with args type: {type(args)}")

    try:
        if not isinstance(args, list):
            logger_s_msg.warning(
                f"Unexpected args format: {type(args)} - Content: {args!r}")
            return

        if len(args) >= 2:
            stream_name_raw = args[0]
            data_content = args[1]
            # F1 feed messages often have [stream_name, data, timestamp_from_feed_server]
            timestamp_for_queue_str = args[2] if len(args) > 2 else None

            # Fallback if F1 doesn't provide a timestamp (unlikely for 'feed')
            if timestamp_for_queue_str is None:
                timestamp_for_queue_str = datetime.datetime.now(
                    datetime.timezone.utc).isoformat() + 'Z'

            stream_name = stream_name_raw
            actual_data = data_content

            if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                stream_name = stream_name_raw[:-2]  # Remove .z suffix
                actual_data = utils._decode_and_decompress(
                    data_content)  # Assuming utils is available
                if actual_data is None:
                    logger_s_msg.warning(
                        f"Failed to decode/decompress data for stream '{stream_name_raw}'. Skipping.")
                    return

            # Ensure actual_data is not None before queuing (it could be None after failed decompression)
            if actual_data is not None:
                try:
                    # The queue item now includes the timestamp string from the feed or generated
                    queue_item = {
                        "stream": stream_name, "data": actual_data, "timestamp": timestamp_for_queue_str}
                    # Use non-blocking put or short timeout
                    session_state.data_queue.put(queue_item, block=False)
                except queue.Full:
                    logger_s_msg.warning(
                        f"Session data queue full! Discarding '{stream_name}' message.")
                except Exception as queue_ex:
                    logger_s_msg.error(
                        f"Error putting message onto session data_queue: {queue_ex}", exc_info=True)
            # else:
                # logger_s_msg.debug(f"Skipping queue put for stream '{stream_name}' due to None data after processing.")
        else:
            logger_s_msg.warning(
                f"'feed' received with unexpected arguments structure: {args!r}")

    except Exception as e:
        logger_s_msg.error(
            f"Error in on_message_session handler: {e}", exc_info=True)


def handle_connect_session(session_state: 'app_state.SessionState'):
    """Callback executed when the session's hub connection is successfully opened."""
    sess_id = session_state.session_id[:8]
    logger_s = logging.getLogger(f"F1App.SignalR.Sess_{sess_id}")
    logger_s.info(f"****** SignalR Connection Opened for session! ******")

    with session_state.lock:
        # Should be "Connecting" or "Initializing"
        if session_state.app_status["state"] == "Live":
            logger_s.warning(
                "handle_connect_session called but state already Live. This might be a reconnect.")
            # If it's a reconnect, we might not need to change state from "Live"
            # but ensure subscription is active.
        else:
            session_state.app_status.update(
                {"state": "Live", "connection": config.TEXT_SIGNALR_SOCKET_CONNECTED_SUBSCRIBING_STATUS})

        hub_conn_for_session = session_state.hub_connection  # Get from session_state

    if hub_conn_for_session:
        try:
            logger_s.info(
                f"Attempting to subscribe to streams: {config.STREAMS_TO_SUBSCRIBE}")
            invocation_id = str(uuid.uuid4())  # Unique ID for this invocation
            # Correct message structure for SignalR Core:
            # Target method on hub is "Subscribe", arguments is an array containing the list of streams.
            # The library's send method might wrap this structure, or if using send_raw_json, ensure correct format.
            # Based on your original code, using send_raw_json with H, M, A, I structure.
            subscribe_message = {
                "H": config.HUB_NAME,  # Hub name
                "M": "Subscribe",     # Method to invoke on the server hub
                # Arguments for the method (list of streams is one arg)
                "A": [config.STREAMS_TO_SUBSCRIBE],
                "I": invocation_id    # Invocation ID
            }
            json_string_payload = json.dumps(subscribe_message)
            # Use the method available in signalrcore library
            hub_conn_for_session.send_raw_json(json_string_payload)

            logger_s.info(
                f"Subscription request sent. Invocation ID: {invocation_id}")
            with session_state.lock:
                session_state.app_status["subscribed_streams"] = config.STREAMS_TO_SUBSCRIBE
                session_state.app_status["connection"] = config.TEXT_SIGNALR_CONNECTED_SUBSCRIBED_STATUS
        except Exception as e:
            logger_s.error(f"Error sending subscription: {e}", exc_info=True)
            with session_state.lock:
                session_state.app_status.update(
                    {"state": "Error", "connection": config.TEXT_SIGNALR_SUBSCRIPTION_ERROR_STATUS})
    else:
        logger_s.error(
            "handle_connect_session called but hub_connection in session_state is None!")
        with session_state.lock:
            session_state.app_status.update(
                {"state": "Error", "connection": config.TEXT_SIGNALR_HUB_OBJECT_MISSING_STATUS})


def handle_disconnect_session(session_state: 'app_state.SessionState'):
    """Callback executed when the session's hub connection is closed."""
    sess_id = session_state.session_id[:8]
    logger_s = logging.getLogger(f"F1App.SignalR.Sess_{sess_id}")
    logger_s.warning("SignalR Connection Closed for session.")

    with session_state.lock:
        # Only update status if not already being stopped by user or due to an error
        if session_state.app_status["state"] not in ["Stopping", "Stopped", "Error", "Playback Complete"]:
            session_state.app_status.update(
                {"connection": config.TEXT_SIGNALR_CLOSED_UNEXPECTEDLY_STATUS, "state": "Stopped"})
            # Clear subscribed streams
            session_state.app_status["subscribed_streams"] = []

    # If the stop_event for this session isn't already set (e.g., by user action), set it.
    if not session_state.stop_event.is_set():
        logger_s.info(
            "Setting session stop_event due to unexpected disconnect.")
        session_state.stop_event.set()


def handle_error_session(session_state: 'app_state.SessionState', error):
    """Callback executed on session's hub connection error."""
    sess_id = session_state.session_id[:8]
    logger_s = logging.getLogger(f"F1App.SignalR.Sess_{sess_id}")

    err_str = str(error)
    # Filter out common errors that occur during normal shutdown
    if "WebSocket connection is already closed" in err_str or \
       "Connection was gracefully closed" in err_str or \
       (isinstance(error, HubError) and "Hub dispatch incoming message failed" in err_str and "System.Threading.Channels.ChannelClosedException" in err_str) or \
       (session_state.app_status.get("state") == "Stopping"):  # If already stopping, this error might be expected
        logger_s.info(
            f"Ignoring expected SignalR error on close/stop: {err_str}")
        return

    # Log full error for unexpected ones
    logger_s.error(
        f"SignalR Connection Error received: {error}", exc_info=True)

    with session_state.lock:
        if session_state.app_status["state"] not in ["Error", "Stopping", "Stopped"]:
            session_state.app_status.update(
                {"connection": config.TEXT_SIGNALR_ERROR_STATUS_PREFIX + type(error).__name__, "state": "Error"})

    if not session_state.stop_event.is_set():
        logger_s.info("Setting session stop_event due to SignalR error.")
        session_state.stop_event.set()


def stop_connection_session(session_state: 'app_state.SessionState'):
    """Stops the SignalR connection for a specific session and cleans up its thread."""
    sess_id = session_state.session_id[:8]
    logger_s = logging.getLogger(f"F1App.SignalR.Sess_{sess_id}")
    logger_s.info("Stop SignalR connection requested for session.")

    # Local copies of thread and hub from session_state to minimize lock time for checks
    s_connection_thread = None
    s_hub_connection = None
    current_s_state = "Unknown"

    with session_state.lock:
        current_s_state = session_state.app_status["state"]
        # Get the thread object for this session
        s_connection_thread = session_state.connection_thread
        # Get the hub object for this session
        s_hub_connection = session_state.hub_connection

        thread_is_running = s_connection_thread and s_connection_thread.is_alive()

        # Check if already stopped or stopping, or if no thread was ever started
        # Added Initializing
        if current_s_state not in ["Connecting", "Live", "Initializing"] and not thread_is_running:
            logger_s.warning(
                f"Stop connection called, but not active or no thread. State={current_s_state}, ThreadAlive={thread_is_running}")
            if not session_state.stop_event.is_set():
                # Ensure event is set if trying to stop a non-active session
                session_state.stop_event.set()
            return

        if current_s_state == "Stopping":
            logger_s.info(
                "Stop connection already in progress for this session.")
            return

        session_state.app_status.update(
            {"state": "Stopping", "connection": config.TEXT_SIGNALR_DISCONNECTING_STATUS})

    # Set the session-specific stop event. This will be noticed by run_connection_session's loop.
    session_state.stop_event.set()
    logger_s.debug("Session stop_event set for its connection thread.")

    # Attempt to stop the hub connection directly
    if s_hub_connection:
        logger_s.info("Attempting immediate hub stop call for session...")
        try:
            s_hub_connection.stop()  # This should trigger on_close if successful
            logger_s.info("Session hub.stop() called.")
        except Exception as e:
            logger_s.error(f"Error during immediate session hub.stop(): {e}")

    # Join the connection thread for this session
    if s_connection_thread and s_connection_thread.is_alive():
        logger_s.info(
            f"Waiting for session connection thread ({s_connection_thread.name}) to join...")
        s_connection_thread.join(timeout=10)  # Configurable timeout
        if s_connection_thread.is_alive():
            logger_s.warning("Session connection thread did not join cleanly.")
        else:
            logger_s.info("Session connection thread joined.")

    # Final status update and cleanup within session_state
    with session_state.lock:
        # If it's still "Stopping", move to "Stopped"
        if session_state.app_status["state"] == "Stopping":
            session_state.app_status.update(
                {"state": "Stopped", "connection": config.TEXT_SIGNALR_DISCONNECTED_STATUS})
        session_state.app_status["subscribed_streams"] = []
        session_state.hub_connection = None  # Clear from session state
        session_state.connection_thread = None  # Clear thread handle

    # Conceptual: Call session-aware live file closing
    # This function will need to be defined in replay.py and accept session_state
    # from replay import close_live_file_session # Conceptual import
    # close_live_file_session(session_state)
    logger_s.warning(
        f"Conceptual call: replay.close_live_file_session(session_state) for session {sess_id}. Needs implementation.")
    # For now, if live_data_file is managed in session_state directly:
    with session_state.lock:
        if session_state.live_data_file and not session_state.live_data_file.closed:
            try:
                logger_s.info(
                    f"Closing live_data_file for session {sess_id} from stop_connection_session.")
                session_state.live_data_file.close()
            except Exception as e_file_close:
                logger_s.error(
                    f"Error closing live_data_file for session {sess_id}: {e_file_close}")
            session_state.live_data_file = None
            session_state.is_saving_active = False
            session_state.current_recording_filename = None

    logger_s.info("Stop connection sequence for session complete.")


print("DEBUG: signalr_client module (multi-session structure) loaded")

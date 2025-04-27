# FILE: /.../signalrcore/transport/websockets/websocket_transport.py
# MODIFIED: Added raw message debug log to on_message
# Includes previous patches for evaluate_handshake return/state and on_open state/callback timing.
# *** PATCH APPLIED: Uncommented JsonHubProtocol import to fix NameError in send() ***

import websocket # Uses websocket-client library
import threading
import requests
import traceback
import uuid
import time
import ssl
import logging # Already present in the provided file
import json # Needed for negotiate logic that was merged into start()
import urllib.parse # Needed for url construction
try:
    # This imports the module you created
    import app_state
except ImportError:
    # Handle case where library might be used outside your app context
    app_state = None
    # Maybe add logging here if self.logger exists at this point
    print("WARNING: app_state module not found by websocket_transport.")
from functools import partial
from .reconnection import ConnectionStateChecker
from .connection import ConnectionState
from ...messages.ping_message import PingMessage
from ...hub.errors import HubError, HubConnectionError, UnAuthorizedHubError
# Import protocols only if needed for type checking, not usually directly used here
# from ...protocol.messagepack_protocol import MessagePackHubProtocol
# --- PATCH START ---
# Uncommented the following line to fix NameError: name 'JsonHubProtocol' is not defined
from ...protocol.json_hub_protocol import JsonHubProtocol
# --- PATCH END ---
from ..base_transport import BaseTransport
from ...helpers import Helpers


class WebsocketTransport(BaseTransport):
    def __init__(self,
            url="",
            protocol=None,
            headers=None,
            keep_alive_interval=15,
            reconnection_handler=None,
            verify_ssl=False,
            skip_negotiation=False,
            enable_trace=False,
            hub_connection=None, # Added argument
            on_message=None,
            **kwargs):

        self.logger = Helpers.get_logger()
        self._hub_connection = hub_connection # Store reference
        self.protocol = protocol
        self._on_message = on_message # HubConnection.on_message callback
        base_kwargs = kwargs.copy()
        super(WebsocketTransport, self).__init__(**base_kwargs)

        self._ws = None
        self.enable_trace = enable_trace
        self._thread = None
        self.skip_negotiation = skip_negotiation
        self.url = url # May be updated by negotiate() or if skip_negotiation=True
        if headers is None: self.headers = dict()
        else: self.headers = headers # Initial headers, may be updated by negotiate()
        self.handshake_received = False
        self.token = None
        self.state = ConnectionState.disconnected
        self.connection_alive = False
        self.verify_ssl = verify_ssl
        self.connection_checker = ConnectionStateChecker(
            lambda: self.send(PingMessage()), keep_alive_interval
        )
        self.reconnection_handler = reconnection_handler

        # Configure websocket-client tracing if library available
        try:
            if len(self.logger.handlers) > 0: websocket.enableTrace(self.enable_trace, self.logger.handlers[0])
            else: websocket.enableTrace(self.enable_trace)
        except Exception as trace_ex:
             self.logger.warning(f"Could not enable websocket trace: {trace_ex}")

        # Store references to the hub's internal handlers (called by transport events)
        # Assumes _hub_connection is a BaseHubConnection instance
        self._internal_on_open_handler = getattr(self._hub_connection, 'on_open_handler', None) if self._hub_connection else None
        self._internal_on_close_handler = getattr(self._hub_connection, 'on_close_handler', None) if self._hub_connection else None
        self._internal_on_error_handler = getattr(self._hub_connection, 'on_error_handler', None) if self._hub_connection else None
        self._internal_on_reconnect_handler = getattr(self._hub_connection, 'on_reconnect_handler', None) if self._hub_connection else None


    def is_running(self): return self.state != ConnectionState.disconnected

    def stop(self):
        # Simplified stop logic
        if self.state != ConnectionState.disconnected:
            self.logger.debug("WebsocketTransport stop sequence initiated.")
            self.state = ConnectionState.disconnected # Mark as disconnected immediately
            self.handshake_received = False
            self.connection_checker.stop()
            if self._ws:
                try: self._ws.close(); self.logger.info("WebSocketApp close() called.")
                except Exception as e: self.logger.error(f"Error closing websocket: {e}")
            # Let the on_close callback handle final state and user callback
        else:
            self.logger.debug(f"WebsocketTransport stop ignored, state is already {self.state}.")

    def start(self):
        """Starts negotiation (if not skipped) and websocket connection."""
        websocket_url_to_connect = self.url
        final_ws_headers = self.headers.copy()
        negotiate_cookie = None
        HUB_NAME = "Streaming" # Define HUB_NAME needed for connectionData

        # --- Negotiation Logic ---
        if not self.skip_negotiation:
            original_url = self.url # Store base URL for constructing connect URL later if needed
            negotiate_url_full = Helpers.get_negotiate_url(original_url)
            self.logger.info(f"Negotiating via: {negotiate_url_full}")
            session = requests.Session()
            # Use headers provided initially for negotiation
            session.headers.update(self.headers)
            try:
                 # Use POST for negotiation as per standard SignalR Core? Blog said GET but POST is safer.
                 response = session.post(negotiate_url_full, verify=self.verify_ssl, timeout=10)
                 self.logger.debug(f"Negotiate response status code: {response.status_code}"); response.raise_for_status()
                 # Extract cookie from session jar AFTER request
                 negotiate_cookie_str = '; '.join([f'{c.name}={c.value}' for c in session.cookies])
                 if negotiate_cookie_str: final_ws_headers['Cookie'] = negotiate_cookie_str; self.logger.info(f"Got negotiation cookie: {negotiate_cookie_str}")
                 else: self.logger.warning("No negotiation cookie received.")

                 neg_data = response.json(); self.logger.debug(f"Negotiate response data: {neg_data}")
                 connection_id = neg_data.get("connectionId"); connection_token = neg_data.get("connectionToken")
                 available_transports = neg_data.get("availableTransports", [])
                 azure_url = neg_data.get('url'); azure_token = neg_data.get('accessToken')

                 # Check if WebSockets is supported by server
                 if not any(t.get("transport") == "WebSockets" for t in available_transports):
                     raise HubConnectionError("WebSockets transport not supported by server.")

                 if azure_url and azure_token: # Azure redirect takes precedence
                      self.logger.info(f"Azure SignalR redirect detected."); websocket_url_to_connect = azure_url if azure_url.startswith("ws") else Helpers.http_to_websocket(azure_url)
                      self.token = azure_token; final_ws_headers = {"Authorization": f"Bearer {self.token}"}; self.logger.debug("Using Azure headers.")
                 elif connection_token: # Standard SignalR Core
                      self.logger.debug("Standard SignalR negotiation.")
                      # F1 endpoint requires /connect path and token in query string
                      ws_params = {"clientProtocol": "1.5", "transport": "webSockets", "connectionToken": connection_token, "connectionData": json.dumps([{"name": HUB_NAME}])}
                      connect_url_base = original_url.replace("https://", "wss://", 1).split('/negotiate')[0]
                      websocket_url_to_connect = f"{connect_url_base}/connect?{urllib.parse.urlencode(ws_params)}"; self.logger.info(f"Constructed WSS URL: {websocket_url_to_connect}")
                 else: raise HubConnectionError("Negotiation response missing ConnectionToken or Azure redirect.")
            except requests.exceptions.RequestException as req_ex: raise HubError(f"Negotiation request failed: {req_ex}")
            except Exception as e: raise HubError(f"Negotiation processing failed: {e}")
        else: # Skipped negotiation
             websocket_url_to_connect = self.url; final_ws_headers = self.headers; self.logger.info("Skipping negotiation step.")

        # --- State Check & Connection ---
        if self.state == ConnectionState.connected: self.logger.warning("Already connected."); return False
        self.state = ConnectionState.connecting; self.logger.debug(f"Connecting to: {websocket_url_to_connect}"); self.logger.debug(f"Using headers: {final_ws_headers}"); self.handshake_received = False
        ws_header_list = [f"{k}: {v}" for k, v in final_ws_headers.items()]

        # Setup websocket-client app instance
        self._ws = websocket.WebSocketApp(websocket_url_to_connect, header=ws_header_list, on_message=self.on_message, on_error=self.on_socket_error, on_close=self.on_close, on_open=self.on_open)

        # --- Modified Thread Target with Error Catching ---
        def run_forever_with_catch():
            thread_logger = logging.getLogger("WSThread") # Separate logger for thread
            try:
                 thread_logger.debug("Thread target: Calling run_forever...")
                 # Pass SSL options based on verify_ssl flag
                 ssl_opts = {}
                 if websocket_url_to_connect.startswith("wss"):
                      ssl_opts = {"cert_reqs": ssl.CERT_REQUIRED if self.verify_ssl else ssl.CERT_NONE}
                      if not self.verify_ssl:
                           ssl_opts["check_hostname"] = False # Disable hostname check if not verifying
                 thread_logger.debug(f"run_forever sslopt: {ssl_opts}")
                 self._ws.run_forever(sslopt=ssl_opts if ssl_opts else None) # Pass None if empty
                 thread_logger.debug("Thread target: run_forever completed.")
            except Exception as thread_ex:
                 thread_logger.error(f"Exception in run_forever thread: {thread_ex}", exc_info=True)
                 if callable(self._internal_on_error_handler): self._internal_on_error_handler(thread_ex)
            finally: thread_logger.debug("Thread target: Exiting run_forever_with_catch.")

        self._thread = threading.Thread(target=run_forever_with_catch, daemon=True); self._thread.start(); self.logger.debug("Websocket run_forever thread started."); return True

    # evaluate_handshake (Patched Version from Response #79)
    def evaluate_handshake(self, message):
        """Evaluates handshake response. MODIFIED to set state and return tuple."""
        self.logger.debug("Evaluating handshake {0}".format(message))
        if self.protocol is None: raise HubConnectionError("Protocol not initialized.")
        msg_response, buffered_messages = self.protocol.decode_handshake(message) # Calls patched BaseHubProtocol.decode_handshake
        if msg_response.error is None or msg_response.error == "":
            self.handshake_received = True
            self.state = ConnectionState.connected # Set state on successful handshake
            self.logger.info("SignalR Handshake successful via evaluate_handshake.")
        else:
            self.logger.error(f"Handshake error from evaluate_handshake: {msg_response.error}")
            self.state = ConnectionState.disconnected
            if callable(self._internal_on_error_handler): self._internal_on_error_handler(HubError(f"Handshake error: {msg_response.error}"))
            # self.stop() # Let on_close handle stop
        return msg_response, buffered_messages # Return tuple

    # on_open (Patched Version from Response #79)
    def on_open(self, wsapp):
        """Callback when websocket-client connection opens."""
        self.logger.debug("-- web socket open --")
        # Set state early to allow sending handshake
        self.state = ConnectionState.connected
        self.logger.debug(f"Transport state set to: {self.state} (in on_open)")
        if self.protocol is None: self.logger.error("Cannot send handshake, protocol not set."); return
        msg = self.protocol.handshake_message(); self.send(msg) # Sends client handshake
        # Trigger hub's handler AFTER sending client handshake
        if callable(self._internal_on_open_handler): self.logger.debug("Calling hub's on_open_handler from transport.on_open"); self._internal_on_open_handler()
        else: self.logger.warning("Transport could not call hub's on_open_handler.")

    # on_close (from response #79)
    def on_close(self, wsapp, close_status_code, close_reason):
        self.logger.debug("-- web socket close --"); self.logger.debug(f"Close Status: {close_status_code}, Reason: {close_reason}")
        previous_state = self.state; self.state = ConnectionState.disconnected; self.handshake_received = False; self.connection_checker.stop()
        if previous_state != ConnectionState.disconnected and callable(self._internal_on_close_handler): self._internal_on_close_handler()
        if self.reconnection_handler is not None and previous_state == ConnectionState.connected: self.logger.info("Attempting reconnection..."); self.handle_reconnect()

    # on_socket_error (with added logging from response #81)
    def on_socket_error(self, wsapp, error):
        self.logger.debug(f"-- web socket error callback triggered with error: {error!r} --")
        if not isinstance(error, (websocket.WebSocketConnectionClosedException, BrokenPipeError, ConnectionResetError)): self.logger.error(traceback.format_exc(5, True))
        self.logger.error(f"Transport Error: {error} Type: {type(error)}")
        previous_state = self.state; self.state = ConnectionState.disconnected; self.handshake_received = False; self.connection_checker.stop()
        if callable(self._internal_on_error_handler): self.logger.debug("Calling hub's on_error_handler"); self._internal_on_error_handler(error)
        if previous_state != ConnectionState.disconnected and callable(self._internal_on_close_handler): self.logger.debug("Calling hub's on_close_handler after socket error"); self._internal_on_close_handler()

    # on_message (Patched Version from Response #79 - Calls hub handler AFTER handshake)
    def on_message(self, wsapp, message):
        """Callback for websocket-client receiving messages."""
        # --- MODIFICATION START: Add Raw String Recording ---
        # Declare globals needed for recording check within this function's scope
        # IMPORTANT: This assumes these variables are accessible globally from your main script.
        # Modifying library files like this has risks and couples the library to your app.
        #global live_data_file, app_status, app_state_lock, record_live_data

        if app_state: # Check if import succeeded
            try:
                # Use app_state.<variable_name>
                with app_state.app_state_lock:
                    is_live_recording_active = (app_state.app_status.get('state') == 'Live' and app_state.record_live_data)
                    file_handle = app_state.live_data_file
    
                if is_live_recording_active and file_handle and not file_handle.closed:
                     # ... (rest of the writing logic using file_handle) ...
                     if isinstance(message, (str, bytes)):
                         try:
                             msg_str = message if isinstance(message, str) else message.decode('utf-8', errors='ignore')
                             if msg_str and msg_str != "{}":
                                file_handle.write(msg_str + "\n")
                         except Exception as write_err:
                             self.logger.error(f"Error writing raw live data from transport: {write_err}")
                     else:
                        self.logger.warning(f"Transport on_message received non-str/bytes for recording: {type(message)}")
            except AttributeError as ae:
                self.logger.error(f"Recording failed: Attribute missing from app_state? {ae}")
            except Exception as record_check_err:
                self.logger.error(f"Error checking/performing recording status in transport: {record_check_err}")
        else:
            self.logger.warning("Skipping recording check because app_state module was not imported.")
        # --- END MODIFIED RECORDING LOGIC ---
        
        # --- ADDED Raw Log Line ---
        self.logger.debug(f"SYNC Raw message received by transport: {message!r}")
        # --- END Added Line ---
        self.logger.debug("Message received{0}".format(message)) # Original debug
        self.connection_checker.last_message = time.time(); parsed_messages = []
        if not self.handshake_received:
            try:
                handshake_response, messages = self.evaluate_handshake(message) # State set inside here if ok
                if handshake_response.error: self.logger.error(f"Handshake evaluation failed: {handshake_response.error}"); return
                if self.handshake_received: # Flag set by evaluate_handshake
                    # Call hub on_open handler AFTER state is confirmed connected
                    if callable(self._internal_on_open_handler): self.logger.debug("Calling hub's on_open_handler AFTER successful handshake."); self._internal_on_open_handler()
                    else: self.logger.warning("Transport could not call hub's on_open_handler after handshake.")
                    # Start keep-alive check
                    if self.reconnection_handler is not None and not self.connection_checker.running: self.logger.debug("Starting keep-alive checker."); self.connection_checker.start()
                parsed_messages.extend(messages) # Process buffered messages
            except Exception as handshake_ex:
                 self.logger.error(f"Handshake processing failed in on_message: {handshake_ex}", exc_info=True)
                 if callable(self._internal_on_error_handler): self._internal_on_error_handler(handshake_ex)
                 return
        else: # Handshake already received
            try:
                if self.protocol is None: raise HubConnectionError("Protocol not initialized.")
                parsed_messages.extend(self.protocol.parse_messages(message))
            except Exception as parse_ex:
                 self.logger.error(f"Failed to parse message: {parse_ex}", exc_info=True)
                 if callable(self._internal_on_error_handler): self._internal_on_error_handler(parse_ex)
                 return
        # Pass successfully parsed messages up to the HubConnection (_on_message)
        if parsed_messages and callable(self._on_message): self._on_message(parsed_messages)

    # send (from response #81 - allows sending when connecting for handshake)
    def send(self, message):
        if self.state not in [ConnectionState.connected, ConnectionState.connecting]: self.logger.warning(f"Cannot send message, state is {self.state}. Msg: {message}"); return
        log_msg = not isinstance(message, PingMessage); is_handshake = hasattr(message, 'protocol') and hasattr(message, 'version')
        if log_msg : self.logger.debug(f"Transport sending message: {message}")
        try:
            # If it's a handshake message, it implicitly uses JSON protocol according to SignalR spec.
            # Otherwise, use the protocol configured for the connection.
            current_protocol = JsonHubProtocol() if is_handshake else self.protocol
            if current_protocol is None: raise HubConnectionError("Protocol missing for encoding.")
            encoded_message = current_protocol.encode(message)
            if log_msg: self.logger.debug(f"Encoded message: {encoded_message!r}")
            # Need to import MessagePack locally if checking type, or import at top
            from signalrcore.protocol.messagepack_protocol import MessagePackHubProtocol # Import locally just in case
            opcode = websocket.ABNF.OPCODE_BINARY if isinstance(current_protocol, MessagePackHubProtocol) else websocket.ABNF.OPCODE_TEXT
            if self._ws: self._ws.send(encoded_message, opcode=opcode)
            else: raise HubConnectionError("WebSocketApp not available for sending.")
            self.connection_checker.last_message = time.time()
            if self.reconnection_handler is not None: self.reconnection_handler.reset()
        except (websocket._exceptions.WebSocketConnectionClosedException, OSError) as ex:
            self.handshake_received = False; self.logger.warning(f"Send failed, connection closed?: {ex}"); self.state = ConnectionState.disconnected
            if callable(self._internal_on_close_handler): self._internal_on_close_handler()
            if self.reconnection_handler is not None: self.handle_reconnect()
        except Exception as ex:
            self.logger.error(f"Unexpected error during send: {ex}", exc_info=True)
            if callable(self._internal_on_error_handler): self._internal_on_error_handler(ex)
            # Decide if the error is fatal and needs re-raising or just logging
            # Re-raising might stop the client, depending on higher-level handling.
            # raise # Optionally re-raise if the error should stop the client.

    # handle_reconnect & attempt_reconnect (from response #81)
    def handle_reconnect(self):
        if self.reconnection_handler is None: return;
        if self.reconnection_handler.reconnecting: self.logger.debug("Already reconnecting."); return
        self.logger.info("Reconnection triggered."); self.reconnection_handler.reconnecting = True; self.state = ConnectionState.reconnecting
        if callable(self._internal_on_reconnect_handler): self._internal_on_reconnect_handler()
        sleep_time = self.reconnection_handler.next()
        if sleep_time is None: self.logger.error("Reconnection handler failed permanently."); self.reconnection_handler.reconnecting = False; self.state = ConnectionState.disconnected;
        else: self.logger.info(f"Attempting reconnect after {sleep_time} seconds..."); threading.Timer(sleep_time, self.attempt_reconnect).start()

    def attempt_reconnect(self):
        if self.state == ConnectionState.connected: self.logger.info("Reconnect attempt aborted, already connected."); self.reconnection_handler.reconnecting = False; return
        self.logger.info("Attempting to reconnect...")
        try: self.start()
        except Exception as ex: self.logger.error(f"Reconnect start attempt failed: {ex}", exc_info=True); self.state = ConnectionState.disconnected; self.reconnection_handler.reconnecting = False; self.handle_reconnect()


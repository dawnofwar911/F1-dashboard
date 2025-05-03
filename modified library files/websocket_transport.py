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
# Near top of websocket_transport.py
from ...messages.message_type import MessageType
from ...messages.invocation_message import InvocationMessage
from ...messages.ping_message import PingMessage
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
        ws_headers = {}
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
                 response = session.post(negotiate_url_full, verify=self.verify_ssl, timeout=10)
                 self.logger.debug(f"Negotiate response status code: {response.status_code}"); response.raise_for_status()

                 # Extract cookie string SEPARATELY
                 negotiate_cookie_str = '; '.join([f'{c.name}={c.value}' for c in session.cookies])
                 if negotiate_cookie_str:
                     self.logger.info(f"Got negotiation cookie: {negotiate_cookie_str}")
                 else:
                     self.logger.warning("No negotiation cookie received.") # Might cause 400 later if cookie required

                 neg_data = response.json()
                 self.logger.info(f"Negotiate Response JSON: {neg_data}")
                 # --- Get ConnectionToken (Correct Case) ---
                 connection_token = neg_data.get("ConnectionToken")

                 # Check if token exists (no need to check TryWebSockets anymore, assume it works if token given)
                 if not connection_token:
                     raise HubConnectionError("Negotiation response missing ConnectionToken.")

                # --- CONSTRUCT URL (FIX DOUBLE ENCODING) ---
                 # Define hub data *without* manually encoding
                 hub_data_json = json.dumps([{"name": HUB_NAME}])

                 # Build parameters dictionary with RAW token and RAW JSON string
                 ws_params = {
                     "clientProtocol": "1.5",
                     "transport": "webSockets",
                     # --- PASS RAW TOKEN ---
                     "connectionToken": connection_token,
                     # --- PASS RAW JSON STRING ---
                     "connectionData": hub_data_json
                 }

                 # Let urlencode handle the encoding
                 connect_url_base = original_url.replace("https://", "wss://", 1).split('/negotiate')[0]
                 # Use urlencode directly on the dict with raw values
                 websocket_url_to_connect = f"{connect_url_base}/connect?{urllib.parse.urlencode(ws_params)}" # urlencode does the job
                 self.logger.info(f"Constructed WSS URL (Fixed Encoding): {websocket_url_to_connect}")
                 # --- END URL CONSTRUCTION FIX ---


            except requests.exceptions.RequestException as req_ex: raise HubError(f"Negotiation request failed: {req_ex}")
            except Exception as e: raise HubError(f"Negotiation processing failed: {e}")
        else: # Skipped negotiation
             # Logic for skipped negotiation might need adjustment if headers/cookies expected
             websocket_url_to_connect = self.url
             # Try to get cookie from initial headers if skipping
             negotiate_cookie_str = self.headers.get("Cookie")
             self.logger.info("Skipping negotiation step.")


        # --- State Check & Connection ---
        if self.state == ConnectionState.connected: self.logger.warning("Already connected."); return False
        self.state = ConnectionState.connecting
        self.logger.debug(f"Connecting to: {websocket_url_to_connect}")

        # --- SET REQUIRED HEADERS (Case Sensitive!) ---
        ws_headers = {
            'User-Agent': 'BestHTTP',
            'Accept-Encoding': 'gzip,identity'
            # DO NOT add Cookie here, use the cookie parameter below
        }
        # Add any *other* custom headers passed initially, overwriting if needed
        ws_headers.update({k: v for k, v in self.headers.items() if k.lower() not in ['user-agent', 'accept-encoding', 'cookie']})
        # --- END SET HEADERS ---

        self.logger.debug(f"Using headers: {ws_headers}") # Log headers being sent
        self.logger.debug(f"Using cookie: {negotiate_cookie_str}") # Log cookie being sent
        self.handshake_received = False

        # Prepare header list from dict for websocket-client
        ws_header_list = [f"{k}: {v}" for k, v in ws_headers.items()]

        # Setup websocket-client app instance
        self._ws = websocket.WebSocketApp(
            websocket_url_to_connect,
            header=ws_header_list,         # <-- Headers list
            cookie=negotiate_cookie_str,    # <-- Pass cookie string here
            on_message=self.on_message,
            on_error=self.on_socket_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        self._thread = threading.Thread(target=self._ws.run_forever, kwargs={'sslopt': {"cert_reqs": ssl.CERT_NONE} if not self.verify_ssl else {}})
        self._thread.daemon = True
        self._thread.start()
        return True # Indicate connection attempt started

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
        # self.state = ConnectionState.connected
        self.logger.debug(f"Transport state set to: {self.state} (in on_open)")
        if self.protocol is None: self.logger.error("Cannot send handshake, protocol not set."); return
        msg = self.protocol.handshake_message(); self.send(msg) # Sends client handshake
        # Trigger hub's handler AFTER sending client handshake
        if callable(self._internal_on_open_handler): self.logger.debug("Calling hub's on_open_handler from transport.on_open"); 
        # self._internal_on_open_handler()
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

    def send_raw(self, raw_data):
        """Sends raw string or bytes data directly over the websocket."""
        self.logger.debug(f"Entering send_raw. Current state: {self.state}")
        available_attrs = list(getattr(self, '__dict__', {}).keys())
        self.logger.debug(f"send_raw: Available attributes: {available_attrs}") # Keep this for now
    
        if self.state != ConnectionState.connected:
            self.logger.warning(f"Attempted send_raw while not connected! State: {self.state}")
            raise HubConnectionError(f"Transport not connected: {self.state}")
    
        # --- START FIX ---
        # Attempt to get the underlying websocket instance using the CORRECT name '_ws'
        socket_instance = getattr(self, '_ws', None)
        # --- END FIX ---
    
        # Check if we successfully retrieved the websocket instance
        if not socket_instance:
            # --- START FIX ---
            # Update error message to reflect checking for '_ws'
            self.logger.error("Cannot send raw data: WebSocket instance attribute ('_ws') not found. getattr returned None.")
            raise HubConnectionError("WebSocket application instance ('_ws') not available.")
            # --- END FIX ---
    
        # Now, try to send the data using the retrieved socket instance
        try:
            # --- START FIX ---
            # Update log message
            self.logger.debug(f"Transport attempting to send raw data via self._ws: {raw_data!r}")
            # Call send on the correct instance
            socket_instance.send(raw_data)
            # Update log message
            self.logger.debug(f"Transport successfully sent raw data via self._ws.")
            # --- END FIX ---

        except websocket.WebSocketConnectionClosedException as ws_closed:
            self.logger.error(f"Failed to send raw data: WebSocket closed. {ws_closed}", exc_info=True)
            self._on_close()
            raise HubConnectionError("WebSocket closed") from ws_closed
        except Exception as ex:
            # --- START FIX ---
            # Update log message
            self.logger.error(f"Failed to send raw data using self._ws: {ex}", exc_info=True)
            # --- END FIX ---
            if callable(self._internal_on_error_handler):
                self._internal_on_error_handler(ex)
            raise HubError(f"Error sending raw data: {ex}") from ex
    # --- End Method ---
    
    # on_message (Patched Version from Response #79 - Calls hub handler AFTER handshake)
    def on_message(self, wsapp, message):
        """Callback for websocket-client receiving messages."""
        # --- MODIFICATION START: Add Raw String Recording ---
        # Declare globals needed for recording check within this function's scope
        # IMPORTANT: This assumes these variables are accessible globally from your main script.
        # Modifying library files like this has risks and couples the library to your app.
        #global live_data_file, app_status, app_state_lock, record_live_data
        save_file_object = None
        is_saving_active = False
        if app_state and hasattr(app_state, 'is_saving_active') and hasattr(app_state, 'live_data_file'):
            try:
                # Access shared state via app_state (assuming these exist)
                is_saving_active = app_state.record_live_data
                save_file_object = app_state.live_data_file
            except AttributeError:
                # Handle cases where attributes might not be initialized yet
                pass
        if is_saving_active and save_file_object is not None:
            try:
                # Ensure the message is a string (it usually is raw from websocket)
                if isinstance(message, bytes):
                    message_str = message.decode('utf-8') # Decode if necessary
                else:
                    message_str = str(message)

                # Write the raw message string followed by a newline
                save_file_object.write(message_str + "\n")
                # Optional: Flush occasionally if buffering is an issue,
                # but standard line buffering might be sufficient.
                # save_file_object.flush()
            except IOError as e:
                # Log error, maybe disable saving?
                self.logger.error(f"Error writing to live data file: {e}")
                # Consider adding logic to set app_state.is_saving_active = False here
            except Exception as e:
                self.logger.error(f"Unexpected error during message saving: {e}", exc_info=True)
        # --- END ADDED SAVE LOGIC ---
        
        # --- ADDED Raw Log Line ---
        #self.logger.debug(f"SYNC Raw message received by transport: {message!r}")
        # --- END Added Line ---
        #self.logger.debug("Message received{0}".format(message)) # Original debug
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
        if parsed_messages:
            hub_conn_ref = getattr(self, '_hub_connection', None)
            if not hub_conn_ref:
                 self.logger.error("Transport: Cannot dispatch message - self._hub_connection reference is missing!")
                 return # Exit if no hub connection reference

            # Log BEFORE attempting standard dispatch
            # Added Hub Ref log to confirm we have the object
            self.logger.debug(f"Transport: Dispatching {len(parsed_messages)} parsed messages via standard _dispatch_message. Hub Ref: {hub_conn_ref!r}")

            # --- ADDED try...except block around the STANDARD dispatch call ---
            try:
                # Log IMMEDIATELY BEFORE the standard call
                self.logger.debug("  Transport: >>> Calling _hub_connection._dispatch_message...")

                # === THE STANDARD CALL ===
                # Call the dispatch method on the hub connection object, passing the list
                hub_conn_ref._dispatch_message(parsed_messages)
                # ========================

                # Log IMMEDIATELY AFTER the standard call returns (if it does)
                self.logger.debug("  Transport: <<< Returned from _hub_connection._dispatch_message.")

            except Exception as dispatch_ex:
                # Log ANY exception occurring during the standard dispatch call
                self.logger.error(f"  Transport: !!! EXCEPTION during _hub_connection._dispatch_message: {dispatch_ex}", exc_info=True)
                # Depending on the error, you might want to trigger the main error handler
                # self._trigger_on_error_handler(dispatch_ex) # Consider if needed
            # --- END ADDED try...except block ---

        elif not parsed_messages and message != self.protocol.record_separator:
            # Optional: Refine logging for empty parses vs pings
            if message != "{}\x1e": # Don't log standard pings excessively
                 self.logger.debug(f"Transport: Parser returned empty list for non-separator/ping message: {message!r}")
            # else: # Optionally log pings at a lower level if needed
            #    self.logger.trace("Transport: Parser returned empty list (likely Ping message)") # Assuming TRACE level exists

        
       
        

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


# FILE: /.../signalrcore/hub/base_hub_connection.py
# MODIFIED: Added hub_connection=self back to transport init, includes previous patches

from operator import inv
import websocket
import threading
import traceback
import uuid
import time
import ssl
from typing import Callable
from signalrcore.messages.message_type import MessageType
from signalrcore.messages.stream_invocation_message\
    import StreamInvocationMessage
from signalrcore.messages.ping_message import PingMessage
from .errors import UnAuthorizedHubError, HubError, HubConnectionError
from signalrcore.helpers import Helpers
from .handlers import StreamHandler, InvocationHandler
from ..transport.websockets.websocket_transport import WebsocketTransport
from ..subject import Subject
from ..messages.invocation_message import InvocationMessage
from ..messages.completion_message import CompletionMessage
from ..messages.stream_item_message import StreamItemMessage

class InvocationResult(object):
    def __init__(self, invocation_id) -> None:
        self.invocation_id = invocation_id
        self.message = None

class BaseHubConnection(object):
    def __init__(
            self,
            url,
            protocol,
            headers=None,
            **kwargs):
        if headers is None:
            self.headers = dict()
        else:
            self.headers = headers
        self.logger = Helpers.get_logger()
        self.handlers = []
        self.stream_handlers = []
        self._on_error_default = lambda error: self.logger.error(
            f"on_error default handler invoked with error: {error}")
        # Store user callbacks
        self._user_on_open_callback = None
        self._user_on_close_callback = None
        self._user_on_error_callback = self._on_error_default
        self._user_on_reconnect_callback = None

        # Initialize Transport, passing self reference back
        self.transport = WebsocketTransport(
            url=url,
            protocol=protocol,
            headers=self.headers,
            on_message=self.on_message, # Pass message handler
            hub_connection=self, # <--- ADDED BACK: Pass self reference
            **kwargs) # Pass other args like keep_alive_interval

    def start(self):
        self.logger.debug("BaseHubConnection attempting to start transport.")
        return self.transport.start()

    def stop(self):
        self.logger.debug("BaseHubConnection attempting to stop transport.")
        return self.transport.stop()

    # --- Internal Handlers (Called by Transport) ---
    def on_open_handler(self):
         self.logger.debug("BaseHubConnection internal on_open_handler triggered.")
         if callable(self._user_on_open_callback): self._user_on_open_callback()
         else: self.logger.warning("No user on_open callback registered to call.") # Changed log msg

    def on_close_handler(self):
         self.logger.debug("BaseHubConnection internal on_close_handler triggered.")
         if callable(self._user_on_close_callback): self._user_on_close_callback()
         else: self.logger.debug("No user on_close callback registered.")

    def on_error_handler(self, error):
         self.logger.debug(f"BaseHubConnection internal on_error_handler triggered with: {error}")
         if callable(self._user_on_error_callback): self._user_on_error_callback(error)
         else: self.logger.debug("No user on_error callback registered.")

    def on_reconnect_handler(self):
         self.logger.debug("BaseHubConnection internal on_reconnect_handler triggered.")
         if callable(self._user_on_reconnect_callback): self._user_on_reconnect_callback()
         else: self.logger.debug("No user on_reconnect callback registered.")


    # --- Public Callback Configuration ---
    def on_close(self, callback): self.logger.debug("User on_close callback registered."); self._user_on_close_callback = callback
    def on_open(self, callback): self.logger.debug("User on_open callback registered."); self._user_on_open_callback = callback
    def on_error(self, callback): self.logger.debug("User on_error callback registered."); self._user_on_error_callback = callback
    def on_reconnect(self, callback): self.logger.debug("User on_reconnect callback registered."); self._user_on_reconnect_callback = callback; # Link to handler if needed ...


    def on(self, event, callback_function: Callable):
        self.logger.debug(f"Handler registered for event '{event}'"); self.handlers.append((event, callback_function))

    def send(self, method, arguments, on_invocation=None, invocation_id=str(uuid.uuid4())) -> InvocationResult:
        from signalrcore.transport.websockets.connection import ConnectionState
        current_state = getattr(getattr(self, 'transport', None), 'state', ConnectionState.disconnected)
        if current_state != ConnectionState.connected: state_name = current_state.name if isinstance(current_state, ConnectionState) else str(current_state); raise HubConnectionError(f"Cannot send: Hub is not connected (State: {state_name}).")
        if not isinstance(arguments, list) and not isinstance(arguments, Subject): raise TypeError("Arguments must be a list or subject")
        result = InvocationResult(invocation_id)
        if isinstance(arguments, list):
            message = InvocationMessage(invocation_id, method, arguments, headers={})
            if on_invocation: self.stream_handlers.append(InvocationHandler(message.invocation_id, on_invocation))
            self.logger.debug(f"Sending InvocationMessage {invocation_id} for target '{method}'"); self.transport.send(message); result.message = message
        if isinstance(arguments, Subject):
            arguments.connection = self; arguments.target = method; self.logger.debug(f"Starting Subject for target '{method}'"); arguments.start(); result.invocation_id = arguments.invocation_id; result.message = arguments
        return result

    # This is the version with improved logging/checking from response #59
    def on_message(self, messages):
        """Handles messages passed up from the transport layer."""
        self.logger.debug(f"HubConnection on_message received: {messages!r}")
        if not isinstance(messages, list): self.logger.warning(f"HubConnection on_message expected list, got {type(messages)}. Wrapping."); messages = [messages]

        for message_item in messages:
            if message_item is None: self.logger.debug("Skipping None item in message list."); continue
            # Import MessageType locally if not available globally or causing issues
            from signalrcore.messages.message_type import MessageType
            if not hasattr(message_item, 'type') or not isinstance(getattr(message_item, 'type', None), MessageType): self.logger.error(f"Skipping item lacking correct 'type' attribute: {message_item!r}"); continue

            self.logger.debug(f"Processing message object: {message_item}")
            try:
                message = message_item
                # Message Processing Logic ... (abbreviated for clarity)
                if message.type == MessageType.invocation_binding_failure: self.logger.error(f"Binding failure: {message}"); #... call error handler ...
                elif message.type == MessageType.ping: self.logger.debug("Ping received - ignored."); continue
                elif message.type == MessageType.invocation: self.logger.debug(f"Invocation for '{message.target}'"); #... call handlers ...
                elif message.type == MessageType.close: self.logger.info(f"Close received"); break
                elif message.type == MessageType.completion: 
                    self.logger.info(
                        f"RECEIVED CompletionMessage - "
                        f"InvocationID: {message.invocation_id}, "
                        f"Result: {getattr(message, 'result', 'N/A')}, "
                        f"Error: {getattr(message, 'error', 'N/A')}"
                    )
                    # Check if it matches our known Subscribe invocation ID from logs
                    # (You might need to store the invocation ID sent by .send())
                    # Example check (assuming you stored it in self.last_subscribe_id):
                    # if hasattr(self, 'last_subscribe_id') and message.invocation_id == self.last_subscribe_id:
                    #    self.logger.info(f"Completion received for Subscribe command.")
    
                    # Find and potentially complete a waiting invocation handler (existing logic)
                    invocation_handler = next((handler for handler in self.invocation_handlers if handler.invocation_id == message.invocation_id), None)
                    if invocation_handler:
                        invocation_handler.complete(message)
                        # Ensure handler is removed AFTER completion
                        try:
                            self.invocation_handlers.remove(invocation_handler)
                        except ValueError:
                            self.logger.warning(f"Attempted to remove invocation handler {message.invocation_id} but it was already removed.")

                elif message.type == MessageType.stream_item: self.logger.debug(f"StreamItem for '{message.invocation_id}'"); #... process stream item ...
                elif message.type == MessageType.stream_invocation: self.logger.debug("StreamInvocation ignored."); pass
                elif message.type == MessageType.cancel_invocation: self.logger.debug(f"CancelInvocation for '{message.invocation_id}'"); #... process cancel ...
                else: self.logger.warning(f"Unhandled message type: {message.type}")
            except Exception as loop_ex: self.logger.error(f"Error processing message item {message_item!r}: {loop_ex}", exc_info=True); # ... call error handler ...

    def stream(self, event, event_params):
        from signalrcore.transport.websockets.connection import ConnectionState
        current_state = getattr(getattr(self, 'transport', None), 'state', None)
        if current_state != ConnectionState.connected: state_name = current_state.name if isinstance(current_state, ConnectionState) else str(current_state); raise HubConnectionError(f"Cannot start stream: Hub not connected (State: {state_name}).")
        invocation_id = str(uuid.uuid4()); message = StreamInvocationMessage(invocation_id, event, event_params, headers={})
        stream_obj = StreamHandler(event, invocation_id); self.stream_handlers.append(stream_obj)
        self.logger.debug(f"Sending StreamInvocationMessage {invocation_id} for target '{event}'"); self.transport.send(message)
        return stream_obj
    
    def _dispatch_message(self, messages):
        """
        Dispatches parsed messages received from the transport to appropriate handlers.

        Args:
            messages (list): A list of parsed message objects from the protocol layer.
        """
        if not isinstance(messages, list):
            self.logger.error(f"BaseHubConnection _dispatch_message received non-list: {type(messages)}")
            return

        # self.logger.debug(f"Dispatching {len(messages)} message(s)...") # Optional log

        for message_item in messages:
            try:
                # --- Invocation Messages (Server calling client method) ---
                if message_item.type == MessageType.invocation:
                    target = getattr(message_item, 'target', 'UNKNOWN_TARGET')
                    inv_id = getattr(message_item, 'invocation_id', 'N/A')
                    self.logger.debug(f"Dispatch: Invocation Target='{target}' (ID: {inv_id})")

                    # Find registered handlers for this target
                    self.logger.debug(f"  Dispatch: Searching handlers for '{target}' in {self.handlers!r}")
                    # === MODIFICATION START ===
                    # Access the target name using index 0 of the tuple 'h'
                    matches = [h for h in self.handlers if h[0] == target]
                    # === MODIFICATION END ===

                    if not matches:
                        self.logger.warning(f"  Dispatch: No handler found for target '{target}'.")
                        continue

                    # Call all matching handlers
                    for handler_tuple in matches: # Renamed loop variable for clarity
                        # === MODIFICATION START ===
                        try:
                            # Extract target and callback from the tuple using indices
                            handler_target = handler_tuple[0]
                            handler_callback = handler_tuple[1]
                            handler_name = getattr(handler_callback, '__name__', str(handler_callback))

                            self.logger.debug(f"    Dispatch: Calling handler '{handler_name}' for '{handler_target}'")
                            # Call the callback function (the second element of the tuple)
                            handler_callback(message_item.arguments)
                            self.logger.debug(f"    Dispatch: Handler '{handler_name}' for '{handler_target}' returned.")
                        except Exception as handler_ex:
                            # Safely get callback details for error logging
                            callback_in_tuple = handler_tuple[1] if len(handler_tuple) > 1 else "UNKNOWN_CALLBACK"
                            name_in_tuple = getattr(callback_in_tuple, '__name__', str(callback_in_tuple))
                            self.logger.error(f"    Dispatch: Error in handler '{name_in_tuple}' for target '{target}': {handler_ex}", exc_info=True)
                            self._trigger_error(handler_ex) # Trigger connection error handler
                        # === MODIFICATION END ===


                # --- Completion Messages (Result of client's call to server) ---
                elif message_item.type == MessageType.completion:
                    inv_id = getattr(message_item, 'invocation_id', 'N/A')
                    self.logger.debug(f"Dispatch: Completion for Invocation ID '{inv_id}'")
                    # Find matching invocation handler (if tracking client calls)
                    # invocation_handler = next((h for h in self.invocation_handlers if h.invocation_id == inv_id), None)
                    # if invocation_handler:
                    #     invocation_handler.complete(message_item)
                    #     try: self.invocation_handlers.remove(invocation_handler)
                    #     except ValueError: pass
                    # else:
                    #     self.logger.warning(f"Dispatch: Completion for unknown invocation ID: {inv_id}")
                    # --- Simplified: Just log if not actively tracking replies ---
                    self.logger.debug(f"Completion message details: Result={getattr(message_item, 'result', 'N/A')}, Error={getattr(message_item, 'error', 'N/A')}")


                # --- Stream Item Messages (Part of a server stream) ---
                elif message_item.type == MessageType.stream_item:
                    inv_id = getattr(message_item, 'invocation_id', 'N/A')
                    self.logger.debug(f"Dispatch: StreamItem for Invocation ID '{inv_id}'")
                    stream_handler = next((h for h in self.stream_handlers if h.invocation_id == inv_id), None)
                    if stream_handler:
                        stream_handler.subject.on_next(message_item.item)
                    else:
                         self.logger.warning(f"Dispatch: StreamItem for unknown invocation ID: {inv_id}")

                # --- Ping Messages ---
                elif message_item.type == MessageType.ping:
                    self.logger.debug("Dispatch: Ping message received (processed by transport/keep-alive).")

                # --- Close Message ---
                elif message_item.type == MessageType.close:
                    error_msg = getattr(message_item, 'error', 'N/A')
                    allow_reconnect = getattr(message_item, 'allow_reconnect', False)
                    self.logger.info(f"Dispatch: Close message received. Reason: {error_msg}, AllowReconnect: {allow_reconnect}")
                    # Stop the connection gracefully
                    self.stop(send_close=False) # Stop without sending another close frame
                    # Note: Reconnect logic might be handled elsewhere based on state/handler

                # --- Other Types ---
                else:
                    self.logger.warning(f"Dispatch: Unhandled message type received: {message_item.type}")

            except AttributeError as attr_err:
                # Error accessing attributes like 'target', 'type', 'invocation_id'
                self.logger.error(f"Dispatch: Attribute error processing message item {message_item!r}: {attr_err}", exc_info=True)
            except Exception as loop_ex:
                # General error processing a single message
                self.logger.error(f"Dispatch: Error processing message item {message_item!r}: {loop_ex}", exc_info=True)
                # self._trigger_error(loop_ex) # Optionally trigger main error handler

    
    def send_raw_json(self, json_string: str):
        """
        Sends a pre-formatted JSON string directly over the transport.
        Appends the standard record separator. USE WITH CAUTION.
        """
        if not self.transport:
            self.logger.error("Cannot send raw JSON: Transport not initialized.")
            raise HubConnectionError("Transport not initialized.")
    
        # Check if the transport supports the raw send method we added
        if not hasattr(self.transport, 'send_raw'):
            self.logger.error("Cannot send raw JSON: Transport does not have 'send_raw' method.")
            raise NotImplementedError("Transport does not support sending raw data.")
    
        try:
            # Append the record separator expected by the SignalR protocol
            record_separator = '\x1e' # Standard JSON protocol separator
            raw_data_with_separator = json_string + record_separator
            self.logger.info(f"Sending raw JSON message (with separator): {json_string}") # Log before adding sep
            self.transport.send_raw(raw_data_with_separator)
        except Exception as e:
            self.logger.error(f"Error in send_raw_json: {e}", exc_info=True)
            # Propagate or handle the error as appropriate
            raise e # Re-raise the exception
    
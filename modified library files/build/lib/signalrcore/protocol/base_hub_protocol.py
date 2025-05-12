# FILE: /.../signalrcore/protocol/base_hub_protocol.py
# MODIFIED: decode_handshake method replaced, added logger to __init__

import json

# Ensure all necessary message types and helpers are imported
from ..messages.handshake.request import HandshakeRequestMessage
from ..messages.handshake.response import HandshakeResponseMessage
from ..messages.invocation_message import InvocationMessage
from ..messages.stream_item_message import StreamItemMessage
from ..messages.completion_message import CompletionMessage
from ..messages.stream_invocation_message import StreamInvocationMessage
from ..messages.cancel_invocation_message import CancelInvocationMessage
from ..messages.ping_message import PingMessage
from ..messages.close_message import CloseMessage
from ..messages.message_type import MessageType
from ..helpers import Helpers


class BaseHubProtocol(object):
    def __init__(self, protocol, version, transfer_format, record_separator):
        self.protocol = protocol
        self.version = version
        self.transfer_format = transfer_format
        self.record_separator = record_separator
        # --- ADDED LINE ---
        self.logger = Helpers.get_logger()
        # --- END ADDED LINE ---

    # --- START REPLACEMENT for get_message (Improved version from response #93) ---
    @staticmethod
    def get_message(dict_message):
        """Converts a dictionary received from the wire to a message object."""
        # Import Message objects locally to potentially avoid circular issues if needed
        from ..messages.invocation_message import InvocationMessage
        from ..messages.stream_item_message import StreamItemMessage
        from ..messages.completion_message import CompletionMessage
        from ..messages.stream_invocation_message import StreamInvocationMessage
        from ..messages.cancel_invocation_message import CancelInvocationMessage
        from ..messages.ping_message import PingMessage
        from ..messages.close_message import CloseMessage
        from ..messages.message_type import MessageType
        from ..helpers import Helpers # For logging

        # 1. Handle empty dict explicitly -> Treat as Ping/Heartbeat
        if not dict_message: # If dict is empty {}
            # Helpers.get_logger().debug("Received empty dict, interpreting as Ping.")
            return PingMessage()

        # 2. Determine message type from "type" field if present
        message_type = None
        if "type" in dict_message:
            try:
                message_type = MessageType(dict_message["type"])
            except ValueError:
                Helpers.get_logger().warning(f"Unknown message type number {dict_message.get('type')} received.")
                return None # Cannot parse unknown numerical type
        else:
            # No "type" key and dict wasn't empty - Invalid? Or default to Close?
            # F1 sends '{"C":...,"S":1,"M":[]}' initially - this has no type. Let's return None for this.
            # The original default to Close was causing issues.
            Helpers.get_logger().warning(f"Message received without 'type' key, cannot determine type: {dict_message}")
            return None # Return None for messages without a type key

        # 3. Use .get() for safer dictionary access for all message types
        # Prepare args dict, converting camelCase keys if necessary (get_message itself doesn't modify input dict)
        args = dict_message.copy() # Work on a copy
        args["invocation_id"] = args.pop("invocationId", None) # Pop camelCase, keep snake_case
        args["stream_ids"] = args.pop("streamIds", None) # Pop camelCase, keep snake_case
        args["allow_reconnect"] = args.pop("allowReconnect", None) # Pop camelCase, keep snake_case
        # Ensure essential keys expected by specific constructors are present or defaulted if needed
        args["headers"] = args.get("headers", {})
        args["error"] = args.get("error")
        args["result"] = args.get("result")
        args["item"] = args.get("item")
        args["target"] = args.get("target")
        args["arguments"] = args.get("arguments")


        # 4. Construct message object based on type
        try:
            if message_type is MessageType.invocation:
                if args["target"] is None or args["arguments"] is None: raise ValueError("Invocation message missing target or arguments")
                return InvocationMessage(**args)
            if message_type is MessageType.stream_item:
                if args["item"] is None: raise ValueError("StreamItem message missing item")
                return StreamItemMessage(**args)
            if message_type is MessageType.completion:
                return CompletionMessage(**args)
            if message_type is MessageType.stream_invocation:
                if args["target"] is None or args["arguments"] is None: raise ValueError("StreamInvocation message missing target or arguments")
                return StreamInvocationMessage(**args)
            if message_type is MessageType.cancel_invocation:
                # CancelInvocation only requires invocation_id which is handled above
                return CancelInvocationMessage(**args)
            if message_type is MessageType.ping:
                return PingMessage() # Ping has no payload usually
            if message_type is MessageType.close:
                # Pass only relevant args if constructor is strict
                return CloseMessage(error=args.get("error"), allow_reconnect=args.get("allow_reconnect"))
            # Should not be reached if all MessageType enum values covered
            Helpers.get_logger().warning(f"Unhandled known message type {message_type} in get_message.")
            return None
        except (KeyError, TypeError, ValueError) as build_ex:
             # Log error if building message object fails (e.g., missing required args for constructor)
             Helpers.get_logger().error(f"Failed to build message object for type {message_type}: {build_ex} - Dict: {dict_message}")
             return None
    # --- END REPLACEMENT for get_message ---


    # --- START REPLACEMENT of decode_handshake ---
    def decode_handshake(self, raw_message: str):
        """Decodes the handshake response message.
           MODIFIED: To handle initial messages that might lack a record separator
           and might not be the standard empty JSON object {}. Assumes success
           if valid JSON is received without an 'error' key."""
        # Use HandshakeResponseMessage from import at top
        # Use json from import at top

        self.logger.debug(f"Attempting to decode handshake/initial message: {raw_message!r}")
        messages = []
        error = None

        try:
            # Find separator if it exists
            separator_index = -1
            try:
                # Ensure raw_message is string if using index
                if not isinstance(raw_message, str):
                     raw_message = raw_message.decode('utf-8') # Assume utf-8 bytes
                separator_index = raw_message.index(self.record_separator)
                self.logger.debug("Record separator found in initial message.")
            except ValueError:
                # Separator not found - maybe it's the whole message?
                self.logger.warning("Record separator missing in initial message.")
                separator_index = len(raw_message) # Process up to the end
            except Exception as decode_err:
                 # Handle potential decode errors if message wasn't string
                 error = f"Could not decode raw message to string: {decode_err}"
                 self.logger.error(error)
                 return HandshakeResponseMessage(error), []


            # Extract the potential handshake JSON part
            potential_handshake_part = raw_message[:separator_index] if separator_index >= 0 else raw_message
            if separator_index == 0: # Message starts with separator? Empty handshake part.
                 potential_handshake_part = ""

            # Extract any potential buffered data AFTER the separator
            buffered_data = ""
            # Ensure separator was actually found before slicing beyond it
            if separator_index >= 0 and separator_index != len(raw_message):
                 buffered_data = raw_message[separator_index + len(self.record_separator):]

            # Try parsing the handshake part as JSON
            if potential_handshake_part:
                # Handle standard empty handshake explicitly first
                if potential_handshake_part == "{}":
                     self.logger.debug("Standard empty handshake response received.")
                     error = None
                # Try parsing non-empty part as JSON
                elif potential_handshake_part.startswith('{') and potential_handshake_part.endswith('}'):
                     try:
                          decoded_response = json.loads(potential_handshake_part)
                          error = decoded_response.get("error", None) # Check for error field
                          if error:
                               self.logger.error(f"Handshake response contains error: {error}")
                          else:
                               self.logger.debug("Parsed non-empty JSON handshake/initial message successfully.")
                               # If it wasn't the standard empty {}, treat it as a regular message
                               # Ensure it has separator for later parsing
                               if not potential_handshake_part.endswith(self.record_separator):
                                    messages.append(potential_handshake_part + self.record_separator)
                               else:
                                    messages.append(potential_handshake_part)

                     except json.JSONDecodeError as json_err:
                          error = f"Invalid handshake JSON received: {potential_handshake_part!r}. Error: {json_err}"
                          self.logger.error(error)
                else:
                     # If it's not empty JSON and not another JSON object, it's invalid
                     error = f"Invalid handshake response format. Expected JSON object. Got: {potential_handshake_part!r}"
                     self.logger.error(error)

            elif not buffered_data:
                  # Received absolutely nothing or only separator? Treat as error.
                  error = "Received empty handshake response."
                  self.logger.error(error)
            # Else: Handshake part was empty, but there might be buffered data

            # Process buffered data if any exists
            if buffered_data:
                 self.logger.debug(f"Processing remaining data after initial part: {buffered_data!r}")
                 try:
                      # Assume remaining data might contain more messages separated correctly
                      # Use parse_messages which should be implemented by subclass (e.g., JsonHubProtocol)
                      # Ensure separator exists for parse_messages splitting logic
                      if not buffered_data.endswith(self.record_separator):
                           buffered_data += self.record_separator
                      messages.extend(self.parse_messages(buffered_data))
                 except Exception as parse_err:
                      self.logger.error(f"Error parsing buffered data after handshake: {parse_err}")
                      if error is None: # If handshake seemed okay, report this parsing error
                           error = f"Error parsing subsequent data: {parse_err}"

            # Return HandshakeResponse(error), list_of_parsed_messages
            return HandshakeResponseMessage(error), messages

        except Exception as e:
            # Catch any other unexpected errors during parsing
            self.logger.error(f"Unexpected error decoding handshake: {e}", exc_info=True)
            return HandshakeResponseMessage(f"Handshake decoding error: {e}"), []
    # --- END REPLACEMENT of decode_handshake ---


    def handshake_message(self) -> HandshakeRequestMessage:
        # This just returns the object; encoding (including separator) happens in self.encode
        return HandshakeRequestMessage(self.protocol, self.version)

    def parse_messages(self, raw_message: str) -> list:
        # This method should be overridden by subclasses like JsonHubProtocol
        # Providing a basic fallback here is problematic, rely on subclass implementation.
        self.logger.warning("BaseHubProtocol.parse_messages called - subclass should override this.")
        # Raise error or return empty list? Returning empty might hide issues.
        raise NotImplementedError("Protocol subclass must implement parse_messages.")


    def write_message(self, hub_message):
        # Subclass (JsonHubProtocol/MessagePackHubProtocol) implements this
        raise NotImplementedError("Protocol must implement write_message.")


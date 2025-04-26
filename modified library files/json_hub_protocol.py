# FILE: /.../signalrcore/protocol/json_hub_protocol.py
# MODIFIED: Replaced parse_messages method to handle {"M": [...]} envelope

import json

from .base_hub_protocol import BaseHubProtocol

# Import message types needed by get_message if called internally
from ..messages.message_type import MessageType
from ..messages.invocation_message import InvocationMessage
from ..messages.stream_item_message import StreamItemMessage
from ..messages.completion_message import CompletionMessage
from ..messages.stream_invocation_message import StreamInvocationMessage
from ..messages.cancel_invocation_message import CancelInvocationMessage
from ..messages.ping_message import PingMessage
from ..messages.close_message import CloseMessage

from json import JSONEncoder

from signalrcore.helpers import Helpers # Already imported by BaseHubProtocol


class MyEncoder(JSONEncoder):
    # https://github.com/PyCQA/pylint/issues/414
    def default(self, o):
        if isinstance(o, MessageType): # Use isinstance
            return o.value
        # Avoid modifying original dict if o is not instance of specific message types
        if hasattr(o, '__dict__'):
            data = o.__dict__.copy() # Work on copy
            # Use pop with default None to avoid KeyError if key missing
            inv_id = data.pop("invocation_id", None)
            if inv_id is not None: data["invocationId"] = inv_id
            stream_ids = data.pop("stream_ids", None)
            if stream_ids is not None: data["streamIds"] = stream_ids
            # Remove internal attributes if necessary before encoding
            data.pop("logger", None)
            return data
        return super(MyEncoder, self).default(o) # Fallback for other types


class JsonHubProtocol(BaseHubProtocol):
    def __init__(self):
        super(JsonHubProtocol, self).__init__("json", 1, "Text", chr(0x1E))
        self.encoder = MyEncoder()
        # self.logger is inherited from BaseHubProtocol's __init__

    # --- START REPLACEMENT of parse_messages ---
    def parse_messages(self, raw: str) -> list:
        """Parses incoming JSON messages, handling the F1 endpoint's {"M": [...]} envelope."""
        # Use self.logger inherited from base class
        self.logger.debug(f"JsonHubProtocol parsing raw: {raw!r}")

        # Messages might be split by record separator or arrive individually
        # Ensure we process each potential JSON object
        message_parts = [part for part in raw.split(self.record_separator) if part]
        if not message_parts:
            return [] # No actual message content

        parsed_message_objects = []
        for index, part in enumerate(message_parts):
            self.logger.debug(f"Processing message part {index}: {part!r}")
            try:
                outer_message = json.loads(part)

                # Check for the {"M": [...]} envelope structure (F1 specific)
                if "M" in outer_message and isinstance(outer_message["M"], list):
                    self.logger.debug(f"Found 'M' envelope with {len(outer_message['M'])} message(s).")
                    for inner_msg_dict in outer_message["M"]:
                        if isinstance(inner_msg_dict, dict):
                            # Reconstruct a message dict that get_message understands
                            # Assume type 1 (Invocation) for messages within M
                            reconstructed_dict = {
                                "type": MessageType.invocation.value, # Treat as invocation from server
                                "target": inner_msg_dict.get("M"),      # Get stream name from inner "M"
                                "arguments": inner_msg_dict.get("A", []) # Get arguments from inner "A"
                                # We ignore "H" (Hub) for now
                            }
                            # Use get_message from BaseHubProtocol to create object
                            # get_message needs access to the message type enum value
                            message_obj = self.get_message(reconstructed_dict)
                            if message_obj:
                                parsed_message_objects.append(message_obj)
                            else:
                                self.logger.warning(f"get_message failed for inner message: {reconstructed_dict}")
                        else:
                             self.logger.warning(f"Item inside 'M' array is not a dictionary: {inner_msg_dict!r}")

                # Check for initial data {"R": ...} structure (less common for live updates)
                elif "R" in outer_message:
                    self.logger.info(f"Received initial state message ('R'): {str(outer_message['R'])[:100]}...")
                    # Skip processing 'R' structure for now as we don't have a handler type for it
                    pass

                # Check if it's a standard message with "type" (like Ping, Close, Completion)
                elif "type" in outer_message:
                    self.logger.debug("Processing message with top-level 'type'.")
                    message_obj = self.get_message(outer_message)
                    if message_obj:
                        parsed_message_objects.append(message_obj)
                    else:
                        self.logger.warning(f"get_message failed for typed message: {outer_message}")

                # Check for empty dict {} case -> Ping/Heartbeat
                elif not outer_message:
                     message_obj = self.get_message(outer_message) # Patched get_message handles this
                     if message_obj:
                          parsed_message_objects.append(message_obj)
                     else: # Should not happen if get_message patch is correct
                          self.logger.warning("get_message failed for empty dict.")

                else:
                     # Unknown structure, wasn't M, R, or typed, not empty
                     # This is where {"C":...} likely ends up - we want to ignore it.
                     self.logger.debug(f"Received message with unknown structure or no type, ignoring: {part!r}")

            except json.JSONDecodeError as json_ex:
                self.logger.error(f"Failed to decode JSON message part: {part!r} - Error: {json_ex}")
            except Exception as ex:
                self.logger.error(f"Error processing message part: {part!r} - Error: {ex}", exc_info=True) # Add traceback

        self.logger.debug(f"JsonHubProtocol returning parsed messages list (count={len(parsed_message_objects)}): {parsed_message_objects!r}")
        return parsed_message_objects
    # --- End Replacement ---


    def encode(self, message):
        # Use self.logger inherited from base class
        encoded = self.encoder.encode(message) + self.record_separator
        self.logger.debug(f"Encoded message: {encoded!r}")
        return encoded
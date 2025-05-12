# FILE: /.../signalrcore/protocol/json_hub_protocol.py
# MODIFIED: Replaced parse_messages method to handle {"M": [...]} envelope

import json
import logging
import datetime
from datetime import timezone

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
    def parse_messages(self, raw_message: str) -> list:
        """
        Parses incoming JSON messages, handling F1's {"M": [...]} envelope,
        Completion messages {"I":..., "R/E":...}, AND initial snapshot messages {"R":{...}}.
        """
        # self.logger.debug(f"JsonHubProtocol parsing raw: {raw_message!r}")
        parsed_message_objects = []
        message_parts = [part for part in raw_message.split(self.record_separator) if part]
    
        for index, part in enumerate(message_parts):
            #self.logger.debug(f"Processing message part {index}: {part!r}")
            if not part: continue
    
            try:
                outer_message = json.loads(part)
                message_obj = None # Reset message_obj for each part
                log_level = logging.DEBUG
                detected_type_str = "Unknown structure"
    
                if isinstance(outer_message, dict):
                    invocation_id = outer_message.get("I") # Check for Invocation ID first
    
                    # --- 2. Check for Initial Snapshot ("R" present, but "I" is NOT) ---
                    if "R" in outer_message: # "I" was None from the check above
                        detected_type_str = "Initial Snapshot 'R' block"
                        log_level = logging.INFO
                        self.logger.log(log_level, f"JsonHubProtocol.parse_messages DETECTED: {detected_type_str}")
                    
                        data_snapshot = outer_message.get("R")
                        #self.logger.debug(f"  R Block: Value type for key 'R' is {type(data_snapshot)}. Value snippet: {str(data_snapshot)[:200]}") # Keep this check
                    
                        if isinstance(data_snapshot, dict):
                            snapshot_timestamp = datetime.datetime.now(timezone.utc).isoformat() + 'Z'
                            item_count = len(data_snapshot) # item_count assigned here
                            #self.logger.debug(f"  R Block: Found {item_count} items in snapshot dict. Timestamp: {snapshot_timestamp}. Starting loop...")
                            processed_count = 0 # processed_count assigned here
                    
                            # Iterate through the streams in the snapshot
                            for stream_name, stream_data in data_snapshot.items():
                                try:
                                    # ... (logic to construct dict, call get_message, check result) ...
                                    constructed_dict = {
                                        "type": MessageType.invocation.value,
                                        "target": "feed",
                                        "arguments": [stream_name, stream_data, snapshot_timestamp]
                                    }
                                    #self.logger.debug(f"    R Block Loop [{stream_name}]: Preparing get_message. Dict: {constructed_dict!r}")
                                    invocation_obj = self.get_message(constructed_dict)
                                    #self.logger.debug(f"    R Block Loop [{stream_name}]: get_message returned: {invocation_obj!r} (Type: {type(invocation_obj)})")
                    
                                    if invocation_obj and invocation_obj.type == MessageType.invocation:
                                        self.logger.debug(f"      R Block Loop [{stream_name}]: Object is valid InvocationMessage. Appending...")
                                        parsed_message_objects.append(invocation_obj)
                                        processed_count += 1
                                        self.logger.debug(f"      R Block Loop [{stream_name}]: Append successful.")
                                    else:
                                        self.logger.error(f"    R Block Loop [{stream_name}]: get_message FAILED or returned wrong type. Result: {invocation_obj!r}")
                    
                                except Exception as item_ex:
                                    self.logger.error(f"    R Block Loop [{stream_name}]: EXCEPTION during processing: {item_ex}", exc_info=True)
                                    continue
                    
                            # --- START FIX: Move this log INSIDE the 'if isinstance...' block ---
                            # Log summary only if we actually processed a dictionary
                            self.logger.debug(f"  R Block: Loop finished. Successfully processed {processed_count}/{item_count} items.")
                            # --- END FIX ---
                    
                        else:
                            # Log if 'R' value is not a dictionary
                            self.logger.warning(f"  R Block: Expected 'R' key to contain a dictionary, but found type {type(data_snapshot)}. Raw part: {part!r}")
                    
                        message_obj = None # Prevent falling through
                    
    
                    # --- 3. Check for F1 Data Envelope ("M") if not Completion or Snapshot ---
                    elif "M" in outer_message:
                        detected_type_str = "'M' block (Update)"
                        log_level = logging.INFO
                        self.logger.log(log_level, f"JsonHubProtocol.parse_messages DETECTED: {detected_type_str}")
                        # ... (Keep existing 'M' block logic from Response #19 / #7 to parse items inside M) ...
                        inner_messages = outer_message.get("M", [])
                        if isinstance(inner_messages, list):
                            for inner_dict in inner_messages:
                                # ... (logic to extract target/args, construct invocation dict, call get_message, append) ...
                                if isinstance(inner_dict, dict):
                                    target = inner_dict.get("M")
                                    arguments = inner_dict.get("A", [])
                                    if target is not None:
                                        constructed_dict = {"type": MessageType.invocation.value,"target": target,"arguments": arguments}
                                        invocation_obj = self.get_message(constructed_dict)
                                        if invocation_obj and invocation_obj.type == MessageType.invocation:
                                            self.logger.debug(f"  Created InvocationMessage for target '{target}' from 'M' block item.")
                                            parsed_message_objects.append(invocation_obj)
                                        # ... else warnings ...
                                    # ... else warnings ...
                                # ... else warnings ...
                        # ... else warnings ...
                        message_obj = None # Handled here
    
                    # --- 4. Check for Standard Typed Messages ("type") ---
                    elif "type" in outer_message:
                        # ... (Keep existing logic for typed messages, including Ping via type 6) ...
                        msg_type = outer_message['type']
                        detected_type_str = f"Typed message (type {msg_type})"
                        log_level = logging.INFO if msg_type != MessageType.ping.value else logging.DEBUG
                        message_obj = self.get_message(outer_message)
    
    
                    # --- 5. Check for Ping ({}) if not handled above (should be caught by type 6 now but keep as fallback) ---
                    elif not outer_message:
                        detected_type_str = "Empty dict {} (Fallback Ping)"
                        log_level = logging.DEBUG
                        message_obj = self.get_message(outer_message) # Should return PingMessage
    
                    # --- Log detection for non-'M'/'R' cases ---
                    if detected_type_str not in ["'M' block (Update)", "Initial Snapshot 'R' block"]:
                       self.logger.log(log_level, f"JsonHubProtocol.parse_messages DETECTED: {detected_type_str}")
    
                    # Append message object if one was created (Completion, Typed, Ping)
                    if message_obj:
                        parsed_message_objects.append(message_obj)
    
                else: # If outer_message wasn't a dict
                    self.logger.warning(f"Received message part that is not a dictionary, ignoring: {part!r}")
    
            # ... (keep existing except blocks) ...
            except json.JSONDecodeError as json_ex:
                self.logger.error(f"Failed to decode JSON part: {part!r} - Error: {json_ex}")
            except Exception as ex:
                self.logger.error(f"Error processing message part: {part!r} - Error: {ex}", exc_info=True)
    
    
        #self.logger.debug(f"JsonHubProtocol returning parsed messages list (count={len(parsed_message_objects)}): {parsed_message_objects!r}")
        return parsed_message_objects
    # --- End Method ---


    def encode(self, message):
        # Use self.logger inherited from base class
        encoded = self.encoder.encode(message) + self.record_separator
        self.logger.debug(f"Encoded message: {encoded!r}")
        return encoded
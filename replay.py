# replay.py
"""
Handles replay file discovery, loading, playback control,
and managing state and file I/O for library-based live recording.
"""

import logging
import os
import json
import time
import datetime
from datetime import timezone, timedelta # Import timedelta
import threading
import queue # For queue.Empty
from pathlib import Path
import math
import re # Import re for timestamp regex

# Import shared state and config
import app_state
import config # For REPLAY_DIR, TARGET_SAVE_DIRECTORY etc.
from utils import sanitize_filename, get_current_or_next_session_info # Import helpers

replay_thread = None

# Import utilities
import utils # For file/session/timestamp helpers

# --- Globals within this module ---\nreplay_thread = None

# Get logger instance
logger = logging.getLogger("F1App.Replay")

# --- File Handling Helpers ---
def ensure_replay_dir_exists():
    """Creates the replay directory if it doesn't exist."""
    replay_dir_path = Path(config.REPLAY_DIR)
    if not replay_dir_path.exists():
        try: replay_dir_path.mkdir(parents=True, exist_ok=True); logger.info(f"Created replay directory: {replay_dir_path}")
        except OSError as e: logger.error(f"Failed to create replay directory '{replay_dir_path}': {e}")
    # Also ensure target save directory exists if different
    target_save_path = Path(config.TARGET_SAVE_DIRECTORY)
    if not target_save_path.exists() and target_save_path != replay_dir_path:
        try: target_save_path.mkdir(parents=True, exist_ok=True); logger.info(f"Created target save directory: {target_save_path}")
        except Exception as e: logger.error(f"Failed to ensure target save directory '{config.TARGET_SAVE_DIRECTORY}': {e}")

def get_replay_files(directory):
    """Gets a list of .data.txt files from the specified directory."""
    ensure_replay_dir_exists()
    dir_path = Path(directory)
    files = []
    if dir_path.exists() and dir_path.is_dir():
        try:
            files = sorted([f.name for f in dir_path.glob('*.data.txt') if f.is_file()])
            logger.debug(f"Found replay files in {directory}: {files}")
        except Exception as e:
            logger.error(f"Error scanning directory '{directory}' for replay files: {e}")
    else:
         logger.warning(f"Replay directory '{directory}' not found or is not a directory.")
    return files

# --- Live Data Saving State Management & File I/O ---
# *** UPDATED init_live_file and close_live_file ***

def init_live_file():
    """
    Prepares the application state AND opens the file for saving live data.
    Names the file based on current/next session info from FastF1 if possible.
    Returns True if setup was successful, False otherwise.
    """
    if not config.TARGET_SAVE_DIRECTORY:
        logger.error("TARGET_SAVE_DIRECTORY is not configured. Cannot save live data.")
        return False

    ensure_replay_dir_exists() # Ensure target dir exists

    # --- Get session info using FastF1 FIRST ---
    logger.info("Attempting to get session info via FastF1 for filename...")
    try:
         event_name, session_name_from_f1 = get_current_or_next_session_info() # Call helper
    except Exception as f1_err:
         # Catch errors during the fastf1 call itself
         logger.error(f"Error calling get_current_or_next_session_info: {f1_err}", exc_info=True)
         event_name, session_name_from_f1 = None, None

    # --- Determine filename prefix ---
    filename_prefix = None
    if event_name and session_name_from_f1:
         # Sanitize parts obtained from FastF1
         event_part = sanitize_filename(event_name)
         session_part = sanitize_filename(session_name_from_f1)
         filename_prefix = f"{event_part}_{session_part}"
         logger.info(f"Using filename prefix from FastF1: {filename_prefix}")
    else:
         # Fallback if FastF1 failed or returned nothing
         logger.warning("Could not get session info from FastF1, using fallback filename prefix 'F1LiveData'.")
         # Optionally, you could *still* try reading app_state.session_details here as a secondary fallback
         # if you expect it might sometimes be populated before init_live_file runs,
         # but the primary approach should be FastF1.
         filename_prefix = "F1LiveData"

    # --- Generate Filename and Path ---
    timestamp = datetime.datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S%Z") # Use UTC timestamp
    filename = f"{filename_prefix}_{timestamp}.data.txt"
    filepath = Path(config.TARGET_SAVE_DIRECTORY) / filename
    temp_file_handle = None

    # --- Open File and Update State ---
    try:
        temp_file_handle = open(filepath, 'a', encoding='utf-8') # Append mode
        logger.info(f"Successfully opened live data file for appending: {filepath}")

        with app_state.app_state_lock:
            # Close previous file if open
            if app_state.live_data_file and not app_state.live_data_file.closed:
                logger.warning("Closing previously open live data file in init_live_file.")
                try: app_state.live_data_file.close()
                except Exception as close_err: logger.error(f"Error closing previous live file: {close_err}")
            # Store new handle and flags
            app_state.current_recording_filename = str(filepath)
            app_state.live_data_file = temp_file_handle
            app_state.is_saving_active = True
            logger.info(f"Live data recording state initialized and file opened. Saving enabled.")

        # Write a header/marker to the file
        header = f"# Recording Started: {datetime.datetime.now(timezone.utc).isoformat()}Z\n"
        # Add FastF1 info to header if available
        header += f"# Session Info (from FastF1 at start): Event='{event_name}', Session='{session_name_from_f1}'\n"
        temp_file_handle.write(header)
        temp_file_handle.flush()

        return True # Success

    except Exception as e:
       logger.error(f"Failed to initialize live recording state or open file {filepath}: {e}", exc_info=True)
       if temp_file_handle:
           try: temp_file_handle.close()
           except: pass
       with app_state.app_state_lock:
           app_state.is_saving_active = False
           app_state.current_recording_filename = None
           app_state.live_data_file = None
       return False

def close_live_file(acquire_lock=True):
    """Closes the live data file handle and clears recording flags in app_state."""
    logger.debug(f"close_live_file called (acquire_lock={acquire_lock})")
    lock_acquired = False
    file_handle_to_close = None # Temporary variable to hold handle outside lock

    try:
        if acquire_lock:
             # Use a timeout to prevent potential deadlocks
             lock_acquired = app_state.app_state_lock.acquire(timeout=2.0)
             if not lock_acquired:
                 logger.error("Failed to acquire app_state_lock in close_live_file! Cannot close file or clear state.")
                 return # Exit if lock not acquired

        # Safely get the handle and clear state variables
        current_filename = app_state.current_recording_filename
        if app_state.live_data_file:
             file_handle_to_close = app_state.live_data_file # Get handle
             logger.info(f"Preparing to close recording file: {current_filename or 'Unknown Filename'}")

        # Clear state regardless of whether handle existed (might have failed init)
        app_state.is_saving_active = False # Signal library to stop attempting writes
        app_state.current_recording_filename = None
        app_state.live_data_file = None # Clear handle reference from state *immediately*

        if file_handle_to_close:
            logger.info("Live recording state cleared.")
        elif current_filename:
            logger.warning(f"Recording filename '{current_filename}' was set, but no file handle found in app_state during close.")
        else:
            logger.debug("No active recording file or handle found during close_live_file.")


    except Exception as e:
         logger.error(f"Error clearing recording state in close_live_file: {e}", exc_info=True)
    finally:
        if lock_acquired:
            try: app_state.app_state_lock.release()
            except threading.ThreadError: logger.warning("Attempted to release lock in close_live_file when not held.")

        # *** Close the file handle *after* releasing the lock ***
        if file_handle_to_close:
            try:
                logger.info(f"Closing file handle for: {current_filename or 'previously recorded file'}")
                # Write a footer before closing
                footer = f"\n# Recording Stopped: {datetime.datetime.now(timezone.utc).isoformat()}Z\n"
                file_handle_to_close.write(footer)
                file_handle_to_close.flush() # Ensure footer is written
                file_handle_to_close.close()
                logger.info("File handle closed successfully.")
            except Exception as close_err:
                logger.error(f"Error closing live data file handle: {close_err}", exc_info=True)

        logger.debug("close_live_file finished.")


# --- Replay Logic ---
# _queue_message_from_replay(), _replay_thread_target(), replay_from_file(), stop_replay()
# should remain the same as in Response #19.

def _queue_message_from_replay(line_content):
    """
    Parses the line_content (expected JSON string) and queues messages
    in the standard dictionary format {"stream":..., "data":..., "timestamp":...}.
    It determines the timestamp based on embedded data if possible.
    Returns the number of messages successfully queued from this line.
    """
    queued_count = 0
    try:
        # Decode the JSON from the raw line content
        message_data = json.loads(line_content)
        default_timestamp = datetime.datetime.now(timezone.utc).isoformat() + 'Z' # Fallback

        # Handle {"M": [...]} blocks
        if isinstance(message_data, dict) and 'M' in message_data and isinstance(message_data['M'], list):
            for msg_container in message_data['M']:
                if isinstance(msg_container, dict) and msg_container.get("M") == "feed":
                    msg_args = msg_container.get("A")
                    if isinstance(msg_args, list) and len(msg_args) >= 2:
                        stream_name_raw = msg_args[0]
                        data_content = msg_args[1]
                        embedded_ts_str = msg_args[2] if len(msg_args) > 2 else None
                        # Use embedded timestamp for the item, or fallback
                        final_timestamp = embedded_ts_str if embedded_ts_str else default_timestamp

                        stream_name = stream_name_raw
                        actual_data = data_content
                        # Decompress if needed
                        if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                            stream_name = stream_name_raw[:-2]
                            actual_data = utils._decode_and_decompress(data_content) # Use utils
                            if actual_data is None: logger.warning(f"Decode fail M block: {stream_name_raw}"); continue # Skip this message

                        # Queue the dictionary
                        if actual_data is not None:
                            item_to_queue = {"timestamp": final_timestamp, "stream": stream_name, "data": actual_data}
                            # Debug log can be verbose, consider commenting out when working
                            # logger.debug(f"REPLAY QUEUE PUT (from M): Type={type(item_to_queue)}, Keys={' '.join(item_to_queue.keys())}, Stream={stream_name}")
                            app_state.data_queue.put(item_to_queue)
                            queued_count += 1
                    else: logger.warning(f"Malformed 'feed' args in M block: {msg_args}")
                # else: Skip non-"feed" messages within "M" block silently

        # Handle {"R": {...}} blocks (Snapshots)
        elif isinstance(message_data, dict) and "R" in message_data:
            snapshot_data = message_data.get("R", {})
            if isinstance(snapshot_data, dict):
                # Use Heartbeat timestamp as the single timestamp for all items in snapshot, or fallback
                snapshot_ts = snapshot_data.get("Heartbeat", {}).get("Utc") or default_timestamp
                for stream_name_raw, stream_data in snapshot_data.items():
                    stream_name = stream_name_raw; actual_data = stream_data
                    if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                        stream_name = stream_name_raw[:-2]
                        actual_data = utils._decode_and_decompress(stream_data) # Use utils
                        if actual_data is None: logger.warning(f"Decode fail R block: {stream_name_raw}"); continue
                    # Queue individual items from the snapshot
                    if actual_data is not None:
                        item_to_queue = {"timestamp": snapshot_ts, "stream": stream_name, "data": actual_data}
                        # logger.debug(f"REPLAY QUEUE PUT (from R): Type={type(item_to_queue)}, Keys={' '.join(item_to_queue.keys())}, Stream={stream_name}")
                        app_state.data_queue.put(item_to_queue)
                        queued_count += 1
            else: logger.warning(f"Snapshot block 'R' content not dict: {type(snapshot_data)}")

        # Handle direct list messages: ["StreamName", data, timestamp?]
        elif isinstance(message_data, list) and len(message_data) >= 2:
             stream_name_raw = message_data[0]
             data_content = message_data[1]
             embedded_ts_str = message_data[2] if len(message_data) > 2 else None
             final_timestamp = embedded_ts_str if embedded_ts_str else default_timestamp

             stream_name = stream_name_raw
             actual_data = data_content
             # Decompress if needed
             if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                 stream_name = stream_name_raw[:-2]
                 actual_data = utils._decode_and_decompress(data_content) # Use utils
                 if actual_data is None: logger.warning(f"Decode fail direct list: {stream_name_raw}"); return 0

             # Queue the dictionary
             if actual_data is not None:
                 item_to_queue = {"timestamp": final_timestamp, "stream": stream_name, "data": actual_data}
                 # logger.debug(f"REPLAY QUEUE PUT (from List): Type={type(item_to_queue)}, Keys={' '.join(item_to_queue.keys())}, Stream={stream_name}")
                 app_state.data_queue.put(item_to_queue)
                 queued_count += 1

        # Silently ignore other valid JSON structures we don't need to queue
        elif isinstance(message_data, dict) and not message_data: pass # Empty dict {}
        elif isinstance(message_data, dict) and ("C" in message_data or "E" in message_data or "G" in message_data or "S" in message_data or "I" in message_data): pass # Control messages
        else:
             logger.warning(f"Unhandled JSON structure type in _queue_message_from_replay: {type(message_data)}")

    except json.JSONDecodeError:
        # Error already logged in _replay_thread_target
        return 0 # Indicate nothing was queued
    except Exception as e:
        logger.error(f"Unexpected error in _queue_message_from_replay for line '{line_content[:100]}...': {e}", exc_info=True)
        return 0 # Indicate nothing was queued

    return queued_count # Return number of messages actually queued from this line

def _replay_thread_target(filename, speed=1.0):
    """
    Reads the replay file line by line, extracting EMBEDDED timestamps
    to calculate delays adjusted by speed, and queues messages.
    """
    # Use globals().get() workaround if the NameError persists in your environment
    # global replay_thread # Keep global for assignment if needed below
    filepath = config.REPLAY_DIR / filename # Use config path if defined
    logger.info(f"Replay thread started for file: {filepath} at speed: {speed}x")

    # --- Speed validation ---
    try:
        playback_speed = float(speed)
        if playback_speed <= 0 or math.isnan(playback_speed) or math.isinf(playback_speed):
             logger.warning(f"Invalid playback speed ({speed}) received. Defaulting to 1.0x.")
             playback_speed = 1.0
    except (ValueError, TypeError):
        logger.warning(f"Could not convert speed ({speed}) to float. Defaulting to 1.0x.")
        playback_speed = 1.0
    # --- End Speed validation ---

    last_message_dt = None # Store the datetime object of the last processed message timestamp used for delay
    lines_processed = 0
    lines_skipped_json_error = 0
    lines_skipped_other = 0
    first_message_processed = False

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if app_state.stop_event.is_set():
                    logger.info("Replay thread: Stop event detected, exiting.")
                    break

                line = line.strip()
                if not line: continue # Skip empty lines

                start_time_line = time.monotonic() # For adjusting sleep based on processing time
                current_message_dt = None # Timestamp for the *current* message used for delay calculation
                timestamp_str_for_delay = None # String version of timestamp used for delay calculation
                time_to_wait = 0 # Initialize delay for this line

                try:
                    # Always try to load the line as JSON
                    raw_message = json.loads(line)
                    message_queued_count = 0 # Track messages queued from this line

                    # --- Timestamp Extraction FOR DELAY ---
                    # Try to find a timestamp suitable for delay calculation (typically A[2])
                    if isinstance(raw_message, dict) and 'M' in raw_message and isinstance(raw_message['M'], list):
                        # Look in the *last* "feed" message within the "M" block for A[2]
                        for msg_container in reversed(raw_message['M']): # Check from the end
                             if isinstance(msg_container, dict) and msg_container.get("M") == "feed":
                                 msg_args = msg_container.get("A")
                                 if isinstance(msg_args, list) and len(msg_args) > 2:
                                     timestamp_str_for_delay = msg_args[2] # Found potential timestamp string
                                     if timestamp_str_for_delay: break # Use the first one found from the end
                    elif isinstance(raw_message, list) and len(raw_message) > 2:
                         # For direct list messages ["StreamName", data, timestamp]
                         timestamp_str_for_delay = raw_message[2]

                    # If we found a timestamp string for delay, parse it
                    if timestamp_str_for_delay:
                         current_message_dt = utils.parse_iso_timestamp_safe(timestamp_str_for_delay, line_num)

                    # --- End Timestamp Extraction FOR DELAY ---

                    # --- Process and Queue the message(s) using the other function ---
                    # This function handles different structures ('M', 'R', list) and puts DICTs on queue
                    queued_count = _queue_message_from_replay(line_content=line) # Pass raw line content

                    if queued_count > 0: lines_processed += queued_count # Count based on actual queued items
                    # Don't increment skipped here, _queue_message handles logging warnings for bad structures

                    # --- Delay Calculation (based on extracted timestamp for delay) ---
                    if current_message_dt: # If we successfully parsed a timestamp suitable for delay
                        if not first_message_processed:
                             time_to_wait = 0 # No delay for the very first timestamped message
                             first_message_processed = True
                        elif last_message_dt: # If we have a timestamp from the previous relevant line
                             try:
                                 time_diff_seconds = (current_message_dt - last_message_dt).total_seconds()
                                 time_to_wait = max(0, time_diff_seconds) # Ensure non-negative
                             except Exception as dt_err:
                                 logger.warning(f"Error calculating time diff line {line_num}: {dt_err}")
                                 time_to_wait = 0 # Default to no delay on error
                        # else: last_message_dt is None (should only happen before first message)

                        # ** IMPORTANT: Update last_message_dt ONLY if we used its timestamp for delay calc **
                        last_message_dt = current_message_dt

                    # else: No usable timestamp found in this line for delay calc, time_to_wait remains 0

                except json.JSONDecodeError:
                     lines_skipped_json_error += 1
                     if line_num > 5: # Be less noisy about initial non-json lines
                         logger.warning(f"Invalid JSON line {line_num} (skipped): {line[:100]}...")
                     continue # Skip sleep logic for lines that aren't valid JSON
                except Exception as e:
                     lines_skipped_other += 1
                     logger.error(f"Error processing line {line_num}: {e} - Line: {line[:100]}...", exc_info=True)
                     continue # Skip sleep logic on unexpected error

                # --- Apply Sleep Logic ---
                if time_to_wait > 0:
                    target_delay = time_to_wait / playback_speed
                    processing_time = time.monotonic() - start_time_line
                    # Simple adjustment: subtract processing time from target delay
                    adjusted_sleep_time = max(0, target_delay - processing_time)

                    max_reasonable_sleep = 5.0 # Prevent excessively long sleeps if timestamps jump weirdly
                    final_sleep = min(adjusted_sleep_time, max_reasonable_sleep)

                    if final_sleep > 0.001: # Avoid sleep calls for negligible amounts
                        # logger.debug(f"Line {line_num}: Wait={time_to_wait:.3f}, Speed={playback_speed:.1f}, Sleep={final_sleep:.3f} (Proc: {processing_time:.3f})")
                        time.sleep(final_sleep)
                # else: No sleep if time_to_wait is 0


            # End of file loop
            logger.info(f"Replay file '{filename}' finished. Queued Messages: {lines_processed}, Skipped JSON: {lines_skipped_json_error}, Skipped Other: {lines_skipped_other}")

    except FileNotFoundError:
        logger.error(f"Replay Error: File not found at {filepath}")
        with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": f"Error: Replay file not found"})
    except Exception as e:
        logger.error(f"Replay Error: An unexpected error occurred reading {filepath}: {e}", exc_info=True)
        with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": f"Error: Replay failed ({type(e).__name__})"})
    finally:
        # Clean up state
        with app_state.app_state_lock:
            if app_state.app_status['state'] == "Replaying":
                # Determine final state based on whether stop was triggered or file ended naturally
                final_state = "Stopped" if app_state.stop_event.is_set() else "Playback Complete"
                final_conn_msg = "Replay Stopped" if app_state.stop_event.is_set() else "Replay Finished"
                app_state.app_status['state'] = final_state
                app_state.app_status['connection'] = final_conn_msg
            app_state.app_status['current_replay_file'] = None
        logger.info(f"Replay thread for '{filename}' finishing execution.")

        # Clean up the global replay_thread variable using the globals().get() workaround if needed
        current_thread_obj = globals().get('replay_thread')
        if threading.current_thread() is current_thread_obj:
             # Need global keyword here because we are *assigning* to the module variable
             global replay_thread
             replay_thread = None


# *** MODIFIED replay_from_file to accept and pass speed ***
def replay_from_file(filename, speed=1.0): # Added speed argument with default
    """Starts the replay process in a background thread with a given speed."""
    global replay_thread
    if replay_thread and replay_thread.is_alive():
        logger.warning("Replay already in progress. Please stop the current replay first.")
        return

    if app_state.stop_event.is_set():
         logger.warning("Stop event was set before starting replay, clearing it.")
         app_state.stop_event.clear()

    logger.info(f"Starting replay for file: {filename} at speed {speed}x")
    with app_state.app_state_lock:
        app_state.app_status.update({
            "state": "Replaying",
            "connection": f"Replaying: {filename} ({speed}x)", # Include speed in status
            "current_replay_file": filename
        })
        # Clear previous data stores if needed (uncomment if desired)
        # logger.info("Clearing previous session data for replay...")
        # app_state.data_store.clear(); app_state.timing_state.clear(); app_state.track_status_data.clear()
        # app_state.session_details.clear(); app_state.race_control_log.clear()

    # Create and start the replay thread, passing filename AND speed
    replay_thread = threading.Thread(target=_replay_thread_target, args=(filename, speed), name="ReplayThread", daemon=True)
    replay_thread.start()

def stop_replay():
    """Stops the currently running replay thread. (Implementation from Response #19)"""
    global replay_thread
    local_thread = replay_thread
    if not local_thread or not local_thread.is_alive():
        logger.info("Stop replay called, but no active replay thread found."); # Ensure state cleanup (omitted for brevity)
        with app_state.app_state_lock:
             if app_state.app_status["state"] == "Replaying": app_state.app_status.update({"state": "Stopped", "connection": "Disconnected (Replay Ended)"})
             app_state.app_status["current_replay_file"] = None
        if replay_thread is local_thread: replay_thread = None
        return
    logger.info("Stopping replay...")
    with app_state.app_state_lock:
        if app_state.app_status["state"] == "Replaying": app_state.app_status.update({"state": "Stopping", "connection": "Stopping Replay..."})
    app_state.stop_event.set()
    logger.info("Waiting for replay thread to join...")
    local_thread.join(timeout=5)
    if local_thread.is_alive(): logger.warning("Replay thread did not stop cleanly.")
    else: logger.info("Replay thread joined successfully.")
    with app_state.app_state_lock:
        app_state.app_status.update({"state": "Stopped", "connection": "Disconnected (Replay Stopped)"})
        app_state.app_status["current_replay_file"] = None
    if replay_thread is local_thread: replay_thread = None
    # IMPORTANT: Consider clearing stop_event in start_live/start_replay instead of here.
    # app_state.stop_event.clear() # Temporarily commented out, clear on start instead.
    logger.info("Stop replay sequence complete.")


print("DEBUG: replay module loaded (with updated file handling)")
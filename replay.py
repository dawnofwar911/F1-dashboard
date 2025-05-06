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

def _queue_message_from_replay(message_data): # Argument is now the parsed message (dict/list)
    """
    Helper for replay loop: Processes parsed message data (dict/list)
    and queues structured items.
    """
    put_count = 0
    try:
        # --- REMOVED redundant json.loads(line_content) ---

        # Now directly check the type of the passed message_data
        if isinstance(message_data, dict) and "R" in message_data:
            # Logic to handle R block (Snapshot)
            snapshot_data = message_data.get("R", {})
            if isinstance(snapshot_data, dict):
                snapshot_ts = snapshot_data.get("Heartbeat", {}).get("Utc") or (datetime.datetime.now(timezone.utc).isoformat() + 'Z')
                for stream_name_raw, stream_data in snapshot_data.items():
                    stream_name = stream_name_raw; actual_data = stream_data
                    if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                        stream_name = stream_name_raw[:-2]
                        actual_data = utils._decode_and_decompress(stream_data)
                        if actual_data is None: logger.warning(f"Failed decode {stream_name_raw} in R"); continue
                    if actual_data is not None:
                        app_state.data_queue.put({"stream": stream_name, "data": actual_data, "timestamp": snapshot_ts})
                        put_count += 1
                # Log moved outside inner loop
                if put_count > 0: logger.debug(f"Queued {put_count} streams from snapshot (R) block.")
            else:
                logger.warning(f"Snapshot block 'R' non-dict: {type(snapshot_data)}")

        elif isinstance(message_data, list) and len(message_data) >= 2:
            # Logic to handle direct list ["StreamName", data, "Timestamp"]
            stream_name_raw = message_data[0]; data_content = message_data[1]
            timestamp_for_queue = message_data[2] if len(message_data) > 2 else (datetime.datetime.now(timezone.utc).isoformat() + 'Z')
            stream_name = stream_name_raw; actual_data = data_content
            if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                stream_name = stream_name_raw[:-2]
                actual_data = utils._decode_and_decompress(data_content)
                if actual_data is None: logger.warning(f"Failed decode {stream_name_raw} list msg"); return 0
            if actual_data is not None:
                app_state.data_queue.put({"stream": stream_name, "data": actual_data, "timestamp": timestamp_for_queue})
                put_count += 1

        elif isinstance(message_data, dict) and not message_data: # Heartbeat {}
             app_state.data_queue.put({"stream": "Heartbeat", "data": {}, "timestamp": datetime.datetime.now(timezone.utc).isoformat() + 'Z'})
             put_count += 1

        # Handle M blocks if they weren't handled in the main loop (they are in Response 49 version)
        # If _replay_thread_target doesn't handle M blocks, add logic here:
        elif isinstance(message_data, dict) and "M" in message_data and isinstance(message_data["M"], list):
            queued_count_m = 0; last_ts_in_m = None
            for msg_container in message_data["M"]:
                if isinstance(msg_container, dict) and msg_container.get("M") == "feed":
                    msg_args = msg_container.get("A")
                    if isinstance(msg_args, list) and len(msg_args) >= 2:
                         snr=msg_args[0]; dc=msg_args[1]; last_ts_in_m = msg_args[2] if len(msg_args)>2 else datetime.datetime.now(timezone.utc).isoformat()+'Z'
                         sn=snr; ad=dc
                         if isinstance(snr, str) and snr.endswith('.z'): sn=snr[:-2]; ad=utils._decode_and_decompress(dc)
                         if ad is not None: app_state.data_queue.put({"stream":sn,"data":ad,"timestamp":last_ts_in_m}); queued_count_m+=1
            put_count += queued_count_m # Add queued count from M block
        # else: logger.debug(f"_queue_message_from_replay skipped: {type(message_data)}")


    except queue.Full:
        logger.warning("Replay: Data queue full! Discarding message(s).")
    except Exception as e:
        # --- CORRECTED ERROR LOGGING ---
        # Convert the message_data (dict/list) to string for logging, then slice
        error_data_str = str(message_data)
        logger.error(f"Unexpected error in _queue_message_from_replay for data '{error_data_str[:100]}...': {e}", exc_info=True)
        # --- END CORRECTION ---
    return put_count # Return how many messages were actually queued


def _replay_thread_target(filename, initial_speed=1.0): # Renamed arg for clarity
    """
    Reads the replay file line by line, extracting EMBEDDED timestamps
    to calculate delays adjusted by the CURRENT speed (read from app_state),
    and queues messages.
    """
    global replay_thread # Ensure global keyword is used if assigning to module variable later
    filepath = config.REPLAY_DIR / filename
    logger.info(f"Replay thread started for file: {filepath} at initial speed: {initial_speed}x")

    # Validate initial speed only for logging, actual speed read from state
    try: initial_playback_speed = float(initial_speed) if not (math.isnan(float(initial_speed)) or math.isinf(float(initial_speed)) or float(initial_speed) <= 0) else 1.0
    except: initial_playback_speed = 1.0

    last_message_dt = None
    lines_processed = 0; lines_skipped_json_error = 0; lines_skipped_other = 0; first_message_processed = False
    playback_status = "Running"
    start_real_time = time.monotonic()
    first_line_dt = None

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if app_state.stop_event.is_set(): logger.info("Replay thread: Stop event detected."); playback_status="Stopped"; break
                line = line.strip();
                if not line: continue

                start_time_line = time.monotonic()
                current_message_dt = None; timestamp_str_for_delay = None; time_to_wait = 0

                try:
                    raw_message = json.loads(line)
                    queued_count = 0

                    # --- Timestamp Extraction FOR DELAY (same as user's code) ---
                    if isinstance(raw_message, dict) and 'M' in raw_message and isinstance(raw_message['M'], list):
                        for msg_container in reversed(raw_message['M']):
                             if isinstance(msg_container, dict) and msg_container.get("M") == "feed":
                                 msg_args = msg_container.get("A");
                                 if isinstance(msg_args, list) and len(msg_args) > 2 and msg_args[2]:
                                     timestamp_str_for_delay = msg_args[2]; break
                    elif isinstance(raw_message, list) and len(raw_message) > 2:
                         timestamp_str_for_delay = raw_message[2]
                    if timestamp_str_for_delay: current_message_dt = utils.parse_iso_timestamp_safe(timestamp_str_for_delay, line_num)
                    # --- End Timestamp Extraction ---

                    # --- Process and Queue ---
                    # Pass the already parsed JSON object to the helper
                    queued_count = _queue_message_from_replay(raw_message) # Pass parsed dict/list
                    if queued_count > 0: lines_processed += queued_count
                    else: lines_skipped_other += 1; continue # Skip delay if nothing useful queued

                    # --- Delay Calculation ---
                    if current_message_dt: # If we successfully parsed a timestamp
                        if not first_message_processed: time_to_wait = 0; first_message_processed = True; first_line_dt = current_message_dt; start_real_time = time.monotonic()
                        elif last_message_dt:
                             try: time_diff_seconds = (current_message_dt - last_message_dt).total_seconds(); time_to_wait = max(0, time_diff_seconds)
                             except Exception as dt_err: logger.warning(f"Time diff error L{line_num}: {dt_err}"); time_to_wait = 0
                        last_message_dt = current_message_dt # Update last timestamp *used for delay calc*

                except json.JSONDecodeError as e: # ADDED COLON ':' and optional 'as e'
                    lines_skipped_json_error += 1
                    # Log less verbosely, include error message
                    if line_num > 5: logger.warning(f"Invalid JSON L{line_num} (skipped): {e} - Line: {line[:100]}...")
                    continue # Skip sleep logic for lines that aren't valid JSON
                # --- >>> END CORRECTION <<< ---
                except queue.Full:
                    logger.warning(f"Replay Queue full L{line_num}"); time.sleep(0.1) # Pause if queue full
                    continue # Skip sleep logic if queue full
                except Exception as e:
                    lines_skipped_other += 1
                    logger.error(f"Error processing L{line_num}: {e} - Line: {line[:100]}...", exc_info=True)
                    continue # Skip sleep logic on unexpected error

                # --- Apply Sleep Logic ---
                if time_to_wait > 0:
                    # --- >>> READ CURRENT SPEED FROM app_state <<< ---
                    current_playback_speed = 1.0 # Default
                    try:
                         with app_state.app_state_lock: current_playback_speed = float(app_state.replay_speed)
                         if current_playback_speed <= 0 or math.isnan(current_playback_speed) or math.isinf(current_playback_speed): current_playback_speed = 1.0
                    except Exception: current_playback_speed = 1.0 # Fallback on error reading state
                    # --- >>> END READ SPEED <<< ---

                    target_delay = time_to_wait / current_playback_speed # Use current speed
                    processing_time = time.monotonic() - start_time_line
                    adjusted_sleep_time = max(0, target_delay - processing_time)
                    final_sleep = min(adjusted_sleep_time, 5.0) # Max sleep 5s

                    if final_sleep > 0.001:
                        # logger.debug(f"L{line_num}: Wait={time_to_wait:.3f}, Speed={current_playback_speed:.1f}, Sleep={final_sleep:.3f}")
                        # Check stop event DURING sleep
                        if app_state.stop_event.wait(final_sleep):
                             logger.info("Replay thread: Stop event detected during sleep.")
                             playback_status = "Stopped"
                             break # Exit outer loop if stopped

            # End of file loop
            if playback_status == "Running": logger.info(f"Replay file '{filename}' finished. Queued: {lines_processed}, SkipJSON: {lines_skipped_json_error}, SkipOther: {lines_skipped_other}"); playback_status="Complete"

    except FileNotFoundError: logger.error(f"Replay Error: File not found at {filepath}"); playback_status="Error - File Not Found"
    except Exception as e: logger.error(f"Replay Error: Unexpected error {filepath}: {e}", exc_info=True); playback_status="Error - Runtime"
    finally: # Cleanup
        logger.info(f"Replay thread finishing. Final Status: {playback_status}")
        with app_state.app_state_lock:
            final_state = "Playback Complete" if playback_status == "Complete" else "Stopped" if playback_status == "Stopped" else "Error"
            final_conn_msg = playback_status
            # Only update state if it was still 'Replaying' or 'Stopping' (avoid overwriting Error set elsewhere)
            if app_state.app_status["state"] in ["Replaying", "Initializing", "Stopping"]:
                 app_state.app_status.update({"state": final_state, "connection": final_conn_msg})
            app_state.app_status['current_replay_file'] = None
        # Clean up module-level thread variable only if it's THIS thread
        if threading.current_thread() is globals().get('replay_thread'):
             globals()['replay_thread'] = None


# *** MODIFIED replay_from_file to accept and pass speed ***
def replay_from_file(data_file_path, replay_speed=1.0):
    """Starts the replay thread, returning True on success, False on failure."""
    global replay_thread

    # --- Pre-checks ---
    if replay_thread and replay_thread.is_alive():
        logger.warning("Replay already in progress. Please stop the current replay first.")
        # --- Add Log ---
        logger.debug("replay_from_file returning False (already running)")
        return False

    replay_file_path_obj = Path(data_file_path)
    if not replay_file_path_obj.is_file():
        logger.error(f"Replay file not found or not a file: {replay_file_path_obj}")
        with app_state.app_state_lock: app_state.app_status.update({"state": "Error", "connection": f"File Not Found"})
        # --- Add Log ---
        logger.debug("replay_from_file returning False (file not found)")
        return False

    # --- Prepare State ---
    # ... (state clearing logic remains the same) ...
    app_state.stop_event.clear(); logger.debug("Stop event cleared (replay).")
    with app_state.app_state_lock:
        logger.info("Replay mode: Clearing previous state...")
        app_state.app_status.update({"state": "Initializing", "connection": f"Preparing: {replay_file_path_obj.name}", "current_replay_file": replay_file_path_obj.name})
        app_state.data_store.clear(); app_state.timing_state.clear(); app_state.track_status_data.clear()
        app_state.session_details.clear(); app_state.race_control_log.clear(); app_state.track_coordinates_cache = {'session_key': None}
        while not app_state.data_queue.empty():
             try: app_state.data_queue.get_nowait()
             except queue.Empty: break
    logger.info("Replay mode: Previous state cleared.")


    # --- Start Thread with Error Handling ---
    try:
        logger.info(f"Starting replay thread for file: {replay_file_path_obj.name} at speed {replay_speed}x")
        replay_thread = threading.Thread(
            target=_replay_thread_target,
            args=(str(replay_file_path_obj), replay_speed),
            name="ReplayThread", daemon=True)
        replay_thread.start()
        logger.info(f"Replay thread initiated successfully for {replay_file_path_obj.name}")
        
        with app_state.app_state_lock:
            app_state.app_status.update({"state": "Replaying", "connection": f"File: {replay_file_path_obj.name}"})
            # --- >>> ADD THIS LOG <<< ---
            logger.debug(f"State set to 'Replaying' in replay_from_file. Current app_status: {app_state.app_status}")
            # --- >>> END ADDED LOG <<< ---
        # --- Add Log ---
        logger.debug("replay_from_file returning True")
        return True # Explicitly return True on success

    except Exception as e:
        logger.error(f"Failed to create or start replay thread: {e}", exc_info=True)
        with app_state.app_state_lock:
            app_state.app_status.update({"state": "Error", "connection": "Replay Thread Failed Start"})
            app_state.app_status['current_replay_file'] = None
        # --- Add Log ---
        logger.debug("replay_from_file returning False (exception)")
        return False # Explicitly return False on error


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
# replay.py
"""
Handles replay file discovery, loading, playback control,
and managing state and file I/O for library-based live recording.
"""

import logging
import os # Keep for os.path operations if any remain, though Path is preferred
import json
import time
import datetime
from datetime import timezone, timedelta
import threading
import queue # For queue.Empty
from pathlib import Path
import math
import re

# Import shared state and config
import app_state
import config # <<< UPDATED: For constants, paths, filenames
import utils
from utils import sanitize_filename, get_current_or_next_session_info, parse_iso_timestamp_safe # Import helpers

replay_thread = None
logger = logging.getLogger("F1App.Replay")

# --- File Handling Helpers ---
def ensure_replay_dir_exists():
    """Creates the replay directory and target save directory if they don't exist."""
    replay_dir_path = Path(config.REPLAY_DIR)
    if not replay_dir_path.exists():
        try:
            replay_dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created replay directory: {replay_dir_path}")
        except OSError as e:
            logger.error(f"Failed to create replay directory '{replay_dir_path}': {e}")

    target_save_path = Path(config.TARGET_SAVE_DIRECTORY)
    if not target_save_path.exists(): # Handles if it's same as REPLAY_DIR or different
        try:
            target_save_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created target save directory: {target_save_path}")
        except Exception as e:
            logger.error(f"Failed to ensure target save directory '{config.TARGET_SAVE_DIRECTORY}': {e}")

def get_replay_files(directory):
    """Gets a list of .data.txt files from the specified directory."""
    ensure_replay_dir_exists()
    dir_path = Path(directory)
    files = []
    if dir_path.exists() and dir_path.is_dir():
        try:
            files = sorted([f.name for f in dir_path.glob('*.data.txt') if f.is_file()])
        except Exception as e:
            logger.error(f"Error scanning directory '{directory}' for replay files: {e}")
    else:
         logger.warning(f"Replay directory '{directory}' not found or is not a directory.")
    return files

def init_live_file():
    """
    Prepares for saving live data. Names file based on session info.
    Returns True if successful, False otherwise.
    """
    if not config.TARGET_SAVE_DIRECTORY:
        logger.error("TARGET_SAVE_DIRECTORY is not configured. Cannot save live data.")
        return False

    ensure_replay_dir_exists()
    logger.info("Attempting to get session info via FastF1 for filename...")
    try:
         event_name, session_name_from_f1 = get_current_or_next_session_info()
    except Exception as f1_err:
         logger.error(f"Error calling get_current_or_next_session_info: {f1_err}", exc_info=True)
         event_name, session_name_from_f1 = None, None

    filename_prefix = None
    if event_name and session_name_from_f1:
         event_part = sanitize_filename(event_name)
         session_part = sanitize_filename(session_name_from_f1)
         filename_prefix = f"{event_part}_{session_part}"
         logger.info(f"Using filename prefix from FastF1: {filename_prefix}")
    else:
         logger.warning(f"Could not get session info from FastF1, using fallback filename prefix '{config.LIVE_DATA_FILENAME_FALLBACK_PREFIX}'.")
         filename_prefix = config.LIVE_DATA_FILENAME_FALLBACK_PREFIX # Use constant

    # Use constant for timestamp format
    timestamp = datetime.datetime.now(timezone.utc).strftime(config.LOG_REPLAY_FILE_HEADER_TS_FORMAT)
    filename = f"{filename_prefix}_{timestamp}.data.txt"
    filepath = Path(config.TARGET_SAVE_DIRECTORY) / filename
    temp_file_handle = None

    try:
        temp_file_handle = open(filepath, 'a', encoding='utf-8')
        logger.info(f"Successfully opened live data file for appending: {filepath}")

        with app_state.app_state_lock:
            if app_state.live_data_file and not app_state.live_data_file.closed:
                logger.warning("Closing previously open live data file in init_live_file.")
                try: app_state.live_data_file.close()
                except Exception as close_err: logger.error(f"Error closing previous live file: {close_err}")
            app_state.current_recording_filename = str(filepath)
            app_state.live_data_file = temp_file_handle
            app_state.is_saving_active = True
            logger.info(f"Live data recording state initialized and file opened. Saving enabled.")

        # Use constants for log messages
        header = config.LOG_REPLAY_FILE_START_MSG_PREFIX + datetime.datetime.now(timezone.utc).isoformat() + "Z\n"
        header += config.LOG_REPLAY_FILE_SESSION_INFO_PREFIX + f"Event='{event_name}', Session='{session_name_from_f1}'\n"
        temp_file_handle.write(header)
        temp_file_handle.flush()
        return True

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
    """Closes the live data file handle and clears recording flags."""
    logger.debug(f"close_live_file called (acquire_lock={acquire_lock})")
    lock_acquired = False
    file_handle_to_close = None
    current_filename_for_log = "Unknown Filename"

    try:
        if acquire_lock:
             lock_acquired = app_state.app_state_lock.acquire(timeout=2.0)
             if not lock_acquired:
                 logger.error("Failed to acquire app_state_lock in close_live_file! Cannot close file or clear state.")
                 return

        current_filename_for_log = app_state.current_recording_filename or current_filename_for_log
        if app_state.live_data_file:
             file_handle_to_close = app_state.live_data_file
             logger.info(f"Preparing to close recording file: {current_filename_for_log}")

        app_state.is_saving_active = False
        app_state.current_recording_filename = None
        app_state.live_data_file = None

        if file_handle_to_close:
            logger.info("Live recording state cleared.")
        elif current_filename_for_log != "Unknown Filename":
            logger.warning(f"Recording filename '{current_filename_for_log}' was set, but no file handle found in app_state.")
        else:
            logger.debug("No active recording file or handle found during close_live_file.")

    except Exception as e:
         logger.error(f"Error clearing recording state in close_live_file: {e}", exc_info=True)
    finally:
        if lock_acquired:
            try: app_state.app_state_lock.release()
            except threading.ThreadError: logger.warning("Attempted to release lock in close_live_file when not held.")

        if file_handle_to_close:
            try:
                logger.info(f"Closing file handle for: {current_filename_for_log}")
                # Use constant for log message
                footer = config.LOG_REPLAY_FILE_STOP_MSG_PREFIX + datetime.datetime.now(timezone.utc).isoformat() + "Z\n"
                file_handle_to_close.write(footer)
                file_handle_to_close.flush()
                file_handle_to_close.close()
                logger.info("File handle closed successfully.")
            except Exception as close_err:
                logger.error(f"Error closing live data file handle: {close_err}", exc_info=True)
        logger.debug("close_live_file finished.")


def _queue_message_from_replay(message_data):
    put_count = 0
    try:
        if isinstance(message_data, dict) and "R" in message_data:
            snapshot_data = message_data.get("R", {})
            if isinstance(snapshot_data, dict):
                snapshot_ts = snapshot_data.get("Heartbeat", {}).get("Utc") or (datetime.datetime.now(timezone.utc).isoformat() + 'Z')
                for stream_name_raw, stream_data in snapshot_data.items():
                    stream_name = stream_name_raw; actual_data = stream_data
                    if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                        stream_name = stream_name_raw[:-2]
                        actual_data = utils._decode_and_decompress(stream_data) # utils is already imported
                        if actual_data is None: logger.warning(f"Failed decode {stream_name_raw} in R"); continue
                    if actual_data is not None:
                        app_state.data_queue.put({"stream": stream_name, "data": actual_data, "timestamp": snapshot_ts})
                        put_count += 1
                if put_count > 0: logger.debug(f"Queued {put_count} streams from snapshot (R) block.")
            else:
                logger.warning(f"Snapshot block 'R' non-dict: {type(snapshot_data)}")

        elif isinstance(message_data, list) and len(message_data) >= 2:
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
            put_count += queued_count_m
    except queue.Full:
        logger.warning("Replay: Data queue full! Discarding message(s).")
    except Exception as e:
        error_data_str = str(message_data)
        logger.error(f"Unexpected error in _queue_message_from_replay for data '{error_data_str[:100]}...': {e}", exc_info=True)
    return put_count


def _replay_thread_target(filename, initial_speed=1.0):
    global replay_thread
    filepath = Path(config.REPLAY_DIR) / filename # Use Path object earlier
    logger.info(f"Replay thread started for file: {filepath} at initial speed: {initial_speed}x")

    try:
        initial_playback_speed = float(initial_speed)
        if math.isnan(initial_playback_speed) or math.isinf(initial_playback_speed) or initial_playback_speed <= 0:
            initial_playback_speed = 1.0
    except: initial_playback_speed = 1.0

    last_message_dt = None
    lines_processed = 0; lines_skipped_json_error = 0; lines_skipped_other = 0; first_message_processed = False
    playback_status = config.REPLAY_STATUS_RUNNING # Use constant
    start_real_time = time.monotonic()
    first_line_dt = None

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if app_state.stop_event.is_set():
                    logger.info("Replay thread: Stop event detected.")
                    playback_status = config.REPLAY_STATUS_STOPPED # Use constant
                    break
                line = line.strip()
                if not line: continue

                start_time_line = time.monotonic()
                current_message_dt = None; timestamp_str_for_delay = None; time_to_wait = 0

                try:
                    raw_message = json.loads(line)
                    queued_count = 0

                    if isinstance(raw_message, dict) and 'M' in raw_message and isinstance(raw_message['M'], list):
                        for msg_container in reversed(raw_message['M']):
                             if isinstance(msg_container, dict) and msg_container.get("M") == "feed":
                                 msg_args = msg_container.get("A");
                                 if isinstance(msg_args, list) and len(msg_args) > 2 and msg_args[2]:
                                     timestamp_str_for_delay = msg_args[2]; break
                    elif isinstance(raw_message, list) and len(raw_message) > 2:
                         timestamp_str_for_delay = raw_message[2]
                    if timestamp_str_for_delay:
                        current_message_dt = parse_iso_timestamp_safe(timestamp_str_for_delay, line_num) # utils. is already imported

                    queued_count = _queue_message_from_replay(raw_message)
                    if queued_count > 0: lines_processed += queued_count
                    else: lines_skipped_other += 1; continue

                    if current_message_dt:
                        if not first_message_processed:
                            time_to_wait = 0; first_message_processed = True; first_line_dt = current_message_dt; start_real_time = time.monotonic()
                        elif last_message_dt:
                             try:
                                 time_diff_seconds = (current_message_dt - last_message_dt).total_seconds()
                                 time_to_wait = max(0, time_diff_seconds)
                             except Exception as dt_err: logger.warning(f"Time diff error L{line_num}: {dt_err}"); time_to_wait = 0
                        last_message_dt = current_message_dt

                except json.JSONDecodeError as e:
                    lines_skipped_json_error += 1
                    if line_num > 5: logger.warning(f"Invalid JSON L{line_num} (skipped): {e} - Line: {line[:100]}...")
                    continue
                except queue.Full:
                    logger.warning(f"Replay Queue full L{line_num}"); time.sleep(0.1)
                    continue
                except Exception as e:
                    lines_skipped_other += 1
                    logger.error(f"Error processing L{line_num}: {e} - Line: {line[:100]}...", exc_info=True)
                    continue

                if time_to_wait > 0:
                    current_playback_speed = 1.0
                    try:
                         with app_state.app_state_lock: current_playback_speed = float(app_state.replay_speed)
                         if current_playback_speed <= 0 or math.isnan(current_playback_speed) or math.isinf(current_playback_speed): current_playback_speed = 1.0
                    except Exception: current_playback_speed = 1.0

                    target_delay = time_to_wait / current_playback_speed
                    processing_time = time.monotonic() - start_time_line
                    adjusted_sleep_time = max(0, target_delay - processing_time)
                    final_sleep = min(adjusted_sleep_time, 5.0)

                    if final_sleep > 0.001:
                        if app_state.stop_event.wait(final_sleep):
                             logger.info("Replay thread: Stop event detected during sleep.")
                             playback_status = config.REPLAY_STATUS_STOPPED # Use constant
                             break

            if playback_status == config.REPLAY_STATUS_RUNNING: # Use constant
                logger.info(f"Replay file '{filename}' finished. Queued: {lines_processed}, SkipJSON: {lines_skipped_json_error}, SkipOther: {lines_skipped_other}")
                playback_status = config.REPLAY_STATUS_COMPLETE # Use constant

    except FileNotFoundError:
        logger.error(f"Replay Error: File not found at {filepath}")
        playback_status = config.REPLAY_STATUS_ERROR_FILE_NOT_FOUND # Use constant
    except Exception as e:
        logger.error(f"Replay Error: Unexpected error {filepath}: {e}", exc_info=True)
        playback_status = config.REPLAY_STATUS_ERROR_RUNTIME # Use constant
    finally:
        logger.info(f"Replay thread finishing. Final Status: {playback_status}")
        with app_state.app_state_lock:
            final_state = "Error" # Default to Error
            if playback_status == config.REPLAY_STATUS_COMPLETE: final_state = "Playback Complete"
            elif playback_status == config.REPLAY_STATUS_STOPPED: final_state = "Stopped"
            # Else it remains "Error" as per initial playback_status or due to exceptions

            final_conn_msg = playback_status # Use the playback_status string directly for connection message

            if app_state.app_status["state"] in ["Replaying", "Initializing", "Stopping"]:
                 app_state.app_status.update({"state": final_state, "connection": final_conn_msg})
            app_state.app_status['current_replay_file'] = None
        if threading.current_thread() is globals().get('replay_thread'):
             globals()['replay_thread'] = None


def replay_from_file(data_file_path, replay_speed=1.0):
    global replay_thread

    if replay_thread and replay_thread.is_alive():
        logger.warning(config.TEXT_REPLAY_ALREADY_RUNNING) # Use constant
        return False

    replay_file_path_obj = Path(data_file_path)
    if not replay_file_path_obj.is_file():
        logger.error(config.TEXT_REPLAY_FILE_NOT_FOUND_ERROR_PREFIX + str(replay_file_path_obj)) # Use constant
        with app_state.app_state_lock:
            app_state.app_status.update({"state": "Error", "connection": config.TEXT_REPLAY_ERROR_FILE_NOT_FOUND_STATUS}) # Use constant
        return False

    app_state.stop_event.clear(); logger.debug("Stop event cleared (replay).")
    with app_state.app_state_lock:
        logger.info(config.TEXT_REPLAY_CLEARING_STATE) # Use constant
        app_state.app_status.update({
            "state": "Initializing",
            "connection": f"Preparing: {replay_file_path_obj.name}",
            "current_replay_file": replay_file_path_obj.name
        })
        app_state.data_store.clear(); app_state.timing_state.clear(); app_state.track_status_data.clear()
        app_state.session_details.clear(); app_state.race_control_log.clear();
        app_state.track_coordinates_cache = {'session_key': None} # Reset track cache
        app_state.lap_time_history.clear() # Clear lap time history
        app_state.telemetry_data.clear() # Clear telemetry data

        while not app_state.data_queue.empty():
             try: app_state.data_queue.get_nowait()
             except queue.Empty: break
    logger.info(config.TEXT_REPLAY_STATE_CLEARED) # Use constant

    try:
        logger.info(f"Starting replay thread for file: {replay_file_path_obj.name} at speed {replay_speed}x")
        # Pass filename string to thread, not Path object if it causes issues with some OS/threading internals
        replay_thread = threading.Thread(
            target=_replay_thread_target,
            args=(replay_file_path_obj.name, replay_speed), # Pass name
            name="ReplayThread", daemon=True)
        replay_thread.start()
        logger.info(f"Replay thread initiated successfully for {replay_file_path_obj.name}")

        with app_state.app_state_lock:
            app_state.app_status.update({"state": "Replaying", "connection": f"File: {replay_file_path_obj.name}"})
            logger.debug(f"State set to 'Replaying' in replay_from_file. Current app_status: {app_state.app_status}")
        return True

    except Exception as e:
        logger.error(f"Failed to create or start replay thread: {e}", exc_info=True)
        with app_state.app_state_lock:
            app_state.app_status.update({"state": "Error", "connection": config.TEXT_REPLAY_ERROR_THREAD_START_FAILED_STATUS}) # Use constant
            app_state.app_status['current_replay_file'] = None
        return False


def stop_replay():
    global replay_thread
    local_thread = replay_thread
    if not local_thread or not local_thread.is_alive():
        logger.info("Stop replay called, but no active replay thread found.")
        with app_state.app_state_lock:
             if app_state.app_status["state"] == "Replaying":
                 app_state.app_status.update({"state": "Stopped", "connection": config.REPLAY_STATUS_CONNECTION_REPLAY_ENDED}) # Use constant
             app_state.app_status["current_replay_file"] = None
        if replay_thread is local_thread: replay_thread = None
        return

    logger.info("Stopping replay...")
    with app_state.app_state_lock:
        if app_state.app_status["state"] == "Replaying":
            app_state.app_status.update({"state": "Stopping", "connection": "Stopping Replay..."}) # Could be constant
    app_state.stop_event.set()
    logger.info("Waiting for replay thread to join...")
    local_thread.join(timeout=5) # Consider making timeout a config const
    if local_thread.is_alive(): logger.warning("Replay thread did not stop cleanly.")
    else: logger.info("Replay thread joined successfully.")

    with app_state.app_state_lock:
        app_state.app_status.update({"state": "Stopped", "connection": config.REPLAY_STATUS_CONNECTION_REPLAY_STOPPED}) # Use constant
        app_state.app_status["current_replay_file"] = None
    if replay_thread is local_thread: replay_thread = None
    logger.info("Stop replay sequence complete.")


print("DEBUG: replay module loaded")
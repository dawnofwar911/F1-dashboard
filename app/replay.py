# replay.py
"""
Handles replay file discovery, loading, playback control,
and managing state and file I/O for live recording, on a per-session basis.
"""

import logging
import os
import json
import time
import datetime  # Keep for datetime objects
from datetime import timezone  # Keep for timezone objects
import threading
import queue  # For queue.Empty
from pathlib import Path
import math
from typing import Any, Optional, List, Dict
# import re # Not used in the provided code

# Import shared state and config
import app_state  # For app_state.SessionState type hint and get_or_create_session_state if needed by callers
import config
import utils  # For sanitize_filename, parse_iso_timestamp_safe, _decode_and_decompress
import data_processing
import signalr_client

logger = logging.getLogger("F1App.Replay")  # Module-level logger

# Global 'replay_thread' is removed. It will be part of session_state.

# --- File Handling Helpers (Session-Aware or Global Utilities) ---


def generate_live_filename_session(session_state: 'app_state.SessionState') -> str:
    """
    Generates a filename for live recording based on the given session's details.
    Format: {year}-{circuit}-{session}.data.txt
    """
    with session_state.lock:
        s_details = session_state.session_details
        year = s_details.get('Year')
        
        # --- START: CORRECTED KEY ACCESS ---
        # Get the circuit name from the 'Meeting' dictionary
        circuit_name = s_details.get('Meeting', {}).get('Name')
        # Get the session name from the 'Name' key
        session_name = s_details.get('Name')
        # --- END: CORRECTED KEY ACCESS ---

    sess_id_log = session_state.session_id[:8]

    if not all([year, circuit_name, session_name]):
        timestamp = datetime.datetime.now(
            timezone.utc).strftime("%Y%m%d_%H%M%S%Z")
        fallback_name = f"{config.LIVE_DATA_FILENAME_FALLBACK_PREFIX}_{timestamp}.data.txt"
        logger.warning(
            f"Session {sess_id_log}: Missing details for structured filename (Year: {year}, Circuit: {circuit_name}, Session: {session_name}). "
            f"Using fallback: {fallback_name}"
        )
        return fallback_name

    s_year = str(year)
    s_circuit = utils.sanitize_filename(str(circuit_name))
    s_session = utils.sanitize_filename(str(session_name))
    
    timestamp_suffix = datetime.datetime.now(
        timezone.utc).strftime("%Y%m%d_%H%M%S%Z")
    final_filename = f"{s_year}-{s_circuit}-{s_session}_{timestamp_suffix}.data.txt"
    logger.info(
        f"Session {sess_id_log}: Generated live filename: {final_filename}")
    return final_filename


def ensure_replay_dir_exists():
    """Creates the replay directory and target save directory if they don't exist. (Global utility)"""
    # This function can remain as a global utility, typically called once at app startup.
    replay_dir_path = Path(config.REPLAY_DIR)
    if not replay_dir_path.exists():
        try:
            replay_dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created replay directory: {replay_dir_path}")
        except OSError as e:
            logger.error(
                f"Failed to create replay directory '{replay_dir_path}': {e}")

    target_save_path = Path(config.TARGET_SAVE_DIRECTORY)
    if not target_save_path.exists():
        try:
            target_save_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created target save directory: {target_save_path}")
        except Exception as e:
            logger.error(
                f"Failed to ensure target save directory '{config.TARGET_SAVE_DIRECTORY}': {e}")


def get_replay_files(directory: str) -> list:
    """Gets a list of .data.txt files from the specified directory. (Global utility)"""
    ensure_replay_dir_exists()  # Ensures directory exists before scanning
    dir_path = Path(directory)
    files = []
    if dir_path.exists() and dir_path.is_dir():
        try:
            # Sort alphabetically, could also sort by modification time if preferred
            files = sorted(
                [f.name for f in dir_path.glob('*.data.txt') if f.is_file()])
        except Exception as e:
            logger.error(
                f"Error scanning directory '{directory}' for replay files: {e}")
    else:
        logger.warning(
            f"Replay directory '{directory}' not found or is not a directory.")
    return files


def init_live_file_session(session_state: 'app_state.SessionState') -> bool:
    """
    Initializes a recording file ONLY if global recording is enabled AND
    this session is the designated global recorder session.
    """
    # Step 1: Check the global setting. If it's off, no one can record.
    # This uses the new utility function to read from settings.json.
    settings = utils.load_global_settings()
    if not settings.get('record_live_sessions'):
        # Log only if the session *thought* it should record, to reduce noise.
        if "auto-recorder-" in session_state.session_id:
             logger.info("Global recording is disabled in settings.json. Recorder will not start.")
        return False

    # Step 2: Check if this session has the special recorder role.
    is_recorder_session = "auto-recorder-" in session_state.session_id
    
    # Step 3: If it's a normal user session, deny permission to save.
    if not is_recorder_session:
        logger.debug(f"Session {session_state.session_id}: Global recording is active, but user sessions cannot save.")
        return False

    # Step 4: If we get here, it's the recorder session and the setting is ON.
    # Proceed with recording. No lock files or temp files are needed.
    sess_id_log = session_state.session_id[:8]
    ensure_replay_dir_exists()
    
    # Generate the final, descriptive filename directly.
    final_filename = generate_live_filename_session(session_state)
    filepath = Path(config.TARGET_SAVE_DIRECTORY) / final_filename

    try:
        with session_state.lock:
            if session_state.live_data_file and not session_state.live_data_file.closed:
                logger.warning(f"Recorder {sess_id_log}: Closing previously open live data file.")
                session_state.live_data_file.close()

            session_state.live_data_file = open(filepath, 'a', encoding='utf-8')
            session_state.is_saving_active = True
            session_state.current_recording_filename = final_filename

            # Add the standard file header
            start_time_str = datetime.datetime.now(timezone.utc).strftime(config.LOG_REPLAY_FILE_HEADER_TS_FORMAT)
            header_msg = f"{config.LOG_REPLAY_FILE_START_MSG_PREFIX}{start_time_str}\n"
            s_details = session_state.session_details
            s_details_for_header = {
                'Year': s_details.get('Year'), 'CircuitName': s_details.get('CircuitName'),
                'EventName': s_details.get('EventName'), 'SessionName': s_details.get('SessionName'),
            }
            header_msg += f"# Recording for SessionID {sess_id_log}: {s_details_for_header}\n"
            session_state.live_data_file.write(header_msg)
            session_state.live_data_file.flush()

        logger.info(f"Designated Recorder {sess_id_log}: Live data recording started. Saving to: {final_filename}")
        return True

    except Exception as e:
        logger.error(f"Recorder {sess_id_log}: Failed to initialize recording file '{filepath.name}': {e}", exc_info=True)
        with session_state.lock:
            session_state.live_data_file = None
            session_state.is_saving_active = False
            session_state.current_recording_filename = None
        return False

def rename_live_file_session(session_state: 'app_state.SessionState'):
    """
    Renames an active temporary recording file to its final, descriptive name
    after session information has been received. It will not rename if details
    are still incomplete.
    """
    sess_id_log = session_state.session_id[:8]
    logger.info(f"Session {sess_id_log}: Checking if recording file needs renaming.")
    
    with session_state.lock:
        temp_filename = session_state.current_recording_filename
        live_file = session_state.live_data_file

    # Generate the new, descriptive filename now that we have session info
    final_filename = generate_live_filename_session(session_state)
    
    # --- THIS IS THE NEW LOGIC ---
    # If the generator returned a fallback name, it means we still don't have
    # all the details. Abort the rename for now and wait for the next trigger.
    if config.LIVE_DATA_FILENAME_FALLBACK_PREFIX in final_filename:
        logger.debug(f"Session {sess_id_log}: Deferring rename, full session details not yet available.")
        return
    # --- END OF NEW LOGIC ---

    temp_filepath = Path(config.TARGET_SAVE_DIRECTORY) / temp_filename
    final_filepath = Path(config.TARGET_SAVE_DIRECTORY) / final_filename

    if live_file and not live_file.closed:
        logger.debug(f"Session {sess_id_log}: Closing file handle for renaming.")
        live_file.close()
        with session_state.lock:
            session_state.live_data_file = None

    try:
        os.rename(temp_filepath, final_filepath)
        logger.info(f"Session {sess_id_log}: Renamed recording from '{temp_filename}' to '{final_filename}'")

        new_file_handle = open(final_filepath, 'a', encoding='utf-8')
        with session_state.lock:
            session_state.live_data_file = new_file_handle
            session_state.current_recording_filename = final_filename
            
    except Exception as e:
        logger.error(f"Session {sess_id_log}: CRITICAL: Failed to rename recording file '{temp_filename}'. Error: {e}", exc_info=True)
        try:
            old_file_handle = open(temp_filepath, 'a', encoding='utf-8')
            with session_state.lock:
                session_state.live_data_file = old_file_handle
        except Exception as e_reopen:
             logger.error(f"Session {sess_id_log}: FAILED to reopen temporary file after rename error: {e_reopen}", exc_info=True)


def close_live_file_session(session_state: 'app_state.SessionState'):
    """Closes the live data recording file for the given session if it's open."""
    sess_id_log = session_state.session_id[:8]
    file_closed_successfully = False
    filename_that_was_closed = None

    with session_state.lock:
        filename_that_was_closed = session_state.current_recording_filename
        if session_state.live_data_file and not session_state.live_data_file.closed:
            logger.info(
                f"Session {sess_id_log}: Closing live data file: {filename_that_was_closed}")
            try:
                stop_time_str = datetime.datetime.now(timezone.utc).strftime(
                    config.LOG_REPLAY_FILE_HEADER_TS_FORMAT)
                footer_msg = f"{config.LOG_REPLAY_FILE_STOP_MSG_PREFIX}{stop_time_str}\n"
                session_state.live_data_file.write(footer_msg)
                session_state.live_data_file.close()
                file_closed_successfully = True
            except Exception as e:
                logger.error(
                    f"Session {sess_id_log}: Error writing footer or closing live data file '{filename_that_was_closed}': {e}")
            finally:  # Ensure state is updated regardless of write error during close
                session_state.live_data_file = None
                session_state.is_saving_active = False
                # Do not clear current_recording_filename here, let init_live_file_session handle it for new files.
                # Or clear it if the intention is that "closed" means no current file. For status, keeping it might be okay.
                # Let's clear it to indicate no active recording file.
                # session_state.current_recording_filename = None
        else:
            logger.debug(
                f"Session {sess_id_log}: close_live_file_session called, but no active file to close.")

        # Always ensure is_saving_active is false after attempting to close or if no file was open.
        session_state.is_saving_active = False

    if file_closed_successfully:
        logger.info(
            f"Session {sess_id_log}: Successfully closed live data file: {filename_that_was_closed}")


def _queue_message_from_replay_session(session_state: 'app_state.SessionState', message_data: Any) -> int:
    """Queues messages from replay data into the session's data_queue."""
    sess_id_log = session_state.session_id[:8]
    put_count = 0
    try:
        # Your existing logic for parsing different message_data structures (R block, list, heartbeat, M block)
        # Replace app_state.data_queue.put with session_state.data_queue.put
        if isinstance(message_data, dict) and "R" in message_data:
            # ... (logic for R block, using session_state.data_queue.put) ...
            snapshot_data = message_data.get("R", {})
            if isinstance(snapshot_data, dict):
                snapshot_ts = snapshot_data.get("Heartbeat", {}).get("Utc") or (
                    datetime.datetime.now(timezone.utc).isoformat() + 'Z')
                for stream_name_raw, stream_data in snapshot_data.items():
                    # ... (decode, decompress logic)
                    stream_name = stream_name_raw
                    actual_data = stream_data
                    if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                        stream_name = stream_name_raw[:-2]
                        actual_data = utils._decode_and_decompress(stream_data)
                        if actual_data is None:
                            logger.warning(f"Session {sess_id_log}: Failed decode {stream_name_raw} in R"); continue
                    if actual_data is not None:
                        session_state.data_queue.put(
                            {"stream": stream_name, "data": actual_data, "timestamp": snapshot_ts}, block=False)
                        put_count += 1
        elif isinstance(message_data, list) and len(message_data) >= 2:
            # ... (logic for list messages, using session_state.data_queue.put) ...
            stream_name_raw = message_data[0]
            data_content = message_data[1]
            timestamp_for_queue = message_data[2] if len(message_data) > 2 else (
                datetime.datetime.now(timezone.utc).isoformat() + 'Z')
            stream_name = stream_name_raw
            actual_data = data_content
            if isinstance(stream_name_raw, str) and stream_name_raw.endswith('.z'):
                stream_name = stream_name_raw[:-2]
                actual_data = utils._decode_and_decompress(data_content)
                if actual_data is None:
                    logger.warning(f"Session {sess_id_log}: Failed decode {stream_name_raw} list msg"); return 0
            if actual_data is not None:
                session_state.data_queue.put(
                    {"stream": stream_name, "data": actual_data, "timestamp": timestamp_for_queue}, block=False)
                put_count += 1
        elif isinstance(message_data, dict) and not message_data:  # Heartbeat {}
            session_state.data_queue.put({"stream": "Heartbeat", "data": {
                                         }, "timestamp": datetime.datetime.now(timezone.utc).isoformat() + 'Z'}, block=False)
            put_count += 1
        elif isinstance(message_data, dict) and "M" in message_data and isinstance(message_data["M"], list):
            # ... (logic for M block, using session_state.data_queue.put) ...
            for msg_container in message_data["M"]:
                if isinstance(msg_container, dict) and msg_container.get("M") == "feed":
                    msg_args = msg_container.get("A")
                    if isinstance(msg_args, list) and len(msg_args) >= 2:
                        snr = msg_args[0]
                        dc = msg_args[1]; ts = msg_args[2] if len(
                            msg_args) > 2 else datetime.datetime.now(timezone.utc).isoformat()+'Z'
                        sn = snr
                        ad = dc
                        if isinstance(snr, str) and snr.endswith('.z'):
                            sn = snr[:-
                                2]; ad = utils._decode_and_decompress(dc)
                        if ad is not None: session_state.data_queue.put(
                            {"stream": sn, "data": ad, "timestamp": ts}, block=False); put_count += 1
        else:
            logger.warning(
                f"Session {sess_id_log}: Unknown message structure in replay data: {str(message_data)[:100]}")

    except queue.Full:
        logger.warning(
            f"Session {sess_id_log}: Replay data queue full! Discarding message(s).")
    except Exception as e:
        error_data_str = str(message_data)
        logger.error(
            f"Session {sess_id_log}: Unexpected error in _queue_message_from_replay_session for data '{error_data_str[:100]}...': {e}", exc_info=True)
    return put_count


def _replay_thread_target_session(session_state: 'app_state.SessionState', filename_str: str, initial_speed: float):
    """Target function for a session's replay thread."""
    sess_id_log = session_state.session_id[:8]
    filepath = Path(config.REPLAY_DIR) / filename_str
    logger.info(
        f"ReplaySess {sess_id_log}: Replay thread STARTED for file: {filepath} at initial speed: {initial_speed}x")

    actual_start_real_time: Optional[float] = None 
    first_interesting_file_timestamp: Optional[datetime.datetime] = None
    # last_processed_file_timestamp is used to calculate deltas between consecutive paced messages
    last_paced_line_file_timestamp: Optional[datetime.datetime] = None 

    lines_processed = 0
    lines_skipped_json_error = 0; lines_skipped_other = 0
    playback_status_str = config.REPLAY_STATUS_RUNNING

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            logger.info(f"ReplaySess {sess_id_log}: Opened replay file {filepath}")
            for line_num, line in enumerate(f, 1):
                if session_state.stop_event.is_set():
                    logger.info(f"ReplaySess {sess_id_log}: Stop event detected in replay thread loop (L{line_num}). Breaking.")
                    playback_status_str = config.REPLAY_STATUS_STOPPED
                    break

                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                current_line_processing_start_time = time.monotonic()
                current_line_has_pacing_timestamp: Optional[datetime.datetime] = None # Timestamp for THIS line, if found
                timestamp_str_for_pacing = None
                
                # Flag to determine if this line contains what we consider "start of action"
                is_first_anchor_message_type = False

                try:
                    raw_message = json.loads(line)
                    
                    # Determine if this raw_message is an M-block with actual feed data
                    if isinstance(raw_message, dict) and "M" in raw_message and isinstance(raw_message["M"], list) and len(raw_message["M"]) > 0:
                        # Check if any sub-message is a "feed"
                        if any(isinstance(m, dict) and m.get("M") == "feed" for m in raw_message["M"]):
                            is_first_anchor_message_type = True 
                            # For pacing, extract the timestamp from the *last* feed message in the M-block
                            for msg_container in reversed(raw_message['M']):
                                if isinstance(msg_container, dict) and msg_container.get("M") == "feed":
                                    msg_args = msg_container.get("A")
                                    if isinstance(msg_args, list) and len(msg_args) > 2 and msg_args[2]:
                                        timestamp_str_for_pacing = msg_args[2]; break
                    
                    # Fallback or alternative: extract pacing timestamp from R-block heartbeat IF no M-block timestamp found yet
                    # AND if we haven't found our first "interesting" anchor yet.
                    # This ensures that if a file *only* has R-blocks for a while, it still paces.
                    if not timestamp_str_for_pacing and isinstance(raw_message, dict) and "R" in raw_message and isinstance(raw_message["R"], dict):
                        hb_ts = raw_message.get("R", {}).get("Heartbeat", {}).get("Utc")
                        if hb_ts: timestamp_str_for_pacing = hb_ts
                        # R-blocks usually aren't the "start of action" for user perception.
                        # is_first_anchor_message_type remains False unless set by M-block logic above.

                    # Fallback for simple list messages (less common for F1 data with pacing)
                    if not timestamp_str_for_pacing and isinstance(raw_message, list) and len(raw_message) > 2:
                        timestamp_str_for_pacing = raw_message[2]
                        # Potentially set is_first_anchor_message_type = True if this list message type is considered "action"
                        # For example: if raw_message[0] == "TimingData" (if that's a possible stream format)

                    if timestamp_str_for_pacing:
                        current_line_has_pacing_timestamp = utils.parse_iso_timestamp_safe(timestamp_str_for_pacing)
                        if not current_line_has_pacing_timestamp:
                             logger.warning(f"ReplaySess {sess_id_log}: L{line_num} - Failed to parse pacing timestamp: '{timestamp_str_for_pacing}' from raw: {str(raw_message)[:100]}")
                    else:
                        logger.debug(f"ReplaySess {sess_id_log}: L{line_num} - No pacing timestamp in: {str(raw_message)[:100]}")

                    queued_count = _queue_message_from_replay_session(session_state, raw_message)
                    if queued_count > 0: lines_processed += queued_count
                    else: lines_skipped_other +=1; logger.debug(f"ReplaySess {sess_id_log}: L{line_num} - No messages queued by _queue_message_from_replay_session."); continue
                    
                    # --- Pacing Logic ---
                    if current_line_has_pacing_timestamp:
                        calculated_time_to_wait_for_original_timing = 0.0

                        if first_interesting_file_timestamp is None: # We are still looking for our anchor
                            if is_first_anchor_message_type: # This line IS our anchor!
                                first_interesting_file_timestamp = current_line_has_pacing_timestamp
                                last_paced_line_file_timestamp = current_line_has_pacing_timestamp
                                actual_start_real_time = time.monotonic() # Anchor real-world time
                                logger.info(f"ReplaySess {sess_id_log}: L{line_num} - FIRST ACTION ANCHOR. FileTS: {first_interesting_file_timestamp.isoformat()}. Real-time anchor set. No initial sleep.")
                                # calculated_time_to_wait_for_original_timing remains 0
                            else: # It's a message with a timestamp (e.g. R-block) but not our "action" anchor
                                last_paced_line_file_timestamp = current_line_has_pacing_timestamp # Keep track of it for next potential delta
                                logger.debug(f"ReplaySess {sess_id_log}: L{line_num} - Processed pre-anchor TS: {current_line_has_pacing_timestamp.isoformat()}. No pacing sleep yet.")
                                # No sleep, process immediately
                        
                        else: # We have an anchor (first_interesting_file_timestamp and actual_start_real_time are set)
                            if last_paced_line_file_timestamp is None: # Should not happen if first_interesting_file_timestamp is set
                                logger.error(f"ReplaySess {sess_id_log}: L{line_num} - Inconsistent state: first_interesting_file_timestamp is set, but last_paced_line_file_timestamp is None. Resetting anchor.")
                                first_interesting_file_timestamp = current_line_has_pacing_timestamp # Re-anchor
                                last_paced_line_file_timestamp = current_line_has_pacing_timestamp
                                actual_start_real_time = time.monotonic()
                            else:
                                file_time_delta_from_last_paced = (current_line_has_pacing_timestamp - last_paced_line_file_timestamp).total_seconds()
                                if file_time_delta_from_last_paced < 0:
                                     logger.warning(f"ReplaySess {sess_id_log}: L{line_num} - Negative/Retrograde time delta ({file_time_delta_from_last_paced:.3f}s). Processing immediately.")
                                     calculated_time_to_wait_for_original_timing = 0.0
                                else:
                                     calculated_time_to_wait_for_original_timing = file_time_delta_from_last_paced
                            
                            # Get current replay speed
                            current_s_replay_speed = 1.0
                            with session_state.lock: current_s_replay_speed = session_state.replay_speed
                            if not (isinstance(current_s_replay_speed, (int,float)) and current_s_replay_speed > 0 and not math.isinf(current_s_replay_speed) and not math.isnan(current_s_replay_speed)):
                                current_s_replay_speed = 1.0
                            
                            # Pacing calculation
                            target_delay_adjusted_for_speed = calculated_time_to_wait_for_original_timing / current_s_replay_speed
                            line_proc_duration = time.monotonic() - current_line_processing_start_time
                            actual_sleep_duration = max(0, target_delay_adjusted_for_speed - line_proc_duration)

                            if actual_sleep_duration > 0.001:
                                logger.debug(f"ReplaySess {sess_id_log}: L{line_num} - Pacing sleep: {actual_sleep_duration:.3f}s. (FileDelta: {calculated_time_to_wait_for_original_timing:.3f}s, Speed: {current_s_replay_speed:.1f}x)")
                                # ... (Chunked sleep logic from Response #11) ...
                                max_sleep_chunk = 1.0; remaining_sleep = actual_sleep_duration
                                while remaining_sleep > 0.001:
                                    chunk = min(remaining_sleep, max_sleep_chunk)
                                    if session_state.stop_event.wait(chunk):
                                        playback_status_str = config.REPLAY_STATUS_STOPPED; break
                                    remaining_sleep -= chunk
                                if playback_status_str == config.REPLAY_STATUS_STOPPED: break
                            
                            last_paced_line_file_timestamp = current_line_has_pacing_timestamp # Update for next iteration

                except json.JSONDecodeError:
                    lines_skipped_json_error += 1
                    if line_num > 10:
                        logger.warning(
                            f"Session {sess_id_log}: Invalid JSON L{line_num} (skipped) in {filename_str}")
                    continue
                except queue.Full:  # Should be caught by _queue_message_from_replay_session, but as fallback
                    logger.warning(
                        f"Session {sess_id_log}: Data queue full during replay L{line_num}. Pausing briefly.")
                    time.sleep(0.1)
                    continue
                except Exception as e_line:
                    lines_skipped_other += 1
                    # exc_info=False for less noise on minor line errors
                    logger.error(
                        f"Session {sess_id_log}: Error processing L{line_num} of {filename_str}: {e_line}", exc_info=False)
                    continue

            if playback_status_str == config.REPLAY_STATUS_RUNNING:  # If loop finished without break
                playback_status_str = config.REPLAY_STATUS_COMPLETE

    except FileNotFoundError:
        logger.error(
            f"Session {sess_id_log}: Replay file not found at {filepath}")
        playback_status_str = config.REPLAY_STATUS_ERROR_FILE_NOT_FOUND
    except Exception as e_thread:
        logger.error(
            f"Session {sess_id_log}: Unexpected error in replay thread for {filepath}: {e_thread}", exc_info=True)
        playback_status_str = config.REPLAY_STATUS_ERROR_RUNTIME
    finally:
        logger.info(f"Session {sess_id_log}: Replay thread for '{filename_str}' finishing. Final Status: {playback_status_str}. Processed: {lines_processed}, JSONSkips: {lines_skipped_json_error}, OtherSkips: {lines_skipped_other}")
        with session_state.lock:
            final_app_state_str = "Error"  # Default
            if playback_status_str == config.REPLAY_STATUS_COMPLETE:
                final_app_state_str = "Playback Complete"
            elif playback_status_str == config.REPLAY_STATUS_STOPPED: final_app_state_str = "Stopped"

            # Only update app_status if this thread was the one responsible for the current replay state
            current_replay_filename_in_app_status = session_state.app_status.get(
                "current_replay_file")
            if current_replay_filename_in_app_status == filename_str:
                # Check if current state is related to replay
                if session_state.app_status["state"] in ["Replaying", "Initializing", "Stopping", "Playback Complete"]:
                    session_state.app_status.update(
                        {"state": final_app_state_str, "connection": playback_status_str})
                session_state.app_status['current_replay_file'] = None  # Clear current file

            if session_state.replay_thread is threading.current_thread():
                session_state.replay_thread = None  # Clear thread handle from session state
        logger.info(
            f"Session {sess_id_log}: Replay thread for '{filename_str}' fully cleaned up.")


def start_replay_session(session_state: 'app_state.SessionState', data_file_path: Path, replay_speed: float = 1.0) -> bool:
    sess_id_log = session_state.session_id[:8]
    filename_str = data_file_path.name
    logger.info(f"ReplaySess {sess_id_log}: ENTERING start_replay_session for file: {filename_str}, speed: {replay_speed}x") # NEW LOG

    with session_state.lock:
        logger.debug(f"ReplaySess {sess_id_log}: Acquired lock to check existing threads.") # NEW LOG
        if session_state.replay_thread and session_state.replay_thread.is_alive():
            logger.warning(
                f"ReplaySess {sess_id_log}: {config.TEXT_REPLAY_ALREADY_RUNNING}. Stopping existing replay.") # MODIFIED LOG
            # Add call to stop_replay_session directly here if needed, or rely on external stop.
            # For now, let's assume it should be stopped before calling start_replay again.
            # replay.stop_replay_session(session_state) # This might be too recursive if called from here
            return False # Or handle more gracefully

        if session_state.connection_thread and session_state.connection_thread.is_alive():
            logger.info(
                f"ReplaySess {sess_id_log}: Stopping active live connection to start replay.") # MODIFIED LOG
            # This relies on signalr_client.stop_connection_session being robust
            signalr_client.stop_connection_session(session_state) # Ensure signalr_client is imported
            logger.debug(f"ReplaySess {sess_id_log}: Live connection stop called. Will attempt to join...") # NEW LOG
            # Wait a bit for the connection thread to actually stop and release resources
            # This join should ideally be outside the lock or handled carefully
            # For now, we proceed, assuming stop_connection_session handles its joins.
            # session_state.connection_thread.join(timeout=5.0) # Potentially problematic under lock
            # if session_state.connection_thread and session_state.connection_thread.is_alive():
            #     logger.warning(f"ReplaySess {sess_id_log}: Live connection thread did not stop cleanly.")
            #     return False # Cannot proceed if live connection didn't stop
            # session_state.connection_thread = None # Should be done by stop_connection_session
            # session_state.hub_connection = None  # Should be done by stop_connection_session
            time.sleep(0.5) # Give a moment for threads to respond to stop signals
            logger.debug(f"ReplaySess {sess_id_log}: Continuing after attempting to stop live connection.")# NEW LOG

    if not data_file_path.is_file():
        logger.error(
            f"ReplaySess {sess_id_log}: {config.TEXT_REPLAY_FILE_NOT_FOUND_ERROR_PREFIX}{data_file_path}") # MODIFIED LOG
        with session_state.lock:
            session_state.app_status.update(
                {"state": "Error", "connection": config.TEXT_REPLAY_ERROR_FILE_NOT_FOUND_STATUS})
        return False

    logger.debug(f"ReplaySess {sess_id_log}: Resetting state variables for replay.") # NEW LOG
    session_state.stop_event.clear()
    session_state.reset_state_variables()
    logger.debug(f"ReplaySess {sess_id_log}: State variables reset.") # NEW LOG

    with session_state.lock:
        logger.info(
            f"ReplaySess {sess_id_log}: {config.TEXT_REPLAY_CLEARING_STATE} for file {filename_str}") # MODIFIED LOG
        session_state.app_status.update({
            "state": "Initializing", # Initializing is good
            "connection": f"Replay Preparing: {filename_str}",
            "current_replay_file": filename_str
        })
        session_state.replay_speed = replay_speed
        # Reset track map states explicitly here too, as done in handle_control_clicks
        session_state.track_coordinates_cache = app_state.INITIAL_SESSION_TRACK_COORDINATES_CACHE.copy() # Ensure app_state imported
        session_state.session_details['SessionKey'] = None 
        session_state.selected_driver_for_map_and_lap_chart = None
        logger.debug(f"ReplaySess {sess_id_log}: app_status and map states updated for replay init.")# NEW LOG


    logger.info(
        f"ReplaySess {sess_id_log}: Starting replay thread for {filename_str} at speed {replay_speed}x") # MODIFIED LOG
    try:
        replay_target_thread = threading.Thread( # Ensure threading imported
            target=_replay_thread_target_session,
            args=(session_state, filename_str, replay_speed),
            name=f"ReplaySess_{sess_id_log}_{filename_str[:10]}", daemon=True
        )
        logger.debug(f"ReplaySess {sess_id_log}: Replay target thread object created: {replay_target_thread.name}")# NEW LOG

        dp_replay_thread = threading.Thread( # Ensure threading imported
            target=data_processing.data_processing_loop_session, # Ensure data_processing imported
            args=(session_state,),
            name=f"DataProc_Replay_{sess_id_log}",
            daemon=True
        )
        logger.debug(f"ReplaySess {sess_id_log}: Data processing thread for replay object created: {dp_replay_thread.name}")# NEW LOG

        with session_state.lock:
            session_state.replay_thread = replay_target_thread
            session_state.data_processing_thread = dp_replay_thread
            logger.debug(f"ReplaySess {sess_id_log}: Thread handles stored in session_state.")# NEW LOG

        replay_target_thread.start()
        logger.info(f"ReplaySess {sess_id_log}: Replay target thread STARTED: {replay_target_thread.name}") # MODIFIED LOG

        dp_replay_thread.start()
        logger.info(
            f"ReplaySess {sess_id_log}: Data processing thread for replay STARTED: {dp_replay_thread.name}") # MODIFIED LOG

        with session_state.lock:
            session_state.app_status.update(
                {"state": "Replaying", "connection": f"Replay: {filename_str}"})
        logger.info(f"ReplaySess {sess_id_log}: start_replay_session COMPLETED successfully. State set to Replaying.") # NEW LOG
        return True

    except Exception as e:
        logger.error(
            f"ReplaySess {sess_id_log}: Failed to create or start replay threads for {filename_str}: {e}", exc_info=True) # MODIFIED LOG
        with session_state.lock:
            session_state.app_status.update(
                {"state": "Error", "connection": config.TEXT_REPLAY_ERROR_THREAD_START_FAILED_STATUS})
            if session_state.app_status.get("current_replay_file") == filename_str: # Clear only if it's this file
                session_state.app_status['current_replay_file'] = None
        return False


def stop_replay_session(session_state: 'app_state.SessionState'):
    sess_id_log = session_state.session_id[:8]
    logger.info(f"ReplaySess {sess_id_log}: Stop replay requested.") # MODIFIED LOG

    s_replay_thread = None
    s_data_processing_thread_for_replay = None # NEW: Get this handle
    current_s_state = "Unknown"
    current_s_replay_file = None

    with session_state.lock:
        s_replay_thread = session_state.replay_thread
        # IMPORTANT: Only get the dp thread if it's the one associated with THIS replay
        # We need a way to distinguish if data_processing_thread is for live or replay.
        # One way: check the name, or clear it if replay stops.
        # For now, assume if replay_thread exists, data_processing_thread is its partner.
        if session_state.replay_thread: # Only consider DP thread if replay thread exists
             s_data_processing_thread_for_replay = session_state.data_processing_thread

        current_s_state = session_state.app_status["state"]
        current_s_replay_file = session_state.app_status.get("current_replay_file")

    if not s_replay_thread or not s_replay_thread.is_alive():
        logger.info(
            f"ReplaySess {sess_id_log}: No active replay reader thread to stop. Current state: {current_s_state}") # MODIFIED LOG
        # Ensure DP thread is also handled if it somehow outlived replay thread
        if s_data_processing_thread_for_replay and s_data_processing_thread_for_replay.is_alive():
            logger.warning(f"ReplaySess {sess_id_log}: Replay reader thread inactive, but DP thread for replay still alive. Signaling it.")
            session_state.stop_event.set() # Ensure it's signalled
            s_data_processing_thread_for_replay.join(timeout=3.0)
            if s_data_processing_thread_for_replay.is_alive():
                logger.error(f"ReplaySess {sess_id_log}: Orphaned DP thread for replay did not join.")
            with session_state.lock: # Clear its handle
                if session_state.data_processing_thread is s_data_processing_thread_for_replay:
                    session_state.data_processing_thread = None

        with session_state.lock:
            if current_s_state in ["Replaying", "Playback Complete", "Stopping", "Initializing"]:
                session_state.app_status.update(
                    {"state": "Stopped", "connection": config.REPLAY_STATUS_CONNECTION_REPLAY_ENDED})
            if current_s_replay_file:
                session_state.app_status["current_replay_file"] = None
            session_state.replay_thread = None
        return

    logger.info(
        f"ReplaySess {sess_id_log}: Actively stopping replay reader thread for {current_s_replay_file}...") # MODIFIED LOG
    with session_state.lock:
        if current_s_state == "Replaying":
            session_state.app_status.update(
                {"state": "Stopping", "connection": "Replay Stopping..."})

    session_state.stop_event.set() # Signal BOTH threads (replay reader and its data processor)

    logger.info(
        f"ReplaySess {sess_id_log}: Waiting for replay reader thread ({s_replay_thread.name}) to join...") # MODIFIED LOG
    s_replay_thread.join(timeout=5.0) # timeout for replay reader

    with session_state.lock: # Re-acquire lock for status update
        if s_replay_thread.is_alive():
            logger.warning(
                f"ReplaySess {sess_id_log}: Replay reader thread ({s_replay_thread.name}) did not join cleanly.") # MODIFIED LOG
            session_state.app_status.update(
                {"state": "Error", "connection": "Replay Reader Stop Failed Join"})
        else:
            logger.info(
                f"ReplaySess {sess_id_log}: Replay reader thread ({s_replay_thread.name}) joined successfully.") # MODIFIED LOG
            if session_state.app_status["state"] == "Stopping": # If it was stopping due to this action
                session_state.app_status.update({"state": "Stopped", "connection": config.REPLAY_STATUS_CONNECTION_REPLAY_STOPPED})
        session_state.replay_thread = None # Clear reader thread handle

    # NOW, handle the data processing thread associated with this replay
    if s_data_processing_thread_for_replay and s_data_processing_thread_for_replay.is_alive():
        logger.info(f"ReplaySess {sess_id_log}: Waiting for replay Data Processing thread ({s_data_processing_thread_for_replay.name}) to join...") # NEW LOG
        s_data_processing_thread_for_replay.join(timeout=5.0) # Increased timeout slightly
        with session_state.lock: # Lock for final DP thread cleanup
            if s_data_processing_thread_for_replay.is_alive():
                logger.warning(f"ReplaySess {sess_id_log}: Replay Data Processing thread ({s_data_processing_thread_for_replay.name}) did not join cleanly.") # NEW LOG
                # If main status is still related to replay, update to error
                if session_state.app_status["state"] in ["Stopping", "Stopped"]:
                     session_state.app_status.update({"state": "Error", "connection": "Replay DP Stop Failed Join"})
            else:
                logger.info(f"ReplaySess {sess_id_log}: Replay Data Processing thread ({s_data_processing_thread_for_replay.name}) joined successfully.") # NEW LOG
            # Clear handle if it's the one we were trying to stop
            if session_state.data_processing_thread is s_data_processing_thread_for_replay:
                 session_state.data_processing_thread = None
    elif s_data_processing_thread_for_replay: # Thread existed but was not alive when checked
        logger.info(f"ReplaySess {sess_id_log}: Replay Data Processing thread ({s_data_processing_thread_for_replay.name}) was already stopped.") # NEW LOG
        with session_state.lock: # Ensure handle is cleared
            if session_state.data_processing_thread is s_data_processing_thread_for_replay:
                 session_state.data_processing_thread = None


    with session_state.lock: # Final status update
        session_state.app_status["current_replay_file"] = None

    logger.info(f"ReplaySess {sess_id_log}: Stop replay sequence complete.") # MODIFIED LOG


print("DEBUG: replay module (multi-session structure) loaded")

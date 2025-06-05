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

logger = logging.getLogger("F1App.Replay")  # Module-level logger

# Global 'replay_thread' is removed. It will be part of session_state.

# --- File Handling Helpers (Session-Aware or Global Utilities) ---


def generate_live_filename_session(session_state: 'app_state.SessionState') -> str:
    """
    Generates a filename for live recording based on the given session's details.
    Format: {year}-{circuit}-{session}.data.txt
    """
    # Access session_details from the passed session_state object
    with session_state.lock:
        s_details = session_state.session_details
        year = s_details.get('Year')
        circuit_name = s_details.get('CircuitName')
        session_name = s_details.get('SessionName')

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

    # Add timestamp to make filenames unique even if session details are identical for some reason
    # (e.g. quick stop/start of recording for the same conceptual session)
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
    """Initializes the live data recording file for the given session."""
    sess_id_log = session_state.session_id[:8]
    with session_state.lock:
        # User's preference for this session
        should_record = session_state.record_live_data

    if not should_record:
        logger.info(
            f"Session {sess_id_log}: Live recording is disabled by preference. No file will be created.")
        # Ensure any previous file for this session is closed if state is inconsistent
        with session_state.lock:
            if session_state.live_data_file and not session_state.live_data_file.closed:
                try:
                    session_state.live_data_file.close()
                except Exception as e:
                    logger.error(
                        f"Session {sess_id_log}: Error closing pre-existing live data file when recording disabled: {e}")
            session_state.live_data_file = None
            session_state.is_saving_active = False
            session_state.current_recording_filename = None
        return False

    ensure_replay_dir_exists()  # Ensure target directory exists
    filename = generate_live_filename_session(
        session_state)  # Generate session-specific filename
    filepath = Path(config.TARGET_SAVE_DIRECTORY) / filename

    try:
        with session_state.lock:  # Protect access to session_state attributes
            # Close any existing open file for this session first
            if session_state.live_data_file and not session_state.live_data_file.closed:
                logger.warning(
                    f"Session {sess_id_log}: Closing previously open live data file: {session_state.current_recording_filename}")
                session_state.live_data_file.close()

            # Open in append mode ('a') to allow continuation or prevent overwrite if filename wasn't perfectly unique
            session_state.live_data_file = open(
                filepath, 'a', encoding='utf-8')
            session_state.is_saving_active = True
            session_state.current_recording_filename = filepath.name

            start_time_str = datetime.datetime.now(timezone.utc).strftime(
                config.LOG_REPLAY_FILE_HEADER_TS_FORMAT)
            header_msg = f"{config.LOG_REPLAY_FILE_START_MSG_PREFIX}{start_time_str}\n"

            s_details = session_state.session_details  # Already under lock
            s_details_for_header = {
                'Year': s_details.get('Year'), 'CircuitName': s_details.get('CircuitName'),
                'EventName': s_details.get('EventName'), 'SessionName': s_details.get('SessionName'),
                'SessionType': s_details.get('Type'), 'SessionStartTimeUTC': s_details.get('SessionStartTimeUTC')
            }
            header_msg += f"# Recording for SessionID {sess_id_log}: {s_details_for_header}\n"
            session_state.live_data_file.write(header_msg)
            session_state.live_data_file.flush()

        logger.info(
            f"Session {sess_id_log}: Live data recording started. Saving to: {filepath.name}")
        return True
    except Exception as e:
        logger.error(
            f"Session {sess_id_log}: Failed to initialize live recording file '{filepath.name}': {e}", exc_info=True)
        with session_state.lock:
            session_state.live_data_file = None
            session_state.is_saving_active = False
            session_state.current_recording_filename = None
        return False


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
        f"Session {sess_id_log}: Replay thread started for file: {filepath} at initial speed: {initial_speed}x")

    last_message_dt = None
    lines_processed = 0
    lines_skipped_json_error = 0; lines_skipped_other = 0
    first_message_processed = False
    playback_status_str = config.REPLAY_STATUS_RUNNING  # Use string from config

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if session_state.stop_event.is_set():
                    logger.info(
                        f"Session {sess_id_log}: Stop event detected in replay thread.")
                    playback_status_str = config.REPLAY_STATUS_STOPPED
                    break

                line = line.strip()
                if not line or line.startswith("#"):
                    continue  # Skip empty lines and comments

                start_time_line_processing = time.monotonic()
                current_message_dt_from_file = None
                timestamp_str_for_delay_calc = None

                try:
                    raw_message = json.loads(line)

                    # Attempt to find a timestamp in the message for pacing
                    # (This logic is simplified; original had more detailed parsing for 'M' blocks)
                    if isinstance(raw_message, dict) and 'M' in raw_message and isinstance(raw_message['M'], list):
                        # Check last message in M block first
                        for msg_container in reversed(raw_message['M']):
                            if isinstance(msg_container, dict) and msg_container.get("M") == "feed":
                                msg_args = msg_container.get("A")
                                if isinstance(msg_args, list) and len(msg_args) > 2 and msg_args[2]:
                                    timestamp_str_for_delay_calc = msg_args[2]
                                    break
                    elif isinstance(raw_message, list) and len(raw_message) > 2:
                        timestamp_str_for_delay_calc = raw_message[2]
                    elif isinstance(raw_message, dict) and "R" in raw_message and isinstance(raw_message["R"], dict):
                        # For "R" blocks, the timestamp for pacing might be less critical or based on Heartbeat
                        hb_ts = raw_message.get("R", {}).get(
                            "Heartbeat", {}).get("Utc")
                        if hb_ts:
                             timestamp_str_for_delay_calc = hb_ts

                    if timestamp_str_for_delay_calc:
                        current_message_dt_from_file = utils.parse_iso_timestamp_safe(
                            timestamp_str_for_delay_calc)

                    queued_count = _queue_message_from_replay_session(
                        session_state, raw_message)
                    if queued_count > 0:
                        lines_processed += queued_count
                    # If nothing queued, likely an issue or empty message
                    else: lines_skipped_other += 1; continue

                    if current_message_dt_from_file:
                        if not first_message_processed:
                            last_message_dt = current_message_dt_from_file
                            first_message_processed = True
                        elif last_message_dt:
                            time_diff_seconds = (
                                current_message_dt_from_file - last_message_dt).total_seconds()
                            time_to_wait_for_original_timing = max(
                                0, time_diff_seconds)

                            current_s_replay_speed = 1.0
                            with session_state.lock:
                                current_s_replay_speed = session_state.replay_speed
                            if current_s_replay_speed <= 0: current_s_replay_speed = 1.0

                            target_delay_adjusted_for_speed = time_to_wait_for_original_timing / \
                                current_s_replay_speed
                            processing_duration_this_line = time.monotonic() - start_time_line_processing
                            actual_sleep_duration = max(
                                0, target_delay_adjusted_for_speed - processing_duration_this_line)

                            if actual_sleep_duration > 0.001:  # Only sleep if meaningful
                                if session_state.stop_event.wait(actual_sleep_duration):
                                    logger.info(
                                        f"Session {sess_id_log}: Stop event during replay sleep.")
                                    playback_status_str = config.REPLAY_STATUS_STOPPED
                                    break
                        last_message_dt = current_message_dt_from_file

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
    """Starts a replay for the given session."""
    sess_id_log = session_state.session_id[:8]
    filename_str = data_file_path.name  # For storing in app_status and logging

    with session_state.lock:
        if session_state.replay_thread and session_state.replay_thread.is_alive():
            logger.warning(
                f"Session {sess_id_log}: {config.TEXT_REPLAY_ALREADY_RUNNING}")
            return False

        # Stop any live connection for this session before starting replay
        if session_state.connection_thread and session_state.connection_thread.is_alive():
            logger.info(
                f"Session {sess_id_log}: Stopping active live connection to start replay.")
            # This will call a session-aware stop_connection. For now, conceptual:
            # signalr_client.stop_connection_session(session_state)
            # Direct stop for now (assuming stop_connection_session will be more robust)
            if session_state.hub_connection:
                try:
                    session_state.hub_connection.stop()
                except: pass # Ignore errors for now
            session_state.stop_event.set()  # Signal connection thread
            session_state.connection_thread.join(timeout=3.0)  # Wait briefly
            session_state.connection_thread = None
            session_state.hub_connection = None
            session_state.app_status['state'] = "Stopped"  # Ensure state reflects stopped live feed
            session_state.app_status['connection'] = "Disconnected (for replay)"

    if not data_file_path.is_file():
        logger.error(
            f"Session {sess_id_log}: {config.TEXT_REPLAY_FILE_NOT_FOUND_ERROR_PREFIX}{data_file_path}")
        with session_state.lock:
            session_state.app_status.update(
                {"state": "Error", "connection": config.TEXT_REPLAY_ERROR_FILE_NOT_FOUND_STATUS})
        return False

    session_state.stop_event.clear()  # Clear stop event for this new replay task

    # Reset relevant parts of session_state for the new replay
    session_state.reset_state_variables()  # This clears queues, data stores, etc.

    with session_state.lock:
        logger.info(
            f"Session {sess_id_log}: {config.TEXT_REPLAY_CLEARING_STATE} for file {filename_str}")
        session_state.app_status.update({
            "state": "Initializing",
            "connection": f"Replay Preparing: {filename_str}",
            "current_replay_file": filename_str
        })
        session_state.replay_speed = replay_speed  # Set initial speed for this session

    try:
        logger.info(
            f"Session {sess_id_log}: Starting replay thread for {filename_str} at speed {replay_speed}x")
        thread = threading.Thread(
            target=_replay_thread_target_session,
            args=(session_state, filename_str, replay_speed),
            name=f"ReplaySess_{sess_id_log}_{filename_str[:10]}", daemon=True
        )
        with session_state.lock:
            session_state.replay_thread = thread  # Store thread handle in session state
        thread.start()
        logger.info(
            f"Session {sess_id_log}: Replay thread initiated for {filename_str}")
        
        dp_s_thread = threading.Thread(
            # Assumes data_processing is imported
            target=data_processing.data_processing_loop_session,
            args=(session_state,),
            name=f"DataProc_Replay_{sess_id_log}",
            daemon=True
        )
        with session_state.lock:
            session_state.data_processing_thread = dp_s_thread
        dp_s_thread.start()
        logger.info(
            f"Session {sess_id_log}: Data processing thread for replay initiated.")

        with session_state.lock:
            session_state.app_status.update(
                {"state": "Replaying", "connection": f"Replay: {filename_str}"})
        return True

    except Exception as e:
        logger.error(
            f"Session {sess_id_log}: Failed to create or start replay thread for {filename_str}: {e}", exc_info=True)
        with session_state.lock:
            session_state.app_status.update(
                {"state": "Error", "connection": config.TEXT_REPLAY_ERROR_THREAD_START_FAILED_STATUS})
            session_state.app_status['current_replay_file'] = None
        return False


def stop_replay_session(session_state: 'app_state.SessionState'):
    """Stops the replay for the given session."""
    sess_id_log = session_state.session_id[:8]
    logger.info(f"Session {sess_id_log}: Stop replay requested.")

    s_replay_thread = None
    current_s_state = "Unknown"
    current_s_replay_file = None

    with session_state.lock:
        s_replay_thread = session_state.replay_thread
        current_s_state = session_state.app_status["state"]
        current_s_replay_file = session_state.app_status.get(
            "current_replay_file")

    if not s_replay_thread or not s_replay_thread.is_alive():
        logger.info(
            f"Session {sess_id_log}: No active replay thread to stop, or already stopping/stopped. Current state: {current_s_state}")
        with session_state.lock:
            # If it was in a replay-related state, transition to Stopped
            if current_s_state in ["Replaying", "Playback Complete", "Stopping", "Initializing"]:
                session_state.app_status.update(
                    {"state": "Stopped", "connection": config.REPLAY_STATUS_CONNECTION_REPLAY_ENDED})
            if current_s_replay_file:  # Ensure current replay file is cleared
                session_state.app_status["current_replay_file"] = None
            session_state.replay_thread = None  # Ensure handle is cleared
        return

    logger.info(
        f"Session {sess_id_log}: Actively stopping replay thread for {current_s_replay_file}...")
    with session_state.lock:
        if current_s_state == "Replaying":  # Mark as Stopping
            session_state.app_status.update(
                {"state": "Stopping", "connection": "Replay Stopping..."})

    session_state.stop_event.set()  # Signal the replay thread to stop

    logger.info(
        f"Session {sess_id_log}: Waiting for replay thread ({s_replay_thread.name}) to join...")
    s_replay_thread.join(timeout=5.0)

    with session_state.lock:  # Final status update
        if s_replay_thread.is_alive():
            logger.warning(
                f"Session {sess_id_log}: Replay thread ({s_replay_thread.name}) did not join cleanly.")
            # State might still be "Stopping", force to "Error" or "Stopped"
            session_state.app_status.update(
                {"state": "Error", "connection": "Replay Stop Failed Join"})
        else:
            logger.info(
                f"Session {sess_id_log}: Replay thread ({s_replay_thread.name}) joined successfully.")
            # Thread sets its final status, but we ensure it's at least Stopped here if it was Stopping
            if session_state.app_status["state"] == "Stopping":
                session_state.app_status.update({"state": "Stopped", "connection": config.REPLAY_STATUS_CONNECTION_REPLAY_STOPPED})

        session_state.app_status["current_replay_file"] = None  # Always clear
        session_state.replay_thread = None  # Clear thread handle
        # session_state.stop_event.clear() # Clear if no other tasks for this session use it. Handled by next task usually.

    logger.info(f"Session {sess_id_log}: Stop replay sequence complete.")


print("DEBUG: replay module (multi-session structure) loaded")

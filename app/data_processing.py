# data_processing.py
"""
Handles processing of data received from a session's data queue.
Updates the session-specific application state.
"""

import logging
import time
import queue  # Needed for queue.Empty exception
import threading  # For type hint if needed, and if starting threads from here
from datetime import datetime, timezone
from copy import deepcopy
from typing import Dict, Any, List, Tuple  # For type hints

# Import shared state definition (for SessionState type hint) and config
import app_state  # For app_state.SessionState
import utils
import config

# Module-level logger
logger = logging.getLogger("F1App.DataProcessing")

# --- Individual Stream Processing Functions (Now Session-Aware) ---


def _process_team_radio(session_state: app_state.SessionState, data: Dict[str, Any]):
    """ Processes TeamRadio stream data for a given session. """
    sess_id_log = session_state.session_id[:8]
    if not isinstance(data, dict) or 'Captures' not in data:
        logger.warning(
            f"Session {sess_id_log}: Unexpected TeamRadio data root format: {type(data)}")
        return

    captures_data = data.get('Captures')
    processed_captures_list: List[Dict[str, Any]] = []

    if isinstance(captures_data, dict):
        processed_captures_list = list(captures_data.values())
    elif isinstance(captures_data, list):
        processed_captures_list = captures_data  # type: ignore
    else:
        logger.warning(
            f"Session {sess_id_log}: TeamRadio 'Captures' field is neither a dict nor a list: {type(captures_data)}")
        return

    if not processed_captures_list:
        return

    new_messages_processed = 0
    # Lock is acquired in the main loop before calling this function
    for capture_item in processed_captures_list:
        if not isinstance(capture_item, dict):
            continue

        utc_time = capture_item.get('Utc')
        racing_num_str = capture_item.get('RacingNumber')
        audio_path = capture_item.get('Path')

        if not all([utc_time, racing_num_str, audio_path]):
            continue

        driver_tla = session_state.timing_state.get(
            str(racing_num_str), {}).get('Tla', str(racing_num_str))

        radio_entry = {
            'Utc': utc_time, 'RacingNumber': str(racing_num_str),
            'Path': audio_path, 'DriverTla': driver_tla
        }
        session_state.team_radio_messages.appendleft(radio_entry)
        new_messages_processed += 1

    if new_messages_processed > 0:
        logger.info(
            f"Session {sess_id_log}: Added {new_messages_processed} new team radio messages.")


def _process_extrapolated_clock(session_state: app_state.SessionState, data_payload: Dict[str, Any], received_timestamp_str: str):
    sess_id_log = session_state.session_id[:8]
    if not isinstance(data_payload, dict):
        logger.warning(
            f"Session {sess_id_log}: Unexpected ExtrapolatedClock data format: {type(data_payload)}")
        return

    session_state.extrapolated_clock_info["Utc"] = data_payload.get("Utc")
    session_state.extrapolated_clock_info["Extrapolating"] = data_payload.get(
        "Extrapolating", False)
    session_state.extrapolated_clock_info["Timestamp"] = received_timestamp_str

    remaining_str = data_payload.get("Remaining")
    if remaining_str:
        session_state.extrapolated_clock_info["Remaining"] = remaining_str

        session_type = session_state.session_details.get("Type", "").lower()
        current_s_app_mode = session_state.app_status.get("state")
        current_s_feed_status = session_state.session_details.get(
            'SessionStatus', 'Unknown')
        msg_dt = utils.parse_iso_timestamp_safe(received_timestamp_str)

        if current_s_app_mode == "Replaying" and session_state.session_start_feed_timestamp_utc_dt is None and msg_dt:
            parsed_remaining_seconds = utils.parse_session_time_to_seconds(
                remaining_str)
            current_q_segment = session_state.qualifying_segment_state.get(
                "current_segment")
            is_valid_timed_segment_for_replay_start = session_type.startswith("practice") or \
                (current_q_segment and current_q_segment not in [
                 "Unknown", "Between Segments", "Ended"])

            if parsed_remaining_seconds is not None and parsed_remaining_seconds > 120 and is_valid_timed_segment_for_replay_start:
                session_state.session_start_feed_timestamp_utc_dt = msg_dt
                session_state.current_segment_scheduled_duration_seconds = parsed_remaining_seconds
                logger.info(
                    f"Session {sess_id_log} Replay: Set session_start_feed_ts='{msg_dt}', "
                    f"current_segment_duration='{parsed_remaining_seconds}'s "
                    f"for segment/session: '{current_q_segment or session_type}'"
                )
                if session_type.startswith("practice"):
                    if not session_state.practice_session_scheduled_duration_seconds or \
                       abs(session_state.practice_session_scheduled_duration_seconds - parsed_remaining_seconds) > 10:  # type: ignore
                        session_state.practice_session_scheduled_duration_seconds = parsed_remaining_seconds
                        logger.info(
                            f"Session {sess_id_log} Replay: Updated practice_session_scheduled_duration_seconds to {parsed_remaining_seconds}s")

        q_state = session_state.qualifying_segment_state
        q_current_segment = q_state.get("current_segment")

        if q_state.get("just_resumed_flag", False) and session_type in [config.SESSION_TYPE_QUALI, config.SESSION_TYPE_SPRINT_SHOOTOUT]:
            if msg_dt:
                q_state["last_official_time_capture_utc"] = msg_dt
            q_state["last_capture_replay_speed"] = session_state.replay_speed
            q_state["session_status_at_capture"] = current_s_feed_status
            q_state["just_resumed_flag"] = False
            logger.debug(
                f"Session {sess_id_log}: Q Segment '{q_current_segment}' just_resumed_flag cleared by EC. Capture time updated to {msg_dt}.")

        elif q_current_segment and \
            q_current_segment not in ["Between Segments", "Ended", "Unknown", None] and \
            current_s_feed_status not in ["Suspended", "Aborted", "Finished", "Ends", "NotStarted", "Inactive"]:
            parsed_remaining_seconds = utils.parse_session_time_to_seconds(
                remaining_str)
            if parsed_remaining_seconds is not None:
                q_state["official_segment_remaining_seconds"] = parsed_remaining_seconds
                if msg_dt:
                    q_state["last_official_time_capture_utc"] = msg_dt
                q_state["last_capture_replay_speed"] = session_state.replay_speed
                q_state["session_status_at_capture"] = current_s_feed_status

        elif q_current_segment and \
            (q_current_segment in ["Between Segments", "Ended"] or
         current_s_feed_status in ["Finished", "Ends"]):
            q_state["official_segment_remaining_seconds"] = 0


def _update_current_qualifying_segment_based_on_status(session_state: app_state.SessionState, session_status_from_feed: str):
    sess_id_log = session_state.session_id[:8]
    session_type = session_state.session_details.get("Type", "").lower()
    q_state = session_state.qualifying_segment_state
    current_segment_in_q_state = q_state.get("current_segment")
    old_segment_in_q_state = q_state.get("old_segment")

    segments_order = config.QUALIFYING_ORDER.get(session_type, [])
    if not segments_order:
        if session_type.startswith("practice"):
            if current_segment_in_q_state != "Practice":
                q_state["current_segment"] = "Practice"
        return

    if session_status_from_feed == "Started":
        newly_determined_segment = None
        if not current_segment_in_q_state or \
           current_segment_in_q_state in ["Unknown", "Ended", "Practice", None, "Between Segments"]:
            if current_segment_in_q_state == "Between Segments":
                if old_segment_in_q_state and old_segment_in_q_state in segments_order:
                    try:
                        current_idx = segments_order.index(
                            old_segment_in_q_state)
                        if current_idx < len(segments_order) - 1:
                            newly_determined_segment = segments_order[current_idx + 1]
                    except ValueError:
                        logger.warning(
                            f"Session {sess_id_log}: Old segment '{old_segment_in_q_state}' not in defined order {segments_order}.")
                        if segments_order:
                            newly_determined_segment = segments_order[0]
            elif segments_order:
                newly_determined_segment = segments_order[0]

        if newly_determined_segment and newly_determined_segment != current_segment_in_q_state:
            q_state["current_segment"] = newly_determined_segment
            logger.debug(
                f"Session {sess_id_log}: _update_q_segment_helper: Tentatively set current_segment to '{newly_determined_segment}'.")


def _process_race_control(session_state: app_state.SessionState, data: Dict[str, Any]):
    sess_id_log = session_state.session_id[:8]
    messages_to_process: List[Dict[str, Any]] = []
    if isinstance(data, dict) and 'Messages' in data:
        messages_payload = data.get('Messages')
        if isinstance(messages_payload, list):
            messages_to_process = messages_payload  # type: ignore
        elif isinstance(messages_payload, dict):
            messages_to_process = list(
                messages_payload.values())  # type: ignore
        else:
            logger.warning(
                f"Session {sess_id_log}: RC 'Messages' not list/dict: {type(messages_payload)}")
            return
    elif data:
        logger.warning(f"Session {sess_id_log}: Unexpected RC format: {type(data)}"); return
    else:
        return

    new_messages_added_to_log = 0
    for msg_dict in messages_to_process:
        if not isinstance(msg_dict, dict):
            continue
        timestamp = msg_dict.get('Utc', 'Timestamp?')
        lap_num_str = str(msg_dict.get('Lap', '-'))
        message_text_from_feed = msg_dict.get('Message', '')
        time_str = "Timestamp?"
        if isinstance(timestamp, str) and 'T' in timestamp:
            try:
                time_str = timestamp.split('T')[1].split('.')[0]
            except:
                time_str = timestamp
        log_entry = f"[{time_str} L{lap_num_str}]: {message_text_from_feed}"
        session_state.race_control_log.appendleft(log_entry)
        new_messages_added_to_log += 1

        category = msg_dict.get('Category')
        flag_status = msg_dict.get('Flag')
        scope = msg_dict.get('Scope')
        sector_number = msg_dict.get('Sector')
        if category == 'Flag':
            if flag_status == 'YELLOW' and scope == 'Sector' and sector_number is not None:
                try:
                    sector_int = int(sector_number)  # type: ignore
                    if sector_int not in session_state.active_yellow_sectors:
                        session_state.active_yellow_sectors.add(sector_int)
                        logger.info(
                            f"Session {sess_id_log}: YELLOW: Sector {sector_int} added. Current: {session_state.active_yellow_sectors}")
                except ValueError:
                    pass
            elif flag_status == 'CLEAR' and scope == 'Sector' and sector_number is not None:
                try:
                    sector_int = int(sector_number)  # type: ignore
                    if sector_int in session_state.active_yellow_sectors:
                        session_state.active_yellow_sectors.discard(sector_int)
                        logger.info(
                            f"Session {sess_id_log}: CLEAR: Sector {sector_int} removed. Current: {session_state.active_yellow_sectors}")
                except ValueError:
                    pass
            elif flag_status == 'GREEN' or (category == 'TrackMessage' and "TRACK CLEAR" in message_text_from_feed.upper()):
                if session_state.active_yellow_sectors:
                    logger.info(
                        f"Session {sess_id_log}: GREEN/TRACK CLEAR: Clearing all yellows. Was: {session_state.active_yellow_sectors}")
                    session_state.active_yellow_sectors.clear()


def _process_weather_data(session_state: app_state.SessionState, data: Dict[str, Any]):
    sess_id_log = session_state.session_id[:8]
    if isinstance(data, dict):
        if 'WeatherData' not in session_state.data_store:
            session_state.data_store['WeatherData'] = {}
        session_state.data_store['WeatherData'].update(data)
    else:
        logger.warning(
            f"Session {sess_id_log}: Unexpected WeatherData format: {type(data)}")


def _update_driver_stint_data(session_state: app_state.SessionState, driver_rno_str: str,
                              stints_payload_from_app_data: Dict[str, Any],
                              driver_timing_state_info: Dict[str, Any]):  # Changed from driver_info_from_timing_state
    sess_id_log = session_state.session_id[:8]
    if not isinstance(stints_payload_from_app_data, dict) or not stints_payload_from_app_data:
        return

    driver_stints_history = session_state.driver_stint_data.setdefault(
        driver_rno_str, [])
    driver_laps_completed = 0
    if driver_timing_state_info.get('NumberOfLaps') is not None:  # Use passed arg
        try:
            driver_laps_completed = int(
                driver_timing_state_info['NumberOfLaps'])
        except (ValueError, TypeError):
            if driver_stints_history:
                driver_laps_completed = driver_stints_history[-1].get(
                    'end_lap', 0)

    try:
        sorted_incoming_stint_keys = sorted(
            stints_payload_from_app_data.keys(), key=int)
    except ValueError:
        logger.error(
            f"Session {sess_id_log} StintUpdate: Stint keys for driver {driver_rno_str} not all sortable. Payload: {stints_payload_from_app_data}")
        return

    for stint_feed_key in sorted_incoming_stint_keys:
        incoming_stint_info = stints_payload_from_app_data[stint_feed_key]
        if not isinstance(incoming_stint_info, dict):
            continue

        existing_stint_entry = next((hist_stint for hist_stint in driver_stints_history if hist_stint.get(
            'feed_stint_key') == stint_feed_key), None)

        parsed_compound = incoming_stint_info.get('Compound')
        if parsed_compound is None and existing_stint_entry:
            parsed_compound = existing_stint_entry.get('compound')
        if not parsed_compound:
            continue

        start_laps_from_feed = 0
        is_new_feed = False
        total_laps_on_tyre_set_feed = 0
        tyres_not_changed_feed = False
        try:
            start_laps_from_feed = int(incoming_stint_info.get('StartLaps', existing_stint_entry.get(
                'start_laps_from_feed_val', 0) if existing_stint_entry else 0))
            is_new_feed_str = str(incoming_stint_info.get('New', str(existing_stint_entry.get(
                'is_new_tyre', False)).lower() if existing_stint_entry else 'false')).lower()
            is_new_feed = (is_new_feed_str == 'true')
            total_laps_on_tyre_set_feed = int(incoming_stint_info.get('TotalLaps', existing_stint_entry.get(
                'tyre_total_laps_at_stint_end', 0) if existing_stint_entry else 0))
            tyres_not_changed_feed_str = str(incoming_stint_info.get('TyresNotChanged', str(
                existing_stint_entry.get('tyres_not_changed', '0')).lower() if existing_stint_entry else '0')).lower()
            tyres_not_changed_feed = (
                tyres_not_changed_feed_str == 'true' or tyres_not_changed_feed_str == '1')
        except (ValueError, TypeError) as e:
            logger.warning(
                f"Session {sess_id_log} StintParse: Error parsing data for stint key '{stint_feed_key}' for {driver_rno_str}: {e}")

        actual_stint_start_lap = start_laps_from_feed
        if existing_stint_entry:
            actual_stint_start_lap = existing_stint_entry['start_lap']
        else:
            if stint_feed_key == "0" and start_laps_from_feed == 0:
                actual_stint_start_lap = 1
            elif stint_feed_key != "0" and start_laps_from_feed == 0:
                actual_stint_start_lap = (
                    driver_laps_completed + 1) if driver_laps_completed > 0 else 1
        if actual_stint_start_lap <= 0:
            actual_stint_start_lap = 1

        # End lap is the last completed lap OF the stint
        current_stint_provisional_end_lap = max(
            driver_laps_completed, actual_stint_start_lap - 1)
        if driver_laps_completed < actual_stint_start_lap:
            current_stint_provisional_end_lap = actual_stint_start_lap - 1  # Not started yet

        laps_run_in_this_stint = max(
            0, current_stint_provisional_end_lap - actual_stint_start_lap + 1)

        if existing_stint_entry:
            existing_stint_entry.update({
                'compound': parsed_compound, 'is_new_tyre': is_new_feed, 'end_lap': current_stint_provisional_end_lap,
                'total_laps_on_tyre_in_stint': laps_run_in_this_stint, 'tyre_total_laps_at_stint_end': total_laps_on_tyre_set_feed,
                'tyres_not_changed': tyres_not_changed_feed
            })
            # logger.debug(f"Session {sess_id_log} StintUpdate: Updated Stint {existing_stint_entry['stint_number']} for {driver_rno_str} (FK {stint_feed_key})")
        else:  # New stint
            if driver_stints_history:  # Finalize previous stint
                prev_stint = driver_stints_history[-1]
                # Ensure it's actually a different stint
                if prev_stint.get('feed_stint_key') != stint_feed_key:
                    if prev_stint.get('end_lap') is None or prev_stint.get('end_lap', 0) < actual_stint_start_lap - 1:
                        final_end_prev = max(
                            actual_stint_start_lap - 1, prev_stint['start_lap']-1)
                        prev_stint['end_lap'] = final_end_prev
                        prev_stint['total_laps_on_tyre_in_stint'] = max(
                            0, final_end_prev - prev_stint['start_lap'] + 1)
                        if prev_stint.get('tyres_not_changed'):
                            prev_stint['tyre_total_laps_at_stint_end'] = prev_stint.get(
                                'tyre_age_at_stint_start', 0) + prev_stint['total_laps_on_tyre_in_stint']

            stint_num_hist = len(driver_stints_history) + 1
            age_at_start = 0
            if not is_new_feed:
                if tyres_not_changed_feed and driver_stints_history and stint_num_hist > 1:
                    prev_hist = driver_stints_history[-1]
                    if prev_hist.get('compound') == parsed_compound:
                        age_at_start = prev_hist.get(
                            'tyre_total_laps_at_stint_end', 0)
                    else:
                        age_at_start = max(
                            0, total_laps_on_tyre_set_feed - laps_run_in_this_stint) if total_laps_on_tyre_set_feed >= laps_run_in_this_stint else 0
                else:
                    age_at_start = max(0, total_laps_on_tyre_set_feed -
                                       laps_run_in_this_stint) if total_laps_on_tyre_set_feed >= laps_run_in_this_stint else 0

            driver_stints_history.append({
                "stint_number": stint_num_hist, "feed_stint_key": stint_feed_key, "start_laps_from_feed_val": start_laps_from_feed,
                "start_lap": actual_stint_start_lap, "compound": parsed_compound, "is_new_tyre": is_new_feed,
                "tyre_age_at_stint_start": age_at_start, "end_lap": current_stint_provisional_end_lap,
                "total_laps_on_tyre_in_stint": laps_run_in_this_stint, "tyre_total_laps_at_stint_end": total_laps_on_tyre_set_feed,
                "tyres_not_changed": tyres_not_changed_feed
            })
            # logger.debug(f"Session {sess_id_log} StintUpdate: Added NEW Stint {stint_num_hist} for {driver_rno_str} (FK {stint_feed_key})")
    session_state.driver_stint_data[driver_rno_str] = sorted(
        driver_stints_history, key=lambda x: x['stint_number'])


def _process_timing_app_data(session_state: app_state.SessionState, data: Dict[str, Any]):
    sess_id_log = session_state.session_id[:8]
    if not session_state.timing_state:
        return

    if isinstance(data, dict) and 'Lines' in data and isinstance(data['Lines'], dict):
        for car_num_str, line_data in data['Lines'].items():
            driver_current_s_state = session_state.timing_state.get(
                car_num_str)  # Use consistent naming
            if driver_current_s_state and isinstance(line_data, dict):
                stints_payload = line_data.get('Stints')
                if isinstance(stints_payload, dict) and stints_payload:
                    # Pass driver's timing state
                    _update_driver_stint_data(
                        session_state, car_num_str, stints_payload, driver_current_s_state)

                if isinstance(stints_payload, dict) and stints_payload:  # For current tyre display
                    try:
                        latest_stint_key = sorted(
                            stints_payload.keys(), key=int)[-1]
                        latest_stint_info = stints_payload[latest_stint_key]
                        if isinstance(latest_stint_info, dict):
                            compound_val = latest_stint_info.get('Compound')
                            if compound_val:
                                driver_current_s_state['TyreCompound'] = str(
                                    compound_val).upper()

                            new_status_val = latest_stint_info.get('New')
                            if isinstance(new_status_val, str):
                                driver_current_s_state['IsNewTyre'] = new_status_val.lower(
                                ) == 'true'
                            elif isinstance(new_status_val, bool):
                                driver_current_s_state['IsNewTyre'] = new_status_val

                            age_det, current_age_val = False, '?'
                            total_laps_val = latest_stint_info.get('TotalLaps')
                            if total_laps_val is not None:
                                try:
                                    current_age_val = int(total_laps_val)
                                    age_det = True
                                except:
                                    pass
                            if not age_det:  # Fallback age calculation
                                start_laps_val = latest_stint_info.get(
                                    'StartLaps')
                                num_laps_val = driver_current_s_state.get(
                                    'NumberOfLaps')
                                if start_laps_val is not None and num_laps_val is not None:
                                    try:
                                        age_calc = int(
                                            num_laps_val) - int(start_laps_val) + 1
                                        current_age_val = age_calc if age_calc >= 0 else '?'
                                    except:
                                        pass
                            driver_current_s_state['TyreAge'] = current_age_val

                            # Pit duration display (your existing logic for this)
                            if 'PitInTime' in latest_stint_info and latest_stint_info['PitInTime'] and \
                               'PitOutTime' in latest_stint_info and latest_stint_info['PitOutTime']:
                                pit_in_s = utils.parse_feed_time_to_seconds(
                                    latest_stint_info['PitInTime'])
                                pit_out_s = utils.parse_feed_time_to_seconds(
                                    latest_stint_info['PitOutTime'])
                                if pit_in_s is not None and pit_out_s is not None and pit_out_s >= pit_in_s:
                                    duration = round(pit_out_s - pit_in_s, 1)
                                    # Check if this is a new completed pit stop for this stint key
                                    if driver_current_s_state.get('last_pit_stint_key_ref') != latest_stint_key or \
                                       driver_current_s_state.get('last_pit_duration') != duration:  # Or if duration changed for same key
                                        driver_current_s_state['last_pit_duration'] = duration
                                        # Wall time for display timeout
                                        driver_current_s_state['last_pit_duration_timestamp'] = time.time(
                                        )
                                        # Track which stint this was for
                                        driver_current_s_state['last_pit_stint_key_ref'] = latest_stint_key
                    except Exception as e_stint:
                        logger.error(
                            f"Session {sess_id_log} Drv {car_num_str}: Error proc Stints in TimingAppData: {e_stint}", exc_info=False)
    elif data:
        logger.warning(
            f"Session {sess_id_log}: Unexpected TimingAppData format: {type(data)}")


def _process_driver_list(session_state: app_state.SessionState, data: Dict[str, Any]):
    sess_id_log = session_state.session_id[:8]
    added_count = 0
    updated_count = 0
    if not isinstance(data, dict):
        logger.warning(
            f"Session {sess_id_log}: Unexpected DriverList stream data format: {type(data)}.")
        return

    for driver_num_str, driver_info_payload in data.items():
        if driver_num_str == "_kf":
            continue  # Skip FastF1 internal key
        if not isinstance(driver_info_payload, dict):
            logger.warning(
                f"Session {sess_id_log}: Skipping invalid driver_info for {driver_num_str}: {driver_info_payload}")
            continue

        is_new_driver = driver_num_str not in session_state.timing_state

        # Ensure default structures are present if new or if existing driver_state is somehow minimal
        if is_new_driver:
            # Initialize if new
            session_state.timing_state[driver_num_str] = {}
            added_count += 1

        # Use setdefault for all keys to ensure they exist with a default from a template
        # This is safer than manual checking for each key.
        # Define a more complete default structure based on what your app expects.
        default_driver_template = {
            "RacingNumber": driver_num_str, "Tla": "N/A", "FullName": "N/A", "TeamName": "N/A",
            "Line": "-", "TeamColour": "FFFFFF", "FirstName": "", "LastName": "",
            "Position": "-", "Time": "-", "GapToLeader": "-", "IntervalToPositionAhead": {"Value": "-"},
            "LastLapTime": {}, "BestLapTime": {},
            # Basic sector structure
            "Sectors": {"0": {"Value": "-"}, "1": {"Value": "-"}, "2": {"Value": "-"}},
            "Status": "On Track", "InPit": False, "Retired": False, "Stopped": False, "PitOut": False,
            "TyreCompound": "-", "TyreAge": "?", "IsNewTyre": False,
            "NumberOfPitStops": 0, "ReliablePitStops": 0,
            "CarData": {}, "PositionData": {}, "PreviousPositionData": {},
            "PersonalBestLapTimeValue": None, "IsOverallBestLap": False,
            "PersonalBestSectors": [None, None, None], "IsOverallBestSector": [False, False, False],
            "current_pit_entry_system_time": None, "pit_entry_replay_speed": None,
            "last_pit_duration": None, "last_pit_duration_timestamp": None,
            "last_pit_stint_key_ref": None, "just_exited_pit_event_time": None,
            "final_live_pit_time_text": None, "final_live_pit_time_display_timestamp": None
        }

        # Get or new empty dict
        current_driver_s_state = session_state.timing_state[driver_num_str]
        for key, default_value in default_driver_template.items():
            current_driver_s_state.setdefault(key, deepcopy(default_value) if isinstance(
                default_value, (dict, list)) else default_value)

        # Now update with incoming data
        for key_from_payload, value_from_payload in driver_info_payload.items():
            if value_from_payload is not None:  # Only update if value is not None from payload
                current_driver_s_state[key_from_payload] = value_from_payload

        # Ensure specific sub-structures are initialized if not present after update
        if not isinstance(current_driver_s_state.get("Sectors"), dict):
            current_driver_s_state["Sectors"] = deepcopy(
                default_driver_template["Sectors"])
        for i in range(3):
            current_driver_s_state["Sectors"].setdefault(
                str(i), {"Value": "-"})

        if is_new_driver:  # Initialize history lists for new drivers
            session_state.lap_time_history[driver_num_str] = []
            session_state.telemetry_data[driver_num_str] = {}
            session_state.driver_stint_data[driver_num_str] = []
        else:
            updated_count += 1  # Count as updated if not new

    if added_count > 0 or updated_count > 0:
        logger.debug(
            f"Session {sess_id_log}: Processed DriverList. Added: {added_count}, Updated: {updated_count}. Total drivers: {len(session_state.timing_state)}")


def _process_timing_data(session_state: app_state.SessionState, data: Dict[str, Any]):
    # (This function is very complex, applying the same pattern:
    #  - Pass session_state
    #  - Access session_state.timing_state, session_state.session_bests, session_state.lap_time_history
    #  - Use session_state.replay_speed for pit timer adjustment if relevant
    #  - Add sess_id_log to logger calls)
    # The core logic for parsing line_data, sectors, laps, pit stops remains the same,
    # but all state reads/writes are to the session_state object.
    sess_id_log = session_state.session_id[:8]
    if not session_state.timing_state:
        return
    if isinstance(data, dict) and 'CutOffTime' in data and len(data) == 1:
        return

    if isinstance(data, dict) and 'Lines' in data and isinstance(data['Lines'], dict):
        # Wall time for pit stop duration display
        current_time_for_pit_calc = time.time()

        for car_num_str, line_data in data['Lines'].items():
            driver_s_state = session_state.timing_state.get(
                car_num_str)  # Renamed for clarity
            if driver_s_state and isinstance(line_data, dict):
                original_last_lap_time_info = driver_s_state.get(
                    'LastLapTime', {}).copy()

                was_in_pit = driver_s_state.get('InPit', False)
                is_in_pit_feed = line_data.get('InPit', was_in_pit)

                if is_in_pit_feed and not was_in_pit:  # Entered
                    driver_s_state['current_pit_entry_system_time'] = current_time_for_pit_calc
                    # From session
                    driver_s_state['pit_entry_replay_speed'] = session_state.replay_speed
                    driver_s_state['final_live_pit_time_text'] = None
                    driver_s_state['just_exited_pit_event_time'] = None
                elif not is_in_pit_feed and was_in_pit:  # Exited
                    entry_wall = driver_s_state.get(
                        'current_pit_entry_system_time')
                    speed_entry = driver_s_state.get(
                        'pit_entry_replay_speed', 1.0)
                    if speed_entry <= 0:
                        speed_entry = 1.0
                    if entry_wall is not None:
                        adj_elapsed = (current_time_for_pit_calc -
                                       entry_wall) * speed_entry
                        driver_s_state['final_live_pit_time_text'] = f"Stop: {adj_elapsed:.1f}s"
                        driver_s_state['final_live_pit_time_display_timestamp'] = current_time_for_pit_calc
                    driver_s_state['current_pit_entry_system_time'] = None
                    driver_s_state['just_exited_pit_event_time'] = current_time_for_pit_calc

                for key in ["Position", "Time", "GapToLeader", "InPit", "Retired", "Stopped", "PitOut", "NumberOfLaps", "NumberOfPitStops"]:
                    if key in line_data:
                        if key == "NumberOfPitStops":
                             driver_s_state[key] = int(
                                 line_data[key]) if line_data[key] is not None else 0
                        else:
                             driver_s_state[key] = line_data[key]

                if "BestLapTime" in line_data:
                    # ... (logic for PersonalBestLapTimeValue and PersonalBestLapTime using driver_s_state)
                    incoming_blt = line_data["BestLapTime"]
                    if isinstance(incoming_blt, dict) and incoming_blt.get("Value"):
                        pb_lap_s = utils.parse_lap_time_to_seconds(
                            incoming_blt.get("Value"))
                        curr_pb_s = driver_s_state.get(
                            "PersonalBestLapTimeValue")
                        if pb_lap_s is not None and (curr_pb_s is None or pb_lap_s < curr_pb_s):
                            driver_s_state["PersonalBestLapTimeValue"] = pb_lap_s
                            driver_s_state["PersonalBestLapTime"] = incoming_blt.copy(
                            )
                    driver_s_state.setdefault("BestLapTime", {}).update(
                        incoming_blt if isinstance(incoming_blt, dict) else {'Value': incoming_blt})

                for key in ["IntervalToPositionAhead", "LastLapTime"]:
                    if key in line_data:
                        val = line_data[key]
                        driver_s_state.setdefault(key, {}).update(
                            val if isinstance(val, dict) else {'Value': val})

                if "Sectors" in line_data and isinstance(line_data["Sectors"], dict):
                    # ... (logic for Sectors, PersonalBestSectors, and session_state.session_bests["OverallBestSectors"])
                    driver_s_state.setdefault("Sectors", {"0": {"Value": "-"}, "1":{"Value":"-"}, "2":{"Value":"-"}}) # Ensure structure
                    for i in range(3):
                        s_idx_str = str(i)
                        s_data_feed = line_data["Sectors"].get(s_idx_str)
                        target_s_state = driver_s_state["Sectors"].setdefault(
                            s_idx_str, {"Value": "-", "PersonalFastest": False, "OverallFastest": False})
                        if s_data_feed is not None:
                            if isinstance(s_data_feed, dict):
                                target_s_state.update(s_data_feed)
                            else:
                                target_s_state['Value'] = s_data_feed
                            if target_s_state.get("Value") == "" or target_s_state.get("Value") is None:
                                target_s_state["Value"] = "-"

                        s_val_str = target_s_state.get("Value")
                        is_pb = target_s_state.get("PersonalFastest", False)
                        if s_val_str and s_val_str != "-":
                            s_seconds = utils.parse_lap_time_to_seconds(
                                s_val_str)
                            if s_seconds is not None:
                                driver_s_state.setdefault("PersonalBestSectors", [None, None, None]) # Ensure list exists
                                curr_pb_s_val = driver_s_state["PersonalBestSectors"][i]
                                if is_pb and (curr_pb_s_val is None or s_seconds < curr_pb_s_val):
                                    driver_s_state["PersonalBestSectors"][i] = s_seconds
                                elif not is_pb and (curr_pb_s_val is None or s_seconds < curr_pb_s_val):
                                    driver_s_state["PersonalBestSectors"][i] = s_seconds

                                overall_best_s_val = session_state.session_bests[
                                    "OverallBestSectors"][i]["Value"]
                                if overall_best_s_val is None or s_seconds < overall_best_s_val:
                                    session_state.session_bests["OverallBestSectors"][i] = {
                                        "Value": s_seconds, "DriverNumber": car_num_str}

                if "Speeds" in line_data and isinstance(line_data["Speeds"], dict):
                    driver_s_state.setdefault(
                        "Speeds", {}).update(line_data["Speeds"])

                status_flags = [flag_name for flag_name, flag_val in [("Retired", driver_s_state.get("Retired")), ("In Pit", driver_s_state.get(
                    "InPit")), ("Stopped", driver_s_state.get("Stopped")), ("Out Lap", driver_s_state.get("PitOut"))] if flag_val]
                driver_s_state["Status"] = ", ".join(status_flags) if status_flags else (
                    "On Track" if driver_s_state.get("Position", "-") != "-" else "Unknown")

                new_llt_info = driver_s_state.get('LastLapTime', {})
                new_llt_str = new_llt_info.get('Value')
                if new_llt_str and new_llt_str != original_last_lap_time_info.get('Value'):
                    llt_s = utils.parse_lap_time_to_seconds(new_llt_str)
                    if llt_s is not None:
                        overall_best_lap_s = utils.parse_lap_time_to_seconds(
                            session_state.session_bests["OverallBestLapTime"]["Value"])
                        is_valid_for_ob = not driver_s_state.get('InPit', False) and not driver_s_state.get(
                            'PitOut', False) and not driver_s_state.get('Stopped', False)
                        if is_valid_for_ob and (overall_best_lap_s is None or llt_s < overall_best_lap_s):
                            session_state.session_bests["OverallBestLapTime"] = {
                                "Value": new_llt_str, "DriverNumber": car_num_str}

                        completed_laps = driver_s_state.get('NumberOfLaps', 0)
                        lap_num_for_hist = completed_laps
                        session_state.lap_time_history.setdefault(
                            car_num_str, [])  # Ensure list exists
                        last_rec_lap_num = session_state.lap_time_history[
                            car_num_str][-1]['lap_number'] if session_state.lap_time_history[car_num_str] else 0
                        if lap_num_for_hist > 0 and lap_num_for_hist > last_rec_lap_num:
                            compound = driver_s_state.get(
                                'TyreCompound', 'UNK')
                            is_valid_hist = new_llt_info.get('OverallFastest', False) or new_llt_info.get(
                                'PersonalFastest', False) or is_valid_for_ob
                            session_state.lap_time_history[car_num_str].append(
                                {'lap_number': lap_num_for_hist, 'lap_time_seconds': llt_s,
                                    'compound': compound, 'is_valid': is_valid_hist}
                            )
        # Post-loop update for overall best flags
        overall_best_lap_holder = session_state.session_bests["OverallBestLapTime"]["DriverNumber"]
        overall_best_sector_holders = [
            s["DriverNumber"] for s in session_state.session_bests["OverallBestSectors"]]
        for drv_state_check in session_state.timing_state.values():  # Iterate values directly
            # Need car_num_str for comparison if it's not an attribute of drv_state_check
            # Assuming RacingNumber is the car_num_str or can be used to find it if needed.
            # This part might need access to the key if drv_state_check doesn't have car_num_str implicitly
            # For now, assuming this logic is adjusted or car_num_str is available in drv_state_check
            car_num_for_check = drv_state_check.get(
                "RacingNumber")  # Or the key if iterating items()
            if car_num_for_check:
                drv_state_check["IsOverallBestLap"] = (
                    overall_best_lap_holder == car_num_for_check)
                is_overall_list = drv_state_check.setdefault(
                     "IsOverallBestSector", [False, False, False])
                for i in range(3):
                     is_overall_list[i] = (
                         overall_best_sector_holders[i] == car_num_for_check)

    elif data:
        logger.warning(
            f"Session {sess_id_log}: Unexpected TimingData format: {type(data)}. Content: {str(data)[:100]}")


def _process_track_status(session_state: app_state.SessionState, data: Dict[str, Any]):
    sess_id_log = session_state.session_id[:8]
    if not isinstance(data, dict):
        logger.warning(f"Session {sess_id_log}: TrackStatus non-dict: {data}")
        return
    new_status = data.get(
        'Status', session_state.track_status_data.get('Status', 'Unknown'))
    new_message = data.get(
        'Message', session_state.track_status_data.get('Message', ''))
    if session_state.track_status_data.get('Status') != new_status or session_state.track_status_data.get('Message') != new_message:
        session_state.track_status_data['Status'] = new_status
        session_state.track_status_data['Message'] = new_message
        logger.info(
            f"Session {sess_id_log}: Track Status Update: Status={new_status}, Message='{new_message}'")


def _process_session_data(session_state: app_state.SessionState, data: Dict[str, Any]):
    # (This function is very complex. Key changes:
    #  - Pass session_state
    #  - All reads/writes to app_state.session_details, app_state.qualifying_segment_state,
    #    app_state.practice_session_*, app_state.app_status, etc. become session_state equivalents.
    #  - Calls to _update_current_qualifying_segment_based_on_status will pass session_state.
    #  - Logging includes sess_id_log)
    sess_id_log = session_state.session_id[:8]
    if not isinstance(data, dict):
        logger.warning(f"Session {sess_id_log}: SessionData non-dict: {data}")
        return
    try:
        status_series = data.get('StatusSeries')
        if isinstance(status_series, dict):
            for entry_key, status_info in status_series.items():
                if not isinstance(status_info, dict):
                    continue
                session_status_from_feed = status_info.get('SessionStatus')
                if not session_status_from_feed:
                    continue

                session_type = session_state.session_details.get(
                    "Type", "").lower()
                current_s_app_mode = session_state.app_status.get("state")
                q_state = session_state.qualifying_segment_state
                segment_in_q_state_before_this_event = q_state.get(
                    "current_segment")
                previous_recorded_feed_status = session_state.session_details.get(
                    'PreviousSessionStatus')

                if session_state.session_details.get('SessionStatus') != session_status_from_feed:
                    logger.info(
                        f"Session {sess_id_log}: Session Status Updated: {session_status_from_feed} (was {session_state.session_details.get('SessionStatus')})")
                session_state.session_details['SessionStatus'] = session_status_from_feed

                if session_type.startswith("practice"):
                    # ... (your existing practice logic, using session_state attributes) ...
                    if session_status_from_feed == "Started":
                        if q_state.get("current_segment") != "Practice":
                            q_state["old_segment"] = segment_in_q_state_before_this_event
                            q_state["current_segment"] = "Practice"
                        if session_state.practice_session_actual_start_utc is None:
                            session_state.practice_session_actual_start_utc = datetime.datetime.now(
                                timezone.utc)
                            scheduled_duration = session_state.session_details.get(
                                'ScheduledDurationSeconds')
                            session_state.practice_session_scheduled_duration_seconds = scheduled_duration if scheduled_duration and scheduled_duration > 0 else 3600
                        if current_s_app_mode == "Replaying":
                            session_state.session_start_feed_timestamp_utc_dt = None
                            session_state.current_segment_scheduled_duration_seconds = session_state.practice_session_scheduled_duration_seconds
                        q_state["just_resumed_flag"] = False
                        if current_s_app_mode == "Live" and session_state.practice_session_scheduled_duration_seconds:
                            q_state["official_segment_remaining_seconds"] = session_state.practice_session_scheduled_duration_seconds
                            q_state["last_official_time_capture_utc"] = session_state.practice_session_actual_start_utc
                    elif session_status_from_feed in ["Finished", "Ends"]:
                        if q_state.get("current_segment") != "Practice Ended":
                            q_state["old_segment"] = q_state.get("current_segment")
                            q_state["current_segment"] = "Practice Ended"
                        q_state["official_segment_remaining_seconds"] = 0
                        q_state["just_resumed_flag"] = False

                elif session_type in ["qualifying", "sprint shootout"]:
                    # ... (your existing qualifying logic, using session_state attributes) ...
                    segments = config.QUALIFYING_ORDER.get(session_type, [])
                    determined_next_segment = segment_in_q_state_before_this_event
                    resuming_this_segment, is_brand_new_q_segment = False, False
                    if session_status_from_feed == "Started":
                        _update_current_qualifying_segment_based_on_status(
                            session_state, session_status_from_feed)
                        determined_next_segment = q_state.get(
                            "current_segment")
                        if determined_next_segment != segment_in_q_state_before_this_event and determined_next_segment in segments:
                            is_brand_new_q_segment = True
                        elif previous_recorded_feed_status in ["Aborted", "Inactive", "Suspended"] and segment_in_q_state_before_this_event in segments and determined_next_segment == segment_in_q_state_before_this_event:
                            resuming_this_segment = True
                    elif session_status_from_feed in ["Finished", "Ends"]:
                        if segment_in_q_state_before_this_event and segment_in_q_state_before_this_event in segments:
                            idx = segments.index(
                                segment_in_q_state_before_this_event)
                            determined_next_segment = "Between Segments" if idx < len(
                                segments) - 1 else "Ended"
                    elif session_status_from_feed in ["Aborted", "Inactive", "Suspended"]:
                        if segment_in_q_state_before_this_event and segment_in_q_state_before_this_event in segments:
                            determined_next_segment = segment_in_q_state_before_this_event
                            if current_s_app_mode == "Replaying" and all([session_state.session_start_feed_timestamp_utc_dt, session_state.current_segment_scheduled_duration_seconds, session_state.current_processed_feed_timestamp_utc_dt]):
                                elapsed_at_pause = (session_state.current_processed_feed_timestamp_utc_dt - \
                                                    session_state.session_start_feed_timestamp_utc_dt).total_seconds()  # type: ignore
                                q_state["official_segment_remaining_seconds"] = max(
                                    0, session_state.current_segment_scheduled_duration_seconds - elapsed_at_pause)  # type: ignore
                                q_state["last_official_time_capture_utc"] = session_state.current_processed_feed_timestamp_utc_dt
                            q_state["just_resumed_flag"] = False
                    if determined_next_segment != segment_in_q_state_before_this_event or resuming_this_segment:
                        q_state["old_segment"] = segment_in_q_state_before_this_event
                        q_state["current_segment"] = determined_next_segment
                        if determined_next_segment in ["Between Segments", "Ended"]:
                            q_state.update({"official_segment_remaining_seconds": 0, "last_official_time_capture_utc":None, "just_resumed_flag":False})
                        elif resuming_this_segment:
                            q_state["last_official_time_capture_utc"] = datetime.datetime.now(
                                timezone.utc)
                            q_state["just_resumed_flag"] = True
                            if current_s_app_mode == "Replaying":
                                session_state.current_segment_scheduled_duration_seconds = q_state.get("official_segment_remaining_seconds", 0)
                                session_state.session_start_feed_timestamp_utc_dt = session_state.current_processed_feed_timestamp_utc_dt
                        elif is_brand_new_q_segment:
                            q_state["just_resumed_flag"] = False
                            default_duration = config.QUALIFYING_SEGMENT_DEFAULT_DURATIONS.get(
                                str(determined_next_segment), 900)
                            q_state["official_segment_remaining_seconds"] = default_duration
                            q_state["last_official_time_capture_utc"] = None
                            if current_s_app_mode == "Replaying":
                                session_state.session_start_feed_timestamp_utc_dt = None
                                session_state.current_segment_scheduled_duration_seconds = None
                session_state.session_details['PreviousSessionStatus'] = session_status_from_feed
    except Exception as e:
        logger.error(
            f"Session {sess_id_log}: Error processing SessionData: {e}", exc_info=True)


def _process_session_info(session_state: app_state.SessionState, data: Dict[str, Any]):
    sess_id_log = session_state.session_id[:8]
    if not isinstance(data, dict):
        logger.warning(f"Session {sess_id_log}: SessionInfo non-dict: {data}")
        return
    try:
        current_s_type_for_comp = session_state.session_details.get(
            "Type", "").lower()
        current_s_key_for_comp = session_state.session_details.get(
            'SessionKey')
        current_cached_track_key_for_comp = session_state.track_coordinates_cache.get(
            'session_key')

        # Calls the util function which is now designed to NOT access global state
        details_to_update, reset_flags, practice_duration_val, fetch_thread_init_info = \
            utils.prepare_session_info_data(data, current_s_type_for_comp,
                                            current_s_key_for_comp, current_cached_track_key_for_comp)

        if reset_flags.get("reset_q_and_practice"):
            session_state.qualifying_segment_state = deepcopy(
                app_state.INITIAL_SESSION_QUALIFYING_SEGMENT_STATE)  # Use app_state for INITIAL
            session_state.session_start_feed_timestamp_utc_dt = None
            session_state.current_segment_scheduled_duration_seconds = None
            session_state.practice_session_actual_start_utc = None

        session_state.session_details.update(details_to_update)

        if practice_duration_val is not None and details_to_update.get("Type", "").lower().startswith("practice"):
            session_state.practice_session_scheduled_duration_seconds = practice_duration_val

        if reset_flags.get("clear_track_cache"):
            session_state.track_coordinates_cache = deepcopy(app_state.INITIAL_SESSION_TRACK_COORDINATES_CACHE)  # Use app_state for INITIAL

        if fetch_thread_init_info:
            # Store for main loop to action
            session_state._pending_background_fetch = fetch_thread_init_info
        else:
            session_state._pending_background_fetch = None

        logger.debug(
            f"Session {sess_id_log}: Processed SessionInfo. Current SessionKey: {session_state.session_details.get('SessionKey')}")
    except Exception as e:
        logger.error(
            f"Session {sess_id_log}: Error processing SessionInfo: {e}", exc_info=True)

# --- Main Processing Loop (Session-Aware) ---


def data_processing_loop_session(session_state: app_state.SessionState):
    sess_id_log = session_state.session_id[:8]
    logger.info(f"Data processing thread started for session: {sess_id_log}")

    processed_count = 0
    last_log_time = time.monotonic()

    while not session_state.stop_event.is_set():
        item = None
        try:
            item = session_state.data_queue.get(
                block=True, timeout=0.1)  # Shorter timeout
            processed_count += 1

            if not isinstance(item, dict) or 'stream' not in item or 'data' not in item:
                logger.warning(
                    f"Session {sess_id_log}: Skipping queue item with unexpected structure: {type(item)}")
                if hasattr(session_state.data_queue, 'task_done'):
                    session_state.data_queue.task_done()
                continue

            stream_name = item['stream']
            actual_data = item['data']
            timestamp = item.get('timestamp')

            if timestamp:
                msg_dt = utils.parse_iso_timestamp_safe(timestamp)
                if msg_dt:
                    with session_state.lock:
                        session_state.current_processed_feed_timestamp_utc_dt = msg_dt

            with session_state.lock:  # Main lock for processing a message
                session_state.data_store[stream_name] = {
                    "data": actual_data, "timestamp": timestamp}
                session_state._pending_background_fetch = None

                try:
                    if stream_name == "Heartbeat":
                        session_state.app_status["last_heartbeat"] = timestamp
                    elif stream_name == "DriverList":
                        _process_driver_list(session_state, actual_data) # type: ignore
                    elif stream_name == "TimingData":
                        _process_timing_data(session_state, actual_data)  # type: ignore
                    elif stream_name == "SessionInfo":
                        _process_session_info(session_state, actual_data) # type: ignore
                    elif stream_name == "SessionData":
                        _process_session_data(session_state, actual_data)  # type: ignore
                    elif stream_name == "TimingAppData":
                        _process_timing_app_data(session_state, actual_data) # type: ignore
                    elif stream_name == "TrackStatus":
                        _process_track_status(session_state, actual_data)  # type: ignore
                    elif stream_name == "WeatherData":
                        _process_weather_data(session_state, actual_data) # type: ignore
                    elif stream_name == "RaceControlMessages":
                        _process_race_control(session_state, actual_data)  # type: ignore
                    elif stream_name == "TeamRadio":
                        _process_team_radio(session_state, actual_data) # type: ignore
                    elif stream_name == "ExtrapolatedClock":
                        _process_extrapolated_clock(session_state, actual_data, timestamp)  # type: ignore
                    elif stream_name == "Position":
                        # Position data prep uses a snapshot, so get snapshot then call prepare
                        current_timing_state_snapshot_for_pos = {k: {'PositionData': v.get('PositionData', {}), 'PreviousPositionData': v.get('PreviousPositionData', {}) } 
                                                                 for k, v in session_state.timing_state.items()}
                        position_batch_updates = utils.prepare_position_data_updates(
                            actual_data, current_timing_state_snapshot_for_pos)  # type: ignore
                        for car_n_str, updates in position_batch_updates.items():
                            if car_n_str in session_state.timing_state:
                                session_state.timing_state[car_n_str]['PreviousPositionData'] = updates['PreviousPositionData']
                                session_state.timing_state[car_n_str]['PositionData'] = updates['PositionData']
                    elif stream_name == "CarData":
                        current_timing_state_snapshot_for_car = {k: {'NumberOfLaps': v.get('NumberOfLaps', -1)}
                                                                 for k, v in session_state.timing_state.items()}
                        car_specific_updates, telemetry_updates = utils.prepare_car_data_updates(
                            actual_data, current_timing_state_snapshot_for_car)  # type: ignore
                        for car_n_str, updates in car_specific_updates.items():
                            if car_n_str in session_state.timing_state:
                                if 'CarData' in updates:
                                    session_state.timing_state[car_n_str].setdefault(
                                        'CarData', {}).update(updates['CarData'])
                        for (car_n_str, lap_n), telem_upd in telemetry_updates.items():
                            session_state.telemetry_data.setdefault(car_n_str, {}).setdefault(
                                lap_n, {'Timestamps': [], **{k_map: [] for k_map in config.CHANNEL_MAP.values()}})
                            session_state.telemetry_data[car_n_str][lap_n]['Timestamps'].extend(
                                telem_upd['Timestamps'])
                            for ch_key_map in config.CHANNEL_MAP.values():
                                session_state.telemetry_data[car_n_str][lap_n][ch_key_map].extend(
                                    telem_upd[ch_key_map])
                except Exception as proc_ex:
                    logger.error(
                        f"Session {sess_id_log}: ERROR processing stream '{stream_name}': {proc_ex}", exc_info=True)

                pending_fetch_info = getattr(
                    session_state, '_pending_background_fetch', None)

            # Start background thread OUTSIDE the main lock
            if pending_fetch_info:
                logger.info(
                    f"Session {sess_id_log}: Initiating background track data fetch for {pending_fetch_info['args_tuple'][0]}.")
                # The target function name is resolved to the actual function here
                target_func = getattr(
                    utils, pending_fetch_info["target_func_name"], None)
                if target_func:
                    # Add session_state to the arguments for the thread target
                    thread_args = pending_fetch_info["args_tuple"] + \
                        (session_state,)
                    fetch_thread = threading.Thread(target=target_func, args=thread_args, daemon=True,
                                                    name=f"TrackFetch_Sess_{sess_id_log}_{pending_fetch_info['args_tuple'][0]}")
                    fetch_thread.start()
                else:
                    logger.error(
                        f"Session {sess_id_log}: Could not find target function '{pending_fetch_info['target_func_name']}' in utils for background fetch.")

                with session_state.lock:
                    session_state._pending_background_fetch = None

            if hasattr(session_state.data_queue, 'task_done'):
                session_state.data_queue.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            logger.error(
                f"Session {sess_id_log}: Unhandled exception in data_processing_loop_session: {e}", exc_info=True)
            if item is not None and hasattr(session_state.data_queue, 'task_done'):
                try:
                    session_state.data_queue.task_done()
                except ValueError:
                    pass 
            time.sleep(0.1)

    logger.info(
        f"Data processing thread for session {sess_id_log} finished. Stop_event: {session_state.stop_event.is_set()}")
    with session_state.lock:
        session_state.data_processing_thread = None


print("DEBUG: data_processing module (multi-session structure) loaded")

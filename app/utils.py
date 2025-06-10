# utils.py
"""
Utility functions for data processing, file handling, and F1 session info,
adapted for a multi-session architecture where applicable.
"""
import logging
import json
import zlib
import base64
import datetime  # Use direct import
from datetime import timezone  # Use direct import
import re
from pathlib import Path
import requests
from typing import Dict, Optional, List, Any, Tuple  # For type hints

# Import for F1 Schedule / Data (conditionally)
try:
    import fastf1
    import pandas as pd
except ImportError:
    logging.warning(
        "FastF1/Pandas not found. Session info features may be limited. Install with: pip install fastf1 pandas")
    fastf1 = None  # type: ignore
    pd = None  # type: ignore

# Import config for constants and app_state for SessionState type hint
import config
import app_state  # Required for app_state.SessionState type hint

# Shapely and numpy are for track map processing
try:
    from shapely.geometry import LineString
    import numpy as np
except ImportError:
    logging.warning(
        "Shapely or NumPy not found. Track map features will be limited.")
    LineString = None  # type: ignore
    np = None  # type: ignore

import plotly.graph_objects as go

logger = logging.getLogger("F1App.Utils")

# --- Utility Functions (Many can remain as is if they are pure or use config) ---

def create_tyre_strategy_figure(driver_stint_data: dict, timing_state: dict):
    """
    Creates a Gantt chart figure visualizing the tyre strategy for all drivers
    using go.Bar for robustness.
    """
    if not driver_stint_data:
        return go.Figure(layout={
            'template': 'plotly_dark', 'xaxis': {'visible': False}, 'yaxis': {'visible': False},
            'annotations': [{'text': 'No stint data available yet.', 'showarrow': False, 'font': {'size': 12}}]
        })

    # --- START: Corrected driver sorting logic ---
    # Create a list of drivers from the timing_state, getting their sortable position
    drivers_with_pos = [
        {'id': num, 'pos': pos_sort_key(data)} 
        for num, data in timing_state.items()
    ]
    # Filter out any drivers that don't have a valid position and sort them
    sorted_drivers = sorted([d for d in drivers_with_pos if d['pos'] != float('inf')], key=lambda x: x['pos'])
    sorted_driver_nums = [d['id'] for d in sorted_drivers]
    # --- END: Corrected driver sorting logic ---

    chart_data = []
    max_lap = 0
    
    for driver_num in sorted_driver_nums:
        stints = driver_stint_data.get(str(driver_num))
        if not stints:
            continue

        driver_tla = timing_state.get(str(driver_num), {}).get('Tla', f'#{driver_num}')
        for stint in stints:
            start_lap = stint.get('start_lap')
            end_lap = stint.get('end_lap')

            if start_lap is None or end_lap is None:
                continue
            
            duration = (end_lap - start_lap) + 1
            if end_lap > max_lap:
                max_lap = end_lap
            
            chart_data.append(dict(
                Driver=driver_tla,
                Start=start_lap,
                Duration=duration,
                Compound=stint.get('compound', 'UNKNOWN').upper()
            ))

    if not chart_data:
        return go.Figure(layout={
            'template': 'plotly_dark', 'xaxis': {'visible': False}, 'yaxis': {'visible': False},
            'annotations': [{'text': 'Processing stint data...', 'showarrow': False, 'font': {'size': 12}}]
        })

    df = pd.DataFrame(chart_data)
    fig = go.Figure()

    # Add a separate Bar trace for each tyre compound
    for compound_name, color in config.TYRE_COMPOUND_COLORS.items():
        df_compound = df[df["Compound"] == compound_name]
        if not df_compound.empty:
            fig.add_trace(go.Bar(
                y=df_compound["Driver"],
                x=df_compound["Duration"],
                base=df_compound["Start"],
                orientation='h',
                name=compound_name,
                marker_color=color,
                text=compound_name[0] if len(compound_name) > 0 else '',
                textposition='inside',
                insidetextanchor='middle',
                width=0.6
            ))
    
    # Update the layout for a stacked Gantt chart appearance
    fig.update_layout(
        template='plotly_dark',
        xaxis_title="Lap Number",
        yaxis_title=None,
        barmode='stack',
        yaxis_autorange="reversed",
        xaxis=dict(range=[0, max_lap + 2]),
        margin=dict(l=40, r=20, t=20, b=30),
        legend=dict(
            traceorder="normal",
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    return fig

def convert_kph_to_mph(kph_values: list[float]) -> list[float]:
    """Converts a list of speed values from KPH to MPH."""
    if not kph_values:
        return []
    try:
        # Use a list comprehension for efficient conversion
        return [kph * config.KPH_TO_MPH_FACTOR for kph in kph_values]
    except (TypeError, ValueError):
        # Handle cases where the list might contain non-numeric data gracefully
        return []


def determine_session_type_from_name(session_name_str: str) -> str:
    s_name_lower = str(session_name_str).lower()
    if "practice" in s_name_lower:
        return config.SESSION_TYPE_PRACTICE
    if "qualifying" in s_name_lower:
        return config.SESSION_TYPE_QUALI
    if "sprint" in s_name_lower and "qualifying" not in s_name_lower:
        return config.SESSION_TYPE_SPRINT
    if "race" in s_name_lower and "pre-race" not in s_name_lower:
        return config.SESSION_TYPE_RACE
    return "Unknown"


def prepare_position_data_updates(actual_data_payload: Dict[str, Any],
                                  current_drivers_timing_state_snapshot: Dict[str,
                                                                              Dict[str, Any]]
                                  ) -> Dict[str, Dict[str, Any]]:
    """
    Parses Position data payload and prepares updates for PositionData and PreviousPositionData.
    Relies on current_drivers_timing_state_snapshot to know which drivers to process and their current PositionData.
    """
    logger_prep = logging.getLogger("F1App.Utils.PrepPosition")
    position_updates: Dict[str, Dict[str, Any]] = {}

    if not isinstance(actual_data_payload, dict) or 'Position' not in actual_data_payload:
        logger_prep.warning(
            f"prepare_position_data_updates: Unexpected format: {type(actual_data_payload)}")
        return position_updates

    position_entries_list = actual_data_payload.get('Position', [])
    if not isinstance(position_entries_list, list):
        logger_prep.warning(
            "prepare_position_data_updates: 'Position' data is not a list.")
        return position_updates

    for entry_group in position_entries_list:
        if not isinstance(entry_group, dict):
            continue
        timestamp_str = entry_group.get('Timestamp')
        if not timestamp_str:
            continue

        entries_dict_payload = entry_group.get('Entries', {})
        if not isinstance(entries_dict_payload, dict):
            continue

        for car_num_str, new_pos_info_payload in entries_dict_payload.items():
            if car_num_str not in current_drivers_timing_state_snapshot:
                continue
            if isinstance(new_pos_info_payload, dict):
                current_driver_timing_state = current_drivers_timing_state_snapshot[car_num_str]
                current_pos_data_for_driver = current_driver_timing_state.get(
                    'PositionData', {})

                new_position_data_for_state = {
                    'X': new_pos_info_payload.get('X'), 'Y': new_pos_info_payload.get('Y'),
                    'Status': new_pos_info_payload.get('Status'), 'Timestamp': timestamp_str
                }
                position_updates[car_num_str] = {
                    # Old current becomes new previous
                    'PreviousPositionData': current_pos_data_for_driver.copy(),
                    'PositionData': new_position_data_for_state
                }
    return position_updates


def prepare_car_data_updates(actual_data_payload: Dict[str, Any],
                             timing_state_snapshot_for_laps: Dict[str,
                                                                  Dict[str, Any]]
                             ) -> Tuple[Dict[str, Any], Dict[Tuple[str, int], Any]]:
    """
    Parses CarData payload and prepares updates for car data and telemetry.
    Relies on timing_state_snapshot_for_laps for NumberOfLaps.
    """
    logger_prep = logging.getLogger("F1App.Utils.PrepCarData")
    car_specific_updates: Dict[str, Any] = {}
    telemetry_specific_updates: Dict[Tuple[str, int], Any] = {}

    if not isinstance(actual_data_payload, dict) or 'Entries' not in actual_data_payload:
        logger_prep.warning(
            f"prepare_car_data_updates: Unexpected format: {type(actual_data_payload)}")
        return car_specific_updates, telemetry_specific_updates

    entries = actual_data_payload.get('Entries', [])
    if not isinstance(entries, list):
        logger_prep.warning(
            "prepare_car_data_updates: 'Entries' is not a list.")
        return car_specific_updates, telemetry_specific_updates

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        utc_time = entry.get('Utc')
        cars_data_from_payload = entry.get('Cars', {})
        if not isinstance(cars_data_from_payload, dict):
            continue

        for car_number, car_details_payload in cars_data_from_payload.items():
            car_num_str = str(car_number)
            driver_timing_info = timing_state_snapshot_for_laps.get(
                car_num_str, {})
            if not driver_timing_info:
                continue
            if not isinstance(car_details_payload, dict):
                continue
            channels_payload = car_details_payload.get('Channels', {})
            if not isinstance(channels_payload, dict):
                continue

            current_car_data_update: Dict[str, Any] = {}
            for channel_num_str_cfg, data_key_cfg in config.CHANNEL_MAP.items():
                if channel_num_str_cfg in channels_payload:
                    current_car_data_update[data_key_cfg] = channels_payload[channel_num_str_cfg]
            current_car_data_update['Utc'] = utc_time

            if car_num_str not in car_specific_updates:
                car_specific_updates[car_num_str] = {}
            car_specific_updates[car_num_str]['CarData'] = current_car_data_update

            completed_laps = driver_timing_info.get('NumberOfLaps', -1)
            current_lap_num = -1
            try:
                current_lap_num = int(completed_laps) + 1
                if current_lap_num <= 0:
                    current_lap_num = 1  # Lap numbers are 1-indexed
            except (ValueError, TypeError):
                continue

            telemetry_key = (car_num_str, current_lap_num)
            if telemetry_key not in telemetry_specific_updates:
                telemetry_specific_updates[telemetry_key] = {'Timestamps': [
                ], **{key: [] for key in config.CHANNEL_MAP.values()}}  # type: ignore

            lap_telemetry_update_ref = telemetry_specific_updates[telemetry_key]
            lap_telemetry_update_ref['Timestamps'].append(utc_time)
            for channel_num_str_cfg, data_key_cfg in config.CHANNEL_MAP.items():
                value = channels_payload.get(channel_num_str_cfg)
                if data_key_cfg in ['RPM', 'Speed', 'Gear', 'Throttle', 'Brake', 'DRS']:
                    try:
                        value = int(value) if value is not None else None
                    except:
                        value = None
                lap_telemetry_update_ref[data_key_cfg].append(
                    value)  # type: ignore

    return car_specific_updates, telemetry_specific_updates


def prepare_session_info_data(raw_session_info_data: Dict[str, Any],
                              current_session_type_lower: str,
                              current_session_key_from_state: Optional[str],
                              current_cached_track_key_from_state: Optional[str]
                              ) -> Tuple[Dict[str, Any], Dict[str, bool], Optional[float], Optional[Dict[str, Any]]]:
    """
    Parses raw SessionInfo data, determines changes, and prepares data for session_state update.
    The 'fetch_thread_init_info' will contain 'target_func_name' and 'args_tuple'.
    The calling function (in data_processing) will add 'session_state' to args_tuple.
    """
    logger_util = logging.getLogger("F1App.Utils.SessionInfoParser")
    parsed_details: Dict[str, Any] = {}
    reset_flags: Dict[str, bool] = {
        "reset_q_and_practice": False, "clear_track_cache": False}
    practice_duration_update_val: Optional[float] = None
    fetch_thread_init_info: Optional[Dict[str, Any]] = None

    if not isinstance(raw_session_info_data, dict):
        logger_util.warning(
            f"prepare_session_info_data: raw_data is not a dict: {type(raw_session_info_data)}")
        return parsed_details, reset_flags, practice_duration_update_val, fetch_thread_init_info

    meeting_info = raw_session_info_data.get('Meeting', {})
    circuit_info = meeting_info.get('Circuit', {})

    parsed_details['Type'] = raw_session_info_data.get('Type')
    parsed_details['Name'] = raw_session_info_data.get('Name')
    parsed_details['Meeting'] = meeting_info if isinstance(
        meeting_info, dict) else {}
    parsed_details['Circuit'] = circuit_info if isinstance(
        circuit_info, dict) else {}
    parsed_details['Country'] = raw_session_info_data.get('Country', {}) if isinstance(
        raw_session_info_data.get('Country'), dict) else {}  # Ensure country is dict
    parsed_details['StartDate'] = raw_session_info_data.get('StartDate')
    parsed_details['EndDate'] = raw_session_info_data.get('EndDate')
    parsed_details['Path'] = raw_session_info_data.get('Path')

    year_str: Optional[str] = None
    start_date_str_val = raw_session_info_data.get('StartDate')
    if start_date_str_val and isinstance(start_date_str_val, str) and len(start_date_str_val) >= 4:
        try:
            year_str = start_date_str_val[:4]
            int(year_str)
        except ValueError:
            year_str = None
    parsed_details['Year'] = year_str

    circuit_key_from_data = parsed_details['Circuit'].get('Key')
    parsed_details['CircuitKey'] = circuit_key_from_data

    new_session_key_val: Optional[str] = None
    if year_str and circuit_key_from_data is not None and str(circuit_key_from_data).strip():
        new_session_key_val = f"{year_str}_{circuit_key_from_data}"
    parsed_details['SessionKey'] = new_session_key_val

    scheduled_duration_s: Optional[float] = None
    s_date_str = parsed_details.get('StartDate')
    e_date_str = parsed_details.get('EndDate')
    if s_date_str and e_date_str:
        start_dt_obj = parse_iso_timestamp_safe(s_date_str)
        end_dt_obj = parse_iso_timestamp_safe(e_date_str)
        if start_dt_obj and end_dt_obj and end_dt_obj > start_dt_obj:
            scheduled_duration_s = (end_dt_obj - start_dt_obj).total_seconds()
    parsed_details['ScheduledDurationSeconds'] = scheduled_duration_s

    new_s_type_lower = str(parsed_details.get("Type", "")).lower()
    if new_s_type_lower != current_session_type_lower:
        reset_flags["reset_q_and_practice"] = True

    if new_session_key_val:
        if current_session_key_from_state != new_session_key_val or current_cached_track_key_from_state != new_session_key_val:
            fetch_thread_init_info = {
                "target_func_name": "_background_track_fetch_and_update_session",
                # session_state to be added by caller
                "args_tuple": (new_session_key_val, year_str, str(circuit_key_from_data))
            }
    else:
        reset_flags["clear_track_cache"] = True

    if new_s_type_lower.startswith("practice") and scheduled_duration_s is not None:
        practice_duration_update_val = scheduled_duration_s

    return parsed_details, reset_flags, practice_duration_update_val, fetch_thread_init_info


def _fetch_track_data_for_cache(session_key: str, year: Optional[str], circuit_key: Optional[str]) -> Optional[Dict[str, Any]]:
    """ Fetches track data from MultiViewer API. Returns data dict or None. """
    # (This function's internal logic remains the same as your version, it doesn't use app_state)
    # Ensure it uses a general logger like 'logger' or 'logging.getLogger("F1App.Utils.TrackFetch")'
    # Removed 'main_logger' to consolidate.
    fetch_logger = logging.getLogger("F1App.Utils.TrackFetch")

    if not year or not circuit_key:
        fetch_logger.error(
            f"Fetch Helper: Invalid year or circuit key ({year}, {circuit_key}) for {session_key}")
        return None

    api_url = config.MULTIVIEWER_CIRCUIT_API_URL_TEMPLATE.format(
        circuit_key=str(circuit_key), year=str(year))
    fetch_logger.info(f"Fetch Helper: API fetch initiated for: {api_url}")

    map_api_data: Optional[Dict[str, Any]] = None
    track_x_coords, track_y_coords, track_linestring_obj, x_range, y_range = [
        None]*5

    try:
        response = requests.get(api_url, headers={'User-Agent': config.MULTIVIEWER_API_USER_AGENT},
                                timeout=config.REQUESTS_TIMEOUT_SECONDS, verify=False)  # Consider verify=True for production
        response.raise_for_status()
        map_api_data = response.json()

        temp_x_api = [float(p) for p in map_api_data.get('x', [])]
        temp_y_api = [float(p) for p in map_api_data.get('y', [])]

        if temp_x_api and temp_y_api and len(temp_x_api) == len(temp_y_api) and len(temp_x_api) > 1 and LineString and np:
            _api_ls = LineString(zip(temp_x_api, temp_y_api))
            if _api_ls.length > 0:
                track_x_coords, track_y_coords, track_linestring_obj = temp_x_api, temp_y_api, _api_ls
                x_min, x_max = np.min(track_x_coords), np.max(track_x_coords)
                y_min, y_max = np.min(track_y_coords), np.max(track_y_coords)
                pad_x = (x_max - x_min) * 0.05
                pad_y = (y_max - y_min) * 0.05
                x_range = [x_min - pad_x, x_max + pad_x]
                y_range = [y_min - pad_y, y_max + pad_y]
                fetch_logger.info(
                    f"Fetch Helper: API SUCCESS for {session_key}. Main track line loaded.")
            else:
                fetch_logger.warning(
                    f"Fetch Helper: API {session_key} provided zero-length main track line.")
        else:
            fetch_logger.warning(
                f"Fetch Helper: API {session_key} no valid x/y for main track line or LineString/np missing.")
    except Exception as e_api:
        fetch_logger.error(
            f"Fetch Helper: API FAILED for {session_key} (main track data): {e_api}", exc_info=True)
        return None

    corners_data_processed, marshal_lights_data_processed, marshal_sector_points_raw, marshal_sector_segments_calculated = [
        None]*4
    if map_api_data:
        # Process Corners
        if 'corners' in map_api_data and isinstance(map_api_data['corners'], list):
            corners_data_processed = [{'number': c['number'], 'x': c['trackPosition'].get('x'), 'y': c['trackPosition'].get('y')}
                                      for c in map_api_data['corners'] if isinstance(c, dict) and 'number' in c and isinstance(c.get('trackPosition'), dict)]
        # Process Marshal Lights
        if 'marshalLights' in map_api_data and isinstance(map_api_data['marshalLights'], list):
            marshal_lights_data_processed = [{'number': l['number'], 'x': l['trackPosition'].get('x'), 'y': l['trackPosition'].get('y')}
                                             for l in map_api_data['marshalLights'] if isinstance(l, dict) and 'number' in l and isinstance(l.get('trackPosition'), dict)]
        # Process Marshal Sectors
        if 'marshalSectors' in map_api_data and isinstance(map_api_data['marshalSectors'], list):
            marshal_sector_points_raw = map_api_data['marshalSectors']
            if track_x_coords and track_y_coords and np:  # Ensure numpy is available
                marshal_sector_segments_calculated = {}
                sorted_ms_api = sorted([ms for ms in marshal_sector_points_raw if isinstance(
                    ms, dict) and ms.get('number') is not None], key=lambda s: s['number'])
                sector_start_indices = {}
                for sector_info in sorted_ms_api:
                    s_num = sector_info.get('number')
                    s_pos_dict = sector_info.get('trackPosition')
                    if s_num is not None and isinstance(s_pos_dict, dict):
                        s_x, s_y = s_pos_dict.get('x'), s_pos_dict.get('y')
                        if s_x is not None and s_y is not None:
                            closest_idx = find_closest_point_index(
                                track_x_coords, track_y_coords, s_x, s_y)
                            if closest_idx is not None:
                                sector_start_indices[s_num] = closest_idx

                sorted_sector_indices_list = sorted(
                    sector_start_indices.items())
                if len(sorted_sector_indices_list) == 1:
                    s_num, s_idx = sorted_sector_indices_list[0]
                    marshal_sector_segments_calculated[s_num] = (
                        0, len(track_x_coords) - 1)
                elif len(sorted_sector_indices_list) > 1:
                    for i_ms in range(len(sorted_sector_indices_list)):
                        curr_s_num, curr_s_idx = sorted_sector_indices_list[i_ms]
                        end_idx = (max(curr_s_idx, sorted_sector_indices_list[i_ms+1][1] - 1)
                                   if i_ms + 1 < len(sorted_sector_indices_list)
                                   else len(track_x_coords) - 1)
                        if curr_s_idx <= end_idx:
                            marshal_sector_segments_calculated[curr_s_num] = (
                                curr_s_idx, end_idx)
                        else:
                            marshal_sector_segments_calculated[curr_s_num] = (
                                curr_s_idx, curr_s_idx)

    cache_update_data = {
        'session_key': session_key, 'x': track_x_coords, 'y': track_y_coords,
        'linestring': track_linestring_obj, 'range_x': x_range, 'range_y': y_range,
        'corners_data': corners_data_processed, 'marshal_lights_data': marshal_lights_data_processed,
        'marshal_sector_points': marshal_sector_points_raw,
        'marshal_sector_segments': marshal_sector_segments_calculated,
        'rotation': map_api_data.get('rotation') if map_api_data else None
    }
    fetch_logger.info(
        f"Fetch Helper: Returning data for {session_key}. Points: {len(track_x_coords or [])}")
    return cache_update_data


def _background_track_fetch_and_update_session(session_key: str, year: Optional[str], circuit_key: Optional[str],
                                               session_state: app_state.SessionState):  # Takes session_state
    """Runs fetch in background and updates the specific session's track_coordinates_cache."""
    sess_id_log = session_state.session_id[:8]
    bg_fetch_logger = logging.getLogger(
        f"F1App.Utils.BGTrackFetch.Sess_{sess_id_log}")
    bg_fetch_logger.info(
        f"Background track fetch started for session_key: {session_key}")

    fetched_data = _fetch_track_data_for_cache(session_key, year, circuit_key)
    if fetched_data:
        with session_state.lock:  # Use the session's lock
            current_session_key_in_s_state = session_state.session_details.get(
                'SessionKey')
            if current_session_key_in_s_state == session_key:
                bg_fetch_logger.info(
                    f"Updating session's track_coordinates_cache for {session_key}.")
                session_state.track_coordinates_cache = fetched_data  # Update session_state
            else:
                bg_fetch_logger.warning(
                    f"Session changed (now {current_session_key_in_s_state}) while fetching for {session_key}. Discarding."
                )
    else:
        bg_fetch_logger.error(
            f"Fetch helper failed for {session_key}. Session's track cache not updated.")
    bg_fetch_logger.info(
        f"Background track fetch finished for session_key: {session_key}.")


def sanitize_filename(name: Any) -> str:
    # (Your existing sanitize_filename - seems okay)
    if not name:
        return "Unknown"
    name_str = str(name).strip()
    name_str = re.sub(r'[\\/:*?"<>|\s\-\:\.,\(\)]+', '_',
                      name_str)  # Added colon, comma, parentheses
    # Remove any remaining non-alphanumeric (excluding underscore)
    name_str = re.sub(r'[^\w_]+', '', name_str)
    name_str = re.sub(r'_+', '_', name_str)  # Consolidate multiple underscores
    name_str = name_str.strip('_')
    return name_str if name_str else "InvalidName"


# Changed return type
def _decode_and_decompress(encoded_data: str) -> Optional[Dict[Any, Any]]:
    # (Your existing _decode_and_decompress - seems okay, added logger context)
    # Ensure it returns a Dict or None, not just any json.loads result
    if not encoded_data or not isinstance(encoded_data, str):
        return None
    try:
        # ... (your padding logic) ...
        missing_padding = len(encoded_data) % 4
        if missing_padding:
            encoded_data += '=' * (4 - missing_padding)
        decoded_data = base64.b64decode(encoded_data)
        decompressed_data = zlib.decompress(decoded_data, -zlib.MAX_WBITS)
        json_data = json.loads(decompressed_data.decode('utf-8'))
        # Ensure it's a dict or None
        return json_data if isinstance(json_data, dict) else None
    except json.JSONDecodeError as e:
        # logger.error(f"JSON decode error after decompression: {e}. Data sample: {str(decompressed_data)[:100]}...", exc_info=False) # decompressed_data might not be defined
        logger.error(
            f"JSON decode error after decompression: {e}. Base64 sample: {encoded_data[:50]}...", exc_info=False)
        return None
    except Exception as e:
        logger.error(
            f"Decode/Decompress error: {e}. Data: {str(encoded_data)[:50]}...", exc_info=False)
        return None


def parse_iso_timestamp_safe(timestamp_str: Optional[str], line_num_for_log: str = "?") -> Optional[datetime.datetime]:
    # (Your existing parse_iso_timestamp_safe - seems okay, ensure it returns Optional[datetime.datetime])
    if not timestamp_str or not isinstance(timestamp_str, str):
        return None
    cleaned_ts = timestamp_str.replace('Z', '+00:00')
    # Microsecond padding/truncating logic from your file
    # (Ensuring it pads to 6 digits for microseconds if present)
    timestamp_to_parse = cleaned_ts
    if '.' in cleaned_ts:
        parts = cleaned_ts.split('.', 1)
        integer_part = parts[0]
        fractional_part_full = parts[1]
        offset_part = ''
        # Extract offset if present after fractional part
        offset_match = re.search(r'([+\-]\d{2}:\d{2})$', fractional_part_full)
        if offset_match:
            offset_part = offset_match.group(1)
            fractional_part = fractional_part_full[:offset_match.start()]
        else:
            fractional_part = fractional_part_full

        if fractional_part:  # Only if fractional part exists
            fractional_part_padded = f"{fractional_part:<06s}"[
                :6]  # Pad with trailing zeros, then truncate to 6
            timestamp_to_parse = f"{integer_part}.{fractional_part_padded}{offset_part}"
        # If no fractional part, timestamp_to_parse remains integer_part + offset_part (which might be empty or +00:00)
        elif offset_part:  # Ensure offset is appended if there was no fractional part but an offset
            timestamp_to_parse = f"{integer_part}{offset_part}"
        # else timestamp_to_parse is just integer_part (if no Z and no explicit offset)

    try:
        parsed_dt = datetime.datetime.fromisoformat(timestamp_to_parse)
        if parsed_dt.tzinfo is None:
            # Assume UTC if naive
            return parsed_dt.replace(tzinfo=timezone.utc)
        # Convert to UTC if timezone-aware
        return parsed_dt.astimezone(timezone.utc)
    except ValueError:  # Try parsing without microseconds if above failed
        try:
            base_ts_no_ms = timestamp_to_parse.split('.')[0]
            # Re-append offset if it was part of the original string and not part of base_ts_no_ms
            # This part needs careful handling of the original offset if present
            original_offset_match = re.search(
                r'([+\-]\d{2}:\d{2})$', timestamp_str)  # Check original for offset
            final_ts_no_ms = base_ts_no_ms
            if original_offset_match:
                final_ts_no_ms += original_offset_match.group(1)
            elif timestamp_str.endswith('Z'):
                final_ts_no_ms += "+00:00"

            parsed_dt_no_ms = datetime.datetime.fromisoformat(final_ts_no_ms)
            if parsed_dt_no_ms.tzinfo is None:
                return parsed_dt_no_ms.replace(tzinfo=timezone.utc)
            return parsed_dt_no_ms.astimezone(timezone.utc)
        except ValueError:
            logger.warning(
                f"Timestamp format error (L{line_num_for_log}): Original='{timestamp_str}', Tried='{timestamp_to_parse}'. Could not parse.")
            return None
    except Exception as e:
        logger.error(
            f"Unexpected error parsing ts (L{line_num_for_log}): Orig='{timestamp_str}', Err: {e}", exc_info=True)
        return None


# format_seconds_to_time_str, parse_session_time_to_seconds, create_empty_figure_with_message
# parse_feed_time_to_seconds, parse_lap_time_to_seconds, convert_utc_str_to_epoch_ms
# find_closest_point_index, get_nested_state, pos_sort_key
# These functions from your utils.py are generally pure or rely on config/inputs only,
# so they don't need changes for session awareness. I'll include them as they were.

def format_seconds_to_time_str(total_seconds: float) -> str:
    if total_seconds is None or total_seconds < 0:
        total_seconds = 0
    hours, remainder = divmod(int(total_seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


# Allow float for precision
def parse_session_time_to_seconds(time_str: Optional[str]) -> Optional[float]:
    if not time_str or time_str == "-":
        return None  # Return None for invalid/missing
    try:
        parts = time_str.split(':')
        if len(parts) == 3:
            return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        if len(parts) == 2:
            return float(parts[0]) * 60 + float(parts[1])
        if len(parts) == 1:
            return float(parts[0])
    except ValueError:
        return None
    return None


def create_empty_figure_with_message(height: int, uirevision: str, message: str, margins: Dict[str, int]) -> go.Figure:
    return go.Figure(layout={
        'template': 'plotly_dark', 'height': height, 'margin': margins, 'uirevision': uirevision,
        'xaxis': {'visible': False, 'range': [0, 1]}, 'yaxis': {'visible': False, 'range': [0, 1]},
        'annotations': [{'text': message, 'xref': 'paper', 'yref': 'paper', 'showarrow': False, 'font': {'size': 12}}]
    })


def parse_feed_time_to_seconds(time_str: Optional[str]) -> Optional[float]:
    if not time_str or not isinstance(time_str, str):
        return None
    try:
        parts = time_str.split(':')
        if len(parts) == 3:  # HH:MM:SS.ms
            s_ms_parts = parts[2].split('.')
            return int(parts[0])*3600 + int(parts[1])*60 + int(s_ms_parts[0]) + (int(s_ms_parts[1])/1000.0 if len(s_ms_parts) > 1 else 0)
        elif len(parts) == 2:  # MM:SS.ms
            s_ms_parts = parts[1].split('.')
            return int(parts[0])*60 + int(s_ms_parts[0]) + (int(s_ms_parts[1])/1000.0 if len(s_ms_parts) > 1 else 0)
        elif '.' in time_str:  # SS.ms
            s_ms_parts = time_str.split('.')
            return int(s_ms_parts[0]) + (int(s_ms_parts[1])/1000.0 if len(s_ms_parts) > 1 else 0)
        return float(time_str)  # Assume seconds if single number
    except:
        return None


def parse_lap_time_to_seconds(time_str: Optional[str]) -> Optional[float]:
    if not time_str or not isinstance(time_str, str) or time_str == '-':
        return None
    # (Using your regex-based parsing logic from the provided file)
    match_min_sec_ms = re.match(r'(\d+):(\d{2})\.(\d{3})', time_str)
    if match_min_sec_ms:
        return int(match_min_sec_ms.group(1))*60 + int(match_min_sec_ms.group(2)) + int(match_min_sec_ms.group(3))/1000.0
    match_sec_ms = re.match(r'(\d+)\.(\d{3})', time_str)
    if match_sec_ms:
        # Corrected group index
        return int(match_sec_ms.group(1)) + int(match_sec_ms.group(2))/1000.0
    match_s_only = re.match(r'(\d+(?:\.\d+)?)', time_str)
    if match_s_only:
        try:
            return float(match_s_only.group(1))
        except ValueError:
            return None
    return None


def convert_utc_str_to_epoch_ms(timestamp_str: Optional[str]) -> Optional[int]:
    if not timestamp_str:
        return None
    dt_object = parse_iso_timestamp_safe(timestamp_str)
    if dt_object:
        return int(dt_object.timestamp() * 1000)
    return None


def find_closest_point_index(track_x_coords: List[float], track_y_coords: List[float], point_x: float, point_y: float) -> Optional[int]:
    if not track_x_coords or not track_y_coords or np is None:
        return None
    track_points = np.array(list(zip(track_x_coords, track_y_coords)))
    target_point = np.array([point_x, point_y])
    distances_squared = np.sum((track_points - target_point)**2, axis=1)
    return np.argmin(distances_squared)  # type: ignore


def get_nested_state(d: Dict[Any, Any], *keys: Any, default: Any = None) -> Any:
    # (Your existing get_nested_state - seems okay)
    val = d
    for key in keys:
        if isinstance(val, dict):
            val = val.get(key)
        elif isinstance(val, list):
            try:
                key_int = int(key)
                if 0 <= key_int < len(val):
                    val = val[key_int]
                else:
                    return default
            except:
                return default
        else:
            return default
        if val is None:
            return default
    return val


def pos_sort_key(item: Dict[str, Any]) -> int:
    # (Your existing pos_sort_key - seems okay)
    pos_str = item.get('Pos', '999')
    if isinstance(pos_str, (int, float)):
        return int(pos_str)  # type: ignore
    if isinstance(pos_str, str) and pos_str.isdigit():
        try:
            return int(pos_str)
        except ValueError:
            return 999
    return 999

# get_current_or_next_session_info: Removed FastF1 cache enabling, assuming global setup.


def get_current_or_next_session_info() -> Tuple[Optional[str], Optional[str]]:
    if fastf1 is None or pd is None:
        logger.error(
            "FastF1 or Pandas not available for get_current_or_next_session_info.")
        return None, None
    try:
        year = datetime.datetime.now().year
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        now = pd.Timestamp.now(tz='UTC')
        # ... (rest of your logic as provided, it's complex and FastF1 specific)
        last_past_session = {'date': pd.Timestamp.min.tz_localize(
            'UTC'), 'event_name': None, 'session_name': None}
        next_future_session = {'date': pd.Timestamp.max.tz_localize(
            'UTC'), 'event_name': None, 'session_name': None}
        for index, event in schedule.iterrows():
            for i in range(1, 6):
                session_date_col = f'Session{i}DateUtc'
                session_name_col = f'Session{i}'
                if session_date_col in event and pd.notna(event[session_date_col]):
                    session_date = event[session_date_col]
                    if not isinstance(session_date, pd.Timestamp):
                        try:
                            session_date = pd.Timestamp(session_date)
                        except:
                            continue
                    if session_date.tzinfo is None:
                        session_date = session_date.tz_localize('UTC')
                    else:
                        session_date = session_date.tz_convert('UTC')
                    if session_date > now:
                        if session_date < next_future_session['date']:
                            next_future_session.update({'date': session_date, 'event_name': event.get(
                                'EventName'), 'session_name': event.get(session_name_col)})
                    elif session_date <= now:
                        if session_date > last_past_session['date']:
                            last_past_session.update({'date': session_date, 'event_name': event.get(
                                'EventName'), 'session_name': event.get(session_name_col)})
        ongoing_window = pd.Timedelta(hours=getattr(
            config, 'FASTF1_ONGOING_SESSION_WINDOW_HOURS', 3))
        if last_past_session.get('event_name') and (now - last_past_session.get('date', now)) <= ongoing_window:
            return last_past_session['event_name'], last_past_session['session_name']
        elif next_future_session.get('event_name'):
            return next_future_session['event_name'], next_future_session['session_name']
        return None, None
    except Exception as e:
        logger.error(
            f"FastF1 Error in get_current_or_next_session_info: {e}", exc_info=True)
        return None, None


def generate_driver_options(session_timing_state: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Generates list of options for driver dropdowns from a session's timing_state.
    """
    options = []
    # Using the module-level logger defined in utils.py
    # logger.debug(f"Generating driver options from session_timing_state keys: {list(session_timing_state.keys())}")

    if not session_timing_state or not isinstance(session_timing_state, dict):
        logger.warning(
            "utils.generate_driver_options received empty or invalid session_timing_state.")
        # Ensure this constant is defined in config.py
        return config.DROPDOWN_NO_DRIVERS_OPTIONS

    driver_list_for_sorting = []
    for driver_num_key, driver_data in session_timing_state.items():  # driver_num_key is the key from timing_state
        if isinstance(driver_data, dict):
            # Use 'RacingNumber' for display, fallback to the dict key 'driver_num_key'
            racing_number_label = str(
                driver_data.get('RacingNumber', driver_num_key))

            # The 'value' for the dropdown should be the key used to retrieve the driver's state
            driver_list_for_sorting.append({
                'value': driver_num_key,
                'number_for_sort': racing_number_label,  # Use for sorting
                'tla': driver_data.get('Tla', '???'),
                'name': driver_data.get('FullName', 'Unknown Driver')
            })
        else:
            logger.warning(
                f"utils.generate_driver_options: Expected dict for driver {driver_num_key}, got {type(driver_data)}")

    # Sort by racing number (as integer if possible for correct numeric sorting)

    def sort_key_for_options(item: Dict[str, Any]) -> int:
        try:
            # Attempt to convert 'number_for_sort' (which is RacingNumber) to int for sorting
            return int(item.get('number_for_sort', 999))
        except (ValueError, TypeError):
            # Fallback for non-integer racing numbers (e.g., if 'UNK' or similar)
            return 999

    sorted_drivers = sorted(driver_list_for_sorting, key=sort_key_for_options)

    for driver_item in sorted_drivers:
        # Label uses TLA, RacingNumber (number_for_sort), and FullName
        label = f"{driver_item['tla']} (#{driver_item['number_for_sort']}) - {driver_item['name']}"
        options.append({'label': label, 'value': driver_item['value']})

    if not options:
        # Ensure this constant is in config.py
        return config.DROPDOWN_NO_DRIVERS_PROCESSED_OPTIONS

    return options

print("DEBUG: utils module (multi-session adaptations) loaded")

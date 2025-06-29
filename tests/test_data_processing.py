import unittest
from unittest.mock import MagicMock, patch
import datetime
import time
import queue
import threading

from app import data_processing, app_state, config, utils

class TestDataProcessing(unittest.TestCase):

    def setUp(self):
        # Create a real queue and event for the data processing loop test
        self.data_queue = queue.Queue()
        self.stop_event = threading.Event()

        # Mock session_state, but use real queue and event
        self.mock_session_state = app_state.SessionState(session_id="test_session_123")
        self.mock_session_state.data_queue = self.data_queue  # Use real queue
        self.mock_session_state.stop_event = self.stop_event  # Use real event
        self.mock_session_state.lock = threading.Lock() # Use a real lock for thread safety
        # Manually set other attributes that are not initialized by default in SessionState
        self.mock_session_state.app_status = {}
        self.mock_session_state.team_radio_messages = MagicMock()
        self.mock_session_state.extrapolated_clock_info = {}
        self.mock_session_state.session_details = {}
        self.mock_session_state.qualifying_segment_state = {"current_segment": "Q1", "just_resumed_flag": True}
        self.mock_session_state.timing_state = {}
        self.mock_session_state.data_processing_thread = None
        self.mock_session_state.data_store = {}
        self.mock_session_state._pending_background_fetch = None
        self.mock_session_state.practice_session_scheduled_duration_seconds = None
        self.mock_session_state.session_start_feed_timestamp_utc_dt = None
        self.mock_session_state.current_segment_scheduled_duration_seconds = None
        self.mock_session_state.replay_speed = 1.0 # Add replay_speed attribute
        self.mock_session_state.driver_stint_data = {}
        self.mock_session_state.live_data_file = None
        self.mock_session_state.is_saving_active = False
        self.mock_session_state.current_recording_filename = None
        self.mock_session_state.all_pit_stop_durations = {}
        self.mock_session_state.lap_time_history = {}

    def tearDown(self):
        # Ensure the processing thread is stopped and joined
        self.stop_event.set()
        if self.mock_session_state.data_processing_thread and self.mock_session_state.data_processing_thread.is_alive():
            self.mock_session_state.data_processing_thread.join(timeout=2)
            if self.mock_session_state.data_processing_thread.is_alive():
                print(f"Warning: Data processing thread for {self.mock_session_state.session_id} did not terminate.")
        # Clear the queue to prevent items from leaking to other tests if not fully processed
        while not self.data_queue.empty():
            try:
                self.data_queue.get_nowait()
            except queue.Empty:
                pass

    def test_process_heartbeat(self):
        timestamp = "2025-06-28T10:00:00.000Z"
        data_processing._process_heartbeat(self.mock_session_state, {}, timestamp)
        self.assertEqual(self.mock_session_state.app_status["last_heartbeat"], timestamp)

    def test_process_team_radio(self):
        radio_data = {
            "Captures": {
                "1": {"Utc": "2025-06-28T10:01:00.000Z", "RacingNumber": "16", "Path": "audio/1.mp3"},
                "2": {"Utc": "2025-06-28T10:02:00.000Z", "RacingNumber": "44", "Path": "audio/2.mp3"}
            }
        }
        self.mock_session_state.timing_state = {
            "16": {"Tla": "LEC"},
            "44": {"Tla": "HAM"}
        }
        data_processing._process_team_radio(self.mock_session_state, radio_data)
        self.assertEqual(self.mock_session_state.team_radio_messages.appendleft.call_count, 2)
        self.mock_session_state.team_radio_messages.appendleft.assert_any_call({
            'Utc': "2025-06-28T10:01:00.000Z", 'RacingNumber': "16", 'Path': "audio/1.mp3", 'DriverTla': "LEC"
        })
        self.mock_session_state.team_radio_messages.appendleft.assert_any_call({
            'Utc': "2025-06-28T10:02:00.000Z", 'RacingNumber': "44", 'Path': "audio/2.mp3", 'DriverTla': "HAM"
        })

    def test_process_race_control(self):
        self.mock_session_state.race_control_log = MagicMock()
        self.mock_session_state.active_yellow_sectors = set()

        rc_data_yellow = {"Messages": [{
            "Utc": "2025-06-28T10:03:00.000Z", "Lap": 5, "Message": "Yellow Flag",
            "Category": "Flag", "Flag": "YELLOW", "Scope": "Sector", "Sector": 1
        }]}
        data_processing._process_race_control(self.mock_session_state, rc_data_yellow)
        self.mock_session_state.race_control_log.appendleft.assert_called_once()
        self.assertIn(1, self.mock_session_state.active_yellow_sectors)

        rc_data_clear = {"Messages": [{
            "Utc": "2025-06-28T10:04:00.000Z", "Lap": 6, "Message": "Track Clear",
            "Category": "Flag", "Flag": "CLEAR", "Scope": "Sector", "Sector": 1
        }]}
        data_processing._process_race_control(self.mock_session_state, rc_data_clear)
        self.assertNotIn(1, self.mock_session_state.active_yellow_sectors)

        rc_data_green = {"Messages": [{
            "Utc": "2025-06-28T10:05:00.000Z", "Lap": 7, "Message": "Green Flag",
            "Category": "Flag", "Flag": "GREEN"
        }]}
        self.mock_session_state.active_yellow_sectors.add(2) # Add another yellow to clear
        data_processing._process_race_control(self.mock_session_state, rc_data_green)
        self.assertTrue(len(self.mock_session_state.active_yellow_sectors) == 0)

    def test_process_weather_data(self):
        weather_data = {"AirTemp": 25.0, "TrackTemp": 30.0, "Humidity": 60.0}
        data_processing._process_weather_data(self.mock_session_state, weather_data)
        self.assertEqual(self.mock_session_state.data_store['WeatherData'], weather_data)

    def test_process_driver_list(self):
        driver_list_data = {
            "16": {"Tla": "LEC", "FullName": "Charles Leclerc", "RacingNumber": "16"},
            "44": {"Tla": "HAM", "FullName": "Lewis Hamilton", "RacingNumber": "44"}
        }
        # Simulate existing driver
        self.mock_session_state.timing_state = {
            "16": {"Tla": "LEC", "FullName": "Charles Leclerc", "RacingNumber": "16", "ExistingKey": "ExistingValue"}
        }
        self.mock_session_state.lap_time_history = {"16": ["some_history"]}
        self.mock_session_state.telemetry_data = {"16": {"some_telemetry": "data"}}
        self.mock_session_state.driver_stint_data = {"16": ["some_stint_data"]}

        data_processing._process_driver_list(self.mock_session_state, driver_list_data)

        # Assertions for existing driver (16)
        self.assertIn("16", self.mock_session_state.timing_state)
        self.assertEqual(self.mock_session_state.timing_state["16"]["Tla"], "LEC")
        self.assertEqual(self.mock_session_state.timing_state["16"]["FullName"], "Charles Leclerc")
        self.assertEqual(self.mock_session_state.timing_state["16"]["RacingNumber"], "16")
        self.assertEqual(self.mock_session_state.timing_state["16"]["ExistingKey"], "ExistingValue") # Ensure existing data is preserved
        self.assertEqual(self.mock_session_state.lap_time_history["16"], ["some_history"]) # Should not be re-initialized

        # Assertions for new driver (44)
        self.assertIn("44", self.mock_session_state.timing_state)
        self.assertEqual(self.mock_session_state.timing_state["44"]["Tla"], "HAM")
        self.assertEqual(self.mock_session_state.timing_state["44"]["FullName"], "Lewis Hamilton")
        self.assertEqual(self.mock_session_state.timing_state["44"]["RacingNumber"], "44")
        self.assertIsInstance(self.mock_session_state.lap_time_history["44"], list)
        self.assertIsInstance(self.mock_session_state.telemetry_data["44"], dict)
        self.assertIsInstance(self.mock_session_state.driver_stint_data["44"], list)

        # Test with empty driver list
        initial_timing_state = self.mock_session_state.timing_state.copy()
        data_processing._process_driver_list(self.mock_session_state, {})
        self.assertEqual(self.mock_session_state.timing_state, initial_timing_state) # Should remain unchanged

        # Test with invalid driver data
        invalid_driver_data = {"1": "invalid"}
        data_processing._process_driver_list(self.mock_session_state, invalid_driver_data)
        # Should not raise error, and timing_state should not be affected for invalid entry
        self.assertEqual(self.mock_session_state.timing_state, initial_timing_state)

    def test_update_driver_stint_data_new_stint_new_tyre(self):
        driver_rno_str = "16"
        stints_payload = {
            "1": {"Compound": "SOFT", "New": "true", "StartLaps": 0, "TotalLaps": 0}
        }
        driver_timing_state_info = {"NumberOfLaps": 0}
        self.mock_session_state.driver_stint_data = {"16": []}

        data_processing._update_driver_stint_data(self.mock_session_state, driver_rno_str, stints_payload, driver_timing_state_info)

        expected_stint = {
            'stint_number': 1, 'feed_stint_key': '1', 'start_laps_from_feed_val': 0,
            'start_lap': 1, 'compound': 'SOFT', 'is_new_tyre': True,
            'tyre_age_at_stint_start': 0, 'end_lap': 0,
            'total_laps_on_tyre_in_stint': 1, 'tyre_total_laps_at_stint_end': 0,
            'tyres_not_changed': False
        }
        self.assertEqual(self.mock_session_state.driver_stint_data["16"][0]['compound'], expected_stint['compound'])
        self.assertEqual(self.mock_session_state.driver_stint_data["16"][0]['is_new_tyre'], expected_stint['is_new_tyre'])
        self.assertEqual(self.mock_session_state.driver_stint_data["16"][0]['start_lap'], expected_stint['start_lap'])

    def test_update_driver_stint_data_existing_stint_same_tyre(self):
        driver_rno_str = "16"
        stints_payload = {
            "1": {"Compound": "SOFT", "New": "false", "StartLaps": 0, "TotalLaps": 5}
        }
        driver_timing_state_info = {"NumberOfLaps": 4}
        self.mock_session_state.driver_stint_data = {"16": [{
            'stint_number': 1, 'feed_stint_key': '1', 'start_laps_from_feed_val': 0,
            'start_lap': 1, 'compound': 'SOFT', 'is_new_tyre': False,
            'tyre_age_at_stint_start': 0, 'end_lap': 0,
            'total_laps_on_tyre_in_stint': 1, 'tyre_total_laps_at_stint_end': 0,
            'tyres_not_changed': False
        }]}

        data_processing._update_driver_stint_data(self.mock_session_state, driver_rno_str, stints_payload, driver_timing_state_info)

        expected_stint = {
            'stint_number': 1, 'feed_stint_key': '1', 'start_laps_from_feed_val': 0,
            'start_lap': 1, 'compound': 'SOFT', 'is_new_tyre': False,
            'tyre_age_at_stint_start': 0, 'end_lap': 4,
            'total_laps_on_tyre_in_stint': 4, 'tyre_total_laps_at_stint_end': 5,
            'tyres_not_changed': False
        }
        self.assertEqual(self.mock_session_state.driver_stint_data["16"][0]['end_lap'], expected_stint['end_lap'])
        self.assertEqual(self.mock_session_state.driver_stint_data["16"][0]['total_laps_on_tyre_in_stint'], expected_stint['total_laps_on_tyre_in_stint'])
        self.assertEqual(self.mock_session_state.driver_stint_data["16"][0]['tyre_total_laps_at_stint_end'], expected_stint['tyre_total_laps_at_stint_end'])

    def test_update_driver_stint_data_new_stint_used_tyre_not_changed(self):
        driver_rno_str = "16"
        stints_payload = {
            "1": {"Compound": "SOFT", "New": "false", "StartLaps": 0, "TotalLaps": 5, "TyresNotChanged": "true"}
        }
        driver_timing_state_info = {"NumberOfLaps": 0}
        self.mock_session_state.driver_stint_data = {"16": []}

        data_processing._update_driver_stint_data(self.mock_session_state, driver_rno_str, stints_payload, driver_timing_state_info)

        expected_stint = {
            'stint_number': 1, 'feed_stint_key': '1', 'start_laps_from_feed_val': 0,
            'start_lap': 1, 'compound': 'SOFT', 'is_new_tyre': False,
            'tyre_age_at_stint_start': 5, 'end_lap': 0,
            'total_laps_on_tyre_in_stint': 1, 'tyre_total_laps_at_stint_end': 5,
            'tyres_not_changed': True
        }
        self.assertEqual(self.mock_session_state.driver_stint_data["16"][0]['tyre_age_at_stint_start'], expected_stint['tyre_age_at_stint_start'])
        self.assertEqual(self.mock_session_state.driver_stint_data["16"][0]['tyres_not_changed'], expected_stint['tyres_not_changed'])

    @patch('app.data_processing._update_driver_stint_data')
    def test_process_timing_app_data(self, mock_update_driver_stint_data):
        timing_app_data = {
            "Lines": {
                "16": {
                    "Stints": {
                        "1": {"Compound": "SOFT", "New": "true", "TotalLaps": 10}
                    },
                    "TyreCompound": "SOFT",
                    "IsNewTyre": "true"
                }
            }
        }
        self.mock_session_state.timing_state = {"16": {'Tla': 'LEC', 'NumberOfLaps': 0}} # Initialize timing_state for driver 16 with some data

        data_processing._process_timing_app_data(self.mock_session_state, timing_app_data)

        mock_update_driver_stint_data.assert_called_once_with(
            self.mock_session_state, "16", timing_app_data["Lines"]["16"]["Stints"], self.mock_session_state.timing_state["16"]
        )
        self.assertEqual(self.mock_session_state.timing_state["16"]["TyreCompound"], "SOFT")
        self.assertTrue(self.mock_session_state.timing_state["16"]["IsNewTyre"])

    def test_process_extrapolated_clock_replay_start(self):
        self.mock_session_state.app_status = {"state": "Replaying"}
        self.mock_session_state.session_details = {"Type": "Practice", "SessionStatus": "Started"}
        self.mock_session_state.qualifying_segment_state = {"current_segment": "Practice"}
        self.mock_session_state.session_start_feed_timestamp_utc_dt = None
        self.mock_session_state.current_segment_scheduled_duration_seconds = None

        clock_data = {"Remaining": "01:00:00"}
        timestamp = "2025-06-28T11:00:00.000Z"
        
        data_processing._process_extrapolated_clock(self.mock_session_state, clock_data, timestamp)
        
        self.assertIsNotNone(self.mock_session_state.session_start_feed_timestamp_utc_dt)
        self.assertEqual(self.mock_session_state.current_segment_scheduled_duration_seconds, 3600)

    def test_process_timing_data_overall_best_lap(self):
        self.mock_session_state.timing_state = {
            "16": {"RacingNumber": "16", "LastLapTime": {}, "NumberOfLaps": 0},
            "44": {"RacingNumber": "44", "LastLapTime": {}, "NumberOfLaps": 0}
        }
        self.mock_session_state.session_bests = {
            "OverallBestLapTime": {"Value": None, "DriverNumber": None},
            "OverallBestSectors": [{"Value": None, "DriverNumber": None} for _ in range(3)]
        }
        self.mock_session_state.lap_time_history = {"16": [], "44": []}

        # Lap 1 for driver 16
        timing_data_1 = {"Lines": {"16": {"LastLapTime": {"Value": "1:25.123"}, "NumberOfLaps": 1}}}
        data_processing._process_timing_data(self.mock_session_state, timing_data_1, "ts1")
        self.assertEqual(self.mock_session_state.session_bests["OverallBestLapTime"]["Value"], "1:25.123")
        self.assertEqual(self.mock_session_state.session_bests["OverallBestLapTime"]["DriverNumber"], "16")

        # Faster lap by driver 44
        timing_data_2 = {"Lines": {"44": {"LastLapTime": {"Value": "1:25.000"}, "NumberOfLaps": 1}}}
        data_processing._process_timing_data(self.mock_session_state, timing_data_2, "ts2")
        self.assertEqual(self.mock_session_state.session_bests["OverallBestLapTime"]["Value"], "1:25.000")
        self.assertEqual(self.mock_session_state.session_bests["OverallBestLapTime"]["DriverNumber"], "44")

    def test_process_timing_data_lap_history(self):
        self.mock_session_state.timing_state = {
            "16": {"RacingNumber": "16", "LastLapTime": {}, "NumberOfLaps": 0, "TyreCompound": "SOFT"}
        }
        self.mock_session_state.session_bests = {
            "OverallBestLapTime": {"Value": None, "DriverNumber": None},
            "OverallBestSectors": [{"Value": None, "DriverNumber": None} for _ in range(3)]
        }
        self.mock_session_state.lap_time_history = {"16": []}

        timing_data = {"Lines": {"16": {"LastLapTime": {"Value": "1:28.500"}, "NumberOfLaps": 1}}}
        data_processing._process_timing_data(self.mock_session_state, timing_data, "ts")

        self.assertEqual(len(self.mock_session_state.lap_time_history["16"]), 1)
        self.assertEqual(self.mock_session_state.lap_time_history["16"][0]['lap_number'], 1)
        self.assertEqual(self.mock_session_state.lap_time_history["16"][0]['lap_time_seconds'], 88.5)
        self.assertEqual(self.mock_session_state.lap_time_history["16"][0]['compound'], "SOFT")

    def test_process_track_status_update(self):
        self.mock_session_state.track_status_data = {"Status": "1", "Message": "AllClear"}
        
        track_status_data = {"Status": "2", "Message": "Yellow Flag"}
        data_processing._process_track_status(self.mock_session_state, track_status_data)
        
        self.assertEqual(self.mock_session_state.track_status_data["Status"], "2")
        self.assertEqual(self.mock_session_state.track_status_data["Message"], "Yellow Flag")

    @patch('app.data_processing.logger')
    def test_process_team_radio_invalid_data(self, mock_logger):
        # Test with 'Captures' being a list instead of a dict of dicts
        radio_data = {"Captures": [{"Utc": "2025-06-28T10:01:00.000Z"}]} # Missing keys
        data_processing._process_team_radio(self.mock_session_state, radio_data)
        self.mock_session_state.team_radio_messages.appendleft.assert_not_called()

        # Test with root data being not a dict
        radio_data_invalid_root = "invalid data"
        data_processing._process_team_radio(self.mock_session_state, radio_data_invalid_root)
        mock_logger.warning.assert_called_with("Session test_ses: Unexpected TeamRadio data root format: <class 'str'>")

    @patch('app.data_processing.replay.rename_live_file_session')
    def test_check_and_trigger_rename(self, mock_rename):
        self.mock_session_state.is_saving_active = True
        self.mock_session_state.current_recording_filename = "recording_temp_12345.txt"
        
        with patch.object(self.mock_session_state, 'lock'):
             data_processing._check_and_trigger_rename(self.mock_session_state)
        
        mock_rename.assert_called_once_with(self.mock_session_state)

    def test_timing_data_overall_best_sector(self):
        self.mock_session_state.timing_state = {
            "16": {"RacingNumber": "16", "Sectors": {"0": {"Value": "-"}, "1": {"Value": "-"}, "2": {"Value": "-"}}, "PersonalBestSectors": [None, None, None]},
            "44": {"RacingNumber": "44", "Sectors": {"0": {"Value": "-"}, "1": {"Value": "-"}, "2": {"Value": "-"}}, "PersonalBestSectors": [None, None, None]}
        }
        

        # Sector 0 for driver 16
        timing_data_1 = {"Lines": {"16": {"Sectors": {"0": {"Value": "25.123"}}}}}
        data_processing._process_timing_data(self.mock_session_state, timing_data_1, "ts1")
        self.assertEqual(self.mock_session_state.session_bests["OverallBestSectors"][0]["Value"], 25.123)
        self.assertEqual(self.mock_session_state.session_bests["OverallBestSectors"][0]["DriverNumber"], "16")

        # Faster Sector 0 by driver 44
        timing_data_2 = {"Lines": {"44": {"Sectors": {"0": {"Value": "25.000"}}}}}
        data_processing._process_timing_data(self.mock_session_state, timing_data_2, "ts2")
        self.assertEqual(self.mock_session_state.session_bests["OverallBestSectors"][0]["Value"], 25.0)
        self.assertEqual(self.mock_session_state.session_bests["OverallBestSectors"][0]["DriverNumber"], "44")

    def test_timing_data_driver_status_retired(self):
        self.mock_session_state.timing_state = {
            "16": {"RacingNumber": "16", "Retired": False, "Status": "On Track"}
        }

        timing_data = {"Lines": {"16": {"Retired": True}}}
        data_processing._process_timing_data(self.mock_session_state, timing_data, "ts")

        self.assertEqual(self.mock_session_state.timing_state["16"]["Status"], "Retired")

    def test_session_data_qualifying_resume_from_suspension(self):
        self.mock_session_state.session_details = {"Type": "Qualifying", "SessionStatus": "Suspended", "PreviousSessionStatus": "Suspended"}
        self.mock_session_state.qualifying_segment_state = {"current_segment": "Q1", "just_resumed_flag": True}
        self.mock_session_state.app_status = {"state": "Live"}
        config.QUALIFYING_ORDER = {"qualifying": ["Q1", "Q2", "Q3"]}

        session_data = {"StatusSeries": {"1": {"SessionStatus": "Started"}}}
        data_processing._process_session_data(self.mock_session_state, session_data)

        self.assertTrue(self.mock_session_state.qualifying_segment_state["just_resumed_flag"])

    def test_update_driver_stint_data_tyres_not_changed(self):
        driver_rno_str = "16"
        stints_payload = {
            "2": {"Compound": "MEDIUM", "New": "false", "StartLaps": 15, "TotalLaps": 10, "TyresNotChanged": "true"}
        }
        driver_timing_state_info = {"NumberOfLaps": 14}
        self.mock_session_state.driver_stint_data = {"16": [{
            'stint_number': 1, 'feed_stint_key': '1', 'start_lap': 1, 'end_lap': 14, 'compound': 'SOFT',
            'tyre_age_at_stint_start': 0, 'total_laps_on_tyre_in_stint': 14, 'tyre_total_laps_at_stint_end': 14
        }]}

        data_processing._update_driver_stint_data(self.mock_session_state, driver_rno_str, stints_payload, driver_timing_state_info)

        self.assertEqual(len(self.mock_session_state.driver_stint_data["16"]), 2)
        self.assertEqual(self.mock_session_state.driver_stint_data["16"][1]['tyre_age_at_stint_start'], 10)

    @patch('app.data_processing.utils.prepare_position_data_updates')
    @patch('app.data_processing.utils.prepare_car_data_updates')
    def test_data_processing_loop_dispatches_position_and_car_data(self, mock_prepare_car_data, mock_prepare_position_data):
        self.mock_session_state.data_queue.put({"stream": "Position", "data": {"some": "pos_data"}, "timestamp": "ts1"})
        self.mock_session_state.data_queue.put({"stream": "CarData", "data": {"some": "car_data"}, "timestamp": "ts2"})
        
        mock_prepare_position_data.return_value = {}
        mock_prepare_car_data.return_value = ({}, {})

        # Run the loop in a separate thread
        processing_thread = threading.Thread(target=data_processing.data_processing_loop_session, args=(self.mock_session_state,))
        processing_thread.start()

        # Wait for mocks to be called, with a timeout to prevent hanging
        timeout = 5  # seconds
        start_time = time.time()
        while not (mock_prepare_position_data.called and mock_prepare_car_data.called) and (time.time() - start_time < timeout):
            time.sleep(0.1) # Wait a bit before checking again

        # Signal the processing thread to stop
        self.mock_session_state.stop_event.set()
        processing_thread.join(timeout=1) # Give the thread a short time to finish

        # Assert that the mocks were called
        mock_prepare_position_data.assert_called_once()
        mock_prepare_car_data.assert_called_once()

        mock_prepare_position_data.assert_called_once()
        mock_prepare_car_data.assert_called_once()

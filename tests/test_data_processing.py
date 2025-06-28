import unittest
from unittest.mock import MagicMock, patch
import datetime
import time
import queue
import threading

from app import data_processing, app_state, config, utils

class TestDataProcessing(unittest.TestCase):

    def setUp(self):
        self.mock_session_state = MagicMock(spec=app_state.SessionState)
        self.mock_session_state.session_id = "test_session_123"
        self.mock_session_state.app_status = {}
        self.mock_session_state.team_radio_messages = MagicMock()
        self.mock_session_state.extrapolated_clock_info = {}
        self.mock_session_state.session_details = {}
        self.mock_session_state.qualifying_segment_state = {}
        self.mock_session_state.timing_state = {}
        self.mock_session_state.data_queue = MagicMock(spec=queue.Queue)
        self.mock_session_state.stop_event = MagicMock(spec=threading.Event)
        self.mock_session_state.stop_event.is_set.return_value = False # Default to False
        self.mock_session_state.lock = MagicMock()
        self.mock_session_state.data_processing_thread = None
        self.mock_session_state.data_store = {}
        self.mock_session_state._pending_background_fetch = None
        self.mock_session_state.practice_session_scheduled_duration_seconds = None
        self.mock_session_state.session_start_feed_timestamp_utc_dt = None
        self.mock_session_state.current_segment_scheduled_duration_seconds = None
        self.mock_session_state.replay_speed = 1.0 # Add replay_speed attribute

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

    @patch('app.utils.parse_iso_timestamp_safe')
    @patch('app.utils.parse_session_time_to_seconds')
    def test_process_extrapolated_clock(self, mock_parse_session_time, mock_parse_iso_timestamp):
        # Test case 1: Basic update
        mock_parse_iso_timestamp.return_value = datetime.datetime(2025, 6, 28, 10, 0, 0, tzinfo=datetime.timezone.utc)
        mock_parse_session_time.return_value = 3600 # 1 hour remaining
        
        data_payload = {"Utc": "2025-06-28T10:00:00.000Z", "Extrapolating": True, "Remaining": "01:00:00"}
        received_timestamp = "2025-06-28T10:00:00.000Z"
        
        data_processing._process_extrapolated_clock(self.mock_session_state, data_payload, received_timestamp)
        
        self.assertEqual(self.mock_session_state.extrapolated_clock_info["Utc"], "2025-06-28T10:00:00.000Z")
        self.assertTrue(self.mock_session_state.extrapolated_clock_info["Extrapolating"])
        self.assertEqual(self.mock_session_state.extrapolated_clock_info["Remaining"], "01:00:00")
        self.assertEqual(self.mock_session_state.extrapolated_clock_info["Timestamp"], received_timestamp)

        # Test case 2: Replay mode, setting session_start_feed_timestamp_utc_dt
        self.mock_session_state.app_status["state"] = "Replaying"
        self.mock_session_state.session_details["Type"] = "Practice"
        self.mock_session_state.session_start_feed_timestamp_utc_dt = None
        self.mock_session_state.qualifying_segment_state["current_segment"] = "Practice"

        mock_parse_iso_timestamp.return_value = datetime.datetime(2025, 6, 28, 10, 0, 0, tzinfo=datetime.timezone.utc)
        mock_parse_session_time.return_value = 3600 # 1 hour remaining

        data_processing._process_extrapolated_clock(self.mock_session_state, data_payload, received_timestamp)
        self.assertIsNotNone(self.mock_session_state.session_start_feed_timestamp_utc_dt)
        self.assertEqual(self.mock_session_state.current_segment_scheduled_duration_seconds, 3600)

    @patch('app.utils.parse_iso_timestamp_safe')
    @patch('app.utils.parse_session_time_to_seconds')
    @patch('time.sleep', side_effect=[None, InterruptedError]) # Allow one sleep, then interrupt
    def test_data_processing_loop_session_heartbeat(self, mock_sleep, mock_parse_session_time, mock_parse_iso_timestamp):
        timestamp = "2025-06-28T10:00:00.000Z"
        heartbeat_item = {'stream': 'Heartbeat', 'data': {}, 'timestamp': timestamp}
        self.mock_session_state.data_queue.put(heartbeat_item)

        # Configure data_queue.get to return the item once, then raise Empty
        self.mock_session_state.data_queue.get.side_effect = [heartbeat_item, queue.Empty]

        # Mock the stop_event.is_set to return False once, then True
        self.mock_session_state.stop_event.is_set.side_effect = [False, True, True]

        # Patch time.sleep to allow one iteration and then stop
        with patch('time.sleep', side_effect=[None, InterruptedError]):
            data_processing.data_processing_loop_session(self.mock_session_state)

        self.assertEqual(self.mock_session_state.app_status["last_heartbeat"], timestamp)
        self.mock_session_state.data_queue.task_done.assert_called_once()

if __name__ == '__main__':
    unittest.main()

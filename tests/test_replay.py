import unittest
from unittest.mock import MagicMock, patch
import datetime
from pathlib import Path
import threading

from app import replay, app_state, constants, utils

class TestReplay(unittest.TestCase):

    def setUp(self):
        self.mock_session_state = MagicMock(spec=app_state.SessionState)
        self.mock_session_state.session_id = "test_session_123"
        self.mock_session_state.session_details = {
            "Type": "Race",
            "Meeting": {"Name": "Austrian Grand Prix"},
            "Name": "Race",
            "StartDate": "2025-07-06T13:00:00Z",
            "Year": 2025 # Added Year
        }
        self.mock_session_state.lock = threading.Lock()

    @patch('app.replay.datetime')
    def test_generate_live_filename_session_full_details(self, mock_datetime):
        # Mock datetime.datetime.now() to return a fixed time for consistent testing
        mock_datetime.datetime.now.return_value = datetime.datetime(2025, 7, 6, 15, 30, 0, tzinfo=datetime.timezone.utc)
        mock_datetime.timezone = datetime.timezone # Ensure timezone is accessible

        # Test with full session details
        filename = replay.generate_live_filename_session(self.mock_session_state)
        expected_filename = "2025-Austrian_Grand_Prix-Race_20250706_130000UTC.data.txt"
        self.assertEqual(filename, expected_filename)

    @patch('app.replay.datetime')
    def test_generate_live_filename_session_missing_details(self, mock_datetime):
        mock_datetime.datetime.now.return_value = datetime.datetime(2025, 7, 6, 15, 30, 0, tzinfo=datetime.timezone.utc)
        mock_datetime.timezone = datetime.timezone

        # Test with missing session details, expecting fallback
        self.mock_session_state.session_details = {
            "Type": "",
            "Meeting": {"Name": ""},
            "Name": ""
        }
        filename = replay.generate_live_filename_session(self.mock_session_state)
        # Fallback prefix is defined in config.py
        expected_filename_prefix = constants.LIVE_DATA_FILENAME_FALLBACK_PREFIX
        self.assertTrue(filename.startswith(expected_filename_prefix))
        self.assertTrue(filename.endswith(".data.txt"))

    @patch('app.replay.datetime')
    def test_generate_live_filename_session_special_chars(self, mock_datetime):
        mock_datetime.datetime.now.return_value = datetime.datetime(2025, 7, 6, 15, 30, 0, tzinfo=datetime.timezone.utc)
        mock_datetime.timezone = datetime.timezone

        self.mock_session_state.session_details = {
            "Type": "Practice 1 (FP1)",
            "Meeting": {"Name": "São Paulo Grand Prix"},
            "Name": "Practice 1",
            "StartDate": "2025-11-07T10:00:00Z",
            "Year": 2025 # Added Year
        }
        filename = replay.generate_live_filename_session(self.mock_session_state)
        expected_filename = "2025-Sao_Paulo_Grand_Prix-FP1_20251107_100000UTC.data.txt"
        self.assertEqual(filename, expected_filename)

    def test_generate_live_filename_session_no_start_date(self):
        self.mock_session_state.session_details = {
            "Type": "Race",
            "Meeting": {"Name": "Test Grand Prix"},
            "Name": "Race",
            "StartDate": None # No start date
        }
        # Mock datetime.datetime.now() to return a fixed time for consistent testing
        with patch('app.replay.datetime') as mock_datetime:
            mock_datetime.datetime.now.return_value = datetime.datetime(2025, 6, 28, 12, 0, 0, tzinfo=datetime.timezone.utc)
            mock_datetime.timezone = datetime.timezone
            filename = replay.generate_live_filename_session(self.mock_session_state)
            expected_filename = f"{constants.LIVE_DATA_FILENAME_FALLBACK_PREFIX}_20250628_120000UTC.data.txt"
            self.assertEqual(filename, expected_filename)


if __name__ == '__main__':
    unittest.main()
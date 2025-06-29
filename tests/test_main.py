import unittest
from unittest.mock import patch, MagicMock, call
import threading
import time
import os

# Patch threading.Thread at the module level before main is imported.
# This prevents the background services from starting real threads during testing.
thread_patcher = patch('threading.Thread')
thread_patcher.start()

# Now that threading.Thread is patched, we can safely import the main module.
from app import main, app_state, schedule_page, settings
from app import config # Keep config for other potential uses if any, but specifically import settings for SESSION_TIMEOUT_HOURS

class TestMain(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        # Stop the module-level patcher after all tests in the class have run.
        thread_patcher.stop()

    def setUp(self):
        # Set up a temporary directory for recordings
        self.temp_dir = "temp_recordings"
        os.makedirs(self.temp_dir, exist_ok=True)
        config.REPLAY_DIR = self.temp_dir

    def tearDown(self):
        # Clean up the temporary directory
        for f in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, f))
        os.rmdir(self.temp_dir)

    @patch('threading.Thread')
    def test_global_auto_recorder_service_thread_creation(self, mock_thread):
        # Ensure the thread is configured to start the service
        # We don't actually start the thread to avoid infinite loop in test
        main.start_background_services() # Call the function that creates the thread
        mock_thread.assert_any_call(target=main.global_auto_recorder_service, daemon=True, name="GlobalAutoRecorder")

    @patch('threading.Thread')
    def test_session_garbage_collector_thread_creation(self, mock_thread):
        # Ensure the thread is configured to start the collector
        # We don't actually start the thread to avoid infinite loop in test
        main.start_background_services() # Call the function that creates the thread
        mock_thread.assert_any_call(target=main.session_garbage_collector, daemon=True, name="SessionGarbageCollector")

    @patch('main.schedule_page.find_next_session_to_connect')
    @patch('main.app_state.get_or_create_session_state')
    @patch('main.app_state.get_session_state', return_value=None) # Mock to simulate no existing session
    @patch('main.utils.load_global_settings') # Patch load_global_settings
    @patch('time.sleep', side_effect=[None, None, None, InterruptedError]) # Allow a few sleeps, then interrupt
    def test_auto_recorder_starts_recording(self, mock_sleep, mock_load_settings, mock_get_session_state, mock_get_or_create_session_state, mock_find_next):
        # Mock load_global_settings to enable recording
        mock_load_settings.return_value = {'record_live_sessions': True}

        # Mock the next session to be live
        mock_find_next.return_value = {
            'session_name': 'Test Race',
            'SessionKey': '1234',
            'SessionInfo': {'Country': 'Testland'}
        }
        # Mock the session state
        mock_session_state = MagicMock()
        mock_get_or_create_session_state.return_value = mock_session_state

        # Patch CURRENT_LIVE_SESSION_INFO and CURRENT_LIVE_INFO_LOCK within the test
        with patch('main.app_state.CURRENT_LIVE_SESSION_INFO', None), \
             patch('main.app_state.CURRENT_LIVE_SESSION_INFO_LOCK'):
            # Run the recorder service, expecting it to be interrupted by mock_sleep
            with self.assertRaises(InterruptedError):
                main.global_auto_recorder_service()

        # Check that a new session was created and recording started
        mock_get_or_create_session_state.assert_called_with('auto-recorder-1234')
        self.assertTrue(mock_session_state.session_details.update.called)

    @patch('main.app_state.remove_session_state') # Mock remove_session_state
    @patch('main.time.time') # Patch time.time to control current time
    @patch('main.time.sleep', side_effect=[None, InterruptedError]) # Allow one sleep, then interrupt
    def test_garbage_collector_removes_stale_sessions(self, mock_sleep, mock_time, mock_remove_session_state):
        # Create a stale session state object
        stale_session_id = "stale_session"
        stale_session_obj = MagicMock()
        stale_session_obj.last_accessed_time = 1000 - (settings.SESSION_TIMEOUT_HOURS * 3600) - 1 # Make it stale

        # Mock SESSIONS_STORE.items() to return our stale session
        with patch('main.app_state.SESSIONS_STORE', {stale_session_id: stale_session_obj}):
            # Set the current time to a specific value
            mock_time.return_value = 1000

            # Run the garbage collector, expecting it to be interrupted by mock_sleep
            with self.assertRaises(InterruptedError):
                main.session_garbage_collector()

            # Check that the stale session was removed
            mock_remove_session_state.assert_called_with(stale_session_id)

    def test_main_index_route(self):
        with main.app.server.test_client() as client:
            response = client.get('/')
            self.assertEqual(response.status_code, 200)

if __name__ == '__main__':
    unittest.main()

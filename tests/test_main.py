# In tests/test_main.py

import pytest
from unittest.mock import patch, MagicMock
import time
import os
import threading

from app import main, app_state, utils, config, schedule_page, settings

@pytest.fixture
def temp_recordings_dir():
    temp_dir = "temp_recordings"
    os.makedirs(temp_dir, exist_ok=True)
    original_replay_dir = settings.REPLAY_DIR
    settings.REPLAY_DIR = temp_dir
    yield temp_dir
    settings.REPLAY_DIR = original_replay_dir # Restore original
    for f in os.listdir(temp_dir):
        os.remove(os.path.join(temp_dir, f))
    os.rmdir(temp_dir)



def test_global_auto_recorder_service_thread_creation(temp_recordings_dir, monkeypatch):
    monkeypatch.delenv("F1_DASHBOARD_IS_TESTING", raising=False)
    with patch('app.main.threading.Thread') as mock_thread:
        main.start_background_services()
        mock_thread.assert_any_call(target=main.global_auto_recorder_service, daemon=True, name="GlobalAutoRecorder")

def test_session_garbage_collector_thread_creation(temp_recordings_dir, monkeypatch):
    monkeypatch.delenv("F1_DASHBOARD_IS_TESTING", raising=False)
    with patch('app.main.threading.Thread') as mock_thread:
        main.start_background_services()
        mock_thread.assert_any_call(target=main.session_garbage_collector, daemon=True, name="SessionGarbageCollector")

def test_auto_recorder_starts_recording(temp_recordings_dir, running_background_services):
    with (
        patch('app.main.schedule_page.find_next_session_to_connect') as mock_find_next,
        patch('app.main.app_state.get_or_create_session_state') as mock_get_or_create,
        patch('app.main.app_state.get_session_state', return_value=None) as mock_get_session,
        patch('app.main.utils.load_global_settings') as mock_load_settings,
        patch('app.main.time.sleep', side_effect=[None, None, None, InterruptedError]) as mock_sleep
    ):

        mock_load_settings.return_value = {'record_live_sessions': True}
        mock_find_next.return_value = {
            'session_name': 'Test Race', 'SessionKey': '1234', 'SessionInfo': {'Country': 'Testland'}
        }
        mock_session_state = MagicMock()
        mock_get_or_create.return_value = mock_session_state

        with patch('app.main.app_state.CURRENT_LIVE_SESSION_INFO', None): # Removed comma
            with patch('app.main.app_state.CURRENT_LIVE_SESSION_INFO_LOCK'): # Nested patch
                with pytest.raises(InterruptedError):
                    main.global_auto_recorder_service()

        mock_get_or_create.assert_called_with('auto-recorder-1234')
        assert mock_session_state.session_details.update.called

def test_garbage_collector_removes_stale_sessions(temp_recordings_dir, running_background_services, monkeypatch):
    stale_session_id = "stale_session"
    stale_session_obj = MagicMock()
    stale_session_obj.last_accessed_time = 1000 - (settings.SESSION_TIMEOUT_HOURS * 3600) - 1

    with (
        patch('app.main.app_state.remove_session_state') as mock_remove_session_state,
        patch('app.main.time.time') as mock_time,
        patch('app.main.time.sleep', side_effect=[None, InterruptedError]) as mock_sleep,
        patch('app.main.app_state.SESSIONS_STORE', {stale_session_id: stale_session_obj}),
        patch('app.main.app_state.SESSIONS_STORE_LOCK', threading.Lock())
    ):

        mock_time.return_value = 1000
        with pytest.raises(InterruptedError):
            main.session_garbage_collector()
        mock_remove_session_state.assert_called_with(stale_session_id)
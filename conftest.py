import pytest
import sys
import os
import threading
from app import main as main_app # Import your main app module

# This fixture should already exist from the previous step
@pytest.fixture(scope='session', autouse=True)
def set_test_environment():
    os.environ['F1_DASHBOARD_IS_TESTING'] = 'True'
    yield
    del os.environ['F1_DASHBOARD_IS_TESTING']


# --- ADD THIS NEW FIXTURE ---
@pytest.fixture(scope='function')
def running_background_services():
    """
    A fixture that explicitly starts the app's background services for a test
    and ensures they are cleaned up afterwards.
    """
    print("\n[Fixture] Starting background services for test...")
    # Start the services and capture the thread objects
    main_app.start_background_services()
    
    # Let the test run
    yield
    
    print("\n[Fixture] Tearing down background services...")
    # In a real scenario, you'd need a way to stop these threads cleanly.
    # For now, we'll rely on the test process exiting, but for long-running
    # services, proper shutdown logic would be needed here.
    # For example, if main_app.start_background_services() returned the threads,
    # you could join them here. Or if there's a main_app.stop_background_services()
    # function.
    # Signal the threads to stop and wait for them to finish
    if hasattr(main_app, 'cleanup_thread') and main_app.cleanup_thread.is_alive():
        # Assuming session_garbage_collector has a way to stop, e.g., a stop event
        # For now, we'll just join, but ideally, you'd set an event for graceful exit.
        print("Joining cleanup_thread...")
        main_app.cleanup_thread.join(timeout=5) # Give it some time to finish

    if hasattr(main_app, 'recorder_thread') and main_app.recorder_thread.is_alive():
        # Assuming global_auto_recorder_service has a way to stop
        print("Joining recorder_thread...")
        main_app.recorder_thread.join(timeout=5) # Give it some time to finish

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'app')))
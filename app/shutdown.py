# app/shutdown.py
import logging
import time
from app import app_state

def _join_thread(logger, session_id, thread_name, thread_obj):
    """Helper to join a thread with logging and a timeout."""
    if thread_obj and thread_obj.is_alive():
        logger.info(
            f"Session {session_id}: Waiting for {thread_name} thread ({thread_obj.name}) to join...")
        thread_obj.join(timeout=5.0)
        if thread_obj.is_alive():
            logger.warning(
                f"Session {session_id}: Thread {thread_obj.name} did not exit cleanly.")
        else:
            logger.info(
                f"Session {session_id}: Thread {thread_obj.name} joined successfully.")

def controlled_shutdown():
    """
    A controlled shutdown sequence that cleans up all active sessions,
    stops threads, and then safely shuts down the logging system.
    This should be called explicitly on application exit instead of relying
    on atexit to avoid race conditions with the logging module's own cleanup.
    """
    logger_shutdown = logging.getLogger("F1App.Shutdown")
    logger_shutdown.info(
        "Initiating controlled application shutdown sequence...")

    active_session_ids = []
    with app_state.SESSIONS_STORE_LOCK:
        active_session_ids = list(app_state.SESSIONS_STORE.keys())
    
    if active_session_ids:
        logger_shutdown.info(
            f"Found {len(active_session_ids)} active session(s) to clean up.")

        for session_id in active_session_ids:
            session_state = app_state.get_session_state(session_id)
            if not session_state:
                logger_shutdown.warning(f"Could not retrieve session state for {session_id} during shutdown. It may have been removed.")
                continue

            logger_shutdown.info(f"Cleaning up session: {session_id}...")
            
            # Set stop event and stop hub connection under a single lock
            with session_state.lock:
                session_state.stop_event.set()
                
                threads_to_join = [
                    ("SignalR Connection", session_state.connection_thread),
                    ("Replay", session_state.replay_thread),
                    ("Data Processing", session_state.data_processing_thread),
                    ("Auto-Connect Monitor", session_state.auto_connect_thread),
                    ("Track Data Fetch", session_state.track_data_fetch_thread),
                ]

                if session_state.hub_connection:
                    try:
                        logger_shutdown.debug(
                            f"Session {session_id}: Attempting to stop session's hub_connection directly.")
                        session_state.hub_connection.stop()
                    except Exception as e_hub_stop:
                        logger_shutdown.error(
                            f"Session {session_id}: Error stopping session's hub_connection: {e_hub_stop}")
            
            # Join threads outside the main session lock to avoid deadlocks
            for thread_name, thread_obj in threads_to_join:
                _join_thread(logger_shutdown, session_id, thread_name, thread_obj)

            # Final cleanup of resources under the lock
            with session_state.lock:
                session_state.connection_thread = None
                session_state.replay_thread = None
                session_state.data_processing_thread = None
                session_state.auto_connect_thread = None
                session_state.hub_connection = None
                session_state.track_data_fetch_thread = None

                if session_state.live_data_file and not session_state.live_data_file.closed:
                    try:
                        session_state.live_data_file.close()
                        logger_shutdown.info(
                            f"Session {session_id}: Closed live_data_file.")
                    except Exception as e:
                        logger_shutdown.error(
                            f"Session {session_id}: Error closing live_data_file: {e}")
                session_state.live_data_file = None
    else:
        logger_shutdown.info("No active sessions found to clean up.")

    # Clear the main session store
    with app_state.SESSIONS_STORE_LOCK:
        if app_state.SESSIONS_STORE:
            app_state.SESSIONS_STORE.clear()
            logger_shutdown.info("Cleared all sessions from SESSIONS_STORE.")
        else:
            logger_shutdown.info("SESSIONS_STORE was already empty.")

    logger_shutdown.info("Controlled application shutdown sequence complete.")
    
    # This is the crucial final step.
    # After all our application logging is done, we tell the logging module to shut down.
    logging.shutdown()

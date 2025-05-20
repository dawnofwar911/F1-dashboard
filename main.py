# main.py
"""
Main application script for the F1 Telemetry Dashboard.
Initializes the Dash app, sets up logging, starts background threads,
and runs the Dash server.
"""

import logging
import sys
import os # Keep for os path operations if any
import threading
import time

# --- Local Module Imports ---
import app_state
import config    # <<< UPDATED: For logging formats and other configs
import utils

from app_instance import app, server # Import app from app_instance

import layout
import callbacks
import signalr_client
import data_processing
import replay

# --- Global Reference for Raw Log Formatter ---
# The formatter object itself doesn't need to be global if only the format string is from config.
# For now, keeping the object as is, but it could be created where needed.
raw_message_formatter = logging.Formatter(config.LOG_FORMAT_RAW_MESSAGE) # <<< UPDATED: Use constant

# --- Logging Setup ---
def setup_logging():
    """Configures logging for the application."""
    # Use constant for log format
    log_formatter = logging.Formatter(config.LOG_FORMAT_DEFAULT) # Assuming config.LOG_FORMAT_DEFAULT is defined

    # --- Configure the ACTUAL Python Root Logger ---
    actual_root_logger = logging.getLogger() # Get the root logger
    actual_root_logger.setLevel(logging.INFO) # Set a permissive level at root; control via specific logger levels

    # Clear any existing handlers from the root logger to prevent duplication
    # This is important if the script is re-run in an environment or if other libs call basicConfig.
    if actual_root_logger.hasHandlers():
        print(f"Root logger initially has handlers: {actual_root_logger.handlers}. Clearing them.", file=sys.stderr)
        actual_root_logger.handlers.clear()

    # Add OUR desired console handler to the root logger
    root_console_handler = logging.StreamHandler(sys.stdout)
    root_console_handler.setFormatter(log_formatter)
    actual_root_logger.addHandler(root_console_handler)
    print(f"Root logger configured with handler: {actual_root_logger.handlers}", file=sys.stderr)


    # --- Configure F1App Logger (your application's main logger) ---
    f1_app_logger = logging.getLogger("F1App")
    f1_app_logger.setLevel(logging.INFO) # Set desired level for your app's components
    f1_app_logger.propagate = True # Ensure it propagates to the root logger (default, but good to be explicit)
    # No need to add a handler here; it will use the root's handler.
    f1_app_logger.info("F1App application logger level set. Will use root handler.")


    # --- Set levels for Library Loggers (they will propagate to the root handler) ---
    # The key is that THEY should not add their own console handlers if we want single output via root.
    
    # For SignalRCoreClient, we set the level here.
    # Handler management will be done in signalr_client.py AFTER its own setup.
    logging.getLogger("SignalRCoreClient").setLevel(logging.WARNING)
    
    # For the base "signalrcore" logger, if it's used.
    logging.getLogger("signalrcore").setLevel(logging.WARNING)


    # --- Configure Werkzeug Logger ---
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.ERROR if not config.DASH_DEBUG_MODE else logging.INFO)
    werkzeug_logger.propagate = True # Let it use the root handler
    # Clear any handlers it might have by default if they cause issues
    if werkzeug_logger.hasHandlers(): # Check and clear Werkzeug's own handlers
         module_logger_temp = logging.getLogger("F1App") # Use your app logger for this meta-log
         module_logger_temp.info(f"Werkzeug logger has handlers: {werkzeug_logger.handlers}. Clearing them.")
         werkzeug_logger.handlers.clear()
    f1_app_logger.info(f"Werkzeug logger level set to {logging.getLevelName(werkzeug_logger.getEffectiveLevel())}. Will use root handler.")


    # --- Set levels for other potentially noisy libraries ---
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('fastf1').setLevel(logging.INFO)

    actual_root_logger.info("Root logger configured. Specific loggers will propagate to it.")


# --- Main Execution Logic ---
if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger("F1App.Main")
    logger.info("Application starting...")

    logger.info("Checking/Creating replay and FastF1 cache directories...")
    replay.ensure_replay_dir_exists() # Also ensures TARGET_SAVE_DIRECTORY
    try:
        config.FASTF1_CACHE_DIR.mkdir(parents=True, exist_ok=True) # Ensure FastF1 cache dir
        logger.info(f"FastF1 cache directory ensured at: {config.FASTF1_CACHE_DIR}")
    except Exception as e:
        logger.error(f"Could not create FastF1 cache directory at {config.FASTF1_CACHE_DIR}: {e}")


    app.layout = layout.create_layout()
    logger.info("Dash layout created and assigned.")

    processing_thread = None
    dash_thread = None
    app_state.stop_event.clear()

    try:
        logger.info("Starting background threads...")

        processing_thread = threading.Thread(
            target=data_processing.data_processing_loop,
            name="DataProcessingThread", daemon=True)
        processing_thread.start()
        logger.info("Data processing thread started.")

        def run_dash_server():
            dash_logger = logging.getLogger("F1App.DashServer")
            dash_logger.info("Dash server thread started.")
            try:
                app.run(
                    host=config.DASH_HOST,
                    port=config.DASH_PORT,
                    debug=config.DASH_DEBUG_MODE,
                    use_reloader=False # Explicitly False for stability in threaded mode
                )
                dash_logger.info("Dash server stopped.") # Might not be reached if run blocks
            except SystemExit:
                 dash_logger.info("Dash server exited (SystemExit).")
            except Exception as e:
                 dash_logger.error(f"Dash server failed: {e}", exc_info=True)
            finally:
                 dash_logger.info("Dash thread finishing.")
                 if not app_state.stop_event.is_set():
                     logger.info("Dash thread setting stop event as it's finishing.")
                     app_state.stop_event.set()

        dash_thread = threading.Thread(target=run_dash_server, name="DashServerThread", daemon=True)
        dash_thread.start()
        logger.info(f"Dash server starting on http://{config.DASH_HOST}:{config.DASH_PORT}")

        logger.info("Entering main monitoring loop...")
        while not app_state.stop_event.is_set():
            proc_thread_alive = processing_thread and processing_thread.is_alive()
            dash_thread_alive = dash_thread and dash_thread.is_alive()

            if not proc_thread_alive:
                 logger.error("CRITICAL: Data Processing thread died! Stopping application.")
                 app_state.stop_event.set(); break

            if not dash_thread_alive:
                 logger.error("CRITICAL: Dash Server thread died! Stopping application.")
                 app_state.stop_event.set(); break
            time.sleep(5)
        logger.info("Stop event detected or essential thread died, exiting monitoring loop.")

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt detected, initiating shutdown.")
        if not app_state.stop_event.is_set(): app_state.stop_event.set()
    except Exception as main_err:
        logger.error(f"Error in main execution block: {main_err}", exc_info=True)
        if not app_state.stop_event.is_set(): app_state.stop_event.set()
    finally:
        logger.info("Starting final cleanup...")
        if not app_state.stop_event.is_set():
            logger.info("Setting stop event (main.py finally block).")
            app_state.stop_event.set()

        with app_state.app_state_lock:
            current_state = app_state.app_status.get("state", "Unknown").lower()

        if "live" in current_state or "connecting" in current_state:
            logger.info("Cleanup: Stopping SignalR connection..."); signalr_client.stop_connection()
        logger.info("Cleanup: Stopping Replay (if running)..."); replay.stop_replay()

        if processing_thread and processing_thread.is_alive():
            logger.info("Cleanup: Waiting for Data Processing thread..."); processing_thread.join(timeout=5.0)
            if processing_thread.is_alive(): logger.warning("Data Processing thread did not exit cleanly.")
            else: logger.info("Data Processing thread joined.")

        # Dash thread is a daemon, it should exit when main thread exits after stop_event.
        # Forcing a join here can sometimes be problematic if the server isn't shutting down gracefully.
        # If dash_thread is still alive, it might indicate an issue with app.run() not respecting shutdown.
        if dash_thread and dash_thread.is_alive():
            logger.info("Cleanup: Dash server thread still alive. Main thread exiting; daemon thread should follow.")
            # dash_thread.join(timeout=2.0) # Optional, short timeout
            # if dash_thread.is_alive(): logger.warning("Dash thread did not exit after short wait.")


        logger.info("Shutdown complete."); print("\n --- App Exited --- \n")

print("DEBUG: main module loaded")
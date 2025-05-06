# main.py
"""
Main application script for the F1 Telemetry Dashboard.
Initializes the Dash app, sets up logging, starts background threads,
and runs the Dash server.
"""

import logging
import sys
import os
import threading
import time
#from dash import Dash
#import dash_bootstrap_components as dbc

# --- Local Module Imports ---
# Import order matters for app initialization
import app_state # Shared state first
import config    # Configuration
import utils     # Utilities

# --- Import the app instance ---
from app_instance import app, server # <<< IMPORT app from new file

# Import modules that need the 'app' object or other state/config
import layout         # Defines app.layout structure via create_layout()
import callbacks      # Registers callbacks with the imported 'app' object
import signalr_client # Contains SignalR connection logic
import data_processing# Contains data processing loop
import replay         # Contains replay logic and file utils

# --- Global Reference for Raw Log Formatter (needed by replay.init_live_file) ---
# Define it here so it's accessible via import in replay.py
raw_message_formatter = logging.Formatter('%(message)s')

# --- Logging Setup ---
def setup_logging():
    """Configures logging for the application."""
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # Use config level? Or keep fixed? Let's use fixed INFO for main, DEBUG set earlier.
    log_level_main = logging.INFO # Default level for console
    log_level_signalr = logging.DEBUG # Need DEBUG for raw messages if using logging method

    # Main application logger
    root_logger = logging.getLogger("F1App") # Get root logger for app modules
    root_logger.setLevel(min(log_level_main, log_level_signalr)) # Set root to lowest level needed

    # Prevent adding handlers multiple times if re-running setup
    if not root_logger.hasHandlers():
        main_console_handler = logging.StreamHandler(sys.stdout)
        main_console_handler.setFormatter(log_formatter)
        main_console_handler.setLevel(log_level_main)
        root_logger.addHandler(main_console_handler)
        # Add file handler maybe?
        # file_handler = logging.FileHandler("f1_dashboard_main.log")
        # file_handler.setFormatter(log_formatter)
        # root_logger.addHandler(file_handler)
        root_logger.info("Main application logger configured.")
    else:
         root_logger.info("Main application logger already configured.")

    # Configure SignalR Core library logger specifically
    signalr_logger = logging.getLogger("signalrcore")
    signalr_logger.setLevel(log_level_signalr)
    # Only add console handler if not already present from root logger inheritance or previous setup
    has_sigr_console = any(isinstance(h, logging.StreamHandler) for h in signalr_logger.handlers)
    if not has_sigr_console:
        signalr_console_handler = logging.StreamHandler(sys.stdout)
        signalr_console_handler.setFormatter(log_formatter)
        # Set console level higher to avoid spam, DEBUG is still needed for file handler
        signalr_console_handler.setLevel(logging.INFO)
        signalr_logger.addHandler(signalr_console_handler)
        signalr_logger.info("SignalR console handler added.")
    else:
         signalr_logger.info("SignalR logger already has console handler.")
         # Ensure existing handlers allow DEBUG if needed? Might not be necessary.

    # Disable overly verbose library loggers (optional)
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('fastf1').setLevel(logging.INFO) # Or WARNING

# --- Main Execution Logic ---
if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger("F1App.Main")
    logger.info("Application starting...")

    logger.info("Checking/Creating replay directory...")
    replay.ensure_replay_dir_exists()

    # --- Assign Layout ---
    # Layout creation doesn't need replay options passed explicitly anymore
    app.layout = layout.create_layout()
    logger.info("Dash layout created and assigned.")

    # --- Start Background Threads ---
    processing_thread = None
    dash_thread = None
    app_state.stop_event.clear()

    try:
        logger.info("Starting background threads...")

        # Start data processing thread
        processing_thread = threading.Thread(
            target=data_processing.data_processing_loop,
            name="DataProcessingThread", daemon=True)
        processing_thread.start()
        logger.info("Data processing thread started.")

        # --- MODIFIED Dash Server Thread Start ---
        def run_dash_server():
            dash_logger = logging.getLogger("F1App.DashServer")
            dash_logger.info("Dash server thread started.")
            try:
                # Use app.run() with debug from config and reloader explicitly False
                app.run(
                    host=config.DASH_HOST,
                    port=config.DASH_PORT,
                    debug=config.DASH_DEBUG_MODE, # Use value from config (should be False)
                    use_reloader=False # Explicitly disable reloader is KEY
                )
                # This part might not be reached if run blocks until shutdown
                dash_logger.info("Dash server stopped.")
            except SystemExit:
                 dash_logger.info("Dash server exited (SystemExit).")
            except Exception as e:
                 dash_logger.error(f"Dash server failed: {e}", exc_info=True)
            finally:
                 dash_logger.info("Dash thread finishing.")
                 if not app_state.stop_event.is_set():
                     logger.info("Dash thread setting stop event."); app_state.stop_event.set()
    
        dash_thread = threading.Thread(target=run_dash_server, name="DashServerThread", daemon=True)
        dash_thread.start()
        logger.info(f"Dash server starting on http://{config.DASH_HOST}:{config.DASH_PORT}")
    
        # --- END MODIFIED Dash Server Thread Start ---

        # --- Main Application Loop (Keep-alive / Monitoring) ---
        logger.info("Entering main monitoring loop...")
        while not app_state.stop_event.is_set():
            # Check essential threads
            proc_thread_alive = processing_thread and processing_thread.is_alive()
            dash_thread_alive = dash_thread and dash_thread.is_alive()

            if not proc_thread_alive:
                 logger.error("CRITICAL: Data Processing thread died! Stopping application.")
                 app_state.stop_event.set()
                 break # Exit monitoring loop

            if not dash_thread_alive:
                 logger.error("CRITICAL: Dash Server thread died! Stopping application.")
                 app_state.stop_event.set()
                 break # Exit monitoring loop

            # Check connection/replay threads only if they are expected to be running
            # This logic might need refinement depending on how thread references are managed
            # conn_thread_obj = getattr(signalr_client, 'connection_thread', None)
            # replay_thread_obj = getattr(replay, 'replay_thread', None)
            # ... checks based on app_state.app_status['state'] ...

            time.sleep(5) # Check every 5 seconds

        logger.info("Stop event detected or essential thread died, exiting monitoring loop.")

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt detected, initiating shutdown.")
        if not app_state.stop_event.is_set(): app_state.stop_event.set()
    except Exception as main_err:
        logger.error(f"Error in main execution block: {main_err}", exc_info=True)
        if not app_state.stop_event.is_set(): app_state.stop_event.set()
    finally:
        # --- Final Cleanup ---
        # (Keep cleanup logic from Response 36)
        logger.info("Starting final cleanup...")
        if not app_state.stop_event.is_set(): logger.info("Setting stop event (cleanup)."); app_state.stop_event.set()
        with app_state.app_state_lock: current_state = app_state.app_status.get("state", "Unknown").lower()

        # Use stop functions which handle internal checks and joining
        if "live" in current_state or "connecting" in current_state: logger.info("Cleanup: Stopping SignalR connection..."); signalr_client.stop_connection()
        logger.info("Cleanup: Stopping Replay (if running)..."); replay.stop_replay()

        if processing_thread and processing_thread.is_alive():
            logger.info("Cleanup: Waiting for Data Processing thread..."); processing_thread.join(timeout=5.0)
            if processing_thread.is_alive(): logger.warning("Data Processing thread did not exit cleanly.")
            else: logger.info("Data Processing thread joined.")
        if dash_thread and dash_thread.is_alive():
             logger.info("Cleanup: Waiting for Dash thread (short)..."); dash_thread.join(timeout=5.0)
             if dash_thread.is_alive(): logger.warning("Dash thread did not exit cleanly.")

        logger.info("Shutdown complete."); print("\n --- App Exited --- \n")

print("DEBUG: main module loaded")
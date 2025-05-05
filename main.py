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
    main_logger = logging.getLogger("F1App.Main") # Ensure logger is obtained after setup
    main_logger.info("Application starting...")

    # Ensure necessary directories exist
    main_logger.info("Checking/Creating replay directory...")
    replay.ensure_replay_dir_exists() # Correctly uses function from replay module

    # --- Get Replay Files for Layout ---
    main_logger.info("Getting replay file options...")
    # Pass the list of files to the layout creation function
    initial_replay_options = replay.get_replay_files(config.REPLAY_DIR)

    # --- Create and Assign Layout ---
    # Pass replay options if layout function needs them
    app.layout = layout.create_layout() # Assuming create_layout handles options internally now
    main_logger.info("Dash layout created and assigned.")

    # --- Start Background Threads ---
    processing_thread = None
    dash_thread = None
    app_state.stop_event.clear() # Ensure stop event is clear initially

    try:
        main_logger.info("Starting background threads...")

        # Start data processing thread
        processing_thread = threading.Thread(target=data_processing.data_processing_loop, name="DataProcessingThread", daemon=True)
        processing_thread.start()
        main_logger.info("Data processing thread started.")

        # Start Dash server in a separate thread
        # Use 0.0.0.0 to make it accessible on the network if needed
        dash_thread = threading.Thread(target=lambda: app.run(debug=False, host='0.0.0.0', port=8050), name="DashServerThread", daemon=True)
        dash_thread.start()
        main_logger.info("Dash server thread started on http://localhost:8050 (or network IP)")


        # --- Main Application Loop (Keep-alive / Monitoring) ---
        main_logger.info("Entering main monitoring loop...")
        while not app_state.stop_event.is_set():
            # --- MODIFIED: Removed direct access to replay.replay_thread ---
            # Check health of essential threads?
            if not processing_thread.is_alive():
                 main_logger.error("CRITICAL: Data Processing thread died unexpectedly!")
                 app_state.stop_event.set() # Trigger shutdown
                 # Optionally update app_status here too
            if not dash_thread.is_alive():
                 main_logger.error("CRITICAL: Dash Server thread died unexpectedly!")
                 app_state.stop_event.set() # Trigger shutdown

            # Add checks for SignalR connection thread if needed (it manages its own lifecycle mostly)
            # conn_thread_obj = signalr_client.connection_thread # Get thread object if needed
            # if conn_thread_obj and not conn_thread_obj.is_alive() and app_state.app_status['state'] == 'Live':
            #     main_logger.error("SignalR connection thread died unexpectedly while Live!")
            #     # Decide how to handle this - attempt reconnect or shutdown?
            #     app_state.stop_event.set() # Example: Trigger shutdown

            time.sleep(5) # Check every 5 seconds

        main_logger.info("Stop event detected, exiting monitoring loop.")

    except KeyboardInterrupt:
        main_logger.info("KeyboardInterrupt detected, initiating shutdown.")
        app_state.stop_event.set() # Signal threads to stop
    except Exception as main_err:
         main_logger.error(f"Error in main execution block: {main_err}", exc_info=True)
         app_state.stop_event.set() # Ensure stop on error
    finally:
        main_logger.info("Starting final cleanup...")
        # Ensure stop event is set for all threads
        if not app_state.stop_event.is_set(): app_state.stop_event.set() # Ensure set

        # --- MODIFIED: Removed direct access to replay.replay_thread ---
        # --- Rely on stop functions to handle internal checks ---

        # Check which connection might still be running and try graceful stop
        # Use app_state to check status rather than thread objects directly where possible
        current_state = app_state.app_status.get("state", "Unknown").lower()

        if "live" in current_state or "connecting" in current_state or "stopping" in current_state: # Check if SignalR might be active
             main_logger.info("Cleanup: Stopping SignalR connection...")
             signalr_client.stop_connection() # Handles its own thread joining and internal checks

        # Call stop_replay regardless; it checks internally if it's running
        main_logger.info("Cleanup: Stopping Replay (if running)...")
        replay.stop_replay() # Handles its own thread joining and internal checks

        # Wait for processing thread (should stop quickly once queue empty/stop set)
        if processing_thread and processing_thread.is_alive():
            main_logger.info("Cleanup: Waiting for Data Processing thread...")
            processing_thread.join(timeout=5)
            if processing_thread.is_alive(): main_logger.warning("Data Processing thread did not exit cleanly.")
            else: main_logger.info("Data Processing thread joined.")

        # Dash thread stopping is handled by app.run_server exiting or SystemExit
        # We don't forcefully join it usually. If dash_thread died, loop would have exited.

        # Final file close call (just in case) - stop_connection/stop_replay should handle this now.
        # replay.close_live_file() # This is likely redundant now, called by stop functions

        main_logger.info("Shutdown complete.")
        print("\n --- App Exited --- \n")


print("DEBUG: main module loaded")
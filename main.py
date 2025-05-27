# main.py
import logging
import sys
import os 
import threading
import time
import faulthandler # Already in your file

import dash
from dash import Input, Output, html, dcc # dcc needed for Location
import dash_bootstrap_components as dbc

# --- Local Module Imports ---
import app_state
import config 
import utils
from app_instance import app, server # Import app AND server from app_instance
import fastf1

# Import the new main_app_layout and the specific page layouts
from layout import main_app_layout, dashboard_content_layout # dashboard_content_layout is your original dashboard content
from schedule_page import schedule_page_layout # The new schedule page

# Import callbacks so they are registered
import callbacks  # Your existing dashboard callbacks
# schedule_page.py also contains its own callbacks, they are registered when schedule_page is imported above.
import signalr_client # Keep if it has setup logic, though usually just functions
import data_processing # Your existing data_processing
import replay # Your existing replay

# --- Logging Setup (from your existing main.py) ---
# raw_message_formatter is defined but seems unused later, can be removed if so.
# raw_message_formatter = logging.Formatter(config.LOG_FORMAT_RAW_MESSAGE) 
def setup_logging():
    log_formatter = logging.Formatter(config.LOG_FORMAT_DEFAULT)
    actual_root_logger = logging.getLogger()
    actual_root_logger.setLevel(logging.INFO) 
    if actual_root_logger.hasHandlers():
        # print(f"Root logger initially has handlers: {actual_root_logger.handlers}. Clearing them.", file=sys.stderr) # DEBUG
        actual_root_logger.handlers.clear()
    root_console_handler = logging.StreamHandler(sys.stdout)
    root_console_handler.setFormatter(log_formatter)
    actual_root_logger.addHandler(root_console_handler)
    # print(f"Root logger configured with handler: {actual_root_logger.handlers}", file=sys.stderr) # DEBUG

    f1_app_logger = logging.getLogger("F1App")
    f1_app_logger.setLevel(logging.INFO) 
    f1_app_logger.propagate = True 
    # f1_app_logger.info("F1App application logger level set. Will use root handler.") # DEBUG

    logging.getLogger("SignalRCoreClient").setLevel(logging.WARNING)
    logging.getLogger("signalrcore").setLevel(logging.WARNING)
    
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.ERROR if not config.DASH_DEBUG_MODE else logging.INFO)
    werkzeug_logger.propagate = True 
    if werkzeug_logger.hasHandlers():
        # module_logger_temp = logging.getLogger("F1App") 
        # module_logger_temp.info(f"Werkzeug logger has handlers: {werkzeug_logger.handlers}. Clearing them.") # DEBUG
        werkzeug_logger.handlers.clear()
    # f1_app_logger.info(f"Werkzeug logger level set to {logging.getLevelName(werkzeug_logger.getEffectiveLevel())}. Will use root handler.") # DEBUG

    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('fastf1').setLevel(logging.INFO) # Keep FastF1 INFO for useful messages
    # actual_root_logger.info("Root logger configured. Specific loggers will propagate to it.") # DEBUG

# --- Initialize FastF1 Cache ---
# This is good to do early. config.FASTF1_CACHE_DIR should be a Path object from your config.
if hasattr(config, 'FASTF1_CACHE_DIR') and config.FASTF1_CACHE_DIR:
    try:
        config.FASTF1_CACHE_DIR.mkdir(parents=True, exist_ok=True) # Ensure it exists
        fastf1.Cache.enable_cache(config.FASTF1_CACHE_DIR)
        # print(f"FastF1 Cache enabled at: {config.FASTF1_CACHE_DIR}") # DEBUG
    except Exception as e:
        print(f"Error enabling FastF1 cache at {config.FASTF1_CACHE_DIR}: {e}")
else:
    print("Warning: FASTF1_CACHE_DIR not defined in config.py as a Path object or is None.")


# --- Assign Main App Layout ---
app.layout = main_app_layout # This now includes the sidebar and the content area

# --- Callback to Update Page Content Based on URL ---
@app.callback(
    Output("page-content", "children"),
    [Input("url", "pathname")]
)
def display_page(pathname):
    if pathname == "/schedule":
        return schedule_page_layout
    elif pathname == "/": # Default to dashboard
        return dashboard_content_layout
    else: # Handle 404 Not Found
        return dbc.Container([
            html.H1("404: Not found", className="text-danger display-3 mt-5"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised.", className="lead"),
            dbc.Button("Go to Dashboard", href="/", color="primary", size="lg")
        ], fluid=True, className="py-5 text-center bg-dark text-light vh-100")
        
        
app.clientside_callback(
    dash.ClientsideFunction(
        namespace='clientside',
        function_name='getTimezone' # This JS function must exist in assets/custom_script.js
    ),
    Output('user-timezone-store-data', 'data'), # This store MUST be in your main_app_layout
    Input('url', 'pathname') # Triggered on page load/navigation
)


# --- Main Execution Logic (adapted from your existing main.py) ---
if __name__ == '__main__':
    faulthandler.enable() # For debugging hard crashes
    setup_logging()
    logger = logging.getLogger("F1App.Main")
    logger.info("Application starting...")

    logger.info("Checking/Creating replay directory (if not done by layout)...")
    replay.ensure_replay_dir_exists() # ensure_replay_dir_exists also ensures TARGET_SAVE_DIRECTORY

    # Note: app.layout is already assigned above.
    # The layout.create_layout() call is now part of defining dashboard_content_layout in layout.py.
    logger.info("Dash layout structure assigned.")

    processing_thread = None
    # dash_thread is not strictly needed to be tracked here if app.run is blocking in main thread
    app_state.stop_event.clear()

    try:
        logger.info("Starting background data processing thread...")
        processing_thread = threading.Thread(
            target=data_processing.data_processing_loop,
            name="DataProcessingThread", daemon=True)
        processing_thread.start()
        logger.info("Data processing thread started.")

        logger.info(f"Dash server starting on http://{config.DASH_HOST}:{config.DASH_PORT}")
        # For development, app.run_server is fine. For production, use Gunicorn/Waitress with `server`
        app.run( # Changed from app.run to app.run_server for Dash convention
            host=config.DASH_HOST,
            port=config.DASH_PORT,
            debug=config.DASH_DEBUG_MODE,
            use_reloader=False # Important: Set to False when running in this threaded manner or with external changes
        )
        # Code here will run after server stops if it's not a hard stop/crash

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt detected, initiating shutdown.")
    except Exception as main_err: # Catch other potential errors during server startup/run
        logger.error(f"Critical error in main execution or server run: {main_err}", exc_info=True)
    finally:
        logger.info("Initiating application shutdown sequence...")
        if not app_state.stop_event.is_set():
            logger.info("Setting global stop event.")
            app_state.stop_event.set()

        # Gracefully stop other components
        with app_state.app_state_lock: # Ensure thread-safe access to app_status
            current_app_mode = app_state.app_status.get("state", "Unknown").lower()
        
        if "live" in current_app_mode or "connecting" in current_app_mode:
            logger.info("Cleanup: Stopping SignalR connection...")
            signalr_client.stop_connection()
        
        logger.info("Cleanup: Stopping Replay (if running)...")
        replay.stop_replay() # Should handle if not running

        if processing_thread and processing_thread.is_alive():
            logger.info("Cleanup: Waiting for Data Processing thread to join...")
            processing_thread.join(timeout=5.0)
            if processing_thread.is_alive():
                logger.warning("Data Processing thread did not exit cleanly after timeout.")
            else:
                logger.info("Data Processing thread joined successfully.")
        
        logger.info("Shutdown complete. --- App Exited ---")

# print("DEBUG: main module logic processed") # DEBUG
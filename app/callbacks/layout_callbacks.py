# app/callbacks/.py
"""
Callbacks for managing the overall application layout, such as
the sidebar, page routing, and collapsible sections.
"""
import time
import inspect
import logging

from dash.dependencies import Input, Output, State
import dash
import dash_bootstrap_components as dbc
from dash import html

from app_instance import app
import config

logger = logging.getLogger(__name__)

@app.callback(
    [Output("sidebar", "style"),
     Output("page-content", "style", allow_duplicate=True),
     Output("sidebar-state-store", "data"),
     Output("sidebar-toggle-signal", "data")],
    [Input("sidebar-toggle", "n_clicks")],
    [State("sidebar-state-store", "data")],
    prevent_initial_call=True
)
def toggle_sidebar(n_clicks, sidebar_state_data):
    is_open_currently = sidebar_state_data.get(
        'is_open', False)  # Default to False if no data

    # if n_clicks is None or n_clicks == 0: # This might prevent toggling if n_clicks starts at 0 and is then 1
    if not n_clicks:  # Simpler check if n_clicks is None or 0 (initial state)
        # Don't toggle on initial load automatically
        new_is_open_state = is_open_currently
    else:
        new_is_open_state = not is_open_currently

    if new_is_open_state:
        sidebar_style_to_apply = config.SIDEBAR_STYLE_VISIBLE
        content_style_to_apply = config.CONTENT_STYLE_WITH_SIDEBAR
    else:
        sidebar_style_to_apply = config.SIDEBAR_STYLE_HIDDEN
        content_style_to_apply = config.CONTENT_STYLE_FULL_WIDTH

    current_store_val = {'is_open': new_is_open_state}

    # Only generate a new signal if there was a click that caused a toggle
    signal_data = dash.no_update
    if n_clicks:  # and (new_is_open_state != is_open_currently): # If state actually changed
        signal_data = time.time()

    return sidebar_style_to_apply, content_style_to_apply, current_store_val, signal_data

@app.callback(
    Output("collapse-controls", "is_open"),
    [Input("collapse-controls-button", "n_clicks")],
    [State("collapse-controls", "is_open")],
    prevent_initial_call=True,
)
def toggle_controls_collapse(n, is_open):
    if n:
        return not is_open
    return is_open
    
@app.callback(
    Output("debug-data-accordion-item", "className"),
    Input("debug-mode-switch", "value"),
)
def toggle_debug_data_visibility(debug_mode_enabled):
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if debug_mode_enabled:
        logger.info("Debug mode enabled: Showing 'Other Data Streams'.")
        return "mt-1" # Bootstrap margin top class
    else:
        logger.info("Debug mode disabled: Hiding 'Other Data Streams'.")
        logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
        return "d-none" # Bootstrap display none class
        
@app.callback(
    [Output('status-alert', 'is_open'),
     Output('status-alert', 'children'),
     Output('status-alert', 'color')],
    [Input('connect-button', 'n_clicks'),
     Input('replay-button', 'n_clicks'),
     Input('stop-reset-button', 'n_clicks'),
     Input('connection-status', 'children')], # Listen to the text output of the status
    [State('replay-file-selector', 'value')],
    prevent_initial_call=True
)
def update_status_alert(connect_clicks, replay_clicks, stop_reset_clicks,
                        connection_status_text, selected_replay_file):
    """
    Shows a temporary alert to the user based on their actions or changes
    in the application's connection status.
    """
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update, dash.no_update, dash.no_update

    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    # Default outputs
    is_open = False
    message = ""
    color = "info"

    # --- Handle Button Clicks for Immediate Feedback ---
    if triggered_id == 'connect-button':
        is_open = True
        message = "Attempting to connect to live feed..."
        color = "info"
        return is_open, message, color

    if triggered_id == 'replay-button':
        if not selected_replay_file:
            is_open = True
            message = "Please select a replay file first!"
            color = "warning"
        else:
            is_open = True
            message = f"Starting replay for {selected_replay_file}..."
            color = "info"
        return is_open, message, color

    if triggered_id == 'stop-reset-button':
        is_open = True
        message = "Session stopped and reset."
        color = "secondary"
        return is_open, message, color

    # --- Handle Status Changes for Asynchronous Events ---
    if triggered_id == 'connection-status':
        status_text = connection_status_text.lower()
        if "error" in status_text or "fail" in status_text or "file not found" in status_text:
            is_open = True
            # We can use the text from the connection-status component directly
            message = f"Error: {connection_status_text}"
            color = "danger"
        elif "playback complete" in status_text:
            is_open = True
            message = "Replay has finished."
            color = "success"
        elif "subscribed" in status_text and "live" in status_text:
            is_open = True
            message = "Live connection established."
            color = "success"
        
        if is_open:
            return is_open, message, color

    return dash.no_update, dash.no_update, dash.no_update

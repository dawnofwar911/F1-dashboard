# app/callbacks/clientside.py
"""
Clientside functions callbacks.
"""
import inspect
import time
import logging

import dash
from dash import no_update, Patch
from dash.dependencies import Input, Output, State, ClientsideFunction

from app_instance import app
import app_state
import config

logger = logging.getLogger(__name__)

@app.callback(
    Output('clientside-update-interval', 'disabled'),
    [Input('connect-button', 'n_clicks'),
     Input('replay-button', 'n_clicks'),
     Input('stop-reset-button', 'n_clicks'),
     Input('interval-component-fast', 'n_intervals')],
    [State('clientside-update-interval', 'disabled'),
     State('replay-file-selector', 'value'),
     State("url", "pathname")]  # <<< ADDED: Get the current page's URL
)
def toggle_clientside_interval(connect_clicks, replay_clicks,
                               stop_reset_clicks,
                               fast_interval_tick, currently_disabled, selected_replay_file, current_pathname: str):
    if current_pathname != '/':
        # If the interval is not already disabled, disable it. Otherwise, do nothing.
        return True if not currently_disabled else dash.no_update
    session_state = app_state.get_or_create_session_state()
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered and ctx.triggered[0] else None

    if not triggered_id:
        return no_update

    with session_state.lock:
        current_app_s = session_state.app_status.get("state", "Idle")

    if triggered_id in ['connect-button', 'replay-button']:
        if triggered_id == 'replay-button' and not selected_replay_file:
            logger.info(
                f"Replay button clicked, but no file selected ({config.TEXT_REPLAY_SELECT_FILE}). Clientside interval remains disabled.") # Use constant
            return True

        logger.debug(
            f"Attempting to enable clientside-update-interval due to '{triggered_id}'.")
        return False

    elif triggered_id == 'stop-reset-button':
        logger.debug(
            f"Disabling clientside-update-interval due to '{triggered_id}'.")
        return True

    elif triggered_id == 'interval-component-fast':
        if current_app_s in ["Live", "Replaying"]:
            if currently_disabled:
                logger.debug(
                    f"Fast interval: App is '{current_app_s}', enabling clientside interval.")
                return False
            return no_update
        else:
            if not currently_disabled:
                logger.debug(
                    f"Fast interval: App is '{current_app_s}', disabling clientside interval.")
                return True
            return no_update

    logger.warning(f"toggle_clientside_interval: Unhandled triggered_id '{triggered_id}'. Returning no_update.")
    return no_update

@app.callback(
    Output('clientside-update-interval', 'interval'),
    Input('replay-speed-slider', 'value'),
    State('clientside-update-interval', 'disabled'),
    prevent_initial_call=True
)
def update_clientside_interval_speed(replay_speed, interval_disabled):
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if interval_disabled or replay_speed is None:
        return dash.no_update

    try:
        speed = float(replay_speed)
        if speed <= 0: speed = 1.0
    except (ValueError, TypeError):
        speed = 1.0

    base_interval_ms = 1250 # Base interval for car marker updates on map
    new_interval_ms = max(350, int(base_interval_ms / speed)) # Ensure it doesn't go too fast

    logger.debug(f"Adjusting clientside-update-interval to {new_interval_ms}ms for replay speed {speed}x")
    logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
    return new_interval_ms
    
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='animateCarMarkers'
    ),
    Output('track-map-graph', 'figure'), # Outputting to figure
    [Input('car-positions-store', 'data'),          # Trigger on new car positions
     Input('track-map-figure-version-store', 'data')], # Trigger if base figure changes (e.g. new track)
    State('track-map-graph', 'figure'),             # Current figure to update
    State('track-map-graph', 'id'),                 # ID of the graph component
    State('clientside-update-interval', 'interval') # Current animation interval speed
)

# Clientside callback for handling resize, if needed (see custom_script.js)
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='setupTrackMapResizeListener' # Ensure this matches your JS function
    ),
    Output('track-map-graph', 'figure', allow_duplicate=True), # Dummy output or can update figure if resize logic is complex
    Input('track-map-graph', 'figure'), # Triggered when figure initially renders or changes
    prevent_initial_call='initial_duplicate' # Avoids running on initial load before figure exists
)

app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='setupClickToFocusListener'
    ),
    Output('track-map-graph', 'id'), # Dummy output, just needs to target something on the graph
    Input('track-map-graph', 'figure'), # Trigger when the figure is first drawn or updated
    prevent_initial_call=False # Allow it to run on initial load
)

app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='pollClickDataAndUpdateStore'
    ),
    Output('clicked-car-driver-number-store', 'data'),
    Input('clientside-click-poll-interval', 'n_intervals'),
    State('js-click-data-holder', 'children'),
    prevent_initial_call=True # Read the data JS wrote
)
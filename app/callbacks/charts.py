# app/callbacks/charts.py
"""
Callbacks for generating and updating all Plotly charts and graphs.
"""
import logging
import time
import inspect
import copy
from typing import Optional
import json

import dash
from dash.dependencies import Input, Output, State
from dash import dcc, html, dash_table, no_update, Patch
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

from app_instance import app
import app_state
import config
import utils

logger = logging.getLogger(__name__)

@app.callback(
    [Output('driver-details-output', 'children'),      # For basic driver Name/Team
     Output('lap-selector-dropdown', 'options'),       # For Telemetry Tab
     Output('lap-selector-dropdown', 'value'),         # For Telemetry Tab
     Output('lap-selector-dropdown', 'disabled'),      # For Telemetry Tab
     Output('telemetry-graph', 'figure'),              # For Telemetry Tab
     Output('stint-history-table', 'data'),            # For Stint History Tab
     Output('stint-history-table', 'columns')],        # For Stint History Tab (if dynamic, else set in layout)
    [Input('driver-select-dropdown', 'value'),         # Driver selected
     Input('driver-focus-tabs', 'active_tab'),         # Which tab is active
     Input('lap-selector-dropdown', 'value')],         # Lap selected for telemetry (if telemetry tab is active)
    [State('telemetry-graph', 'figure'),               # Current telemetry figure state
     State('stint-history-table', 'columns'),          # Current columns for stint table (if needed)
     State('session-preferences-store', 'data'),
    ]
)
def update_driver_focus_content(selected_driver_number, active_tab_id, 
                                selected_lap_for_telemetry, 
                                current_telemetry_figure, current_stint_table_columns, session_prefs: Optional[dict]):
    session_state = app_state.get_or_create_session_state()
    overall_callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START_OVERALL")

    # --- FIXED ---
    # Load the 'use_mph' preference from the session preferences store.
    # Fallback to the default value from the config if it's not set.
    session_prefs = session_prefs or {}
    use_mph_pref = session_prefs.get('use_mph', config.USE_MPH)
    # --- END FIX ---
    
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered and ctx.triggered[0] else 'N/A'
    
    logger.debug(
        f"'{func_name}': Trigger='{triggered_id}', Driver='{selected_driver_number}', "
        f"ActiveTab='{active_tab_id}', SelectedLap='{selected_lap_for_telemetry}'"
    )

    driver_basic_details_children = [html.P(config.TEXT_DRIVER_SELECT, style={'fontSize':'0.8rem', 'padding':'5px'})]
    telemetry_lap_options = config.DROPDOWN_NO_LAPS_OPTIONS
    telemetry_lap_value = None
    telemetry_lap_disabled = True
    fig_telemetry = utils.create_empty_figure_with_message(
        config.TELEMETRY_WRAPPER_HEIGHT, config.INITIAL_TELEMETRY_UIREVISION,
        config.TEXT_DRIVER_SELECT_LAP, config.TELEMETRY_MARGINS_EMPTY
    )
    stint_history_data = []
    stint_history_columns_output = no_update 

    if not selected_driver_number:
        if current_telemetry_figure and \
           current_telemetry_figure.get('layout', {}).get('uirevision') == config.INITIAL_TELEMETRY_UIREVISION:
            fig_telemetry_output = no_update
        else:
            fig_telemetry_output = fig_telemetry
        
        logger.debug(f"Callback '{func_name}' END_OVERALL (No Driver). Total Took: {time.monotonic() - overall_callback_start_time:.4f}s")
        return (driver_basic_details_children, telemetry_lap_options, telemetry_lap_value, telemetry_lap_disabled, fig_telemetry_output,
                stint_history_data, stint_history_columns_output)

    driver_num_str = str(selected_driver_number)
    driver_info_state = {}
    all_stints_for_driver = []
    available_telemetry_laps = []

    # --- Initial Data Fetch (Locking for session_state access) ---
    lock_acquisition_start_time = time.monotonic()
    with session_state.lock:
        lock_acquired_time = time.monotonic()
        logger.debug(f"Lock in '{func_name}' (Initial Fetch) - ACQUIRED. Wait: {lock_acquired_time - lock_acquisition_start_time:.4f}s")
        critical_section_start_time = time.monotonic()
        
        driver_info_state = session_state.timing_state.get(driver_num_str, {}).copy()
        all_stints_for_driver = copy.deepcopy(session_state.driver_stint_data.get(driver_num_str, []))
        available_telemetry_laps = sorted(list(session_state.telemetry_data.get(driver_num_str, {}).keys()))
        
        logger.debug(f"Lock in '{func_name}' (Initial Fetch) - HELD for critical section: {time.monotonic() - critical_section_start_time:.4f}s")

    # --- Driver Basic Details ---
    if driver_info_state:
        tla = driver_info_state.get('Tla', '?')
        driver_basic_details_children = [
            html.H6(f"#{driver_info_state.get('RacingNumber', driver_num_str)} {tla} - {driver_info_state.get('FullName', 'Unknown')}", 
                    style={'marginTop': '0px', 'marginBottom':'2px', 'fontSize':'0.9rem'}),
            html.P(f"Team: {driver_info_state.get('TeamName', '?')}", 
                   style={'fontSize':'0.75rem', 'marginBottom':'0px', 'color': 'lightgrey'})
        ]
    else:
        driver_basic_details_children = [html.P(f"Details for driver {driver_num_str} not found.", style={'color':'orange'})]
        tla = driver_num_str

    # --- Tab Specific Logic ---
    if active_tab_id == "tab-telemetry":
        if available_telemetry_laps:
            telemetry_lap_options = [{'label': f'Lap {l}', 'value': l} for l in available_telemetry_laps]
            telemetry_lap_disabled = False
            
            if triggered_id in ['driver-select-dropdown', 'driver-focus-tabs'] or \
               not selected_lap_for_telemetry or \
               selected_lap_for_telemetry not in available_telemetry_laps:
                telemetry_lap_value = available_telemetry_laps[-1]
            else:
                telemetry_lap_value = selected_lap_for_telemetry
        
            if telemetry_lap_value:
                data_plot_uirevision_telemetry = f"telemetry_data_{driver_num_str}_{telemetry_lap_value}"

                if current_telemetry_figure and \
                   current_telemetry_figure.get('layout',{}).get('uirevision') == data_plot_uirevision_telemetry and \
                   triggered_id == 'driver-focus-tabs':
                    logger.debug(f"'{func_name}': Telemetry figure for {driver_num_str} Lap {telemetry_lap_value} already rendered, no_update on tab switch.")
                    fig_telemetry = no_update
                else:
                    lap_data = {}
                    with session_state.lock:
                        lap_data = copy.deepcopy(session_state.telemetry_data.get(driver_num_str, {}).get(telemetry_lap_value, {}))

                    if lap_data:
                        timestamps_str = lap_data.get('Timestamps', [])
                        timestamps_dt = [utils.parse_iso_timestamp_safe(ts) for ts in timestamps_str]
                        valid_indices = [i for i, dt_obj in enumerate(timestamps_dt) if dt_obj is not None]

                        if valid_indices:
                            timestamps_plot = [timestamps_dt[i] for i in valid_indices]
                            channels = ['Speed', 'RPM', 'Throttle', 'Brake', 'Gear', 'DRS']
                            
                            subplot_titles = list(channels)
                            if use_mph_pref:
                                try:
                                    speed_index = subplot_titles.index('Speed')
                                    subplot_titles[speed_index] = 'Speed (MPH)'
                                except ValueError:
                                    pass # 'Speed' not in channels, ignore

                            fig_telemetry = make_subplots(
                                rows=len(channels), cols=1, shared_xaxes=True,
                                subplot_titles=subplot_titles, vertical_spacing=0.06
                            )

                            for i, channel in enumerate(channels):
                                y_data_raw = lap_data.get(channel, [])
                                y_data_plot = [(y_data_raw[idx] if idx < len(y_data_raw) else None) for idx in valid_indices]
                                
                                if channel == 'Speed' and use_mph_pref:
                                    y_data_plot = utils.convert_kph_to_mph(y_data_plot)

                                if channel == 'DRS':
                                    drs_plot = [1 if val in [10, 12, 14] else 0 for val in y_data_plot]
                                    fig_telemetry.add_trace(go.Scattergl(x=timestamps_plot, y=drs_plot, mode='lines', name=channel, line_shape='hv', connectgaps=False), row=i+1, col=1)
                                    fig_telemetry.update_yaxes(fixedrange=True, tickvals=[0,1], ticktext=['Off','On'], range=[-0.1,1.1], row=i+1, col=1, title_text="", title_standoff=2, title_font_size=9, tickfont_size=8)
                                else:
                                    fig_telemetry.add_trace(go.Scattergl(x=timestamps_plot, y=y_data_plot, mode='lines', name=channel, connectgaps=False), row=i+1, col=1)
                                    fig_telemetry.update_yaxes(fixedrange=True, row=i+1, col=1, title_text="", title_standoff=2, title_font_size=9, tickfont_size=8)
                            
                            fig_telemetry.update_layout(
                                template='plotly_dark', height=config.TELEMETRY_WRAPPER_HEIGHT,
                                hovermode="x unified", showlegend=False, margin=config.TELEMETRY_MARGINS_DATA,
                                title_text=f"<b>{tla} - Lap {telemetry_lap_value} Telemetry</b>",
                                title_x=0.5, title_y=0.98, title_font_size=12,
                                uirevision=data_plot_uirevision_telemetry,
                                annotations=[] 
                            )

    elif active_tab_id == "tab-stint-history":
        fig_telemetry = no_update
        if all_stints_for_driver:
            stint_history_data = []
            for stint_entry in all_stints_for_driver:
                processed_entry = stint_entry.copy()
                processed_entry['is_new_tyre_display'] = 'Y' if stint_entry.get('is_new_tyre') else 'N'
                stint_history_data.append(processed_entry)
        else:
            stint_history_data = [{
                'stint_number': "No stint data available.", 'start_lap': '-', 'compound': '-', 
                'is_new_tyre_display': '-', 'tyre_age_at_stint_start': '-', 
                'end_lap': '-', 'total_laps_on_tyre_in_stint': '-', 
                'tyre_total_laps_at_stint_end': '-'
            }]

    else: # Unknown or default tab
        fig_telemetry = no_update

    logger.debug(f"Callback '{func_name}' END_OVERALL. Total Took: {time.monotonic() - overall_callback_start_time:.4f}s")
    return (driver_basic_details_children, telemetry_lap_options, telemetry_lap_value, telemetry_lap_disabled, fig_telemetry,
            stint_history_data, stint_history_columns_output)

@app.callback(
    Output('current-track-layout-cache-key-store', 'data'),
    Input('interval-component-medium', 'n_intervals'),
    State('current-track-layout-cache-key-store', 'data')
)
def update_current_session_id_for_map(n_intervals, existing_session_id_in_store):
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    with session_state.lock:
        year = session_state.session_details.get('Year')
        circuit_key = session_state.session_details.get('CircuitKey')
        app_status_state = session_state.app_status.get("state", "Idle")

    if not year or not circuit_key or app_status_state in ["Idle", "Stopped", "Error"]:
        if existing_session_id_in_store is not None:
            # Clear the selected driver if session changes or becomes invalid
            with session_state.lock:
                if session_state.selected_driver_for_map_and_lap_chart is not None:
                    logger.debug("Clearing selected_driver_for_map_and_lap_chart due to invalid/changed session.")
                    session_state.selected_driver_for_map_and_lap_chart = None
            return None
        return dash.no_update

    current_session_id = f"{year}_{circuit_key}"

    if current_session_id != existing_session_id_in_store:
        logger.debug(
            f"Updating current-track-layout-cache-key-store to: {current_session_id}. Clearing selected driver.")
        with session_state.lock: # Clear selected driver on session change
            session_state.selected_driver_for_map_and_lap_chart = None
        logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
        return current_session_id

    return dash.no_update
    
@app.callback(
    Output('car-positions-store', 'data'),
    Input('clientside-update-interval', 'n_intervals'),
)
def update_car_data_for_clientside(n_intervals):
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if n_intervals == 0: # Or check if None
        return dash.no_update
    
    lock_acquisition_start_time = time.monotonic()
    with session_state.lock:
        lock_acquired_time = time.monotonic()
        logger.debug(f"Lock in '{func_name}' - ACQUIRED. Wait: {lock_acquired_time - lock_acquisition_start_time:.4f}s")
    
        critical_section_start_time = time.monotonic()
        current_app_status = session_state.app_status.get("state", "Idle")
        timing_state_snapshot = session_state.timing_state.copy()
        # Get the currently selected driver for highlighting
        selected_driver_rno = session_state.selected_driver_for_map_and_lap_chart
        logger.debug(f"Lock in '{func_name}' - HELD for critical section: {time.monotonic() - critical_section_start_time:.4f}s")

    if current_app_status not in ["Live", "Replaying"] or not timing_state_snapshot:
        # Ensure to include selected_driver even if inactive, so JS can clear highlight
        return {'status': 'inactive', 'timestamp': time.time(), 'selected_driver': selected_driver_rno}


    processed_car_data = {}
    for car_num_str, driver_state in timing_state_snapshot.items():
        if not isinstance(driver_state, dict):
            continue

        pos_data = driver_state.get('PositionData')
        if not pos_data or 'X' not in pos_data or 'Y' not in pos_data:
            continue

        try:
            x_val = float(pos_data['X'])
            y_val = float(pos_data['Y'])
        except (TypeError, ValueError):
            continue

        team_colour_hex = driver_state.get('TeamColour', '808080')
        if not team_colour_hex.startswith('#'):
            team_colour_hex = '#' + team_colour_hex

        processed_car_data[car_num_str] = {
            'x': x_val,
            'y': y_val,
            'color': team_colour_hex,
            'tla': driver_state.get('Tla', car_num_str),
            'status': driver_state.get('Status', 'Unknown').lower()
        }

    if not processed_car_data: # If after processing, there's nothing, send no update
        return {'status': 'active_no_cars', 'timestamp': time.time(), 'selected_driver': selected_driver_rno}


    # Add the selected driver information to the output for JS
    output_data = {
        'status': 'active', # Indicate data is active
        'timestamp': time.time(),
        'selected_driver': selected_driver_rno, # Pass the selected driver's racing number
        'cars': processed_car_data
    }
    logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
    return output_data
    
@app.callback(
    Output('track-map-graph', 'figure', allow_duplicate=True),
    Output('track-map-figure-version-store', 'data', allow_duplicate=True),
    Output('track-map-yellow-key-store', 'data'),
    [Input('interval-component-medium', 'n_intervals'),
     Input('current-track-layout-cache-key-store', 'data'),
     Input('sidebar-toggle-signal', 'data')],
    [State('track-map-graph', 'figure'),
     State('track-map-figure-version-store', 'data'),
     State('track-map-yellow-key-store', 'data'),
     State("url", "pathname")], # <<< ADDED: Get the current page's URL
    prevent_initial_call='initial_duplicate'
)
def initialize_track_map(n_intervals, expected_session_id, sidebar_toggled_signal, # <<< ADDED ARGUMENT
                         current_track_map_figure_state,
                         current_figure_version_in_store_state,
                         previous_rendered_yellow_key_from_store, current_pathname: str):
    if current_pathname != '/':
        return dash.no_update, dash.no_update, dash.no_update
    session_state = app_state.get_or_create_session_state()
    overall_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    ctx = dash.callback_context
    triggered_prop_id = ctx.triggered[0]['prop_id'] if ctx.triggered and ctx.triggered[0] else 'Unknown.Trigger' # Defensive access
    triggering_input_id = triggered_prop_id.split('.')[0]

    logger.debug(f"INIT_TRACK_MAP Trigger: {triggering_input_id}, SID: {expected_session_id}, PrevYellowKey: {previous_rendered_yellow_key_from_store}, SidebarSignal: {sidebar_toggled_signal}")

    lock_acquisition_start_time = time.monotonic()
    with session_state.lock:
        lock_acquired_time = time.monotonic()
        logger.debug(f"Lock in '{func_name}' - ACQUIRED. Wait: {lock_acquired_time - lock_acquisition_start_time:.4f}s")
        
        critical_section_start_time = time.monotonic()
        cached_data = session_state.track_coordinates_cache.copy()
        driver_list_snapshot = session_state.timing_state.copy() 
        active_yellow_sectors_snapshot = set(session_state.active_yellow_sectors)
        logger.debug(f"Lock in '{func_name}' - HELD for critical section: {time.monotonic() - critical_section_start_time:.4f}s")

    if not expected_session_id or not isinstance(expected_session_id, str) or '_' not in expected_session_id:
        fig_empty = utils.create_empty_figure_with_message(config.TRACK_MAP_WRAPPER_HEIGHT, f"empty_map_init_{time.time()}", config.TEXT_TRACK_MAP_DATA_WILL_LOAD, config.TRACK_MAP_MARGINS)
        fig_empty.layout.plot_bgcolor = 'rgb(30,30,30)'; fig_empty.layout.paper_bgcolor = 'rgba(0,0,0,0)'
        return fig_empty, f"empty_map_ver_{time.time()}", ""

    is_cache_ready_for_base = (cached_data.get('session_key') == expected_session_id and cached_data.get('x') and cached_data.get('y'))
    if not is_cache_ready_for_base:
        fig_loading = utils.create_empty_figure_with_message(config.TRACK_MAP_WRAPPER_HEIGHT, f"loading_{expected_session_id}_{time.time()}", f"{config.TEXT_TRACK_MAP_LOADING_FOR_SESSION_PREFIX}{expected_session_id}...", config.TRACK_MAP_MARGINS)
        fig_loading.layout.plot_bgcolor = 'rgb(30,30,30)'; fig_loading.layout.paper_bgcolor = 'rgba(0,0,0,0)'
        return fig_loading, f"loading_ver_{time.time()}", ""

    corners_c = len(cached_data.get('corners_data') or [])
    lights_c = len(cached_data.get('marshal_lights_data') or [])
    layout_structure_version = "v3.3_placeholders"
    target_persistent_layout_uirevision = f"trackmap_layout_{expected_session_id}_c{corners_c}_l{lights_c}_{layout_structure_version}"
    active_yellow_sectors_key_for_current_render = "_".join(sorted(map(str, list(active_yellow_sectors_snapshot))))

    needs_full_rebuild = False
    current_layout_uirevision_from_state = current_track_map_figure_state.get('layout', {}).get('uirevision') if current_track_map_figure_state and current_track_map_figure_state.get('layout') else None
    
    is_sidebar_toggle_trigger = triggering_input_id == 'sidebar-toggle-signal'

    if triggering_input_id == 'current-track-layout-cache-key-store': 
        needs_full_rebuild = True
    elif not current_track_map_figure_state or not current_track_map_figure_state.get('data') or not current_track_map_figure_state.get('layout'): # Check data too
        needs_full_rebuild = True
    # If uirevision is different AND it's not a temporary one from a previous sidebar toggle, then rebuild.
    elif current_layout_uirevision_from_state != target_persistent_layout_uirevision and \
         not (current_layout_uirevision_from_state and current_layout_uirevision_from_state.startswith("trackmap_resized_view_")):
        needs_full_rebuild = True

    processed_previous_yellow_key = previous_rendered_yellow_key_from_store
    if previous_rendered_yellow_key_from_store is None: processed_previous_yellow_key = ""

    # If not a full rebuild, and yellow sectors haven't changed, AND sidebar didn't toggle, then no update.
    if not needs_full_rebuild and \
       processed_previous_yellow_key == active_yellow_sectors_key_for_current_render and \
       not is_sidebar_toggle_trigger:
        logger.debug(f"INIT_TRACK_MAP --- No structural change, yellow key same, sidebar not toggled. No Python figure update.")
        return no_update, no_update, no_update

    figure_output: go.Figure
    version_store_output = dash.no_update # Default to no_update for version unless changed
    yellow_key_store_output = active_yellow_sectors_key_for_current_render

    # Determine the uirevision for the output figure
    final_uirevision_for_output_figure = target_persistent_layout_uirevision
    if is_sidebar_toggle_trigger:
        final_uirevision_for_output_figure = f"trackmap_resized_view_{time.time()}" # Unique uirevision for resize
        logger.info(f"Sidebar toggle: Using NEW uirevision for map: {final_uirevision_for_output_figure}")
        version_store_output = f"track_resized_ver_{time.time()}" 


    if needs_full_rebuild or (is_sidebar_toggle_trigger and not current_track_map_figure_state):
        rebuild_start_time = time.monotonic()
        logger.info(f"Performing FULL track map data rebuild. Target Layout uirevision: {final_uirevision_for_output_figure}")
        fig_data = []
        valid_corners = [c for c in (cached_data.get('corners_data') or []) if c.get('x') is not None and c.get('y') is not None]
        valid_lights = [m for m in (cached_data.get('marshal_lights_data') or []) if m.get('x') is not None and m.get('y') is not None]
        
        fig_data.append(go.Scatter(x=list(cached_data['x']), y=list(cached_data['y']), mode='lines', line=dict(color='grey', width=getattr(config, 'TRACK_LINE_WIDTH', 2)), name='Track', hoverinfo='none'))
        if valid_corners:
            fig_data.append(go.Scatter(
                x=[c['x'] for c in valid_corners], y=[c['y'] for c in valid_corners], mode='markers+text', 
                marker=dict(size=config.CORNER_MARKER_SIZE, color=config.CORNER_MARKER_COLOR, symbol='circle-open'),
                text=[str(c['number']) for c in valid_corners], textposition=config.CORNER_TEXT_POSITION,
                textfont=dict(size=config.CORNER_TEXT_SIZE, color=config.CORNER_TEXT_COLOR),
                dx=config.CORNER_TEXT_DX, dy=config.CORNER_TEXT_DY, name='Corners', hoverinfo='text'))
        if valid_lights:
            fig_data.append(go.Scatter(x=[m['x'] for m in valid_lights], y=[m['y'] for m in valid_lights], mode='markers', marker=dict(size=getattr(config, 'MARSHAL_MARKER_SIZE', 5), color=getattr(config, 'MARSHAL_MARKER_COLOR', 'orange'), symbol='diamond'), name='Marshal Posts', hoverinfo='text', text=[f"M{m['number']}" for m in valid_lights]))
        for i in range(config.MAX_YELLOW_SECTOR_PLACEHOLDERS):
            fig_data.append(go.Scatter(x=[None], y=[None], mode='lines', line=dict(color=getattr(config, 'YELLOW_FLAG_COLOR', 'yellow'), width=getattr(config, 'YELLOW_FLAG_WIDTH', 4)), name=f"{config.YELLOW_FLAG_PLACEHOLDER_NAME_PREFIX}{i}", hoverinfo='name', opacity=getattr(config, 'YELLOW_FLAG_OPACITY', 0.7), visible=False))
        for car_num_str_init, driver_state_init in driver_list_snapshot.items():
            if not isinstance(driver_state_init, dict): continue
            tla_init = driver_state_init.get('Tla', car_num_str_init); team_color_hex_init = driver_state_init.get('TeamColour', '808080')
            if not team_color_hex_init.startswith('#'): team_color_hex_init = '#' + team_color_hex_init.replace("#", "")
            if len(team_color_hex_init) not in [4, 7]: team_color_hex_init = '#808080'
            racing_number_for_uid = driver_state_init.get('RacingNumber', car_num_str_init)
            fig_data.append(go.Scatter(x=[None], y=[None], mode='markers+text', name=tla_init, uid=str(racing_number_for_uid), marker=dict(size=getattr(config, 'CAR_MARKER_SIZE', 8), color=team_color_hex_init, line=dict(width=1, color='Black')), textfont=dict(size=getattr(config, 'CAR_MARKER_TEXT_SIZE', 8), color='white'), textposition='middle right', hoverinfo='text', text=tla_init))

        fig_layout = go.Layout(
            template='plotly_dark', 
            uirevision=final_uirevision_for_output_figure, # Use the determined uirevision
            autosize=True,                                
            xaxis=dict(visible=False, fixedrange=True, 
                       range=list(cached_data.get('range_x', [0,1])), 
                       autorange=False, 
                       automargin=True),
            yaxis=dict(visible=False, fixedrange=True, 
                       scaleanchor="x", scaleratio=1, 
                       range=list(cached_data.get('range_y', [0,1])), 
                       autorange=False, 
                       automargin=True),
            showlegend=False, plot_bgcolor='rgb(30,30,30)', paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'), margin=config.TRACK_MAP_MARGINS,
            height=None, width=None, # Explicitly None for autosize
            annotations=[]
        )
        figure_output = go.Figure(data=fig_data, layout=fig_layout)
        if version_store_output is dash.no_update : 
            version_store_output = f"trackbase_rebuilt_{expected_session_id}_{time.time()}"
        logger.debug(f"'{func_name}' - FULL track map rebuild: {time.monotonic() - rebuild_start_time:.4f}s")
    else: # Not a full structural rebuild, but update existing figure (e.g., for yellow flags OR sidebar toggle with existing figure)
        update_existing_start_time = time.monotonic()
        logger.debug(f"Updating existing track map figure. Target uirevision: {final_uirevision_for_output_figure}")
        figure_output = go.Figure(current_track_map_figure_state) 
        
        if not figure_output.layout: # Ensure layout object exists
            figure_output.layout = go.Layout() 

        figure_output.layout.uirevision = final_uirevision_for_output_figure

        # Explicitly re-apply ranges and ensure autorange is False for existing figure
        figure_output.layout.xaxis = figure_output.layout.xaxis or {}
        figure_output.layout.xaxis.range = list(cached_data.get('range_x', [0,1]))
        figure_output.layout.xaxis.autorange = False
        figure_output.layout.xaxis.automargin = True
        
        figure_output.layout.yaxis = figure_output.layout.yaxis or {}
        figure_output.layout.yaxis.range = list(cached_data.get('range_y', [0,1]))
        figure_output.layout.yaxis.autorange = False
        figure_output.layout.yaxis.scaleanchor = "x" 
        figure_output.layout.yaxis.scaleratio = 1
        figure_output.layout.yaxis.automargin = True
            
        figure_output.layout.autosize = True
        figure_output.layout.height = None 
        figure_output.layout.width = None  
        
        if version_store_output is dash.no_update and is_sidebar_toggle_trigger: 
            version_store_output = f"track_sidebar_updated_ver_{time.time()}"
        elif version_store_output is dash.no_update:
            version_store_output = current_figure_version_in_store_state
        logger.debug(f"'{func_name}' - Existing map figure update (pre-yellow): {time.monotonic() - update_existing_start_time:.4f}s")

    # --- COMMON YELLOW FLAG UPDATE LOGIC (Applied to figure_output whether rebuilt or existing) ---
    if figure_output is not dash.no_update and cached_data.get('marshal_sector_segments') and cached_data.get('x'):
        yellow_flag_start_time = time.monotonic()
        track_x_full = cached_data['x']; track_y_full = cached_data['y']
        
        # Determine placeholder_trace_offset based on current figure_output structure
        # This assumes a fixed order: Track, Corners (if any), Lights (if any), then Yellows
        placeholder_trace_offset = 1 # For 'Track'
        if any(trace.name == 'Corners' for trace in figure_output.data):
            placeholder_trace_offset += 1
        if any(trace.name == 'Marshal Posts' for trace in figure_output.data):
            placeholder_trace_offset +=1
        
        logger.debug(f"Recalculated Placeholder Offset: {placeholder_trace_offset}. Active Yellows: {active_yellow_sectors_snapshot}")

        # First, reset all yellow flag placeholders to invisible
        for i in range(config.MAX_YELLOW_SECTOR_PLACEHOLDERS):
            trace_index_for_placeholder = placeholder_trace_offset + i
            if trace_index_for_placeholder < len(figure_output.data) and \
               figure_output.data[trace_index_for_placeholder].name.startswith(config.YELLOW_FLAG_PLACEHOLDER_NAME_PREFIX) or \
               figure_output.data[trace_index_for_placeholder].name.startswith("Yellow Sector"): # Catch renamed ones too
                figure_output.data[trace_index_for_placeholder].x = [None]
                figure_output.data[trace_index_for_placeholder].y = [None]
                figure_output.data[trace_index_for_placeholder].visible = False
                figure_output.data[trace_index_for_placeholder].name = f"{config.YELLOW_FLAG_PLACEHOLDER_NAME_PREFIX}{i}" # Reset name

        # Then, activate the current yellow sectors
        for sector_num_active in active_yellow_sectors_snapshot:
            placeholder_idx_for_sector = sector_num_active - 1 
            if 0 <= placeholder_idx_for_sector < config.MAX_YELLOW_SECTOR_PLACEHOLDERS:
                trace_index_to_update = placeholder_trace_offset + placeholder_idx_for_sector
                if trace_index_to_update < len(figure_output.data): 
                    segment_indices = cached_data['marshal_sector_segments'].get(sector_num_active)
                    if segment_indices:
                        start_idx, end_idx = segment_indices
                        if 0 <= start_idx < len(track_x_full) and 0 <= end_idx < len(track_x_full) and start_idx <= end_idx:
                            x_seg = track_x_full[start_idx : end_idx + 1]; y_seg = track_y_full[start_idx : end_idx + 1]
                            if len(x_seg) >= 1:
                                figure_output.data[trace_index_to_update].x = list(x_seg)
                                figure_output.data[trace_index_to_update].y = list(y_seg)
                                figure_output.data[trace_index_to_update].visible = True
                                figure_output.data[trace_index_to_update].name = f"Yellow Sector {sector_num_active}" # Rename active
                                figure_output.data[trace_index_to_update].mode = 'lines' if len(x_seg) > 1 else 'markers'
                                if len(x_seg) == 1 and hasattr(config, 'YELLOW_FLAG_MARKER_SIZE'): 
                                    figure_output.data[trace_index_to_update].marker = dict(color=getattr(config, 'YELLOW_FLAG_COLOR', 'yellow'), size=getattr(config, 'YELLOW_FLAG_MARKER_SIZE', 8))
    # --- End yellow sector common logic ---
    
        logger.debug(f"'{func_name}' - Yellow flag processing: {time.monotonic() - yellow_flag_start_time:.4f}s")
    
    # Final assurance of layout properties before returning
    if figure_output is not dash.no_update:
        if not hasattr(figure_output, 'layout') or not figure_output.layout:
            figure_output.layout = go.Layout() 
        
        figure_output.layout.autosize = True
        if cached_data.get('range_x'):
            figure_output.layout.xaxis = figure_output.layout.xaxis or {}
            figure_output.layout.xaxis.range = list(cached_data.get('range_x'))
            figure_output.layout.xaxis.autorange = False
        else: # Should not happen if cache is ready
            figure_output.layout.xaxis = figure_output.layout.xaxis or {}
            figure_output.layout.xaxis.autorange = True

        if cached_data.get('range_y'):
            figure_output.layout.yaxis = figure_output.layout.yaxis or {}
            figure_output.layout.yaxis.range = list(cached_data.get('range_y'))
            figure_output.layout.yaxis.autorange = False
        else: # Should not happen if cache is ready
            figure_output.layout.yaxis = figure_output.layout.yaxis or {}
            figure_output.layout.yaxis.autorange = True
        
        figure_output.layout.yaxis.scaleanchor="x" 
        figure_output.layout.yaxis.scaleratio=1   
        
        figure_output.layout.height = None 
        figure_output.layout.width = None  
        
        # If the uirevision wasn't updated due to sidebar toggle, ensure it's the target_persistent_layout_uirevision
        if figure_output.layout.uirevision != final_uirevision_for_output_figure and not is_sidebar_toggle_trigger:
            figure_output.layout.uirevision = target_persistent_layout_uirevision


        logger.debug(f"Outputting map figure. Uirevision: {getattr(figure_output.layout, 'uirevision', 'N/A')}")
    
    logger.debug(f"Callback '{func_name}' END. Total time: {time.monotonic() - overall_start_time:.4f}s")
    return figure_output, version_store_output, yellow_key_store_output
    
@app.callback(
    [Output('lap-time-driver-dropdown', 'options'),
     Output('lap-time-driver-dropdown-2', 'options'),
     Output('driver-select-dropdown', 'options')], # Add second output
    Input('interval-component-medium', 'n_intervals')
)
def update_driver_dropdown_options(n_intervals):
    """
    Periodically updates the driver dropdown options for both dropdowns
    based on the current driver list.
    """
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    logger.debug("Attempting to update driver dropdown options...")
    options = config.DROPDOWN_NO_DRIVERS_OPTIONS # Use constant
    try:
        with session_state.lock:
            timing_state_copy = session_state.timing_state.copy()

        options = utils.generate_driver_options(timing_state_copy) # This helper already uses config constants for error states
        logger.debug(f"Updating driver dropdown options: {len(options)} options generated.")
    except Exception as e:
         logger.error(f"Error generating driver dropdown options: {e}", exc_info=True)
         options = config.DROPDOWN_ERROR_LOADING_DRIVERS_OPTIONS # Use constant
    logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
    return options, options, options

@app.callback(
    Output('lap-time-driver-selector', 'options'),
    Input('interval-component-slow', 'n_intervals')
)
def update_lap_chart_driver_options(n_intervals):
    session_state = app_state.get_or_create_session_state()
    with session_state.lock:
        timing_state_copy = session_state.timing_state.copy()
    # utils.generate_driver_options already handles empty/error cases with config constants
    options = utils.generate_driver_options(timing_state_copy) #
    return options


@app.callback(
    Output('lap-time-progression-graph', 'figure'),
    # --- MODIFIED: Listen to the two specific dropdowns for this chart ---
    Input('lap-time-driver-dropdown', 'value'),
    Input('lap-time-driver-dropdown-2', 'value'),
    # -------------------------------------------------------------------
    Input('interval-component-medium', 'n_intervals'),
    State('lap-time-progression-graph', 'figure')
)
def update_lap_time_progression_chart(driver1_rno, driver2_rno, n_intervals, current_figure_state):
    """
    Updates the lap time progression chart for one or two selected drivers.
    """
    session_state = app_state.get_or_create_session_state()
    overall_callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START_OVERALL")

    ctx = dash.callback_context
    triggered_input_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else 'N/A'
    logger.debug(f"'{func_name}' triggered by: {triggered_input_id}")
    
    # --- ADDED: Combine the two driver inputs into a single list ---
    selected_drivers_rnos = [d for d in [driver1_rno, driver2_rno] if d]
    # -------------------------------------------------------------

    fig_empty_lap_prog = utils.create_empty_figure_with_message(
        config.LAP_PROG_WRAPPER_HEIGHT, config.INITIAL_LAP_PROG_UIREVISION,
        config.TEXT_LAP_PROG_SELECT_DRIVERS, config.LAP_PROG_MARGINS_EMPTY
    )

    if not selected_drivers_rnos:
        logger.debug(f"Callback '{func_name}' END_OVERALL (No drivers selected). Total Took: {time.monotonic() - overall_callback_start_time:.4f}s")
        return fig_empty_lap_prog

    # The rest of your code works perfectly with the new list of drivers
    sorted_selection_key = "_".join(sorted(list(set(str(rno) for rno in selected_drivers_rnos))))
    data_plot_uirevision = f"lap_prog_data_{sorted_selection_key}"

    with session_state.lock:
        lock_acquired_time = time.monotonic()
        lap_history_snapshot = {str(rno): list(session_state.lap_time_history.get(str(rno), [])) for rno in selected_drivers_rnos}
        timing_state_snapshot = {str(rno): session_state.timing_state.get(str(rno), {}).copy() for rno in selected_drivers_rnos}

    python_and_plotly_prep_start_time = time.monotonic()

    fig_with_data = go.Figure(layout={
        'template': 'plotly_dark', 'uirevision': data_plot_uirevision,
        'height': config.LAP_PROG_WRAPPER_HEIGHT,
        'margin': config.LAP_PROG_MARGINS_DATA,
        'xaxis_title': 'Lap Number', 'yaxis_title': 'Lap Time (s)',
        'hovermode': 'x unified', 'title_text': 'Lap Time Progression', 'title_x':0.5, 'title_font_size':14,
        'showlegend':True, 'legend_title_text':'Drivers', 'legend_font_size':10,
        'annotations': []
    })

    data_actually_plotted = False
    min_time_overall, max_time_overall, max_laps_overall = float('inf'), float('-inf'), 0
    
    traces_to_add = []

    for driver_rno_str in selected_drivers_rnos:
        driver_laps = lap_history_snapshot.get(driver_rno_str, [])
        if not driver_laps: continue

        driver_info = timing_state_snapshot.get(driver_rno_str, {})
        tla = driver_info.get('Tla', driver_rno_str)
        team_color_hex = driver_info.get('TeamColour', 'FFFFFF')
        if not team_color_hex.startswith('#'): team_color_hex = '#' + team_color_hex

        valid_laps = [lap for lap in driver_laps if lap.get('is_valid', True)]
        if not valid_laps: continue

        data_actually_plotted = True
        lap_numbers = [lap['lap_number'] for lap in valid_laps]
        lap_times_sec = [lap['lap_time_seconds'] for lap in valid_laps]

        if lap_numbers: max_laps_overall = max(max_laps_overall, max(lap_numbers))
        if lap_times_sec:
            min_time_overall = min(min_time_overall, min(lap_times_sec))
            max_time_overall = max(max_time_overall, max(lap_times_sec))
        
        hover_texts_parts = []
        for lap in valid_laps:
            total_seconds = lap['lap_time_seconds']
            minutes = int(total_seconds // 60)
            seconds_part = total_seconds % 60
            time_formatted = f"{minutes}:{seconds_part:06.3f}" if minutes > 0 else f"{seconds_part:.3f}"
            hover_texts_parts.append(f"<b>{tla}</b><br>Lap: {lap['lap_number']}<br>Time: {time_formatted}<br>Tyre: {lap.get('compound', 'N/A')}<extra></extra>")
        
        traces_to_add.append(go.Scatter(
            x=lap_numbers, y=lap_times_sec, mode='lines+markers', name=tla,
            marker=dict(color=team_color_hex, size=5), line=dict(color=team_color_hex, width=1.5),
            hovertext=hover_texts_parts, hoverinfo='text'
        ))
    
    if traces_to_add:
        fig_with_data.add_traces(traces_to_add)

    logger.debug(f"'{func_name}' - Python Data Prep & Plotly Traces Added took: {time.monotonic() - python_and_plotly_prep_start_time:.4f}s")

    if not data_actually_plotted:
        fig_empty_lap_prog.layout.annotations[0].text = config.TEXT_LAP_PROG_NO_DATA
        fig_empty_lap_prog.layout.uirevision = data_plot_uirevision 
        logger.debug(f"Callback '{func_name}' END_OVERALL (No data plotted). Total Took: {time.monotonic() - overall_callback_start_time:.4f}s")
        return fig_empty_lap_prog

    axes_update_start_time = time.monotonic()
    if min_time_overall != float('inf') and max_time_overall != float('-inf'):
        padding = (max_time_overall - min_time_overall) * 0.05 if max_time_overall > min_time_overall else 0.5
        fig_with_data.update_yaxes(visible=True, range=[min_time_overall - padding, max_time_overall + padding], autorange=False)
    else:
        fig_with_data.update_yaxes(visible=True, autorange=True)

    if max_laps_overall > 0:
        fig_with_data.update_xaxes(visible=True, range=[0.5, max_laps_overall + 0.5], autorange=False)
    else:
        fig_with_data.update_xaxes(visible=True, autorange=True)
    logger.debug(f"'{func_name}' - Plotly Axes Update took: {time.monotonic() - axes_update_start_time:.4f}s")
    
    logger.debug(f"Callback '{func_name}' END_OVERALL. Total Took: {time.monotonic() - overall_callback_start_time:.4f}s")
    return fig_with_data
    
@app.callback(
    Output('driver-select-dropdown', 'value'),
    Input('clicked-car-driver-number-store', 'data'),
    State('driver-select-dropdown', 'options'),
    prevent_initial_call=True
)
def update_dropdown_from_map_click(click_data_json_str, dropdown_options): # Renamed arg
    session_state = app_state.get_or_create_session_state()
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if click_data_json_str is None:
        return dash.no_update

    try:
        # The data from the store is now the JSON string written by JS
        click_data = json.loads(click_data_json_str)
        clicked_driver_number_str = str(click_data.get('carNumber')) # Ensure it's a string

        if clicked_driver_number_str is None or clicked_driver_number_str == 'None': # Check for None or 'None' string
            with session_state.lock: # If click is invalid, clear selection
                if session_state.selected_driver_for_map_and_lap_chart is not None:
                    logger.info("Map click invalid, clearing session_state.selected_driver_for_map_and_lap_chart.")
                    session_state.selected_driver_for_map_and_lap_chart = None
            return dash.no_update

        logger.info(f"Map click: Attempting to select driver number: {clicked_driver_number_str} for telemetry dropdown.")

        # Update session_state with the clicked driver
        with session_state.lock:
            session_state.selected_driver_for_map_and_lap_chart = clicked_driver_number_str
            logger.info(f"Updated session_state.selected_driver_for_map_and_lap_chart to: {clicked_driver_number_str}")


        if dropdown_options and isinstance(dropdown_options, list):
            valid_driver_numbers = [opt['value'] for opt in dropdown_options if 'value' in opt]
            if clicked_driver_number_str in valid_driver_numbers:
                logger.info(f"Map click: Setting driver-select-dropdown (telemetry) to: {clicked_driver_number_str}")
                logger.debug(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
                return clicked_driver_number_str
            else:
                logger.warning(f"Map click: Driver number {clicked_driver_number_str} not found in telemetry dropdown options: {valid_driver_numbers}")
                # Even if not in telemetry dropdown, keep it selected in session_state for map/lap chart
                return dash.no_update # Don't change telemetry dropdown if invalid for it
    except json.JSONDecodeError:
        logger.error(f"update_dropdown_from_map_click: Could not decode JSON from store: {click_data_json_str}")
        with session_state.lock: session_state.selected_driver_for_map_and_lap_chart = None # Clear on error
    except Exception as e:
        logger.error(f"update_dropdown_from_map_click: Error processing click data: {e}")
        with session_state.lock: session_state.selected_driver_for_map_and_lap_chart = None # Clear on error

    return dash.no_update

# NEW CALLBACK to update Lap Progression Chart Driver Selection
@app.callback(
    Output('lap-time-driver-selector', 'value'),
    Input('clicked-car-driver-number-store', 'data'), # Triggered by map click via JS
    State('lap-time-driver-selector', 'options'),   # To check if driver is valid option
    State('lap-time-driver-selector', 'value'),     # Current selection (to potentially keep if multi-select later)
    prevent_initial_call=True
)
def update_lap_chart_driver_selection_from_map_click(click_data_json_str, lap_chart_options, current_lap_chart_selection):
    callback_start_time = time.monotonic()
    func_name = inspect.currentframe().f_code.co_name
    logger.debug(f"Callback '{func_name}' START")
    if click_data_json_str is None:
        return dash.no_update

    try:
        click_data = json.loads(click_data_json_str)
        clicked_driver_number_str = str(click_data.get('carNumber')) # Ensure string

        if clicked_driver_number_str is None or clicked_driver_number_str == 'None':
             # If click is invalid, potentially clear selection or do nothing
            return dash.no_update # Or return [] to clear selection

        logger.info(f"Map click: Attempting to select driver {clicked_driver_number_str} in lap progression chart.")

        if lap_chart_options and isinstance(lap_chart_options, list):
            valid_driver_numbers_for_lap_chart = [opt['value'] for opt in lap_chart_options if 'value' in opt]

            if clicked_driver_number_str in valid_driver_numbers_for_lap_chart:
                # For now, we'll make it select ONLY the clicked driver.
                # If you want to ADD to a multi-selection, the logic would be:
                # current_selection = list(current_lap_chart_selection) if current_lap_chart_selection else []
                # if clicked_driver_number_str not in current_selection:
                #     current_selection.append(clicked_driver_number_str)
                # return current_selection
                logger.info(f"Map click: Setting lap-time-driver-selector to: [{clicked_driver_number_str}]")
                logger.info(f"Callback '{func_name}' END. Took: {time.monotonic() - callback_start_time:.4f}s")
                return [clicked_driver_number_str] # Lap chart dropdown expects a list for its 'value'
            else:
                logger.warning(f"Map click: Driver {clicked_driver_number_str} not in lap chart options. Lap chart selection unchanged.")
                return dash.no_update # Or return [] to clear if driver not available
        else:
            logger.warning("Map click: No options available for lap chart driver selector.")
            return dash.no_update

    except json.JSONDecodeError:
        logger.error(f"update_lap_chart_driver_selection_from_map_click: Could not decode JSON: {click_data_json_str}")
    except Exception as e:
        logger.error(f"update_lap_chart_driver_selection_from_map_click: Error: {e}")
    return dash.no_update
    
@app.callback(
    Output('tyre-strategy-graph', 'figure'),
    Input('interval-component-slow', 'n_intervals') # Update every 5 seconds
)
def update_tyre_strategy_chart(n_intervals):
    """Periodically updates the tyre strategy chart."""
    session_state = app_state.get_or_create_session_state()
    if not session_state:
        return dash.no_update

    with session_state.lock:
        # Take a snapshot of the necessary data under lock
        stint_data_snapshot = dict(session_state.driver_stint_data)
        timing_state_snapshot = {k: {'Tla': v.get('Tla')} for k, v in session_state.timing_state.items()}

    # Pass the snapshots to the figure generation function
    return utils.create_tyre_strategy_figure(stint_data_snapshot, timing_state_snapshot)
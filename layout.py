# layout.py
import logging
import dash_bootstrap_components as dbc
from dash import dcc, html, dash_table
import config
import replay


def create_layout():
    logger = logging.getLogger("F1App.Layout")
    logger.info("Creating application layout (redesign attempt)...")

    try:
        replay.ensure_replay_dir_exists()
        replay_file_options = replay.get_replay_files(config.REPLAY_DIR)
    except Exception as e:
        logger.error(f"Failed to get replay files during layout creation: {e}")
        replay_file_options = []

    # --- Column definitions (from Response 2) ---
    timing_table_columns = [
        {"name": "No.", "id": "No."}, {"name": "Car",
                                       "id": "Car"}, {"name": "Pos", "id": "Pos"},
        {"name": "Tyre", "id": "Tyre"}, {"name": "Lap Time", "id": "Time"},
        {"name": "Interval", "id": "Interval"}, {"name": "Gap", "id": "Gap"},
        {"name": "Last Lap", "id": "Last Lap"}, {
            "name": "Best Lap", "id": "Best Lap"},
        {"name": "S1", "id": "S1"}, {"name": "S2",
                                     "id": "S2"}, {"name": "S3", "id": "S3"},
        {"name": "Pits", "id": "Pits"}, {"name": "Status", "id": "Status"},
        {'name': 'Speed', 'id': 'Speed', 'type': 'numeric'},
        {'name': 'Gear', 'id': 'Gear', 'type': 'numeric'},
        {'name': 'RPM', 'id': 'RPM', 'type': 'numeric'}, {
            'name': 'DRS', 'id': 'DRS'},
    ]
    tyre_style_base = {
        'textAlign': 'center', 'fontWeight': 'bold', 'border': '1px solid #444',
    }
    # --- End Column definitions ---

    stores_and_intervals = [
        dcc.Interval(id='interval-component-map-animation',
                     interval=100, n_intervals=0),
        dcc.Interval(id='interval-component-timing',
                     interval=350, n_intervals=0),
        dcc.Interval(id='interval-component-fast',
                     interval=500, n_intervals=0),
        dcc.Interval(id='interval-component-medium',
                     interval=1000, n_intervals=0),
        dcc.Interval(id='interval-component-slow',
                     interval=5000, n_intervals=0),
        dcc.Interval(id='interval-component-real-slow',
                     interval=10000, n_intervals=0),
        html.Div(id='dummy-output-for-controls', style={'display': 'none'}),
        dcc.Store(id='car-positions-store'),
        dcc.Store(id='current-track-layout-cache-key-store'),
        dcc.Interval(id='clientside-update-interval',
                     interval=1250, n_intervals=0, disabled=True)
    ]

    # --- Header Zone ---
    header_zone = dbc.Row([
        dbc.Col(html.H2("F1 Live Dashboard", className="mb-0"),
                md=4),  # Main Title
        dbc.Col(html.Div(id='session-info-display'), md=5,
                className="text-center align-self-center"),
        dbc.Col([
            html.Div(id='connection-status',
                     children="Status: Initializing..."),
            html.Div(id='track-status-display', children="Track: Unknown")
        ], md=3, className="text-end align-self-center")
    ], className="mb-3 p-2 bg-dark text-white rounded", id='header-zone', align="center")

    # --- Control Zone (Consider making this collapsible or a modal later) ---
    # Using dbc.Card for better visual grouping of controls
    control_card_content = [
        dbc.Row([
            dbc.Col(dbc.Button("Connect", id="connect-button",
                    color="success", size="sm"), width="auto"),
            dbc.Col(dbc.Button("Disconnect", id="disconnect-button",
                    color="warning", size="sm"), width="auto"),
            dbc.Col(dbc.Checkbox(id='record-data-checkbox', label="Record Live Data", value=False,
                                 className="form-check-inline ms-3"), width="auto", className="align-self-center"),
        ], className="mb-2"),
        dbc.Row([
            dbc.Col(dcc.Dropdown(id='replay-file-selector', options=replay_file_options,
                                 placeholder="Select replay file...", style={'color': '#333'}), md=5),
            dbc.Col(dcc.Slider(id='replay-speed-slider', min=0.1, max=10, step=0.1, value=1.0,
                               marks={0.5: '0.5x', 1: '1x',
                                      2: '2x', 5: '5x', 10: '10x'},
                               tooltip={"placement": "bottom", "always_visible": False}), md=4, className="align-self-center"),
            dbc.Col(dbc.Button("Replay", id="replay-button",
                    color="primary", size="sm"), width="auto"),
            dbc.Col(dbc.Button("Stop Replay", id="stop-replay-button",
                    color="danger", size="sm"), width="auto"),
        ], align="center")
    ]
    control_zone = dbc.Card(dbc.CardBody(
        control_card_content), className="mb-3", id='control-zone')

    # --- Main Data Zone ---
    main_data_zone = dbc.Row([
        # Left Column: Timing, Race Control, Other Data
        dbc.Col([
            html.H4("Live Timing"),
            html.P(id='timing-data-timestamp',
                   style={'fontSize': 'small', 'color': 'grey'}),
            dash_table.DataTable(
                id='timing-data-actual-table', columns=timing_table_columns, data=[],
                fixed_rows={'headers': True},
                style_table={'height': '60vh', 'overflowY': 'auto',
                             'overflowX': 'auto'},  # Adjusted height
                style_cell={'minWidth': '40px', 'width': '70px', 'maxWidth': '150px',
                            'overflow': 'hidden', 'textOverflow': 'ellipsis',
                            'textAlign': 'left', 'padding': '5px',
                            'backgroundColor': 'rgb(50, 50, 50)', 'color': 'white'},
                style_header={
                    'backgroundColor': 'rgb(30, 30, 30)', 'fontWeight': 'bold', 'border': '1px solid grey'},
                style_data={'borderBottom': '1px solid grey'},
                style_data_conditional=[
                    {'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(60, 60, 60)'},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "S " || {Tyre} = "S"'},
                        'backgroundColor': '#D90000', 'color': 'white', **tyre_style_base},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "M " || {Tyre} = "M"'},
                        'backgroundColor': '#EBC000', 'color': '#383838', **tyre_style_base},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "H " || {Tyre} = "H"'},
                        'backgroundColor': '#E0E0E0', 'color': '#383838', **tyre_style_base},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "I " || {Tyre} = "I"'},
                        'backgroundColor': '#00A300', 'color': 'white', **tyre_style_base},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "W " || {Tyre} = "W"'},
                        'backgroundColor': '#0077FF', 'color': 'white', **tyre_style_base},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} = "-"'},
                        'backgroundColor': 'inherit', 'color': 'grey', 'textAlign': 'center'},
                    {'if': {'column_id': 'Pos'}, 'textAlign': 'center',
                        'fontWeight': 'bold', 'width': '40px', 'minWidth': '40px'},
                    {'if': {'column_id': 'No.'}, 'textAlign': 'right',
                        'width': '40px', 'minWidth': '40px', 'paddingRight': '2px'},
                    {'if': {'column_id': 'Car'}, 'textAlign': 'left',
                        'width': '50px', 'minWidth': '50px'},
                    {'if': {'column_id': 'Pits'}, 'textAlign': 'center',
                        'width': '40px', 'minWidth': '40px'},
                ],
                tooltip_duration=None
            ),
            html.Hr(),
            # Consider making Race Control and Other Data collapsible to save space
            dbc.Accordion([
                dbc.AccordionItem(
                    dcc.Textarea(id='race-control-log-display', value='Waiting...',
                                 style={'width': '100%', 'height': '15vh', 'backgroundColor': '#333',
                                        'color': '#DDD', 'border': '1px solid grey',
                                        'fontFamily': 'monospace', 'fontSize': 'small'},
                                 readOnly=True),
                    title="Race Control Messages", item_id="rcm-accordion"
                ),
                dbc.AccordionItem(
                    html.Div(id='other-data-display',
                             style={'maxHeight': '20vh', 'overflowY': 'auto', 'border': '1px solid grey',
                                    'padding': '10px', 'fontSize': 'x-small'}),  # Made font smaller
                    title="Other Data Streams (Debug)", item_id="other-data-accordion"
                )
            ], start_collapsed=True, flush=True, active_item=None)  # Start collapsed

        ], md=7, id='main-timing-col'),

        # Right Column: Map, Driver Details, Telemetry
        dbc.Col([
            html.H4("Track Map"),
            dcc.Graph(id='track-map-graph', style={'height': '350px', 'marginBottom': '10px'},  # Slightly reduced height
                      config={'displayModeBar': False, 'scrollZoom': False, 'dragmode': False}),
            html.Div(id='dummy-cache-output',
                     style={'display': 'none'}),  # For map cache
            html.Hr(),
            html.H4("Driver Focus"),
            dcc.Dropdown(id='driver-select-dropdown', options=[], placeholder="Select Driver...",
                         style={'color': '#333', 'marginBottom': '10px'}),
            dbc.Row([
                dbc.Col(html.Label("Lap:"), width="auto",
                        className="pe-0 align-self-center"),
                dbc.Col(dcc.Dropdown(id='lap-selector-dropdown', options=[], placeholder="Lap",
                                     style={'minWidth': '80px',
                                            'color': '#333'},
                                     clearable=False, searchable=False, disabled=True), width=3)
            ], className="mb-2 align-items-center"),
            # Reduced height
            dcc.Graph(id='telemetry-graph', style={'height': '200px'}),
            html.Div(id='driver-details-output',
                     style={'maxHeight': '10vh', 'overflowY': 'auto',  # Reduced height
                            'border': '1px solid #444', 'padding': '5px',
                            'fontSize': 'small', 'marginTop': '10px'}),
        ], md=5, id='contextual-info-col')
    ], id='main-data-zone')

    # --- Analysis Zone (e.g., Lap Time Chart) ---
    analysis_zone = dbc.Row([
        dbc.Col([
            html.H4("Lap Time Progression"),
            dcc.Dropdown(
                id='lap-time-driver-selector', options=[], value=[], multi=True,
                placeholder="Select drivers for lap chart...",
                style={'marginBottom': '10px', 'color': '#333'}
            ),
            dcc.Graph(id='lap-time-progression-graph',
                      style={'height': '350px'})  # Adjusted height
        ], md=12)
    ], className="mt-4 mb-3", id='analysis-zone')

    # --- Assemble the final layout ---
    app_layout = dbc.Container([
        # Keep stores and intervals at the top level
        html.Div(stores_and_intervals),
        header_zone,
        control_zone,
        main_data_zone,
        analysis_zone,
        # Add a small footer?
        html.Footer(dbc.Row(dbc.Col(html.Small("F1 Dashboard v0.x",
                    className="text-muted"), className="text-center py-3")))
    ], fluid=True, className="dbc dbc-slate")  # Ensure theme class is applied if using dbc-Bootswatch theme

    logger.info("Layout creation finished.")
    return app_layout
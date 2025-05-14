# layout.py
import logging
import dash_bootstrap_components as dbc
from dash import dcc, html, dash_table
import config
import replay


def create_layout():
    logger = logging.getLogger("F1App.Layout")
    logger.info("Creating application layout (redesign refinement)...")

    try:
        replay.ensure_replay_dir_exists()
        replay_file_options = replay.get_replay_files(config.REPLAY_DIR)
    except Exception as e:
        logger.error(f"Failed to get replay files during layout creation: {e}")
        replay_file_options = []

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
        # These telemetry columns might make the main table too wide.
        # Consider moving them to a dedicated "Driver Telemetry Details" view or removing
        # if they are redundant with the telemetry graph section.
        # For now, commented out to save horizontal space in the main table.
        # {'name': 'Speed', 'id': 'Speed', 'type': 'numeric'},
        # {'name': 'Gear', 'id': 'Gear', 'type': 'numeric'},
        # {'name': 'RPM', 'id': 'RPM', 'type': 'numeric'}, {'name': 'DRS', 'id': 'DRS'},
    ]
    tyre_style_base = {'textAlign': 'center',
                       'fontWeight': 'bold', 'border': '1px solid #444'}

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
                width="auto", lg=4),  # Give title more defined space
        dbc.Col(html.Div(id='session-info-display', style={'fontSize': '0.9rem'}),
                lg=5, className="text-center align-self-center"),  # Center session info
        dbc.Col([
            html.Div(id='connection-status', children="Status: Initializing...",
                     style={'fontSize': '0.8rem'}),
            html.Div(id='track-status-display',
                     children="Track: Unknown", style={'fontSize': '0.8rem'})
        ], lg=3, className="text-end align-self-center")  # Status to the right
    ], className="mb-3 p-2 bg-dark text-white rounded", id='header-zone', align="center")

    # --- Control Zone ---
    # Removed 'size="sm"' from dcc.Dropdown as it's not a valid prop.
    # Adjusted column widths for better flow.
    control_card_content = [
        dbc.Row([
            dbc.Col(dbc.Button("Connect", id="connect-button",
                    color="success", size="sm"), width="auto", className="me-1"),
            dbc.Col(dbc.Button("Disconnect", id="disconnect-button",
                    color="warning", size="sm"), width="auto"),
            dbc.Col(dbc.Checkbox(id='record-data-checkbox', label="Record Live Data", value=False,
                                 className="form-check-inline ms-md-3"), width="auto", className="align-self-center mt-2 mt-md-0"),  # Margin top on small screens
        ], className="mb-2 justify-content-start justify-content-md-start"),  # Align controls left
        dbc.Row([
            dbc.Col(dcc.Dropdown(id='replay-file-selector', options=replay_file_options,
                                 placeholder="Select replay file...", style={'color': '#333', 'minWidth': '180px'}),
                    xs=12, sm=6, md=4, lg=4, className="mb-2 mb-sm-0"),  # Responsive width for dropdown
            dbc.Col(dcc.Slider(id='replay-speed-slider', min=0.1, max=10, step=0.1, value=1.0,
                               marks={0.5: '0.5x', 1: '1x',
                                      2: '2x', 5: '5x', 10: '10x'},
                               tooltip={"placement": "bottom", "always_visible": False}),
                    xs=12, sm=6, md=4, lg=4, className="align-self-center mb-2 mb-sm-0 px-md-3"),  # Responsive width for slider
            dbc.Col(dbc.Button("Replay", id="replay-button",
                    color="primary", size="sm"), width="auto", className="me-1"),
            dbc.Col(dbc.Button("Stop Replay", id="stop-replay-button",
                    color="danger", size="sm"), width="auto"),
        ], align="center", className="justify-content-start justify-content-md-start")  # Align replay controls left
    ]
    # Control zone now uses dbc.Collapse for better space management
    control_zone = html.Div([
        dbc.Button(
            "Show/Hide Controls",
            id="collapse-controls-button",
            className="mb-2",
            color="secondary",
            n_clicks=0,
            size="sm"
        ),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(control_card_content)),
            id="collapse-controls",
            is_open=True,  # Or False to start collapsed
        )
    ], className="mb-3", id='control-zone-wrapper')

    # --- Main Data Zone ---
    main_data_zone = dbc.Row([
        # Left Column: Timing, Race Control, Other Data
        dbc.Col([
            html.H4("Live Timing"),
            html.P(id='timing-data-timestamp', style={
                   'fontSize': '0.8rem', 'color': 'grey', 'marginBottom': '2px'}),  # Reduced margin
            dash_table.DataTable(
                id='timing-data-actual-table', columns=timing_table_columns, data=[],
                fixed_rows={'headers': True},
                style_table={'minHeight': '50vh', 'height': 'calc(70vh - 100px)', 'maxHeight': '65vh',  # More flexible height
                             'overflowY': 'auto', 'overflowX': 'auto'},
                style_cell={'minWidth': '35px', 'width': '60px', 'maxWidth': '120px',  # Slightly narrower defaults
                            'overflow': 'hidden', 'textOverflow': 'ellipsis',
                            'textAlign': 'left', 'padding': '4px', 'fontSize': '0.85rem',  # Smaller padding/font
                            'backgroundColor': 'rgb(50, 50, 50)', 'color': 'white'},
                style_header={'backgroundColor': 'rgb(30, 30, 30)', 'fontWeight': 'bold',
                              'border': '1px solid grey', 'padding': '4px', 'fontSize': '0.9rem'},
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
                        'fontWeight': 'bold', 'width': '35px', 'minWidth': '35px'},
                    {'if': {'column_id': 'No.'}, 'textAlign': 'right',
                        'width': '35px', 'minWidth': '35px', 'paddingRight': '2px'},
                    {'if': {'column_id': 'Car'}, 'textAlign': 'left',
                        'width': '45px', 'minWidth': '45px'},  # TLA column
                    {'if': {'column_id': 'Pits'}, 'textAlign': 'center',
                        'width': '35px', 'minWidth': '35px'},
                    # Ensure specific time columns are wide enough
                    {'if': {'column_id': 'Last Lap'}, 'minWidth': '70px'},
                    {'if': {'column_id': 'Best Lap'}, 'minWidth': '70px'},
                    {'if': {'column_id': 'Lap Time'}, 'minWidth': '70px'},
                ],
                tooltip_duration=None
            ),
            # Accordion for less critical info, starts collapsed
            dbc.Accordion([
                dbc.AccordionItem(
                    dcc.Textarea(id='race-control-log-display', value='Waiting...',
                                 style={'width': '100%', 'height': '150px',  # Fixed height for textarea
                                        'backgroundColor': '#2B2B2B',  # Slightly lighter dark
                                        'color': '#E0E0E0', 'border': '1px solid #444',
                                        'fontFamily': 'monospace', 'fontSize': '0.75rem'},
                                 readOnly=True),
                    title="Race Control Messages", item_id="rcm-accordion"
                ),
                dbc.AccordionItem(
                    html.Div(id='other-data-display',
                             style={'maxHeight': '150px', 'overflowY': 'auto',  # Fixed height
                                    'border': '1px solid #444', 'padding': '8px',
                                    'fontSize': '0.7rem', 'backgroundColor': '#2B2B2B'}),
                    title="Other Data Streams (Debug)", item_id="other-data-accordion"
                )
            ], start_collapsed=True, flush=True, className="mt-3", active_item=None)
        ], lg=7, md=12, id='main-timing-col', className="mb-3 mb-lg-0"),  # Takes full width on medium and below

        # Right Column: Map, Driver Details, Telemetry
        dbc.Col([
            dbc.Card(dbc.CardBody([  # Wrap right column content in a card for visual consistency
                html.H4("Track Map", className="card-title"),
                dcc.Graph(id='track-map-graph', style={'height': '300px'},  # Adjusted height
                          config={'displayModeBar': False, 'scrollZoom': False, 'dragmode': False, 'autosizable': True, 'responsive': True}),
            ])),
            html.Div(id='dummy-cache-output', style={'display': 'none'}),
            dbc.Card(dbc.CardBody([  # Another card for Driver Focus
                html.H4("Driver Focus", className="card-title"),
                dcc.Dropdown(id='driver-select-dropdown', options=[], placeholder="Select Driver...",
                             style={'color': '#333', 'marginBottom': '10px'}),
                dbc.Row([
                     dbc.Col(html.Label("Lap:", style={
                             'fontSize': '0.9rem'}), width="auto", className="pe-0 align-self-center"),
                     dbc.Col(dcc.Dropdown(id='lap-selector-dropdown', options=[], placeholder="Lap",
                                          style={
                                              'minWidth': '70px', 'color': '#333', 'fontSize': '0.9rem'},
                                          clearable=False, searchable=False, disabled=True), className="ps-1", width=True)  # Simpler width control
                     ], className="mb-2 align-items-center"),
                # Adjusted height
                dcc.Graph(id='telemetry-graph', style={'height': '180px'}),
                html.Div(id='driver-details-output',
                         style={'maxHeight': '80px', 'overflowY': 'auto',  # Adjusted height
                                'border': '1px solid #444', 'padding': '5px',
                                'fontSize': '0.8rem', 'marginTop': '10px', 'backgroundColor': '#2B2B2B'}),
            ]), className="mt-3")
        ], lg=5, md=12, id='contextual-info-col')
    ], id='main-data-zone', className="mb-3")

    # --- Analysis Zone (e.g., Lap Time Chart) ---
    analysis_zone = dbc.Row([
        dbc.Col([
            dbc.Card(dbc.CardBody([  # Wrap analysis in a card
                html.H4("Lap Time Progression", className="card-title"),
                dcc.Dropdown(
                    id='lap-time-driver-selector', options=[], value=[], multi=True,
                    placeholder="Select drivers for lap chart...",
                    style={'marginBottom': '10px', 'color': '#333'}
                ),
                dcc.Graph(id='lap-time-progression-graph', style={'height': '330px'},  # Adjusted height
                          config={'autosizable': True, 'responsive': True})
            ]))
        ], md=12)
    ], className="mt-3 mb-3", id='analysis-zone')

    app_layout = dbc.Container([
        html.Div(stores_and_intervals),
        header_zone,
        control_zone,  # Now includes the collapse button and the collapsible card
        main_data_zone,
        analysis_zone,
        html.Footer(dbc.Row(dbc.Col(html.Small("F1 Dashboard",
                    className="text-muted"), className="text-center py-3")))
    ], fluid=True, className="dbc dbc-slate p-2")  # Added small padding to main container

    logger.info("Layout refinement finished.")
    return app_layout

# layout.py
import logging
import dash_bootstrap_components as dbc
from dash import dcc, html, dash_table
import plotly.graph_objects as go # <<< MAKE SURE THIS IS IMPORTED
import config
import replay

def create_layout():
    logger = logging.getLogger("F1App.Layout")
    logger.info("Creating application layout (R8 base + refined graph height control)...")

    try:
        replay.ensure_replay_dir_exists()
        replay_file_options = replay.get_replay_files(config.REPLAY_DIR)
    except Exception as e:
        logger.error(f"Failed to get replay files during layout creation: {e}")
        replay_file_options = []

    timing_table_columns = [
        {"name": "No.", "id": "No."}, {"name": "Car", "id": "Car"}, {"name": "Pos", "id": "Pos"},
        {"name": "Tyre", "id": "Tyre"}, {"name": "Lap Time", "id": "Time"}, 
        {"name": "Interval", "id": "Interval"}, {"name": "Gap", "id": "Gap"},
        {"name": "Last Lap", "id": "Last Lap"}, {"name": "Best Lap", "id": "Best Lap"},
        {"name": "S1", "id": "S1"}, {"name": "S2", "id": "S2"}, {"name": "S3", "id": "S3"},
        {"name": "Pits", "id": "Pits"}, {"name": "Status", "id": "Status"},
    ]
    tyre_style_base = {'textAlign': 'center', 'fontWeight': 'bold', 'border': '1px solid #444'}

    stores_and_intervals = html.Div([ # Wrapped in a Div
        dcc.Interval(id='interval-component-map-animation', interval=100, n_intervals=0),
        dcc.Interval(id='interval-component-timing', interval=350, n_intervals=0),
        dcc.Interval(id='interval-component-fast', interval=500, n_intervals=0),
        dcc.Interval(id='interval-component-medium', interval=1000, n_intervals=0),
        dcc.Interval(id='interval-component-slow', interval=5000, n_intervals=0),
        dcc.Interval(id='interval-component-real-slow', interval=10000, n_intervals=0),
        html.Div(id='dummy-output-for-controls', style={'display': 'none'}),
        dcc.Store(id='car-positions-store'),
        dcc.Store(id='current-track-layout-cache-key-store'),
        dcc.Interval(id='clientside-update-interval', interval=1250, n_intervals=0, disabled=True)
    ])

    header_zone = dbc.Row([
        dbc.Col(html.H2("F1 Live Dashboard", className="mb-0"), width="auto", lg=4),
        dbc.Col(html.Div(id='session-info-display'), # <<< This will now mainly show Session Name, Circuit
                lg=5, className="text-center align-self-center" # Keep style if desired
               ), 
        dbc.Col(html.Div(id='connection-status', children="Status: Initializing..."), # Connection status can stay here
                lg=3, className="text-end align-self-center")
    ], className="mb-2 p-2 bg-dark text-white rounded", id='header-zone', align="center") # Reduced mb slightly

    control_card_content_list = [ # Ensuring this is a list for CardBody
        dbc.Row([
            dbc.Col(dbc.Button("Connect", id="connect-button", color="success", size="sm"), width="auto", className="me-1"),
            dbc.Col(dbc.Button("Disconnect", id="disconnect-button", color="warning", size="sm"), width="auto"),
            dbc.Col(dbc.Checkbox(id='record-data-checkbox', label="Record Live Data", value=False, 
                                 className="form-check-inline ms-md-3"), width="auto", className="align-self-center mt-2 mt-md-0"),
        ], className="mb-2 justify-content-start justify-content-md-start"),
        dbc.Row([
            dbc.Col(dcc.Dropdown(id='replay-file-selector', options=replay_file_options, 
                                 placeholder="Select replay file...", style={'color': '#333', 'minWidth': '180px'}), 
                    xs=12, sm=6, md=4, lg=4, className="mb-2 mb-sm-0"),
            dbc.Col(dcc.Slider(id='replay-speed-slider', min=0.1, max=10, step=0.1, value=1.0, 
                               marks={0.5:'0.5x', 1:'1x', 2:'2x', 5:'5x', 10:'10x'}, 
                               tooltip={"placement": "bottom", "always_visible": False}), 
                    xs=12, sm=6, md=4, lg=4, className="align-self-center mb-2 mb-sm-0 px-md-3"),
            dbc.Col(dbc.Button("Replay", id="replay-button", color="primary", size="sm"), width="auto", className="me-1"),
            dbc.Col(dbc.Button("Stop Replay", id="stop-replay-button", color="danger", size="sm"), width="auto"),
        ], align="center", className="justify-content-start justify-content-md-start")
    ]
    control_zone = html.Div([
        dbc.Button("Show/Hide Controls",id="collapse-controls-button",className="mb-2",color="secondary",n_clicks=0,size="sm"),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(children=control_card_content_list)), # Pass the list here
            id="collapse-controls",
            is_open=True, 
        )
    ], className="mb-3", id='control-zone-wrapper')
    
    # --- >>> NEW: Prominent Status & Weather Bar <<< ---
    status_weather_bar = dbc.Row([
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    html.Div([
                        html.Strong("Track Status: ", style={'marginRight':'5px'}),
                        html.Span(id='prominent-track-status-text', children="CLEAR", 
                                  style={'fontWeight':'bold', 'padding':'2px 5px', 'borderRadius':'4px'}) 
                        # Styling for text color/background will be via callback
                    ]),
                    className="p-2 text-center", # Compact padding
                    style={'minHeight':'55px', 'display':'flex', 'alignItems':'center', 'justifyContent':'center'}
                ), 
                id='prominent-track-status-card', # ID for callback styling
                color="secondary", # Default, will change with status
                inverse=True # For dark themes if card color is dark
            ), 
            lg=3, md=4, sm=6, xs=12, className="mb-2 mb-lg-0" # Responsive
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    html.Div([ # Wrapper for icon and text
                    html.Span(id='weather-main-icon', className="me-2", style={'fontSize': '1.5rem'}), # Larger icon
                    html.Div(id='prominent-weather-display', children="Weather: Loading...",
                             style={'fontSize':'0.8rem', 'lineHeight':'1.2'}) # Adjusted line height
                ], style={'display': 'flex', 'alignItems': 'center'}),
                className="p-2" 
            ),
            id='prominent-weather-card', # ID for potential card styling
            color="light", 
            style={'minHeight':'55px'}
        ),
        lg=9, md=8, sm=6, xs=12
    )
], className="mb-3", id='status-weather-bar')
    
    main_data_zone = dbc.Row([
        dbc.Col([ # Left Column (Structure from Response 8)
            html.H4("Live Timing"),
            html.P(id='timing-data-timestamp', style={'fontSize':'0.8rem', 'color':'grey', 'marginBottom':'2px'}),
            dash_table.DataTable(
                id='timing-data-actual-table', columns=timing_table_columns, data=[],
                fixed_rows={'headers': True},
                style_table={
                    # Try a larger fixed height first to see if it fills the space
                    'height': '720px', # Example: Increased from 600px or vh calculations
                    'minHeight': '650px', # A decent minimum
                                        # Adjust this value based on your screen to fit ~20-21 rows
                    # 'minHeight': '650px', # If using vh, a minHeight is good. With fixed height, this is less critical.
                    # 'maxHeight': '85vh', # Can still use maxHeight with fixed height to limit on huge screens
                    'overflowY': 'auto', 
                    'overflowX': 'auto'
                },
                style_cell={
                    'minWidth': '30px',  # Can be slightly smaller for very narrow columns
                    'width': '60px',   # Default initial width - many will be overridden
                    'maxWidth': '170px', 
                    'overflow': 'hidden', 
                    'textOverflow': 'ellipsis',
                    'textAlign': 'left', 
                    'padding': '3px 5px', # Slightly adjusted padding
                    'fontSize':'0.8rem', # Slightly smaller font for more density
                    'backgroundColor': 'rgb(50, 50, 50)', 
                    'color': 'white',
                    'whiteSpace': 'normal', # Allow text to wrap in cell if really needed (for headers)
                    'height': 'auto'        # Allow cell height to adjust to wrapped text
                },
                style_header={
                    'backgroundColor': 'rgb(30, 30, 30)', 
                    'fontWeight': 'bold', 
                    'border': '1px solid #444', # Darker border for header
                    'padding': '6px', 
                    'fontSize':'0.85rem', # Slightly smaller header font
                    'textAlign': 'center', # Center header text
                    'whiteSpace': 'normal', # Allow header text to wrap
                    'height': 'auto'        # Allow header row height to adjust
                },
                style_data={'borderBottom': '1px solid grey'},
                style_data_conditional=[
                    {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(60, 60, 60)'},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "S " || {Tyre} = "S"'}, 'backgroundColor': '#D90000', 'color': 'white', **tyre_style_base},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "M " || {Tyre} = "M"'}, 'backgroundColor': '#EBC000', 'color': '#383838', **tyre_style_base},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "H " || {Tyre} = "H"'}, 'backgroundColor': '#E0E0E0', 'color': '#383838', **tyre_style_base},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "I " || {Tyre} = "I"'}, 'backgroundColor': '#00A300', 'color': 'white', **tyre_style_base},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "W " || {Tyre} = "W"'}, 'backgroundColor': '#0077FF', 'color': 'white', **tyre_style_base},
                    {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} = "-"'}, 'backgroundColor': 'inherit', 'color': 'grey', 'textAlign': 'center'},
                    {'if': {'column_id': 'Pos'}, 'textAlign': 'center', 'fontWeight': 'bold', 'width': '35px', 'minWidth':'35px'},
                    {'if': {'column_id': 'No.'}, 'textAlign': 'right', 'width': '35px', 'minWidth':'35px', 'paddingRight':'2px'},
                    {'if': {'column_id': 'Car'}, 'textAlign': 'left', 'width': '45px', 'minWidth':'45px'},
                    {'if': {'column_id': 'Pits'}, 'textAlign': 'center', 'width': '45px', 'minWidth':'35px'},
                    {'if': {'column_id': 'Lap Time'}, 'width': '70px', 'minWidth': '70px', 'maxWidth': '85px', 'textAlign': 'right', 'paddingRight':'5px'},
                    {'if': {'column_id': 'Interval'}, 'width': '75px', 'minWidth': '65px', 'maxWidth': '80px', 'textAlign': 'right', 'paddingRight':'5px'},
                    {'if': {'column_id': 'Gap'},      'width': '70px', 'minWidth': '70px', 'maxWidth': '85px', 'textAlign': 'right', 'paddingRight':'5px'},
                    {'if': {'column_id': 'Last Lap'}, 'width': '70px', 'minWidth': '70px', 'maxWidth': '85px', 'textAlign': 'right', 'paddingRight':'5px'},
                    {'if': {'column_id': 'Best Lap'}, 'width': '70px', 'minWidth': '70px', 'maxWidth': '85px', 'textAlign': 'right', 'paddingRight':'5px'},
                    {'if': {'column_id': 'S1'},       'width': '55px', 'minWidth': '55px', 'maxWidth': '65px', 'textAlign': 'right', 'paddingRight':'5px'},
                    {'if': {'column_id': 'S2'},       'width': '55px', 'minWidth': '55px', 'maxWidth': '65px', 'textAlign': 'right', 'paddingRight':'5px'},
                    {'if': {'column_id': 'S3'},       'width': '55px', 'minWidth': '55px', 'maxWidth': '65px', 'textAlign': 'right', 'paddingRight':'5px'},
                    {'if': {'column_id': 'Status'},   'width': '80px', 'minWidth': '80px', 'maxWidth': '100px'},
                ],
                tooltip_duration=None
            ),
            dbc.Accordion([
                dbc.AccordionItem(
                    children=[dcc.Textarea(id='race-control-log-display', value='Waiting...', 
                                 style={'width': '100%', 'height': '140px', # slightly less height
                                        'backgroundColor': '#2B2B2B', 'color': '#E0E0E0', 
                                        'border': '1px solid #444', 'fontFamily': 'monospace', 
                                        'fontSize':'0.75rem'}, readOnly=True)],
                    title="Race Control Messages", item_id="rcm-accordion"
                ),
                dbc.AccordionItem(
                    children=[html.Div(id='other-data-display',  # Content is updated by existing callback
                                       style={'maxHeight': '140px', 'overflowY': 'auto',
                                              'border': '1px solid #444', 'padding': '8px',
                                              'fontSize': '0.7rem', 'backgroundColor': '#2B2B2B'})],
                    title="Other Data Streams (Debug)",
                    item_id="other-data-accordion",
                    id="debug-data-accordion-item"  # <<< NEW ID FOR VISIBILITY CONTROL
                )
            ], start_collapsed=True, flush=True, className="mt-3", active_item="rcm-accordion")
        ], lg=7, md=12, id='main-timing-col', className="mb-3 mb-lg-0"),

        # --- >>> Right Column: Focusing on vertical space and graph containers <<< ---
        dbc.Col([
            dbc.Card( # Card for Track Map
                dbc.CardBody([
                    html.H5("Track Map", className="card-title mb-2"),
                    html.Div( # Explicitly sized wrapper for the graph
                        style={'height': '360px', 'width': '100%'}, # *** ADJUSTED HEIGHT *
                        children=[
                            dcc.Graph(
                                id='track-map-graph',
                                style={'height': '100%', 'width': '100%'}, # Graph fills wrapper
                                figure=go.Figure(layout={
                                    'template': 'plotly_dark', 'uirevision': 'track_map_main_layout', # Unique uirevision
                                    'xaxis': {'visible': False, 'range': [0,1]}, 
                                    'yaxis': {'visible': False, 'range': [0,1], 'scaleanchor':'x', 'scaleratio':1},
                                    'margin': {'l': 2, 'r': 2, 't': 2, 'b': 2}, # Minimal margins
                                    'plot_bgcolor': 'rgb(30,30,30)', # Explicitly set for empty state too
                                    'paper_bgcolor': 'rgba(0,0,0,0)' # Consistent with data state
                                }),
                                config={
                                    'displayModeBar': False, 
                                    'scrollZoom': False, 
                                    'dragmode': False, 
                                    'autosizable': True,  # <<< Try False, let responsive and JS handle it
                                    'responsive': True     # <<< This is key for window resize
                                }
                            )
                        ]
                    )
                ]), className="mb-2" # Reduced margin-bottom for tighter packing
            ),
            # html.Div(id='dummy-cache-output', style={'display': 'none'}), # Already in Response 8
    
            dbc.Card( # Card for Driver Focus
                dbc.CardBody([
                    html.H5("Driver Focus", className="card-title mb-2"),
                    dcc.Dropdown(
                        id='driver-select-dropdown', options=[], placeholder="Select Driver...",
                        style={'color': '#333', 'marginBottom':'10px', 'fontSize': '0.9rem'}
                    ),
                    dbc.Row([
                         dbc.Col(html.Label("Lap:", style={'fontSize':'0.85rem'}), width="auto", 
                                 className="pe-0 align-self-center"),
                         dbc.Col(dcc.Dropdown(
                                     id='lap-selector-dropdown', options=[], placeholder="Lap",
                                     style={'minWidth': '70px', 'color': '#333', 'fontSize':'0.85rem'},
                                     clearable=False, searchable=False, disabled=True
                                 ), className="ps-1", width=True)
                    ], className="mb-2 align-items-center g-1"),
                    html.Div( # Explicitly sized wrapper for telemetry graph
                        style={'height': '320px'}, # *** ADJUSTED HEIGHT ***
                        children=[
                            dcc.Graph(
                                id='telemetry-graph',
                                style={'height': '100%', 'width': '100%'}, # Graph fills wrapper
                                figure=go.Figure(layout={
                                    'template': 'plotly_dark', 'uirevision': 'telemetry_main_layout', # Unique uirevision
                                    'annotations': [{'text': "Select driver & lap", 'xref': 'paper', 
                                                     'yref': 'paper', 'showarrow': False, 'font': {'size': 10}}],
                                    'xaxis': {'visible': False, 'range': [0,1]}, 
                                    'yaxis': {'visible': False, 'range': [0,1]},
                                    'margin': {'l': 30, 'r': 5, 't': 10, 'b': 20} # Adjusted margins
                                })
                            )
                        ]
                    ),
                    html.Div(
                        id='driver-details-output',
                        style={ # Explicit height, content will scroll
                            'height': '80px', # *** ADJUSTED HEIGHT ***
                            'overflowY': 'auto', 'border': '1px solid #444', 
                            'padding': '5px', 'fontSize': '0.8rem', 'marginTop':'10px', 
                            'backgroundColor': '#2B2B2B'
                        }
                    ),
                ]) # No className="mt-3" here, handled by parent card's mb or overall column flow
            )
        ], lg=5, md=12, id='contextual-info-col', 
           style={'display': 'flex', 'flexDirection': 'column'} # Try flex column for children
        ),
    ], id='main-data-zone', className="mb-2") # Reduced margin

    # --- Analysis Zone (Lap Time Chart) ---
    analysis_zone = dbc.Row([
        dbc.Col([
            dbc.Card(dbc.CardBody([
                html.H5("Lap Time Progression", className="card-title mb-2"),
                dcc.Dropdown(
                    id='lap-time-driver-selector', options=[], value=[], multi=True,
                    placeholder="Select drivers for lap chart...",
                    style={'marginBottom': '10px', 'color': '#333'}
                ),
                html.Div( # Explicitly sized wrapper
                    style={'height': '320px'}, # *** ADJUSTED HEIGHT ***
                    children=[
                        dcc.Graph(
                            id='lap-time-progression-graph',
                            style={'height': '100%', 'width': '100%'}, # Graph fills wrapper
                            figure=go.Figure(layout={
                                'template': 'plotly_dark', 'uirevision': 'lap_prog_main_layout', # Unique uirevision
                                'annotations': [{'text': "Select drivers", 'xref': 'paper', 
                                                 'yref': 'paper', 'showarrow': False, 'font': {'size': 12}}],
                                'xaxis': {'visible': False, 'range': [0,1]}, 
                                'yaxis': {'visible': False, 'range': [0,1]},
                                'margin': {'l': 35, 'r': 5, 't': 20, 'b': 30} # Adjusted margins
                            }),
                            config={'autosizable': True, 'responsive': True}
                        )
                    ]
                )
            ]))
        ], md=12)
    ], className="mt-2 mb-3", id='analysis-zone') # Adjusted margins

    # --- Footer with Debug Switch ---
    app_footer = html.Footer(
        dbc.Row([
            dbc.Col(html.Small("F1 Dashboard", className="text-muted"),
                    width="auto", className="me-auto align-self-center"),
            dbc.Col(
                dbc.Switch(
                    id="debug-mode-switch",  # Same ID as before
                    label="Debug Streams",   # Shorter label for footer
                    value=False,
                    className="form-check-inline"  # Standard Bootstrap class for alignment
                ), width="auto", className="align-self-center"
            )
        ], className="text-center py-2 mt-3 border-top", justify="between")  # justify="between"
    )

    app_layout = dbc.Container([
        stores_and_intervals, # Back to being a direct child
        header_zone,
        control_zone,
        status_weather_bar,
        main_data_zone,
        analysis_zone,
        app_footer
    ], fluid=True, className="dbc dbc-slate p-2")

    logger.info("Layout based on R8 with refined graph stability applied.")
    return app_layout
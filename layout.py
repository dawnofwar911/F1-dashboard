# layout.py
"""
Defines the Dash application layout structure.
"""

import logging
import dash_bootstrap_components as dbc
from dash import dcc, html, dash_table

# Import config and replay file helper - needed to build layout
import config
import replay # For get_replay_files

# No 'app' import needed here

def create_layout():
    """Creates and returns the Dash app layout."""
    logger = logging.getLogger("F1App.Layout") # Get logger if needed
    logger.info("Creating application layout...")

    # Get replay file options dynamically
    try:
        replay.ensure_replay_dir_exists() # Ensure dir exists
        replay_file_options = replay.get_replay_files(config.REPLAY_DIR)
    except Exception as e:
        logger.error(f"Failed to get replay files during layout creation: {e}")
        replay_file_options = []

    # --- Define DataTable Columns Locally ---
    timing_table_columns = [
        {"name": "No.", "id": "No."}, # <<< ADDED Driver Number
        {"name": "Car", "id": "Car"},
        {"name": "Pos", "id": "Pos"},
        {"name": "Tyre", "id": "Tyre"}, # Tyre compound and age
        {"name": "Lap Time", "id": "Time"}, # Renamed for clarity from "Time"
        {"name": "Interval", "id": "Interval"},
        {"name": "Gap", "id": "Gap"},
        {"name": "Last Lap", "id": "Last Lap"},
        {"name": "Best Lap", "id": "Best Lap"},
        {"name": "S1", "id": "S1"},
        {"name": "S2", "id": "S2"},
        {"name": "S3", "id": "S3"},
        {"name": "Pits", "id": "Pits"}, # <<< ADDED Pit Stops
        {"name": "Status", "id": "Status"},
        {'name': 'Speed', 'id': 'Speed', 'type': 'numeric'},
        {'name': 'Gear', 'id': 'Gear', 'type': 'numeric'},
        {'name': 'RPM', 'id': 'RPM', 'type': 'numeric'},
        {'name': 'DRS', 'id': 'DRS'},
    ]
    # --- End Column Definition ---
    
    tyre_style_base = { # Base style for tyre cells
        'textAlign': 'center',
        'fontWeight': 'bold',
        'border': '1px solid #444', # Add a subtle border to colored cells
        # 'borderRadius': '5px', # Slightly rounded corners for the cell
        # 'padding': '2px 4px',
        # 'minWidth': '60px', # Ensure enough width for "S 12L"
    }
    
    track_map_config = {
        'staticPlot': False,
        'displayModeBar': False,
        'scrollZoom': False,
        'editable': False,
        'edits': {
            'annotationPosition': False, 'annotationTail': False, 'annotationText': False,
            'axisTitleText': False, 'colorbarLabel': False, 'colorbarTitleText': False,
            'legendPosition': False, 'legendText': False, 'shapePosition': False, 'titleText': False
        },
        'autosizable': True,
        'responsive': True,
        'displaylogo': False
    }
    
   
    stores_and_intervals_for_clientside = [
        dcc.Store(id='car-positions-store'),
        dcc.Store(id='current-track-layout-cache-key-store'),
        dcc.Interval(
            id='clientside-update-interval',
            interval=1250, 
            n_intervals=0,
            disabled=True
        )
    ]

    layout = dbc.Container([
        dcc.Interval(id='interval-component-map-animation', interval=100, n_intervals=0),
        dcc.Interval(id='interval-component-timing', interval=300, n_intervals=0),
        dcc.Interval(id='interval-component-fast', interval=500, n_intervals=0),
        dcc.Interval(id='interval-component-medium', interval=1000, n_intervals=0),
        dcc.Interval(id='interval-component-slow', interval=5000, n_intervals=0),
        dcc.Interval(id='interval-component-real-slow', interval=10000, n_intervals=0),
        html.Div(id='dummy-output-for-controls', style={'display': 'none'}),
        html.Div(children=stores_and_intervals_for_clientside),

        dbc.Row(dbc.Col(html.H1("F1 Live Timing SignalR Viewer"), width=12), className="mb-3"),
        dbc.Row([dbc.Col(html.Div(id='session-info-display'), width=12)], className="mb-3", id='session-details-row'),
        dbc.Row([
            dbc.Col(html.Div(id='connection-status', children="Status: Initializing..."), width="auto"),
            dbc.Col(html.Div(id='track-status-display', children="Track: Unknown"), width="auto", style={'marginLeft': '20px'}),
        ], className="mb-3 align-items-center"),
        dbc.Row([
            dbc.Col(dbc.Button("Connect", id="connect-button", color="success"), width="auto", className="mb-1"),
            dbc.Col(dbc.Button("Disconnect", id="disconnect-button", color="warning"), width="auto", className="mb-1 me-3"),
            dbc.Col(dbc.Checkbox(id='record-data-checkbox', label="Record Live Data", value=False, className="form-check-inline"), width="auto", className="mb-1 align-self-center"),
            dbc.Col(dcc.Dropdown(id='replay-file-selector', options=replay_file_options, placeholder="Select replay file...", style={'minWidth': '200px', 'color': '#333'}), width=True, className="mb-1"),
            dbc.Col(dcc.Slider(id='replay-speed-slider', min=0.0, max=10, step=0.5, value=1.0, marks={1:'1x', 5:'5x', 10:'10x'}), width=2, className="mb-1 align-self-center", style={'minWidth':'150px'}),
            dbc.Col(dbc.Button("Replay", id="replay-button", color="primary"), width="auto", className="mb-1"),
            dbc.Col(dbc.Button("Stop Replay", id="stop-replay-button", color="danger"), width="auto", className="mb-1"),
        ], className="mb-3 align-items-center g-1"),
        
        dbc.Row([
            dbc.Col([
                html.H4("Live Timing"),
                html.P(id='timing-data-timestamp', children="Waiting...", style={'fontSize':'small', 'color':'grey'}),
                dash_table.DataTable(
                    id='timing-data-actual-table', columns=timing_table_columns, data=[],
                    fixed_rows={'headers': True},
                    style_table={'height': '65vh', 'overflowY': 'auto', 'overflowX': 'auto'},
                    style_cell={ # Default cell style
                        'minWidth': '40px', 'width': '70px', 'maxWidth': '150px', # Adjusted widths
                        'overflow': 'hidden', 'textOverflow': 'ellipsis',
                        'textAlign': 'left', 'padding': '5px',
                        'backgroundColor': 'rgb(50, 50, 50)', 'color': 'white'
                    },
                    style_header={
                        'backgroundColor': 'rgb(30, 30, 30)', 'fontWeight': 'bold',
                        'border': '1px solid grey'
                    },
                    style_data={'borderBottom': '1px solid grey'},
                    style_data_conditional=[
                        {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(60, 60, 60)'},
                        # --- MODIFIED TYRE STYLES ---
                        # Soft Tyre (Red)
                        {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "S " || {Tyre} = "S"'}, # Match "S " or just "S"
                         'backgroundColor': '#D90000', 'color': 'white', **tyre_style_base},
                        # Medium Tyre (Yellow)
                        {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "M " || {Tyre} = "M"'},
                         'backgroundColor': '#EBC000', 'color': '#383838', **tyre_style_base}, # Darker text for yellow
                        # Hard Tyre (White/Light Grey)
                        {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "H " || {Tyre} = "H"'},
                         'backgroundColor': '#E0E0E0', 'color': '#383838', **tyre_style_base}, # Darker text for light grey
                        # Intermediate Tyre (Green)
                        {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "I " || {Tyre} = "I"'},
                         'backgroundColor': '#00A300', 'color': 'white', **tyre_style_base},
                        # Wet Tyre (Blue)
                        {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "W " || {Tyre} = "W"'},
                         'backgroundColor': '#0077FF', 'color': 'white', **tyre_style_base},
                        # Unknown/No Tyre
                        {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} = "-"'},
                         'backgroundColor': 'inherit', 'color': 'grey', 'textAlign': 'center'},
                        # Style for Position column
                        {'if': {'column_id': 'Pos'}, 'textAlign': 'center', 'fontWeight': 'bold', 'width': '40px', 'minWidth':'40px'},
                        # Style for Driver Number column
                        {'if': {'column_id': 'No.'}, 'textAlign': 'right', 'width': '40px', 'minWidth':'40px', 'paddingRight':'2px'},
                         # Style for Car (TLA) column
                        {'if': {'column_id': 'Car'}, 'textAlign': 'left', 'width': '50px', 'minWidth':'50px'},
                        # Style for Pits column
                        {'if': {'column_id': 'Pits'}, 'textAlign': 'center', 'width': '40px', 'minWidth':'40px'},
                    ],
                    tooltip_duration=None
                ),
                html.Hr(),
                html.H4("Other Data Streams"),
                html.Div(id='other-data-display', style={'maxHeight': '20vh', 'overflowY': 'auto', 'border': '1px solid grey', 'padding': '10px', 'fontSize': 'small'}),
                html.H4("Race Control Messages"),
                dcc.Textarea(id='race-control-log-display', value='Waiting...', style={
                             'width': '100%', 'height': '15vh', 'backgroundColor': '#333', 'color': '#DDD', 'border': '1px solid grey', 'fontFamily': 'monospace'}, readOnly=True)
            ], md=7),

            dbc.Col([
                html.H4("Track Map"),
                dcc.Graph(id='track-map-graph', style={'height': '450px', 'marginBottom': '10px'},config={
                    'displayModeBar': False, 'scrollZoom': False, 'dragmode': False }),
                html.Div(id='dummy-cache-output', style={'display': 'none'}),
                html.H4("Driver Details & Telemetry"),
                dcc.Dropdown(
                    id='driver-select-dropdown', options=[], placeholder="Select Driver...",
                    style={'color': '#333', 'marginBottom':'10px'}
                ),
                dbc.Row([
                     dbc.Col(html.Label("Lap:"), width="auto", className="pe-0 align-self-center"),
                     dbc.Col(
                          dcc.Dropdown(
                               id='lap-selector-dropdown', options=[], placeholder="Lap",
                               style={'minWidth': '80px', 'color': '#333'},
                               clearable=False, searchable=False, disabled=True
                          ), width=3
                     )
                ], className="mb-2 align-items-center"),
                dcc.Graph(id='telemetry-graph', style={'height': '25vh'}),
                html.Div(id='driver-details-output', style={'maxHeight': '15vh', 'overflowY': 'auto', 'border': '1px solid #444', 'padding': '5px', 'fontSize': 'small', 'marginTop':'10px'}),
                html.Hr()
            ], md=5),
            dbc.Row([
            dbc.Col([
                html.H4("Lap Time Progression"),
                dcc.Dropdown(
                    id='lap-time-driver-selector',
                    options=[], # Populated by callback
                    value=[],   # Initially no drivers selected, or select a few by default
                    multi=True,
                    placeholder="Select drivers for lap chart...",
                    style={'marginBottom': '10px', 'color': '#333'}
                ),
                dcc.Graph(id='lap-time-progression-graph', style={'height': '400px'})
            ], md=12) # Full width, or adjust as needed
        ], className="mt-4 mb-3"), # Add some margin top
        ])
    ], fluid=True)

    logger.info("Layout creation finished.")
    return layout
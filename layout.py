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
        {"name": "Car", "id": "Car"}, {"name": "Pos", "id": "Pos"}, {"name": "Tyre", "id": "Tyre"},
        {"name": "Time", "id": "Time"}, {"name": "Interval", "id": "Interval"}, {"name": "Gap", "id": "Gap"},
        {"name": "Last Lap", "id": "Last Lap"}, {"name": "Best Lap", "id": "Best Lap"},
        {"name": "S1", "id": "S1"}, {"name": "S2", "id": "S2"}, {"name": "S3", "id": "S3"},
        {"name": "Status", "id": "Status"},
        # Added telemetry columns
        {'name': 'Speed', 'id': 'Speed', 'type': 'numeric'},
        {'name': 'Gear', 'id': 'Gear', 'type': 'numeric'},
        {'name': 'RPM', 'id': 'RPM', 'type': 'numeric'},
        {'name': 'DRS', 'id': 'DRS'},
    ]
    # --- End Column Definition ---

    layout = dbc.Container([
        # --- Added Interval Components ---
        dcc.Interval(id='interval-component-fast', interval=500, n_intervals=0),
        dcc.Interval(id='interval-component-medium', interval=1000, n_intervals=0),
        dcc.Interval(id='interval-component-slow', interval=5000, n_intervals=0),
        html.Div(id='dummy-output-for-controls', style={'display': 'none'}),
        # --- End Added Intervals ---

        dbc.Row(dbc.Col(html.H1("F1 Live Timing SignalR Viewer"), width=12), className="mb-3"),

        # Session Details Row (Keep as is)
        dbc.Row([
            dbc.Col(html.Div(id='session-info-display'), width=12)
        ], className="mb-3", id='session-details-row'),

        # Status Row (Keep as is)
        dbc.Row([
             # Use the IDs from the callbacks file ('connection-status', 'track-status-display')
            dbc.Col(html.Div(id='connection-status', children="Status: Initializing..."), width="auto"),
            dbc.Col(html.Div(id='track-status-display', children="Track: Unknown"), width="auto", style={'marginLeft': '20px'}),
            # Remove heartbeat display for now unless needed
            # dbc.Col(html.Div(id='heartbeat-display'), width="auto", style={'marginLeft': '20px'}),
        ], className="mb-3 align-items-center"), # Added align-items-center

        # Control Row (Keep structure, ensure IDs match callbacks.py)
        dbc.Row([
            dbc.Col(dbc.Button("Connect", id="connect-button", color="success"), width="auto", className="mb-1"), # Added margin bottom
            dbc.Col(dbc.Button("Disconnect", id="disconnect-button", color="warning"), width="auto", className="mb-1 me-3"), # Added margin bottom/end
            dbc.Col(dbc.Checkbox(id='record-data-checkbox', label="Record Live Data", value=False, className="form-check-inline"), width="auto", className="mb-1 align-self-center"), # Align checkbox
            dbc.Col(dcc.Dropdown(id='replay-file-selector', options=replay_file_options, placeholder="Select replay file...", style={'minWidth': '200px', 'color': '#333'}), width=True, className="mb-1"), # Let dropdown take available width
            dbc.Col(dcc.Slider(id='replay-speed-slider', min=0.1, max=10, step=0.1, value=1.0, marks={1:'1x', 5:'5x', 10:'10x'}), width=2, className="mb-1 align-self-center", style={'minWidth':'150px'}), # Give slider some minimum width
            dbc.Col(dbc.Button("Replay", id="replay-button", color="primary"), width="auto", className="mb-1"),
            dbc.Col(dbc.Button("Stop Replay", id="stop-replay-button", color="danger"), width="auto", className="mb-1"),
        ], className="mb-3 align-items-center g-1"), # Use g-1 for smaller gutters between columns
        


        # --- >>> RESTRUCTURED DATA AREA <<< ---
        dbc.Row([
            # --- Left Column ---
            dbc.Col([
                html.H4("Live Timing"),
                # Use timing-data-timestamp ID from callbacks
                html.P(id='timing-data-timestamp', children="Waiting...", style={'fontSize':'small', 'color':'grey'}),
                dash_table.DataTable(
                    id='timing-data-actual-table', columns=timing_table_columns, data=[],
                    fixed_rows={'headers': True},
                    style_table={'height': '65vh', 'overflowY': 'auto', 'overflowX': 'auto'}, # Adjusted height
                    # Keep styling...
                    style_cell={'minWidth': '50px', 'width': '80px', 'maxWidth': '120px','overflow': 'hidden','textOverflow': 'ellipsis','textAlign': 'left','padding': '5px','backgroundColor': 'rgb(50, 50, 50)','color': 'white'},
                    style_header={'backgroundColor': 'rgb(30, 30, 30)','fontWeight': 'bold','border': '1px solid grey'},
                    style_data={'borderBottom': '1px solid grey'},
                    style_data_conditional=[ {'if': {'row_index': 'odd'},'backgroundColor': 'rgb(60, 60, 60)'}, {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "SOFT"'},'backgroundColor': '#FF3333', 'color': 'black', 'fontWeight': 'bold'}, {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "MEDIUM"'},'backgroundColor': '#FFF333', 'color': 'black', 'fontWeight': 'bold'}, {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "HARD"'},'backgroundColor': '#FFFFFF', 'color': 'black', 'fontWeight': 'bold'}, {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "INTERMEDIATE"'},'backgroundColor': '#33FF33', 'color': 'black', 'fontWeight': 'bold'}, {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "WET"'},'backgroundColor': '#3333FF', 'color': 'white', 'fontWeight': 'bold'}, {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} = "-"'},'backgroundColor': 'inherit', 'color': 'grey'}, ],
                    tooltip_duration=None
                ),
                html.Hr(),
                html.H4("Other Data Streams"),
                # Use other-data-display ID from callbacks
                html.Div(id='other-data-display', style={'maxHeight': '20vh', 'overflowY': 'auto', 'border': '1px solid grey', 'padding': '10px', 'fontSize': 'small'}) # Adjusted height
            ], md=7), # 7 columns for timing/other

            # --- Right Column ---
            dbc.Col([
                html.H4("Track Map"),
                dcc.Graph(id='track-map-graph', style={'height': '30vh', 'marginBottom': '10px'}), # Adjusted height

                html.H4("Driver Details & Telemetry"),
                dcc.Dropdown(
                    id='driver-select-dropdown', # <<< DRIVER SELECTOR
                    options=[], # Populated by callback
                    placeholder="Select Driver...",
                    style={'color': '#333', 'marginBottom':'10px'}
                ),
                # --- >>> ADDED Lap Selector and Telemetry Graph <<< ---
                dbc.Row([
                     dbc.Col(html.Label("Lap:"), width="auto", className="pe-0 align-self-center"),
                     dbc.Col(
                          dcc.Dropdown(
                               id='lap-selector-dropdown', # <<< LAP SELECTOR
                               options=[], placeholder="Lap",
                               style={'minWidth': '80px', 'color': '#333'},
                               clearable=False, searchable=False, disabled=True
                          ), width=3 # Adjust width
                     )
                ], className="mb-2 align-items-center"),
                dcc.Graph(id='telemetry-graph', style={'height': '25vh'}), # <<< TELEMETRY GRAPH AREA
                # --- >>> END ADDED <<< ---
                html.Div(id='driver-details-output', style={'maxHeight': '15vh', 'overflowY': 'auto', 'border': '1px solid #444', 'padding': '5px', 'fontSize': 'small', 'marginTop':'10px'}), # Area for other details

                html.Hr(),
                html.H4("Race Control Messages"),
                # Use race-control-log-display ID from callbacks
                dcc.Textarea(id='race-control-log-display', value='Waiting...', style={'width': '100%', 'height': '15vh', 'backgroundColor': '#333', 'color': '#DDD', 'border': '1px solid grey', 'fontFamily': 'monospace'}, readOnly=True)

            ], md=5) # 5 columns for map/details/rc
        ])
        # --- >>> END RESTRUCTURED DATA AREA <<< ---

    ], fluid=True) # Use fluid container

    logger.info("Layout creation finished.")
    return layout

# The following line is typically NOT needed if layout is imported by main.py/app_instance.py
# print("DEBUG: layout module loaded")
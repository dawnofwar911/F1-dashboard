# layout.py
"""
Defines the Dash application layout structure.
"""
import dash_bootstrap_components as dbc
from dash import dcc, html, dash_table

# Import config and replay file helper - needed to build layout
import config
import replay # For get_replay_files

# No 'app' import needed here

def create_layout():
    """Creates and returns the Dash app layout."""
    # Get replay file options dynamically when layout is created
    replay.ensure_replay_dir_exists() # Ensure dir exists
    replay_file_options = replay.get_replay_files(config.REPLAY_DIR)

    layout = dbc.Container([
        dbc.Row(dbc.Col(html.H1("F1 Live Timing SignalR Viewer"), width=12), className="mb-3"),

        # Session Details Row
        dbc.Row([
            dbc.Col(html.Div(id='session-info-display'), width=12)
        ], className="mb-3", id='session-details-row'),

        # Status Row
        dbc.Row([
            dbc.Col(html.Div(id='status-display'), width="auto"),
            dbc.Col(html.Div(id='heartbeat-display'), width="auto", style={'marginLeft': '20px'}),
            dbc.Col(html.Div(id='track-status-display', children="Track: Unknown"), width="auto", style={'marginLeft': '20px'}),
        ], className="mb-3"),

        # Control Row
        dbc.Row([
            dbc.Col([
                 html.Div([
                     dbc.Button("Start Live Feed", id="start-button", color="success", className="me-2"),
                     # Assumes app_state is imported where Checkbox 'value' is defined OR set by callback later
                     # For direct value setting here, layout.py would need app_state.
                     # Let's rely on the callback to set initial state if needed, or set value prop directly.
                     # Defaulting value=False seems safest if app_state not imported here.
                     dbc.Checkbox(id='record-data-checkbox', label="Record Live Data", value=False, # Default unchecked visually
                                  className="form-check-inline", inputStyle={"marginRight": "5px"}),
                 ], style={'display': 'flex', 'alignItems': 'center'})
            ], width="auto"),
            dbc.Col(dbc.Button("Stop Feed / Replay", id="stop-button", color="danger", className="me-1"), width="auto"),
            dbc.Col([
                 html.Div([
                     dcc.Dropdown(id='replay-file-dropdown', options=replay_file_options, placeholder="Select replay file...",
                                  style={'minWidth': '300px', 'flexGrow': '1', 'color': '#333'}, className="me-2"),
                     html.Button('Refresh List', id='refresh-replay-list-button', n_clicks=0,
                                 style={'height': '38px', 'minWidth': '110px'}, className="me-2"),
                     dbc.Input(id="replay-speed-input", placeholder="Speed", type="number", min=0.1, step=0.1, value=1.0,
                               debounce=True, style={'width': '90px'}, className="me-2"),
                     dbc.Button("Start Replay", id="replay-button", color="primary", className="me-1"),
                 ], style={'display': 'flex', 'alignItems': 'center', 'width': '100%', 'flexWrap': 'wrap'})
            ], width=True),
        ], className="mb-3 align-items-center"),

        # Data Display Row (Live Data + Timing Table)
        dbc.Row([
             dbc.Col([
                 html.H3("Latest Data (Non-Timing)"),
                 html.Div(id='live-data-display', style={'maxHeight': '300px', 'overflowY': 'auto', 'border': '1px solid grey', 'padding': '10px', 'marginBottom': '10px'}),
                 html.H3("Timing Data Details"),
                 html.Div(id='timing-data-table', children=[
                     html.P(id='timing-data-timestamp', children="Waiting for data..."),
                     # Use columns from config
                     dash_table.DataTable(id='timing-data-actual-table', columns=config.TIMING_TABLE_COLUMNS, data=[],
                                          fixed_rows={'headers': True}, style_table={'height': '400px', 'overflowY': 'auto', 'overflowX': 'auto'},
                                          style_cell={'minWidth': '50px', 'width': '80px', 'maxWidth': '120px','overflow': 'hidden','textOverflow': 'ellipsis','textAlign': 'left','padding': '5px','backgroundColor': 'rgb(50, 50, 50)','color': 'white'},
                                          style_header={'backgroundColor': 'rgb(30, 30, 30)','fontWeight': 'bold','border': '1px solid grey'},
                                          style_data={'borderBottom': '1px solid grey'},
                                          style_data_conditional=[
                                              {'if': {'row_index': 'odd'},'backgroundColor': 'rgb(60, 60, 60)'},
                                              {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "SOFT"'},'backgroundColor': '#FF3333', 'color': 'black', 'fontWeight': 'bold'},
                                              {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "MEDIUM"'},'backgroundColor': '#FFF333', 'color': 'black', 'fontWeight': 'bold'},
                                              {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "HARD"'},'backgroundColor': '#FFFFFF', 'color': 'black', 'fontWeight': 'bold'},
                                              {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "INTERMEDIATE"'},'backgroundColor': '#33FF33', 'color': 'black', 'fontWeight': 'bold'},
                                              {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} contains "WET"'},'backgroundColor': '#3333FF', 'color': 'white', 'fontWeight': 'bold'},
                                              {'if': {'column_id': 'Tyre', 'filter_query': '{Tyre} = "-"'},'backgroundColor': 'inherit', 'color': 'grey'},
                                          ], tooltip_duration=None)
                 ])
             ], width=12)
        ]),

        # Race Control Row
        dbc.Row([
            dbc.Col([
                html.H3("Race Control Messages"),
                dcc.Textarea(id='race-control-log-display', value='Waiting for Race Control messages...',
                             style={'width': '100%', 'height': '200px', 'backgroundColor': '#333', 'color': '#DDD', 'border': '1px solid grey', 'fontFamily': 'monospace'},
                             readOnly=True)
            ], width=12)
        ], className="mb-3"),

        # Track Map Row
        dbc.Row([
            dbc.Col(dcc.Graph(id='track-map-graph', style={'height': '60vh'}))
        ], className="mt-3"),

        # Interval component
        dcc.Interval(id='interval-component', interval=500, n_intervals=0), # You might want interval in config?

    ], fluid=True)

    return layout

print("DEBUG: layout module loaded")
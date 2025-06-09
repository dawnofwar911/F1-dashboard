# layout.py
import logging
import dash_bootstrap_components as dbc
from dash import dcc, html, dash_table # Removed Input, Output, State as they are for callbacks
import plotly.graph_objects as go

# Import config for constants and replay for file listing
import config 
import replay 
import utils # Make sure utils is imported if create_empty_figure_with_message is used

sidebar_header = dbc.Row(
    [dbc.Col(html.H4(config.APP_TITLE, className="app-title"), className="text-center")],
    className="my-3",
)

sidebar = html.Div([ 
    sidebar_header,
    html.Hr(style={'borderColor': '#34495e'}),
    dbc.Nav([
        dbc.NavLink(
            [html.I(className="fas fa-tachometer-alt me-2"), "Live Dashboard"],
            href="/", active="exact", className="nav-link-custom mb-1"
        ),
        dbc.NavLink(
            [html.I(className="fas fa-calendar-alt me-2"), "Race Schedule"],
            href="/schedule", active="exact", className="nav-link-custom mb-1"
        ),
        dbc.NavLink(
            [html.I(className="fas fa-cog me-2"), "Settings"],
            href="/settings", active="exact", className="nav-link-custom mb-1"
        )
    ], vertical=True, pills=True, className="flex-grow-1"),
    html.Div([
        html.Small("F1 Dashboard v0.2.1", className="text-muted") 
    ], className="text-center mt-auto p-2", style={'position':'absolute', 'bottom':'0', 'left':'0', 'right':'0'})
], style=config.SIDEBAR_STYLE_HIDDEN, id="sidebar")

# --- Sidebar Toggle Button ---
# (sidebar_toggle_button remains unchanged)
sidebar_toggle_button = dbc.Button(
    html.I(className="fas fa-bars"), 
    id="sidebar-toggle",
    n_clicks=0,
    className="position-fixed top-0 start-0 m-2 p-2", 
    style={"zIndex": 1032, "fontSize": "1.2rem", "border": "none", "background": "rgba(0,0,0,0.3)"},
    color="light",
    outline=True
)

content_area = html.Div(id="page-content", style=config.CONTENT_STYLE_FULL_WIDTH)


# --- Main App Layout Definition (CORRECTED) ---
main_app_layout = html.Div([
    dcc.Location(id="url", refresh=False),
    
    # --- Globally Shared Stores ---
    # Only stores that need to be accessed across multiple pages go here.
    dcc.Store(id='session-preferences-store', storage_type='local'),
    dcc.Store(id='sidebar-state-store', data={'is_open': False}, storage_type='local'),
    dcc.Store(id='sidebar-toggle-signal', data=None),
    dcc.Store(id='user-session-id', storage_type='session'),
    dcc.Store(id='user-timezone-store-data', storage_type='session'),
    
    # --- Main Page Components ---
    sidebar_toggle_button,
    sidebar,
    content_area,
    
    # --- Dummy Divs for callbacks without direct UI outputs on the main layout ---
    html.Div(id='dummy-output-for-controls', style={'display': 'none'}),
    html.Div(id='js-click-data-holder', children=None, style={'display': 'none'}),
    html.Div(id='dummy-output-for-autostart-thread', style={'display': 'none'}),
])

def define_dashboard_layout():
    logger = logging.getLogger("F1App.Layout")
    # logger.info("Creating application layout...") # Can be noisy

    try:
        replay.ensure_replay_dir_exists()
        replay_file_options = replay.get_replay_files(config.REPLAY_DIR)
    except Exception as e:
        logger.error(f"Failed to get replay files during layout creation: {e}")
        replay_file_options = []
        
    stores_and_intervals = html.Div([
        dcc.Interval(id='interval-component-map-animation', interval=100, n_intervals=0),
        dcc.Interval(id='interval-component-timing', interval=350, n_intervals=0),
        dcc.Interval(id='interval-component-fast', interval=500, n_intervals=0),
        dcc.Interval(id='interval-component-medium', interval=1000, n_intervals=0),
        dcc.Interval(id='interval-component-slow', interval=5000, n_intervals=0),
        dcc.Interval(id='interval-component-real-slow', interval=10000, n_intervals=0),
        dcc.Store(id='car-positions-store'),
        dcc.Store(id='current-track-layout-cache-key-store'),
        dcc.Store(id='track-map-figure-version-store'),
        dcc.Store(id='track-map-yellow-key-store', storage_type='memory', data=""),
        dcc.Store(id='clicked-car-driver-number-store', storage_type='memory'),
        dcc.Interval(id='clientside-click-poll-interval', interval=100, n_intervals=0), 
        dcc.Interval(id='clientside-update-interval', interval=1250, n_intervals=0, disabled=True)
    ])

    tyre_style_base = {'textAlign': 'center', 'fontWeight': 'bold'}
    # ... (Your style definitions: PERSONAL_BEST_STYLE, OVERALL_BEST_STYLE, etc. remain unchanged) ...
    PERSONAL_BEST_STYLE = {'backgroundColor': '#28a745', 'color': 'white', 'fontWeight': 'bold'} 
    OVERALL_BEST_STYLE = {'backgroundColor': '#6f42c1', 'color': 'white', 'fontWeight': 'bold'}  
    REGULAR_LAP_SECTOR_STYLE = {'backgroundColor': '#ffc107', 'color': '#343a40', 'fontWeight': 'normal'}
    IN_PIT_STYLE = {'backgroundColor': '#dc3545', 'color': 'white', 'fontWeight': 'bold', 'textAlign': 'center'}
    PIT_DURATION_STYLE = {'backgroundColor': '#007bff', 'color': 'white', 'fontWeight': 'bold', 'textAlign': 'center'}

    header_zone = dbc.Row([
        # ... (header_zone remains unchanged) ...
        dbc.Col(html.H2(config.APP_TITLE, className="mb-0"), width="auto", lg=4), 
        dbc.Col(html.Div(id='session-info-display', children=config.TEXT_SESSION_INFO_AWAITING), 
                lg=5, className="text-center align-self-center"),
        dbc.Col(html.Div(id='connection-status', children=config.STATUS_INITIALIZING), 
                lg=3, className="text-end align-self-center")
    ], className="mb-2 p-2 bg-dark text-white rounded", id='header-zone', align="center")

    # --- MODIFIED control_card_content_list ---
    control_card_content_list = [
        dbc.Row([
            dbc.Col(dbc.Button("Connect Live", id="connect-button", color="success", size="sm"), width="auto", className="me-1"),
        ], className="mb-2 justify-content-start justify-content-md-start"),

        html.Hr(style={'marginTop': '15px', 'marginBottom': '15px'}), # Separator

        dbc.Row([
            dbc.Col(dcc.Dropdown(id='replay-file-selector', options=replay_file_options,
                                 placeholder=config.TEXT_REPLAY_SELECT_FILE, style={'color': '#333', 'minWidth': '180px'}),
                    xs=12, sm=6, md=4, lg=4, className="mb-2 mb-sm-0"),
            dbc.Col(dcc.Slider(id='replay-speed-slider', min=0.1, max=10, step=0.1, value=1.0,
                               marks={0.5:'0.5x', 1:'1x', 2:'2x', 5:'5x', 10:'10x'},
                               tooltip={"placement": "bottom", "always_visible": False}),
                    xs=12, sm=6, md=4, lg=4, className="align-self-center mb-2 mb-sm-0 px-md-3"),
            dbc.Col(dbc.Button("Start Replay", id="replay-button", color="primary", size="sm"), width="auto", className="me-1"),
        ], align="center", className="justify-content-start justify-content-md-start mb-2"),
        dbc.Row([
            dbc.Col(
                dbc.Button("Stop & Reset Session", id="stop-reset-button", color="danger", outline=True, size="sm", className="w-100"),
                xs=12, sm=6, md=4, lg=4
            )
        ],className="justify-content-start justify-content-md-start mt-2")
    ]
    # --- END MODIFIED control_card_content_list ---
    
    status_alert = dbc.Alert(
    id="status-alert",
    is_open=False,
    duration=4000, # Alert will disappear after 4 seconds
    fade=True,
    )

    control_zone = html.Div([
        # ... (control_zone structure remains unchanged, uses control_card_content_list) ...
        dbc.Button("Show/Hide Controls",id="collapse-controls-button",className="mb-2",color="secondary",n_clicks=0,size="sm"),
        dbc.Collapse(
            dbc.Card(dbc.CardBody(children=control_card_content_list)),
            id="collapse-controls",
            is_open=True, 
        )
    ], className="mb-3", id='control-zone-wrapper')

    # ... (lap_and_session_time_info_component, status_weather_bar, main_data_zone, analysis_zone, app_footer remain unchanged) ...
    lap_and_session_time_info_component = html.Div(
        [
            html.Div(
                [
                    html.Span("Laps: ", className='lap-time-label', id='lap-counter-label'),
                    html.Span("0/0", id='lap-counter', className='lap-time-value')
                ],
                id='lap-counter-div',
                className='lap-time-info-item', 
                style={'display': 'inline-block', 'margin-right': '20px', 'color': 'white',
                       'font-size': '0.9rem'}
            ),
            html.Div(
                [
                    html.Span(id='session-timer-label', className='lap-time-label',
                              style={'margin-right': '5px', 'color': 'white', 'font-size': '0.9rem'}),
                    html.Span("00:00:00", id='session-timer', className='lap-time-value',
                              style={'color': 'white', 'font-weight': 'bold', 'font-size': '0.9rem'})
                ],
                id='session-timer-div',
                className='lap-time-info-item',
                style={'display': 'inline-block'}
            )
        ],
        id='lap-time-info', 
        className='lap-time-info-container text-center',
        style={'padding': '0px'}
    )

    status_weather_bar = dbc.Row([
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    children=[lap_and_session_time_info_component],
                    className="p-2", 
                    style={'minHeight':'55px', 'display':'flex', 'alignItems':'center', 'justifyContent':'center'}
                ),
                color="dark",
                inverse=True,
                id="lap-session-timer-card"
            ),
            lg=4, md=5, sm=12, xs=12, className="mb-2 mb-lg-0",
            id='lap-session-timer-column'
        ),
        dbc.Col( 
            dbc.Card( 
                dbc.CardBody( 
                    html.Div([ 
                        html.Strong("Track Status: ", style={'marginRight':'5px'}), 
                        html.Span(id='prominent-track-status-text', children=config.TEXT_TRACK_STATUS_DEFAULT_LABEL, 
                                  style={'fontWeight':'bold', 'padding':'2px 5px', 'borderRadius':'4px'}) 
                    ]), 
                    className="p-2 text-center", 
                    style={'minHeight':'55px', 'display':'flex', 'alignItems':'center', 'justifyContent':'center'} 
                ), 
                id='prominent-track-status-card', 
                color="secondary", 
                inverse=True 
            ), 
            lg=3, md=3, sm=12, xs=12, className="mb-2 mb-lg-0", 
            id='track-status-column' 
        ),
        dbc.Col( 
            dbc.Card( 
                dbc.CardBody( 
                    html.Div([ 
                        html.Span(id='weather-main-icon', className="me-2", style={'fontSize': '1.5rem'}), 
                        html.Div(id='prominent-weather-display', children=config.TEXT_WEATHER_AWAITING, 
                                 style={'fontSize':'0.8rem', 'lineHeight':'1.2'}) 
                    ], style={'display': 'flex', 'alignItems': 'center'}), 
                    className="p-2" 
                ), 
                id='prominent-weather-card', 
                color="light", 
                style={'minHeight':'55px'} 
            ), 
            lg=5, md=4, sm=12, xs=12, 
            id='weather-column' 
        )
    ], className="mb-3", id='status-weather-bar', align="center") 

    main_data_zone = dbc.Row([
        dbc.Col([
            html.H4("Live Timing"),
            html.P(id='timing-data-timestamp', children=config.TEXT_WAITING_FOR_DATA, style={'fontSize':'0.8rem', 'color':'grey', 'marginBottom':'2px'}),
            dash_table.DataTable(
                id='timing-data-actual-table',
                fixed_rows={'headers': True},
                style_table={'height': '750px', 'minHeight': '650px', 'overflowY': 'auto', 'overflowX': 'auto'},
                style_cell={
                    'minWidth': '30px', 'width': 'auto', 
                    'overflow': 'hidden', 'textOverflow': 'ellipsis', 
                    'textAlign': 'left', 'padding': '1px 5px', 'fontSize':'0.70rem',
                    'backgroundColor': 'rgb(50, 50, 50)', 'color': 'white',
                    'whiteSpace': 'normal', 
                    'height': 'auto',
                    'lineHeight': '1.2'
                },
                css=[
                    {
                        'selector': 'td div.cell-markdown > p',
                        'rule': '''
                                margin-top: 0 !important;
                                margin-bottom: 0 !important;
                                padding-top: 0.1em !important;
                                padding-bottom: 0.1em !important;
                                line-height: 1.1 !important;
                        '''
                    },
                ],
                style_header={
                    'backgroundColor': 'rgb(30, 30, 30)', 'fontWeight': 'bold',
                    'border': '1px solid #444',
                    'padding': '2px 5px', 
                    'fontSize': '0.75rem', 
                    'textAlign': 'center',
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'lineHeight': '1.3'
                },
                style_data={ 
                    'borderBottom': '1px solid grey',
                    'padding': '1px 5px', 
                    'lineHeight': '1.2'
                },
                style_data_conditional=[
                    {'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(60, 60, 60)'},
                    {'if': {'column_id': 'Tyre'},
                        'width': '50px',
                        'minWidth': '45px',
                        'maxWidth': '60px'
                       },
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
                        'backgroundColor': 'inherit', 
                        'color': 'grey',
                        'textAlign': 'center',
                        'fontWeight': 'bold' 
                       },
                    {'if': {'column_id': 'Pos'}, 'textAlign': 'center', 'fontWeight': 'bold',
                        'width': '35px', 'minWidth': '35px', 'maxWidth': '40px'},
                    {'if': {'column_id': 'No.'}, 'textAlign': 'right', 'width': '35px',
                        'minWidth': '35px', 'maxWidth': '40px', 'paddingRight': '2px'},
                    {'if': {'column_id': 'Car'}, 'textAlign': 'left',
                        'width': '45px', 'minWidth': '45px', 'maxWidth': '55px'},
                    {'if': {'column_id': 'Pits'},
                       'width': '80px',
                       'minWidth': '70px',
                       'maxWidth': '100px',
                       'textAlign': 'center',
                       'whiteSpace': 'nowrap'
                       },
                    {'if': {'column_id': 'IntervalGap'},
                        'width': '75px',
                        'minWidth': '70px',
                        'maxWidth': '90px',
                        'textAlign': 'right',
                        'paddingRight': '5px'
                       },
                    {'if': {'column_id': 'Best Lap', 'filter_query': '{IsOverallBestLap_Str} = "TRUE" && {Best Lap} != "-"'}, **OVERALL_BEST_STYLE},
                    {'if': {'column_id': 'Last Lap', 'filter_query': '{IsLastLapEventOverallBest_Str} = "TRUE" && {Last Lap} != "-"'}, **OVERALL_BEST_STYLE},
                    {'if': {'column_id': 'S1', 'filter_query': '{IsS1EventOverallBest_Str} = "TRUE" && {S1} != "-"'}, **OVERALL_BEST_STYLE},
                    {'if': {'column_id': 'S2', 'filter_query': '{IsS2EventOverallBest_Str} = "TRUE" && {S2} != "-"'}, **OVERALL_BEST_STYLE},
                    {'if': {'column_id': 'S3', 'filter_query': '{IsS3EventOverallBest_Str} = "TRUE" && {S3} != "-"'}, **OVERALL_BEST_STYLE},
                    {'if': {'column_id': 'Last Lap', 'filter_query': '{IsLastLapPersonalBest_Str} = "TRUE" && {IsLastLapEventOverallBest_Str} = "FALSE" && {Last Lap} != "-"'}, **PERSONAL_BEST_STYLE},
                    {'if': {'column_id': 'S1', 'filter_query': '{IsPersonalBestS1_Str} = "TRUE" && {IsS1EventOverallBest_Str} = "FALSE" && {S1} != "-"'}, **PERSONAL_BEST_STYLE},
                    {'if': {'column_id': 'S2', 'filter_query': '{IsPersonalBestS2_Str} = "TRUE" && {IsS2EventOverallBest_Str} = "FALSE" && {S2} != "-"'}, **PERSONAL_BEST_STYLE},
                    {'if': {'column_id': 'S3', 'filter_query': '{IsPersonalBestS3_Str} = "TRUE" && {IsS3EventOverallBest_Str} = "FALSE" && {S3} != "-"'}, **PERSONAL_BEST_STYLE},
                    {'if': {'column_id': 'Last Lap', 'filter_query': '{IsLastLapPersonalBest_Str} = "FALSE" && {IsLastLapEventOverallBest_Str} = "FALSE" && {Last Lap} != "-"'}, **REGULAR_LAP_SECTOR_STYLE},
                    {'if': {'column_id': 'S1', 'filter_query': '{IsPersonalBestS1_Str} = "FALSE" && {IsS1EventOverallBest_Str} = "FALSE" && {S1} != "-"'}, **REGULAR_LAP_SECTOR_STYLE},
                    {'if': {'column_id': 'S2', 'filter_query': '{IsPersonalBestS2_Str} = "FALSE" && {IsS2EventOverallBest_Str} = "FALSE" && {S2} != "-"'}, **REGULAR_LAP_SECTOR_STYLE},
                    {'if': {'column_id': 'S3', 'filter_query': '{IsPersonalBestS3_Str} = "FALSE" && {IsS3EventOverallBest_Str} = "FALSE" && {S3} != "-"'}, **REGULAR_LAP_SECTOR_STYLE},
                    {'if': {'column_id': 'Pits',
                             'filter_query': '{PitDisplayState_Str} = "IN_PIT_LIVE"'}, **IN_PIT_STYLE},
                    {'if': {'column_id': 'Pits',
                             'filter_query': '{PitDisplayState_Str} = "SHOW_COMPLETED_DURATION"'}, **PIT_DURATION_STYLE},
                    {'if': {'column_id': ['Pos', 'No.', 'Car', 'IntervalGap', 'Pits', 'Status'], 
                             'filter_query': '{QualiHighlight_Str} = "RED_DANGER"'},
                       **config.QUALIFYING_DANGER_RED_STYLE},
                    {'if': {'column_id': ['Pos', 'No.', 'Car', 'Tyre', 'Last Lap', 'IntervalGap', 'Best Lap', 'S1', 'S2', 'S3', 'Pits', 'Status'], 
                             'filter_query': '{QualiHighlight_Str} = "GREY_ELIMINATED"'},
                       **config.QUALIFYING_ELIMINATED_STYLE},
                    {'if': {'column_id': 'Last Lap'}, 'width': '70px', 'minWidth': '70px',
                        'maxWidth': '85px', 'textAlign': 'right', 'paddingRight': '5px'},
                    {'if': {'column_id': 'Best Lap'}, 'width': '70px', 'minWidth': '70px',
                        'maxWidth': '85px', 'textAlign': 'right', 'paddingRight': '5px'},
                    {'if': {'column_id': 'S1'},       'width': '55px', 'minWidth': '55px',
                        'maxWidth': '65px', 'textAlign': 'right', 'paddingRight': '5px'},
                    {'if': {'column_id': 'S2'},       'width': '55px', 'minWidth': '55px',
                        'maxWidth': '65px', 'textAlign': 'right', 'paddingRight': '5px'},
                    {'if': {'column_id': 'S3'},       'width': '55px', 'minWidth': '55px',
                        'maxWidth': '65px', 'textAlign': 'right', 'paddingRight': '5px'},
                    {'if': {'column_id': 'Status'},   'width': '80px',
                        'minWidth': '80px', 'maxWidth': '100px'},
                ],
                tooltip_duration=None
            ),
            dbc.Accordion([
                dbc.AccordionItem(
                    children=[dcc.Textarea(id='race-control-log-display', value=config.TEXT_RC_WAITING,
                                            style={'width': '100%', 'height': '140px',
                                                   'backgroundColor': '#2B2B2B', 'color': '#E0E0E0',
                                                   'border': '1px solid #444', 'fontFamily': 'monospace',
                                                   'fontSize':'0.75rem'}, readOnly=True)],
                    title="Race Control Messages", item_id="rcm-accordion"
                ),
                dbc.AccordionItem( 
                    children=[
                        html.Div(
                            id='team-radio-display',
                            style={
                                'maxHeight': '200px', 
                                'overflowY': 'auto',
                                'border': '1px solid #444',
                                'padding': '8px',
                                'fontSize': '0.75rem',
                                'backgroundColor': '#2B2B2B',
                                'color': '#E0E0E0' 
                            }
                        )
                    ],
                    title="Team Radio", 
                    item_id="team-radio-accordion"
                ), 
                dbc.AccordionItem(
                    children=[html.Div(id='other-data-display',
                                       style={'maxHeight': '140px', 'overflowY': 'auto',
                                              'border': '1px solid #444', 'padding': '8px',
                                              'fontSize': '0.7rem', 'backgroundColor': '#2B2B2B'})],
                    title="Other Data Streams (Debug)",
                    item_id="other-data-accordion",
                    id="debug-data-accordion-item" 
                )
            ], start_collapsed=True, flush=True, className="mt-3", active_item="rcm-accordion")
        ], lg=7, md=12, id='main-timing-col', className="mb-3 mb-lg-0"),

        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.H5("Track Map", className="card-title mb-2"),
                    html.Div(
                        style={'height': f'{config.TRACK_MAP_WRAPPER_HEIGHT}px', 'width': '100%'}, 
                        children=[
                            dcc.Graph(
                                id='track-map-graph',
                                style={'height': '100%', 'width': '100%'},
                                figure=go.Figure(layout={
                                    'template': 'plotly_dark',
                                    'uirevision': config.INITIAL_TRACK_MAP_UIREVISION, 
                                    'xaxis': {'visible': False, 'range': [0,1], 'fixedrange': True},
                                    'yaxis': {'visible': False, 'range': [0,1], 'scaleanchor':'x', 'scaleratio':1, 'fixedrange': True},
                                    'margin': config.TRACK_MAP_MARGINS, 
                                    'plot_bgcolor': 'rgb(30,30,30)',
                                    'paper_bgcolor': 'rgba(0,0,0,0)',
                                    'dragmode': False # Disable all drag modes
                                }),
                                config={
                                    'displayModeBar': False,
                                    'scrollZoom': False, # Explicitly disable scroll zoom
                                    'autosizable': True,
                                    'responsive': True
                                }
                            )
                        ]
                    )
                ]), className="mb-2"
            ),
            dbc.Card(
                dbc.CardBody([
                    html.H5("Driver Focus", className="card-title mb-2"),
                    dcc.Dropdown(
                        id='driver-select-dropdown', options=[], placeholder=config.TEXT_DRIVER_SELECT,
                        style={'color': '#333', 'marginBottom':'10px', 'fontSize': '0.9rem'}
                    ),
                    html.Div(id='driver-details-output',
                             style={'marginBottom':'10px', 'fontSize': '0.8rem', 'minHeight': '40px'}),
                    dbc.Tabs(
                        id="driver-focus-tabs",
                        active_tab="tab-telemetry", 
                        children=[
                            dbc.Tab(label="Telemetry", tab_id="tab-telemetry", children=[
                                dbc.Row([
                                     dbc.Col(html.Label("Lap:", style={'fontSize':'0.85rem'}), width="auto",
                                             className="pe-0 align-self-center"),
                                     dbc.Col(dcc.Dropdown(
                                             id='lap-selector-dropdown', options=[], placeholder="Lap",
                                             style={'minWidth': '70px', 'color': '#333', 'fontSize':'0.85rem'},
                                             clearable=False, searchable=False, disabled=True
                                         ), className="ps-1", width=True)
                                ], className="mb-2 align-items-center g-1"),
                                html.Div(
                                    style={'height': f'{config.TELEMETRY_WRAPPER_HEIGHT}px'}, 
                                    children=[
                                        dcc.Graph(
                                            id='telemetry-graph',
                                            style={'height': '100%', 'width': '100%'},
                                            figure=go.Figure(layout={
                                                'template': 'plotly_dark',
                                                'uirevision': config.INITIAL_TELEMETRY_UIREVISION, 
                                                'annotations': [{'text': config.TEXT_DRIVER_SELECT_LAP, 'xref': 'paper', 
                                                                 'yref': 'paper', 'showarrow': False, 'font': {'size': 10}}],
                                                'xaxis': {'visible': False, 'range': [0,1]},
                                                'yaxis': {'visible': False, 'range': [0,1]},
                                                'margin': config.TELEMETRY_MARGINS_EMPTY 
                                            })
                                        )
                                    ]
                                )
                            ]), 
                            dbc.Tab(label="Stint History", tab_id="tab-stint-history", children=[
                                html.Div( 
                                    dash_table.DataTable(
                                        id='stint-history-table',
                                        columns=[ 
                                            {'name': 'Stint', 'id': 'stint_number'},
                                            {'name': 'Lap In', 'id': 'start_lap'},
                                            {'name': 'Compound', 'id': 'compound'},
                                            {'name': 'New', 'id': 'is_new_tyre_display'},
                                            {'name': 'Age (Start)', 'id': 'tyre_age_at_stint_start'},
                                            {'name': 'Lap Out', 'id': 'end_lap'},
                                            {'name': 'Stint Laps', 'id': 'total_laps_on_tyre_in_stint'},
                                            {'name': 'Total Tyre Age', 'id': 'tyre_total_laps_at_stint_end'},
                                        ],
                                        style_table={'height': f'{config.TELEMETRY_WRAPPER_HEIGHT}px', 'overflowY': 'auto', 'marginTop': '10px'},
                                        style_cell={
                                            'textAlign': 'center', 'padding': '3px', 'fontSize':'0.75rem',
                                            'backgroundColor': 'rgb(60, 60, 60)', 'color': 'white',
                                            'border': '1px solid rgb(80,80,80)'
                                        },
                                        style_header={
                                            'backgroundColor': 'rgb(40, 40, 40)',
                                            'fontWeight': 'bold',
                                            'textAlign': 'center',
                                            'padding': '5px'
                                        },
                                        style_data_conditional=[
                                            {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(50, 50, 50)'},
                                            # Add rules for compounds
                                            {'if': {'column_id': 'compound', 'filter_query': '{compound} = "SOFT"'},
                                             'backgroundColor': '#D90000', 'color': 'white', 'fontWeight': 'bold'},
                                            {'if': {'column_id': 'compound', 'filter_query': '{compound} = "MEDIUM"'},
                                             'backgroundColor': '#EBC000', 'color': '#383838', 'fontWeight': 'bold'},
                                            {'if': {'column_id': 'compound', 'filter_query': '{compound} = "HARD"'},
                                             'backgroundColor': '#E0E0E0', 'color': '#383838', 'fontWeight': 'bold'},
                                            {'if': {'column_id': 'compound', 'filter_query': '{compound} = "INTERMEDIATE"'},
                                             'backgroundColor': '#00A300', 'color': 'white', 'fontWeight': 'bold'},
                                            {'if': {'column_id': 'compound', 'filter_query': '{compound} = "WET"'},
                                             'backgroundColor': '#0077FF', 'color': 'white', 'fontWeight': 'bold'},
                                        ]
                                    ),
                                    style={'height': f'{config.TELEMETRY_WRAPPER_HEIGHT}px', 'marginTop': '5px'}
                                )
                            ]) 
                        ] 
                    ) 
                ]) 
            ) 
        ], lg=5, md=12, id='contextual-info-col',
           style={'display': 'flex', 'flexDirection': 'column'}
        ),
    ], id='main-data-zone', className="mb-2")

    analysis_zone = dbc.Row([
        dbc.Col([
            dbc.Card(dbc.CardBody([
                html.H5("Lap Time Progression", className="card-title mb-2"),
                dcc.Dropdown(
                    id='lap-time-driver-selector', options=[], value=[], multi=True,
                    placeholder=config.TEXT_LAP_CHART_SELECT_DRIVERS_PLACEHOLDER,
                    style={'marginBottom': '10px', 'color': '#333'}
                ),
                html.Div(
                    style={'height': f'{config.LAP_PROG_WRAPPER_HEIGHT}px'}, 
                    children=[
                        dcc.Graph(
                            id='lap-time-progression-graph',
                            style={'height': '100%', 'width': '100%'},
                            figure=go.Figure(layout={
                                'template': 'plotly_dark',
                                'uirevision': config.INITIAL_LAP_PROG_UIREVISION, 
                                'annotations': [{'text': config.TEXT_LAP_PROG_SELECT_DRIVERS, 'xref': 'paper', 
                                                 'yref': 'paper', 'showarrow': False, 'font': {'size': 12}}],
                                'xaxis': {'visible': False, 'range': [0,1]},
                                'yaxis': {'visible': False, 'range': [0,1]},
                                'margin': config.LAP_PROG_MARGINS_EMPTY 
                            }),
                            config={'autosizable': True, 'responsive': True}
                        )
                    ]
                )
            ]))
        ], md=12)
    ], className="mt-2 mb-3", id='analysis-zone')

    app_footer = html.Footer(
        dbc.Row([
            dbc.Col(html.Small("F1 Dashboard", className="text-muted"),
                    width="auto", className="me-auto align-self-center"),
            dbc.Col(
                dbc.Switch(
                    id="debug-mode-switch",
                    label="Debug Streams",
                    value=False,
                    className="form-check-inline"
                ), width="auto", className="align-self-center"
            )
        ], className="text-center py-2 mt-3 border-top", justify="between")
    )

    dashboard_page_content = dbc.Container([
        stores_and_intervals,
        status_alert,
        header_zone,
        control_zone,
        status_weather_bar,
        main_data_zone,
        analysis_zone,
        app_footer
    ], fluid=True, className="dbc dbc-slate p-2") # Assuming dbc-slate is your theme class

    return dashboard_page_content
    
dashboard_content_layout = define_dashboard_layout()

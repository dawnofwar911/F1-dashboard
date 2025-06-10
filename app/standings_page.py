# app/standings_page.py
import dash_bootstrap_components as dbc
from dash import html, dash_table, dcc

def create_standings_layout():
    """Creates the layout for the championship standings page with tabs."""
    
    # Define columns for the polished driver table
    driver_columns = [
        {'name': 'Pos', 'id': 'position'},
        {'name': '#', 'id': 'driverNumber'}, # New
        {'name': 'Driver', 'id': 'driver_name'},
        {'name': 'Team', 'id': 'constructor_name'},
        {'name': 'Points', 'id': 'points'},
        {'name': 'Wins', 'id': 'wins'},
    ]
    
    # Define columns for the new constructor table
    constructor_columns = [
        {'name': 'Pos', 'id': 'position'},
        {'name': 'Team', 'id': 'constructorName'},
        {'name': 'Nationality', 'id': 'constructorNationality'},
        {'name': 'Points', 'id': 'points'},
        {'name': 'Wins', 'id': 'wins'},
    ]

    table_style_cell = {
        'textAlign': 'left', 'padding': '10px',
        'backgroundColor': 'rgb(50, 50, 50)', 'color': 'white',
        'border': '1px solid #444'
    }
    table_style_header = {
        'backgroundColor': 'rgb(30, 30, 30)', 'fontWeight': 'bold',
        'border': '1px solid #444',
        'whiteSpace': 'normal',
        'height': 'auto',
    }
    
    layout = dbc.Container([
        dcc.Store(id='live-standings-data-store'), # To hold processed live data
        dcc.Interval(
            id='standings-interval-component',
            interval=5*1000, # 5 seconds
            n_intervals=0
        ),
        dbc.Row([
            dbc.Col(html.H2("Championship Standings", className="my-4"), width="auto"),
            # ADD THIS MISSING COLUMN AND DIV
            dbc.Col(html.Div(id='standings-title-badge'), className="d-flex align-items-center")
        ], justify="center", align="center"),
        
        # --- TABS FOR DRIVERS AND CONSTRUCTORS ---
        dbc.Tabs(
            [
                dbc.Tab(label="Drivers", tab_id="tab-drivers", children=[
                    dbc.Row(dbc.Col(
                        dbc.Spinner(
                            dash_table.DataTable(
                                id='driver-standings-table',
                                columns=driver_columns,
                                style_cell=table_style_cell,
                                style_header=table_style_header,
                                fixed_rows={'headers': True},
                                style_table={'height': '65vh', 'overflowY': 'auto'},
                                style_data_conditional=[
                                    {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(60, 60, 60)'},
                                    {'if': {'column_id': ['position', 'driverNumber', 'points', 'wins']}, 'textAlign': 'center'},
                                    {'if': {'column_id': 'points'}, 'fontWeight': 'bold'},
                                ]
                            )
                        ),
                    className="p-4"),)
                ]),
                dbc.Tab(label="Constructors", tab_id="tab-constructors", children=[
                     dbc.Row(dbc.Col(
                        dbc.Spinner(
                            dash_table.DataTable(
                                id='constructor-standings-table', # New Table
                                columns=constructor_columns,
                                style_cell=table_style_cell,
                                style_header=table_style_header,
                                fixed_rows={'headers': True},
                                style_table={'height': '65vh', 'overflowY': 'auto'},
                                style_data_conditional=[
                                    {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(60, 60, 60)'},
                                    {'if': {'column_id': ['position', 'points', 'wins']}, 'textAlign': 'center'},
                                    {'if': {'column_id': 'points'}, 'fontWeight': 'bold'},
                                ]
                            )
                        ),
                    className="p-4"),)
                ]),
            ],
            id="standings-tabs",
            active_tab="tab-drivers",
        )
    ], fluid=True, className="p-3")
    
    return layout

standings_page_layout = create_standings_layout()
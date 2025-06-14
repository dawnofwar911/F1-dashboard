# app/historical_analysis_page.py
"""
Defines the layout for the Historical Session Analysis page.
"""
import dash_bootstrap_components as dbc
from dash import dcc, html
from datetime import datetime

def create_historical_layout():
    """Creates the UI for selecting and displaying historical session data."""

    # Generate a list of years from 2018 to the current year
    current_year = datetime.now().year
    year_options = [{'label': str(y), 'value': y} for y in range(current_year, 2017, -1)]

    controls = dbc.Card(
        dbc.CardBody([
            html.H4("Select a Past Session", className="card-title"),
            html.P("Choose a year, event, and session, then click 'Load' to view the data.", className="card-text text-muted"),
            html.Hr(),
            dbc.Row([
                dbc.Col(dcc.Dropdown(id='historical-year-dropdown', options=year_options, placeholder="1. Select Year"), md=3, className="mb-2 mb-md-0"),
                dbc.Col(dcc.Dropdown(id='historical-event-dropdown', placeholder="2. Select Event", disabled=True), md=4, className="mb-2 mb-md-0"),
                dbc.Col(dcc.Dropdown(id='historical-session-dropdown', placeholder="3. Select Session", disabled=True), md=3, className="mb-2 mb-md-0"),
                dbc.Col(dbc.Button("Load Session", id='historical-load-button', disabled=True, n_clicks=0, color="primary"), width=12, lg=2)
            ], align="center")
        ]),
        className="mb-3"
    )

    display_area = html.Div([
        dcc.Store(id='historical-laps-data-store'), # To hold the main laps DataFrame
        dcc.Store(id='historical-telemetry-data-store'), # <-- ADD THIS LINE
         dbc.Spinner(
            html.Div(id='historical-charts-display-area'), # The content will be rendered here
            color="primary",
        )
    ])

    layout = dbc.Container([
        html.H2("Historical Session Analysis", className="my-4"),
        controls,
        html.Hr(),
        display_area
    ], fluid=True, className="p-3")
    
    return layout

# Create an instance of the layout for the router to import
historical_analysis_page_layout = create_historical_layout()
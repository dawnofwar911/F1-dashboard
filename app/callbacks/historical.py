# app/callbacks/historical.py
"""
Callbacks for the Historical Analysis page.
"""
import logging
import fastf1
import pandas as pd
from dash.dependencies import Input, Output, State
from dash import no_update, dcc, html
import dash_bootstrap_components as dbc
from io import StringIO

import plotly.graph_objects as go

from app_instance import app
from historical_data_fetcher import load_historical_laps, load_historical_telemetry
from utils import (
    create_lap_position_chart, 
    create_tyre_degradation_chart, 
    create_telemetry_comparison_chart
)

logger = logging.getLogger(__name__)

@app.callback(
    Output('historical-event-dropdown', 'options'),
    Output('historical-event-dropdown', 'disabled'),
    Input('historical-year-dropdown', 'value')
)
def update_event_dropdown(selected_year):
    """Populates the event dropdown based on the selected year."""
    if not selected_year:
        return [], True  # Return empty options and keep it disabled

    try:
        logger.info(f"Fetching event schedule for year: {selected_year}")
        schedule = fastf1.get_event_schedule(selected_year, include_testing=False)
        
        # We use 'EventName' for the label and the value
        event_options = [
            {'label': event['EventName'], 'value': event['EventName']}
            for index, event in schedule.iterrows()
        ]
        
        return event_options, False # Return options and enable the dropdown
    except Exception as e:
        logger.error(f"Failed to fetch schedule for year {selected_year}: {e}")
        return [], True
        
@app.callback(
    Output('historical-session-dropdown', 'options'),
    Output('historical-session-dropdown', 'disabled'),
    Output('historical-session-dropdown', 'value'), # Reset value on change
    Input('historical-event-dropdown', 'value'),
    State('historical-year-dropdown', 'value')
)
def update_session_dropdown(selected_event, selected_year):
    """Populates the session dropdown based on the selected event."""
    # Don't do anything if we don't have both a year and an event
    if not selected_event or not selected_year:
        return [], True, None

    try:
        logger.info(f"Fetching sessions for event: {selected_year} {selected_event}")
        
        # Get the schedule for the selected year (this will be cached by fastf1)
        schedule = fastf1.get_event_schedule(selected_year, include_testing=False)
        
        # Find the specific row for the selected event
        event_details = schedule[schedule['EventName'] == selected_event].iloc[0]
        
        # Extract all session names that are not empty
        session_options = []
        session_keys = ['Session1', 'Session2', 'Session3', 'Session4', 'Session5']
        for key in session_keys:
            session_name = event_details[key]
            if pd.notna(session_name):
                 session_options.append({'label': session_name, 'value': session_name})
        
        logger.info(f"Found {len(session_options)} sessions for {selected_event}.")
        
        # Return the options and enable the dropdown
        return session_options, False, None

    except Exception as e:
        logger.error(f"Failed to fetch sessions for {selected_year} {selected_event}: {e}", exc_info=True)
        return [], True, None
        
@app.callback(
    Output('historical-load-button', 'disabled'),
    Input('historical-year-dropdown', 'value'),
    Input('historical-event-dropdown', 'value'),
    Input('historical-session-dropdown', 'value')
)
def toggle_load_button_disabled_state(year, event, session):
    """Enables the 'Load Session' button only when all three dropdowns have a value."""
    # The button is disabled if not all values are truthy (i.e., not None or empty)
    return not (year and event and session)
    
# 4. Main callback when 'Load' is clicked. THIS ONE IS MODIFIED.
@app.callback(
    Output('historical-charts-display-area', 'children'),
    Output('historical-laps-data-store', 'data'),
    Input('historical-load-button', 'n_clicks'),
    [State('historical-year-dropdown', 'value'),
     State('historical-event-dropdown', 'value'),
     State('historical-session-dropdown', 'value')],
    prevent_initial_call=True
)
def load_historical_data(n_clicks, year, event, session):
    """
    Triggered by the 'Load' button. Fetches historical data and displays all
    analysis charts within a clean, tabbed interface.
    """
    if not all([year, event, session]):
        return "Please make a complete selection.", None
    
    laps_df = load_historical_laps(year, event, session)

    if laps_df.empty:
        return dbc.Alert("Error: Could not load lap data for the selected session.", color="danger"), None

    # --- Create the components needed for all tabs ---
    lap_chart_figure = create_lap_position_chart(laps_df, year)
    driver_options = [{'label': tla, 'value': tla} for tla in sorted(laps_df['Driver'].unique())]
    empty_figure = go.Figure(layout={'template': 'plotly_dark'}) # Placeholder for empty graphs

    # --- Build the new tabbed layout ---
    tabbed_layout = html.Div([
        dbc.Tabs(
            id="historical-analysis-tabs",
            active_tab="tab-lap-positions",
            children=[
                # Tab 1: Lap Position Chart
                dbc.Tab(
                    dcc.Graph(id="lap-position-chart", figure=lap_chart_figure),
                    label="Lap Positions",
                    tab_id="tab-lap-positions",
                    className="pt-3" # Add some padding to the top of the tab content
                ),
                
                # Tab 2: Tyre Degradation Analysis
                dbc.Tab([
                    html.Div([
                        html.P("Select a driver and stint to analyze tyre performance drop-off.", className="mt-3 text-muted"),
                        dbc.Row([
                            dbc.Col(dcc.Dropdown(id='historical-driver-dropdown', options=driver_options, placeholder="Select Driver...")),
                            dbc.Col(dcc.Dropdown(id='historical-stint-dropdown', placeholder="Select Stint...", disabled=True)),
                        ], className="my-3"),
                        dbc.Spinner(dcc.Graph(id='tyre-degradation-graph'))
                    ], className="p-2")
                ], label="Tyre Degradation", tab_id="tab-tyre-degradation"),
                
                # Tab 3: Telemetry Comparison
                dbc.Tab([
                     html.Div([
                        html.P("Select two laps to compare detailed telemetry.", className="mt-3 text-muted"),
                        dbc.Row([
                            dbc.Col([
                                html.Label("Driver 1", className="fw-bold"),
                                dcc.Dropdown(id='historical-driver-1-dropdown', options=driver_options, placeholder="Select..."),
                                dcc.Dropdown(id='historical-lap-1-dropdown', placeholder="Select Lap...", disabled=True, className="mt-2"),
                            ], md=5),
                            dbc.Col(
                                html.H2("vs", className="text-center text-muted"),
                                md=2, className="d-flex align-items-center justify-content-center"
                            ),
                            dbc.Col([
                                html.Label("Driver 2", className="fw-bold"),
                                dcc.Dropdown(id='historical-driver-2-dropdown', options=driver_options, placeholder="Select..."),
                                dcc.Dropdown(id='historical-lap-2-dropdown', placeholder="Select Lap...", disabled=True, className="mt-2"),
                            ], md=5),
                        ], className="my-3"),
                        dbc.Row(dbc.Col(dbc.Button("Compare Laps", id="historical-compare-button", disabled=True, n_clicks=0, color="primary"))),
                        dbc.Row(dbc.Col(
                            dbc.Spinner(dcc.Graph(id='historical-telemetry-graph')),
                            className="mt-3"
                        ))
                     ], className="p-2")
                ], label="Telemetry H2H", tab_id="tab-telemetry-h2h"),
            ],
        )
    ], className="mt-3")

    # Store the lap data in the browser for other callbacks to use
    json_laps = laps_df.to_json(date_format='iso', orient='split')

    return tabbed_layout, json_laps

# 5. NEW: Callback to populate Stint dropdown
@app.callback(
    Output('historical-stint-dropdown', 'options'),
    Output('historical-stint-dropdown', 'disabled'),
    Input('historical-driver-dropdown', 'value'),
    State('historical-laps-data-store', 'data')
)
def update_stint_dropdown(selected_driver, laps_data_json):
    if not selected_driver or not laps_data_json:
        return [], True

    # FIX: Wrap the JSON string in StringIO to avoid the FutureWarning
    laps_df = pd.read_json(StringIO(laps_data_json), orient='split')
    
    driver_laps = laps_df[laps_df['Driver'] == selected_driver]
    stints = sorted(driver_laps['Stint'].unique())
    
    stint_options = [{'label': f'Stint {int(s)}', 'value': s} for s in stints]
    return stint_options, False

# 6. NEW: Callback to generate and display the degradation chart
@app.callback(
    Output('tyre-degradation-graph', 'figure'),
    Input('historical-stint-dropdown', 'value'),
    [State('historical-driver-dropdown', 'value'),
     State('historical-laps-data-store', 'data')]
)
def update_tyre_degradation_chart(selected_stint, selected_driver, laps_data_json):
    if not all([selected_stint, selected_driver, laps_data_json]):
        return go.Figure(layout={'template': 'plotly_dark', 'annotations': [{'text': 'Select a driver and stint to view analysis.', 'showarrow': False}]})

    # FIX 1: Wrap the JSON string in StringIO
    laps_df = pd.read_json(StringIO(laps_data_json), orient='split')
    
    stint_df = laps_df[(laps_df['Driver'] == selected_driver) & (laps_df['Stint'] == selected_stint)]
    
    # FIX 2: Convert to seconds and remove outliers on a new DataFrame to avoid the SettingWithCopyWarning
    clean_stint_df = stint_df.copy()
    clean_stint_df['LapTime'] = pd.to_timedelta(clean_stint_df['LapTime']).dt.total_seconds()
    
    q1 = clean_stint_df['LapTime'].quantile(0.25)
    q3 = clean_stint_df['LapTime'].quantile(0.75)
    iqr = q3 - q1
    
    # Filter by creating a new DataFrame slice
    clean_stint_df = clean_stint_df[~((clean_stint_df['LapTime'] < (q1 - 1.5 * iqr)) |(clean_stint_df['LapTime'] > (q3 + 1.5 * iqr)))]

    return create_tyre_degradation_chart(clean_stint_df)

def _populate_lap_options(selected_driver, laps_data_json):
    """Helper function to generate lap options for a driver."""
    if not selected_driver or not laps_data_json:
        return [], True, None # options, disabled, value

    laps_df = pd.read_json(StringIO(laps_data_json), orient='split')
    driver_laps = laps_df[laps_df['Driver'] == selected_driver].sort_values(by="LapNumber")

    lap_options = [{'label': f"Lap {int(lap['LapNumber'])} ({lap['LapTime']})", 'value': int(lap['LapNumber'])} 
                   for _, lap in driver_laps.iterrows() if pd.notna(lap.get('LapTime'))]
    
    return lap_options, False, None if not lap_options else lap_options[-1]['value']


@app.callback(
    Output('historical-lap-1-dropdown', 'options'),
    Output('historical-lap-1-dropdown', 'disabled'),
    Output('historical-lap-1-dropdown', 'value'),
    Input('historical-driver-1-dropdown', 'value'),
    State('historical-laps-data-store', 'data')
)
def update_lap_dropdown_1(selected_driver, laps_data_json):
    return _populate_lap_options(selected_driver, laps_data_json)


@app.callback(
    Output('historical-lap-2-dropdown', 'options'),
    Output('historical-lap-2-dropdown', 'disabled'),
    Output('historical-lap-2-dropdown', 'value'),
    Input('historical-driver-2-dropdown', 'value'),
    State('historical-laps-data-store', 'data')
)
def update_lap_dropdown_2(selected_driver, laps_data_json):
    return _populate_lap_options(selected_driver, laps_data_json)


@app.callback(
    Output('historical-compare-button', 'disabled'),
    Input('historical-lap-1-dropdown', 'value'),
    Input('historical-lap-2-dropdown', 'value')
)
def toggle_compare_button_disabled_state(lap1, lap2):
    """Enables the 'Compare' button only when both lap dropdowns have a value."""
    return not (lap1 and lap2)
    
@app.callback(
    Output('historical-telemetry-graph', 'figure'),
    Input('historical-compare-button', 'n_clicks'),
    [State('historical-year-dropdown', 'value'),
     State('historical-event-dropdown', 'value'),
     State('historical-session-dropdown', 'value'),
     State('historical-driver-1-dropdown', 'value'),
     State('historical-lap-1-dropdown', 'value'),
     State('historical-driver-2-dropdown', 'value'),
     State('historical-lap-2-dropdown', 'value'),
     State('session-preferences-store', 'data')],
    prevent_initial_call=True
)
def update_telemetry_comparison_chart(n_clicks, year, event, session, driver1, lap1, driver2, lap2, prefs):
    """
    Triggered by the 'Compare Laps' button. Loads telemetry and generates the
    detailed comparison chart.
    """
    if not all([year, event, session, driver1, lap1, driver2, lap2]):
        return no_update

    # Load the session with telemetry data
    session_obj = load_historical_telemetry(year, event, session)
    if session_obj is None:
        return go.Figure(layout={'annotations': [{'text': 'Failed to load telemetry data.'}]})

    use_mph = prefs.get('use_mph', False) if prefs else False

    # Generate the figure using our new utility function
    return create_telemetry_comparison_chart(session_obj, driver1, lap1, driver2, lap2, use_mph)
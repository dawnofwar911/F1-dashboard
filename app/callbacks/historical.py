# app/callbacks/historical.py
"""
Callbacks for the Historical Analysis page.
"""
import logging
import fastf1
import pandas as pd
from dash.dependencies import Input, Output, State
from dash import no_update, dcc

from app_instance import app, server
from historical_data_fetcher import load_historical_laps
from utils import create_lap_position_chart

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
    
@app.callback(
    Output('historical-display-area', 'children'),
    Input('historical-load-button', 'n_clicks'),
    [State('historical-year-dropdown', 'value'),
     State('historical-event-dropdown', 'value'),
     State('historical-session-dropdown', 'value')],
    prevent_initial_call=True
)
def load_historical_data(n_clicks, year, event, session):
    """
    Triggered by the 'Load' button. Fetches historical lap data and
    displays the lap position chart.
    """
    if not all([year, event, session]):
        return "Please make a complete selection (Year, Event, and Session)."
    
    # Step 1: Fetch the lap data
    laps_df = load_historical_laps(year, event, session)

    if laps_df.empty:
        return dbc.Alert("Error: Could not load lap data for the selected session.", color="danger")

    # Step 2: Generate the chart from the data
    lap_chart_figure = create_lap_position_chart(laps_df, year)

    # Step 3: Return the chart to be displayed
    return dcc.Graph(figure=lap_chart_figure)
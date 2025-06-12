# app/historical_data_fetcher.py
"""
Contains functions for fetching and transforming historical F1 data using the
fastf1 library into the application's standard session_state format.
"""
import logging
import fastf1
import pandas as pd

import app_state
import utils

logger = logging.getLogger(__name__)

def load_and_transform_historical_session(session_state: app_state.SessionState, year: int, event_name: str, session_identifier: str) -> bool:
    """
    Loads a historical session from fastf1, transforms the data, and updates the
    provided session_state object.

    Returns:
        True if successful, False otherwise.
    """
    try:
        logger.info(f"Loading historical data for {year} {event_name} - {session_identifier}...")
        
        # Reset the session state to ensure no old data remains
        session_state.reset_state_variables()

        # Load the session data from fastf1. This is the main network call.
        session = fastf1.get_session(year, event_name, session_identifier)
        session.load(laps=True, telemetry=False, weather=False, messages=False) # Laps are essential

        # --- Transform and Populate session_state.session_details ---
        with session_state.lock:
            session_state.session_details = {
                "Year": session.event['EventDate'].year,
                "EventName": session.event['EventName'],
                "Name": session.name,
                "SessionKey": session.session_key,
                "Type": session.name, # Use session name as type for now
                "CircuitName": session.event['Location'],
                "Meeting": {"Name": session.event.get('EventName')},
            }
            logger.info(f"Historical session_details populated: {session_state.session_details}")

        # --- Transform and Populate session_state.timing_state ---
        # We'll use the final classification if available, otherwise the latest lap data
        results = session.results
        if results is None or results.empty:
            logger.warning("No final results found, using lap data for timing state.")
            # Fallback logic if needed, for now we require results
            return False

        timing_data = {}
        for _, driver in results.iterrows():
            driver_num = str(driver['DriverNumber'])
            timing_data[driver_num] = {
                'RacingNumber': driver_num,
                'FullName': driver['FullName'],
                'Tla': driver['Abbreviation'],
                'TeamName': driver['TeamName'],
                'TeamColour': driver.get('TeamColor', '808080'),
                'Position': str(int(driver['Position'])),
                'Time': driver['Time'].strftime('%H:%M:%S.%f')[:-3] if pd.notna(driver['Time']) else "DNF",
                'GapToLeader': str(driver['GapToLeader']),
                'NumberOfLaps': str(int(driver['Laps'])),
                'NumberOfPitStops': str(int(driver['Q1'])), # Note: This is a hack, full pit data is complex
                'Status': driver['Status'],
                # Add placeholders for other data types
                'LastLapTime': {'Value': str(driver['Q2']) if pd.notna(driver['Q2']) else '-'},
                'BestLapTime': {'Value': str(driver['Q3']) if pd.notna(driver['Q3']) else '-'},
                'Sectors': {"0": {}, "1": {}, "2": {}},
                'Speeds': {}, 'CarData': {}, 'PositionData': {}
            }
        
        with session_state.lock:
            session_state.timing_state = timing_data
            session_state.app_status['state'] = 'Historical'
            logger.info(f"Populated timing_state with {len(timing_data)} drivers.")

        return True

    except Exception as e:
        logger.error(f"Failed to load or transform historical session: {e}", exc_info=True)
        with session_state.lock:
            session_state.app_status['state'] = 'Error'
            session_state.app_status['connection'] = "Failed to load historical data."
        return False
        
def load_historical_laps(year: int, event_name: str, session_identifier: str) -> pd.DataFrame:
    """
    Loads lap data for a historical session from fastf1.

    Returns:
        A pandas DataFrame containing the lap data, or an empty DataFrame if it fails.
    """
    try:
        logger.info(f"Loading historical lap data for {year} {event_name} - {session_identifier}...")
        
        session = fastf1.get_session(year, event_name, session_identifier)
        session.load(laps=True, telemetry=False, weather=False, messages=False)
        
        # Return the laps DataFrame, which contains all the data we need
        return session.laps

    except Exception as e:
        logger.error(f"Failed to load historical laps: {e}", exc_info=True)
        return pd.DataFrame() # Return an empty DataFrame on error
        
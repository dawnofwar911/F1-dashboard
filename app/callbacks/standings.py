# app/callbacks/standings.py
"""
Callbacks for generating and updating all data for the standings page.
"""
import logging
import time
import inspect
import copy
from typing import Optional
from datetime import datetime

import dash
from dash.dependencies import Input, Output, State
from dash import dcc, html, dash_table, no_update, Patch
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import dash_bootstrap_components as dbc

from app_instance import app
import app_state
import config
import utils
from schedule_page import get_championship_standings, get_constructor_standings

logger = logging.getLogger(__name__)


@app.callback(
    Output('driver-standings-table', 'data'),
    Output('constructor-standings-table', 'data'),
    Output('standings-title-badge', 'children'),
    Input('url', 'pathname'),
    Input('standings-tabs', 'active_tab'),
    Input('standings-interval-component', 'n_intervals')
)
def update_standings_tables(pathname, active_tab, n_intervals):
    """
    This single callback populates the standings tables, prioritizing
    live prediction data when a session is active.
    """
    if pathname != '/standings':
        return [], [], None

    session_state = app_state.get_or_create_session_state()
    if not session_state:
        return [], [], None

    is_live_session = session_state.app_status.get("state") == "Live"
    live_standings_data = session_state.live_standings

    # --- Use Live Data if session is active AND live data has been received ---
    if is_live_session and live_standings_data:
        badge = dbc.Badge("Live Projection", color="danger", className="ms-2")
        if active_tab == 'tab-drivers':
            return live_standings_data.get('drivers', []), [], badge
        elif active_tab == 'tab-constructors':
            return [], live_standings_data.get('teams', []), badge
    
    # --- Fallback to Official Standings for all other cases ---
    badge = dbc.Badge("Official", color="success", className="ms-2")
    current_year = datetime.now().year
    
    if active_tab == 'tab-drivers':
        driver_data = get_championship_standings(year=current_year)
        return driver_data, [], badge
    elif active_tab == 'tab-constructors':
        constructor_data = get_constructor_standings(year=current_year)
        return [], constructor_data, badge
            
    return [], [], None
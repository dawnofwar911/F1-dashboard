# app/callbacks/routing.py
"""
This file contains the master callback for rendering different pages based on URL.
"""
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from dash import html

from app_instance import app

# Import each page's layout from its own file
from layout import dashboard_content_layout
from schedule_page import schedule_page_layout
from standings_page import standings_page_layout
from settings_layout import create_settings_layout

@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname")
)
def display_page(pathname: str):
    """This function is the master router for the application."""
    if pathname == "/schedule":
        return schedule_page_layout
    elif pathname == "/standings":
        return standings_page_layout
    elif pathname == "/settings":
        return create_settings_layout()
    elif pathname == "/":
        return dashboard_content_layout
    else:
        # Return a 404 error page
        return dbc.Container([
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ], className="py-3")
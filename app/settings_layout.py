# app/settings_layout.py

from dash import html
import dash_bootstrap_components as dbc
import config # For default values

def create_settings_layout():
    """Creates the layout for the /settings page."""
    
    layout = dbc.Container(
        [
            html.H3("Application Settings", className="mb-4"),
            
            # --- Display Settings ---
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H5("Display Preferences"),
                            html.P(
                                "Changes are saved automatically to your browser and will apply on the next data update.",
                                className="text-muted small"
                            ),
                            html.Hr(),
                            dbc.Switch(
                                id="hide-retired-drivers-switch",
                                label="Hide Retired/Out Drivers",
                                value=config.HIDE_RETIRED_DRIVERS, # Set initial default
                                className="mb-3"
                            ),
                            dbc.Switch(
                                id="use-mph-switch",
                                label="Display Speed in MPH (instead of KPH)",
                                value=config.USE_MPH, # Set initial default
                                className="mb-3"
                            ),
                        ],
                        width=12,
                        lg=6
                    ),
                ],
                className="mb-5"
            ),
            
            # --- Live Session Settings ---
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H5("Live Session Preferences"),
                            html.P(
                                "These settings control live session connections and data handling.",
                                className="text-muted small"
                            ),
                            html.Hr(),
                            dbc.Switch(
                                id="session-auto-connect-switch",
                                label="Enable Auto-Connect",
                                value=False,
                                className="mb-3"
                            ),
                            # --- ADDED THIS SWITCH ---
                            dbc.Switch(
                                id="record-data-switch",
                                label="Record Live Session Data",
                                value=False, # Default to off
                                className="mb-3"
                            ),
                        ],
                        width=12,
                        lg=6
                    )
                ]
            ),
        ],
        fluid=True,
        className="py-4"
    )
    return layout
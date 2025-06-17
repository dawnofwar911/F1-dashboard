# app/settings_layout.py

from dash import html
import dash_bootstrap_components as dbc
import config # For default values

def create_settings_layout():
    """Creates the layout for the /settings page."""
    
    layout = dbc.Container(
        [
            html.H3("Application Settings", className="mb-4"),
            
            # --- Display Settings (No changes here) ---
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
                                value=config.HIDE_RETIRED_DRIVERS,
                                className="mb-3"
                            ),
                            dbc.Switch(
                                id="use-mph-switch",
                                label="Display Speed in MPH (instead of KPH)",
                                value=config.USE_MPH,
                                className="mb-3"
                            ),
                        ],
                        width=12,
                        lg=6
                    ),
                ],
                className="mb-5"
            ),
            
            # --- Live Session Settings (No changes here) ---
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
                                label="Enable Auto-Connect (for this browser session)",
                                value=False,
                                className="mb-3"
                            ),
                        ],
                        width=12,
                        lg=6
                    )
                ]
            ),
            
            # --- NEW: Global / Admin Settings Section ---
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H5("Global / Admin Settings", className="mt-5"),
                            html.P(
                                "These settings affect the entire application for all users.",
                                className="text-muted small"
                            ),
                            html.Hr(),
                            dbc.Button(
                                "Configure Global Settings", 
                                id="admin-settings-button", 
                                color="primary"
                            ),
                        ],
                         width=12,
                         lg=6
                    )
                ]
            ),

            # --- NEW: Modals and Hidden Panels ---
            # These components are part of the layout but are not visible until a callback changes them.
            
            # 1. The modal for password entry
            dbc.Modal(
                [
                    dbc.ModalHeader("Admin Access"),
                    dbc.ModalBody(
                        dbc.Input(id="admin-password-input", type="password", placeholder="Enter admin password...")
                    ),
                    dbc.ModalFooter(
                        dbc.Button("Login", id="admin-login-button", color="primary")
                    ),
                ],
                id="admin-password-modal",
                is_open=False, # Hidden by default
            ),

            # 2. The actual settings panel, hidden by default
            html.Div(
                [
                    html.Hr(className="my-4"),
                    html.H5("Global Settings Panel"),
                    dbc.Switch(
                        id="global-record-sessions-switch",
                        label="Enable Live Session Recording (Global)",
                        value=False, # This will be updated by a callback
                    ),
                    # You can add other global settings here in the future
                ],
                id="admin-settings-panel",
                style={"display": "none"}, # Hidden by default
            ),
        ],
        fluid=True,
        className="py-4"
    )
    return layout
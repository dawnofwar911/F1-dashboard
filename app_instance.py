# app_instance.py
"""
Defines the central Dash app instance.
"""
import dash
import dash_bootstrap_components as dbc

# Define the app instance here
app = dash.Dash(__name__,
                external_stylesheets=[dbc.themes.SLATE],
                suppress_callback_exceptions=True) # Keep suppression True for now

# Optional: Set the title
app.title = "F1 Timing Dashboard"

# Expose server for potential deployment
server = app.server

print("DEBUG: Dash app instance created in app_instance.py")
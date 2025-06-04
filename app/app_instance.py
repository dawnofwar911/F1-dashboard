# app_instance.py
"""
Defines the central Dash app instance.
"""
import dash
import dash_bootstrap_components as dbc
import config # For APP_TITLE
import flask 
import os 

# --- Define External Stylesheets ---
FA = "https://use.fontawesome.com/releases/v5.15.4/css/all.css"
external_stylesheets = [
    dbc.themes.SLATE, 
    FA,               
    '/assets/custom.css' 
]

# --- Create and Configure the Flask server instance ---
flask_server = flask.Flask(__name__)

# --- Set a secret key for Flask sessions ---
# FOR PRODUCTION (e.g., when run in Docker by your friend):
# 1. Generate a strong, random key using:
#    python -c "import os, binascii; print(binascii.hexlify(os.urandom(32)).decode())"
# 2. This generated key MUST be set as an environment variable named FLASK_SECRET_KEY
#    when starting the Docker container.
#    - Example with `docker run`: 
#      docker run -e FLASK_SECRET_KEY="your_production_key" ... your_image
#    - Example with `docker-compose.yml`:
#      environment:
#        - FLASK_SECRET_KEY=your_production_key
#    DO NOT hardcode the production key in this file or the Dockerfile.
#
# FOR DEVELOPMENT:
# The fallback key below is for development convenience if FLASK_SECRET_KEY is not set.
flask_server.secret_key = os.environ.get("FLASK_SECRET_KEY", "a_different_strong_dev_key_for_f1_dashboard_sessions_123!")

# --- Define the Dash app instance ---
app = dash.Dash(__name__,
                server=flask_server, 
                external_stylesheets=external_stylesheets,
                suppress_callback_exceptions=True,
                update_title=None)

app.title = config.APP_TITLE 
server = app.server 

# --- Eruda Debug Script (Conditional) ---
eruda_script = ""
if config.DASH_DEBUG_MODE:
    eruda_script = """
                <script src="//cdn.jsdelivr.net/npm/eruda"></script>
                <script>eruda.init();</script>"""

# --- Custom HTML Index String ---
app.index_string = f"""<!DOCTYPE html>
<html>
    <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        {{%favicon%}}
        {{%css%}}
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
            {eruda_script}
        </footer>
    </body>
</html>"""

print("DEBUG: Dash app instance updated in app_instance.py (Flask server configured for sessions)")
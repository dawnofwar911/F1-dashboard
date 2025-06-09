# In app/app_instance.py

import dash
import dash_bootstrap_components as dbc
import config
import flask
import os
import sys # Import sys to allow exiting
import logging

logger = logging.getLogger(__name__)

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
# This logic checks if the app is in production. If so, the SECRET_KEY
# environment variable MUST be set. Otherwise, it will log an error and exit.
# This prevents accidentally running in production with a weak development key.

secret_key = os.environ.get('SECRET_KEY') # Changed from FLASK_SECRET_KEY for consistency

if not secret_key:
    # If no secret key is set in the environment:
    if config.IS_PRODUCTION:
        # In a production environment, a fixed secret key is required.
        error_msg = "FATAL: SECRET_KEY environment variable is not set in production mode. Application cannot start."
        logger.error(error_msg)
        sys.exit(error_msg) # Exit with an error message
    else:
        # In development, generate a new key for each startup.
        logger.debug("SECRET_KEY environment variable not found. Generating a temporary key for development.")
        flask_server.secret_key = str(os.urandom(32))
else:
    # Use the secret key from the environment variable.
    logger.info("Flask server initialized with SECRET_KEY from environment variable.")
    flask_server.secret_key = secret_key

# --- Define the Dash app instance ---
app = dash.Dash(__name__,
                server=flask_server,
                external_stylesheets=external_stylesheets,
                suppress_callback_exceptions=True,
                update_title=None)

app.title = config.APP_TITLE
server = app.server # This re-assigns the server object, which is standard practice

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

logger.debug("Dash app instance updated in app_instance.py (Flask server configured for sessions)")
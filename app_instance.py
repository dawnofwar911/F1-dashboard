# app_instance.py
"""
Defines the central Dash app instance.
"""
import dash
import dash_bootstrap_components as dbc
import config # <<< ADDED: For APP_TITLE

# Define the app instance here
app = dash.Dash(__name__,
                external_stylesheets=[dbc.themes.SLATE],
                suppress_callback_exceptions=True) # Keep suppression True for now

# Optional: Set the title using constant from config
app.title = config.APP_TITLE # <<< UPDATED

# Expose server for potential deployment
server = app.server

# Eruda debug console (optional, can be removed for "production")
# Consider making the Eruda script inclusion conditional based on DASH_DEBUG_MODE
eruda_script = ""
if config.DASH_DEBUG_MODE: # Conditionally include Eruda
    eruda_script = """
            <script src="//cdn.jsdelivr.net/npm/eruda"></script>
            <script>eruda.init();</script>"""

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

print("DEBUG: Dash app instance created in app_instance.py")
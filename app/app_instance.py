# app_instance.py
"""
Defines the central Dash app instance.
"""
import dash
import dash_bootstrap_components as dbc
import config # For APP_TITLE

# --- Define External Stylesheets ---
# Add Font Awesome if not already present or ensure it's there
FA = "https://use.fontawesome.com/releases/v5.15.4/css/all.css"
external_stylesheets = [
    dbc.themes.SLATE, # Your existing theme
    FA,                # Font Awesome for icons
    '/assets/custom.css'
]

# Define the app instance here
app = dash.Dash(__name__,
                external_stylesheets=external_stylesheets, # Use the list
                suppress_callback_exceptions=True,
                update_title=None)

app.title = config.APP_TITLE 
server = app.server

eruda_script = ""
if config.DASH_DEBUG_MODE:
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

print("DEBUG: Dash app instance updated in app_instance.py (FontAwesome added if new)")
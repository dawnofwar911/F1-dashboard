# New file: callbacks/admin.py
from dash import Input, Output, State, callback, clientside_callback, no_update
import config
import utils

# Callback to open the password modal
@callback(
    Output("admin-password-modal", "is_open"),
    Input("admin-settings-button", "n_clicks"),
    prevent_initial_call=True,
)
def open_admin_modal(n_clicks):
    return True

# Callback to check password and show the settings panel
@callback(
    Output("admin-settings-panel", "style"),
    Output("admin-password-modal", "is_open", allow_duplicate=True),
    Input("admin-login-button", "n_clicks"),
    State("admin-password-input", "value"),
    prevent_initial_call=True,
)
def check_admin_password(n_clicks, password):
    if password == config.ADMIN_PASSWORD and config.ADMIN_PASSWORD is not None:
        return {"display": "block"}, False # Show panel, hide modal
    return no_update, True # Keep modal open, do not change panel

# Callback to load the current setting value into the switch
@callback(
    Output("global-record-sessions-switch", "value"),
    Input("admin-settings-panel", "style"), # Trigger when panel is shown
)
def load_current_recording_setting(panel_style):
    if panel_style and panel_style.get("display") == "block":
        settings = utils.load_global_settings()
        return settings.get("record_live_sessions", False)
    return no_update

# Callback to save the setting when the switch is toggled
@callback(
    Output("global-record-sessions-switch", "id"), # Dummy output, does nothing
    Input("global-record-sessions-switch", "value"),
    prevent_initial_call=True,
)
def save_recording_setting(is_enabled):
    settings = utils.load_global_settings()
    settings["record_live_sessions"] = is_enabled
    utils.save_global_settings(settings)
    return "global-record-sessions-switch" # Return the same ID

# Remember to import this new admin.py file in callbacks/__init__.py
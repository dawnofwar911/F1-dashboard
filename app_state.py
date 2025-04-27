# --- app_state.py ---
import threading
# import queue # Add if moving data_queue etc.

app_status = {"state": "Idle", "connection": "Disconnected", "subscribed_streams": [], "last_heartbeat": None}
app_state_lock = threading.Lock()
record_live_data = True # Default
live_data_file = None

# Add other shared globals here if moved
# ...

print("DEBUG: app_state module loaded")
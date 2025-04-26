# --- extract_track_data.py ---
import fastf1
import fastf1.plotting
import os
import json
import numpy as np
import pandas as pd

# --- Configuration ---
CACHE_DIR = os.path.join(os.getcwd(), 'fastf1_cache') # Use the same cache as your main app
OUTPUT_DIR = os.path.join(os.getcwd(), 'track_maps') # Where to save JSON files
YEAR = 2023
# EVENT = "Abu Dhabi Grand Prix" # Use Name or Location from SessionInfo
EVENT = "Yas Marina Circuit" # Using ShortName might be more reliable if Name varies
SESSION = 'Q' # 'R', 'Q', 'FP1', etc.

# --- Ensure directories exist ---
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# --- Enable Cache ---
try:
    fastf1.Cache.enable_cache(CACHE_DIR)
    print(f"FastF1 cache enabled at: {CACHE_DIR}")
except Exception as e:
    print(f"Error enabling cache: {e}")
    exit()

# --- Load Session ---
try:
    print(f"Loading session: {YEAR} {EVENT} {SESSION}...")
    # Try loading by event name or location recognized by FastF1
    # Check FastF1 docs or use fastf1.get_event_schedule(YEAR) to find valid names
    session = fastf1.get_session(YEAR, EVENT, SESSION)
    session.load(telemetry=False, laps=True, weather=False)
    print("Session loaded.")
except Exception as e:
    print(f"Error loading session: {e}")
    exit()

# --- Get Track Coordinates from Fastest Lap ---
try:
    print("Getting track coordinates...")
    laps = session.laps.pick_not_deleted().pick_accurate()
    if laps.empty:
        print("No accurate laps found for this session.")
        exit()

    fastest_lap = laps.pick_fastest()
    if fastest_lap is None or not isinstance(fastest_lap, pd.Series):
         print("Could not pick fastest lap.")
         exit()

    pos = fastest_lap.get_car_data(pad=1).add_distance()
    circuit_info = session.get_circuit_info(pos['SessionTime']) # Get circuit info relative to lap time
    rotation_angle = circuit_info.rotation # <<< Get the rotation angle

    track_x_raw, track_y_raw = fastf1.plotting.rotate(pos['X'], pos['Y'], rotation_angle) # Rotate

    # Convert to lists for JSON serialization
    track_x = track_x_raw.tolist()
    track_y = track_y_raw.tolist()

    # Calculate ranges with padding
    x_min, x_max = np.min(track_x), np.max(track_x)
    y_min, y_max = np.min(track_y), np.max(track_y)
    padding_x = (x_max - x_min) * 0.05
    padding_y = (y_max - y_min) * 0.05
    x_range = [x_min - padding_x, x_max + padding_x]
    y_range = [y_min - padding_y, y_max + padding_y]

    print("Track coordinates extracted and rotated.")

except Exception as e:
    print(f"Error getting track coordinates: {e}")
    exit()

# --- Prepare Data for Saving ---
output_data = {
    'circuit_short_name': circuit_info.short_name, # Use short name from circuit_info
    'year': YEAR,
    'event': EVENT,
    'session': SESSION,
    'rotation_angle': rotation_angle, # <<< Save the angle
    'x': track_x,
    'y': track_y,
    'range_x': x_range,
    'range_y': y_range
}

# --- Save to JSON ---
# Use circuit short name for filename consistency
filename_base = circuit_info.short_name.lower().replace(" ", "_").replace("circuit", "").strip("_")
output_filename = os.path.join(OUTPUT_DIR, f"{filename_base}.json")

try:
    print(f"Saving data to: {output_filename}")
    with open(output_filename, 'w') as f:
        json.dump(output_data, f, indent=2)
    print("Save complete.")
except Exception as e:
    print(f"Error saving JSON file: {e}")
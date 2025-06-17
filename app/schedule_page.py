# schedule_page.py
"""
Handles the layout and callbacks for the F1 Race Schedule page.
"""
import dash
# State might not be used here
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc
import pandas as pd
import fastf1
from fastf1.ergast import Ergast
from datetime import datetime, timedelta
import pytz  # For timezone handling
import logging
from typing import List, Dict, Any, Optional  # For type hinting


from app_instance import app  # Assuming app is imported for callbacks
import config
import utils  # For parse_iso_timestamp_safe
# from pathlib import Path # Not needed if FastF1 cache handled globally

# --- Setup Logger for this Module ---
logger = logging.getLogger("F1App.SchedulePage")

def find_next_session_to_connect(lead_time_minutes: int) -> Optional[dict]:
    """
    Scans the full F1 schedule and finds the next upcoming session that is
    within the connection lead time. Returns the session details dict or None.
    """
    full_schedule_data = get_current_year_schedule_with_sessions()
    if not full_schedule_data:
        return None

    now_utc = datetime.now(pytz.utc)
    next_session_to_connect = None
    min_future_start_time = datetime.max.replace(tzinfo=pytz.utc)

    for event in full_schedule_data:
        event_official_name = event.get('OfficialEventName', event.get('EventName', 'Unknown Event'))
        event_year = utils.parse_iso_timestamp_safe(event.get('EventDate')).year if event.get('EventDate') and utils.parse_iso_timestamp_safe(event.get('EventDate')) else now_utc.year
        for session_detail in event.get('Sessions', []):
            session_name = session_detail.get('SessionName')
            session_date_utc_str = session_detail.get('SessionDateUTC')
            if session_date_utc_str and session_name:
                session_dt_utc = utils.parse_iso_timestamp_safe(session_date_utc_str)
                if session_dt_utc and session_dt_utc > now_utc and session_dt_utc < min_future_start_time:
                    min_future_start_time = session_dt_utc
                    next_session_to_connect = {
                        'event_name': event_official_name, 'session_name': session_name,
                        'start_time_utc': session_dt_utc, 'year': event_year,
                        'circuit_name': event.get('Location', "N/A"), 'circuit_key': event.get('CircuitKey'),
                        'session_type': utils.determine_session_type_from_name(session_name),
                        'unique_id': f"{event_year}_{event_official_name}_{session_name}",
                        'SessionKey': session_detail.get('SessionKey'),
                        'SessionInfo': session_detail, # Pass the whole dict for flexibility
                    }

    if next_session_to_connect:
        time_to_session = next_session_to_connect['start_time_utc'] - now_utc
        # Check if the found session is within the lead time window
        if time_to_session.total_seconds() <= (lead_time_minutes * 60) and time_to_session.total_seconds() > -300: # Ensure it's not too far in the past
            return next_session_to_connect
            
    return None
    
def is_session_over(session_start_time: datetime, duration_hours: int = 3) -> bool:
    """Checks if a session is likely over based on its start time and a duration."""
    if not session_start_time:
        return True
    return datetime.now(pytz.utc) > (session_start_time + timedelta(hours=duration_hours))

def get_championship_standings(year: int) -> list:
    """
    Fetches the latest driver championship standings for a given year using Ergast.
    """
    logger.info(f"Fetching DRIVER standings for year: {year}")
    try:
        ergast = Ergast()
        results_list = ergast.get_driver_standings(season=year).content
        if not results_list: return []
        
        standings_df = results_list[0]
        standings_df['driver_name'] = standings_df['givenName'] + ' ' + standings_df['familyName']
        standings_df['constructor_name'] = standings_df['constructorNames'].str[0]
        output_columns = ['position', 'driverNumber', 'driverCode', 'driver_name', 'constructor_name', 'points', 'wins']
        
        for col in ['driverNumber', 'driverCode']:
            if col in standings_df.columns:
                standings_df[col] = standings_df[col].fillna('-')
        
        return standings_df[output_columns].to_dict('records')
    except Exception as e:
        logger.error(f"Failed to fetch driver standings: {e}", exc_info=True)
        return []

def get_constructor_standings(year: int) -> list:
    """
    Fetches the latest constructor championship standings for a given year using Ergast.
    """
    logger.info(f"Fetching CONSTRUCTOR standings for year: {year}")
    try:
        ergast = Ergast()
        # The data is a list containing one DataFrame
        results_list = ergast.get_constructor_standings(season=year).content
        if not results_list:
            logger.warning(f"No constructor standings data returned from Ergast for {year}.")
            return []
        
        standings_df = results_list[0]
        
        # The required column names from your standings_page.py layout
        output_columns = ['position', 'constructorName', 'constructorNationality', 'points', 'wins']
        
        # Create a new DataFrame with only the columns we need
        final_df = standings_df[output_columns].copy()
        
        return final_df.to_dict('records')

    except Exception as e:
        logger.error(f"Failed to fetch or process constructor standings: {e}", exc_info=True)
        return []

def get_current_year_schedule_with_sessions(year: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Fetches the F1 schedule for a given year (defaults to current year) 
    and processes session dates into UTC ISO strings.
    Results are cached for 15 minutes. Cache key is the 'year'.
    FastF1's own cache should be enabled globally (e.g., in main.py).
    """
    if year is None:
        year = datetime.now().year

    logger.info(
        f"Executing get_current_year_schedule_with_sessions for year: {year} (Cache key: {year})")

    schedule_data_list = []
    try:
        # FastF1 cache is assumed to be enabled globally in main.py
        # No need for: fastf1.Cache.enable_cache(...) here.

        schedule_df = fastf1.get_event_schedule(year, include_testing=False)
        logger.debug(
            f"Raw event schedule for {year} fetched. Rows: {len(schedule_df) if schedule_df is not None else 'None'}")

        if schedule_df is not None and not schedule_df.empty:
            for index, event_row in schedule_df.iterrows():
                event_data_dict = event_row.to_dict()
                event_name_for_logs = event_row.get('EventName', 'N/A')
                logger.debug(
                    f"Processing event: {event_name_for_logs} (Round: {event_row.get('RoundNumber', 'N/A')})")

                # Ensure critical date fields are ISO format strings
                # QualifyingDateUtc might not always be present directly
                for key in ['EventDate', 'Session1DateUtc', 'Session2DateUtc', 'Session3DateUtc', 'Session4DateUtc', 'Session5DateUtc', 'SprintDateUtc', 'QualifyingDateUtc']:
                    if key in event_data_dict and isinstance(event_data_dict[key], pd.Timestamp):
                        if pd.notna(event_data_dict[key]):
                            # Ensure UTC, then ISO format
                            ts_val = event_data_dict[key]
                            if ts_val.tzinfo is None:
                                event_data_dict[key] = ts_val.tz_localize(
                                    'UTC').isoformat()
                            else:
                                event_data_dict[key] = ts_val.tz_convert(
                                    'UTC').isoformat()
                        else:
                            event_data_dict[key] = None  # Handle NaT as None

                # Ensure RoundNumber is int
                if 'RoundNumber' in event_data_dict and pd.notna(event_data_dict['RoundNumber']):
                    event_data_dict['RoundNumber'] = int(
                        event_data_dict['RoundNumber'])
                else:
                    # Default or placeholder
                    event_data_dict['RoundNumber'] = 0

                sessions_data_list_for_event = []
                session_name_columns = {  # Map FastF1 column name to preferred display name
                    'Session1': event_row.get('Session1'),
                    'Session2': event_row.get('Session2'),
                    'Session3': event_row.get('Session3'),
                    'Session4': event_row.get('Session4'),  # Often Qualifying
                    'Session5': event_row.get('Session5'),  # Often Race
                    # For Sprint race itself
                    'Sprint': event_row.get('Sprint'),
                    # If distinct Sprint Quali name column exists
                    'Sprint Qualifying': event_row.get('Sprint Qualifying')
                }

                for f1_col_name_prefix, display_session_name in session_name_columns.items():
                    # If the session name itself is NaN (e.g. no Session5)
                    if pd.isna(display_session_name):
                        continue

                    session_date_utc_col = f'{f1_col_name_prefix}DateUtc'
                    session_date_utc_iso = event_data_dict.get(
                        session_date_utc_col)

                    if session_date_utc_iso and pd.notna(session_date_utc_iso):
                        sessions_data_list_for_event.append({
                            # Ensure string
                            'SessionName': str(display_session_name),
                            'SessionDateUTC': session_date_utc_iso
                        })
                        logger.debug(
                            f"  Added session: {display_session_name} at {session_date_utc_iso}")

                # Sort sessions by date before adding to the event
                if sessions_data_list_for_event:
                    sessions_data_list_for_event.sort(
                        key=lambda s: s['SessionDateUTC'])

                event_data_dict['Sessions'] = sessions_data_list_for_event
                schedule_data_list.append(event_data_dict)
        else:
            logger.warning(
                f"No events found in schedule for year {year} by FastF1.")

    except Exception as e_main_fetch:
        logger.error(
            f"Major error fetching/processing F1 schedule for {year}: {e_main_fetch}", exc_info=True)
        return []

    logger.info(
        f"Finished fetching schedule for {year}. Total events processed: {len(schedule_data_list)}")
    return schedule_data_list

# --- Helper Functions (from your file) ---


def format_session_time_local(utc_iso_str: Optional[str], user_timezone_str: str = 'UTC') -> str:
    if not utc_iso_str:
        return "N/A"
    try:
        # Ensure the string is a valid ISO format for fromisoformat, particularly the Z for UTC
        dt_utc_aware = utils.parse_iso_timestamp_safe(utc_iso_str)
        if not dt_utc_aware:
            return "Invalid Date Data (parse)"

        user_tz = pytz.timezone(user_timezone_str)
        local_dt = dt_utc_aware.astimezone(user_tz)
        return local_dt.strftime("%a, %d %b %Y - %H:%M (%Z)")
    except pytz.UnknownTimeZoneError:
        logger.warning(
            f"Unknown timezone '{user_timezone_str}'. Displaying time in UTC.")
        dt_utc_aware = utils.parse_iso_timestamp_safe(
            utc_iso_str)  # Reparse for safety
        return dt_utc_aware.strftime("%a, %d %b %Y - %H:%M (UTC)") if dt_utc_aware else "Invalid Date"
    except Exception as e:
        logger.error(
            f"Error converting time '{utc_iso_str}' to timezone '{user_timezone_str}': {e}", exc_info=False)
        return "Date Conversion Error"


def calculate_countdown(target_utc_iso_str: Optional[str]):
    # (Your existing calculate_countdown function logic - seems okay)
    if not target_utc_iso_str:
        return 0, 0, 0, 0, True, "N/A"
    try:
        target_dt_utc = utils.parse_iso_timestamp_safe(target_utc_iso_str)
        if not target_dt_utc:
            return 0, 0, 0, 0, True, "Invalid Target Date"

        now_utc = datetime.now(pytz.utc)
        delta = target_dt_utc - now_utc

        if delta.total_seconds() < 0:
            return 0, 0, 0, 0, True, target_dt_utc.strftime("%d %b, %H:%M UTC")

        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return days, hours, minutes, seconds, False, target_dt_utc.strftime("%d %b %H:%M UTC")
    except Exception as e:
        logger.error(
            f"Error calculating countdown for {target_utc_iso_str}: {e}", exc_info=False)
        return 0, 0, 0, 0, True, "Error in date"


# --- Layout Definition ---
# (Your existing schedule_page_layout - seems okay)
schedule_page_layout = dbc.Container(fluid=True, className="pt-4 px-4", children=[
    dcc.Store(id='f1-schedule-store-data'),
    dbc.Row(id='schedule-countdown-display-row', className="mb-4 text-center", children=[
        dbc.Col(lg=6, md=12, className="mb-3", children=[
            dbc.Card(className="h-100 shadow-sm", children=[dbc.CardBody([
                html.H5("Next Session", className="card-title text-light"),
                html.H4(id='next-session-countdown-name',
                        children="Loading...", className="fw-normal"),
                html.P(id='next-session-countdown-text',
                       className="display-5 fw-bold my-2", children="--d --h --m --s"),
                html.Small(id='next-session-countdown-datetime-local',
                           children=" ", className="text-muted")
            ])])
        ]),
        dbc.Col(lg=6, md=12, children=[
            dbc.Card(className="h-100 shadow-sm", children=[dbc.CardBody([
                html.H5("Next Race", className="card-title text-danger"),
                html.H4(id='next-race-countdown-name',
                        children="Loading...", className="fw-normal"),
                html.P(id='next-race-countdown-text',
                       className="display-5 fw-bold my-2", children="--d --h --m --s"),
                html.Small(id='next-race-countdown-datetime-local',
                           children=" ", className="text-muted")
            ])])
        ])
    ]),
    dbc.Row([
        dbc.Col([
            html.H3("Full Race Schedule", className="mb-3 text-light"),
            dcc.Loading(
                id="loading-schedule-display", type="default",
                children=html.Div(id='schedule-events-accordion',
                                  children=[dbc.Spinner(color="primary", spinner_class_name="m-auto d-block")])
            )
        ])
    ]),
    dcc.Interval(id='schedule-page-interval-component', interval=1 *
                 1000, n_intervals=0, disabled=False),  # For countdowns
    dcc.Interval(id='schedule-page-fetch-interval-component', interval=15 * 60 *
                 1000, n_intervals=0, disabled=False)  # Fetch every 15 mins, matching cache
])

# --- Callbacks for Schedule Page ---


@callback(
    Output('f1-schedule-store-data', 'data'),
    Input('schedule-page-fetch-interval-component', 'n_intervals')
)
def fetch_f1_schedule_data_callback(n_intervals: int):
    # This callback now calls the cached function.
    # The year argument for get_current_year_schedule_with_sessions defaults to current year.
    logger.debug(
        f"Callback: Fetch_f1_schedule_data_callback triggered by interval (n={n_intervals}).")
    schedule = get_current_year_schedule_with_sessions()  # Year defaults to current
    logger.info(
        f"Callback: Fetched {len(schedule)} events for schedule store (using cached function).")
    return schedule


@callback(
    Output('schedule-events-accordion', 'children'),
    Input('f1-schedule-store-data', 'data'),
    Input('user-timezone-store-data', 'data')  # From main_app_layout
)
def display_f1_schedule_callback(schedule_data: Optional[List[Dict[str, Any]]], user_timezone_json: Optional[str]):
    # (Your existing display_f1_schedule_callback logic, ensuring it correctly uses
    #  the structure from the new get_current_year_schedule_with_sessions - seems mostly compatible)
    ctx = dash.callback_context
    triggered_id = ctx.triggered_id
    logger.debug(
        f"Callback: Display schedule triggered by {triggered_id or 'initial load/data change'}")

    user_timezone_str = "UTC"  # Default
    # Assuming it's a direct string like "Europe/London"
    if user_timezone_json and isinstance(user_timezone_json, str):
        user_timezone_str = user_timezone_json
    # Add more robust parsing if user_timezone_json might be a dict string from dcc.Store e.g. {'timezone': 'Europe/London'}

    if not user_timezone_str:  # Should not happen if store provides default or clientside callback sets it
        logger.info(
            "Display schedule: User timezone not available. Displaying in UTC.")
        user_timezone_str = "UTC"
        # return dbc.Alert("Detecting your timezone to display local times...", color="info", className="mt-3 text-center")

    if not schedule_data:
        logger.info("Display schedule: No schedule data available yet.")
        return dbc.Alert("Schedule data is loading or unavailable.", color="warning", className="mt-3 text-center")

    accordion_items = []
    now_utc = datetime.now(pytz.utc)
    first_upcoming_event_idx = -1

    def get_event_sort_key(event_dict):
        # Your existing sort key logic
        date_str = event_dict.get('EventDate')
        if date_str:
            dt = utils.parse_iso_timestamp_safe(date_str)
            return dt if dt else datetime.max.replace(tzinfo=pytz.utc)
        return datetime.max.replace(tzinfo=pytz.utc)

    try:
        sorted_schedule_data = sorted(schedule_data, key=get_event_sort_key)
    except Exception as e_sort:
        logger.error(f"Error sorting schedule data: {e_sort}", exc_info=True)
        sorted_schedule_data = schedule_data  # Fallback to unsorted

    for i, event in enumerate(sorted_schedule_data):
        event_name = event.get('OfficialEventName',
                               event.get('EventName', 'Unknown Event'))
        round_number_val = event.get('RoundNumber', '')
        round_number = f"R{round_number_val}" if round_number_val else ""

        event_header_date_str = "Date TBC"
        event_status = "Status TBC"
        item_class_name = "mb-2 bg-dark text-light event-accordion-item"
        title_class_name = "fw-bold"

        # Determine event status and header date based on its sessions
        all_session_dates_utc_for_event = []
        if event.get('Sessions'):
            for s_detail in event['Sessions']:
                s_dt_utc = utils.parse_iso_timestamp_safe(
                    s_detail.get('SessionDateUTC'))
                if s_dt_utc:
                    all_session_dates_utc_for_event.append(s_dt_utc)

        if all_session_dates_utc_for_event:
            first_session_dt_utc_for_event = min(
                all_session_dates_utc_for_event)
            last_session_dt_utc_for_event = max(
                all_session_dates_utc_for_event)
            event_header_date_str = format_session_time_local(
                first_session_dt_utc_for_event.isoformat(), user_timezone_str).split(' - ')[0]

            if last_session_dt_utc_for_event < now_utc:
                event_status = "Completed"
                item_class_name += " event-completed opacity-75"
            elif first_session_dt_utc_for_event <= now_utc <= (last_session_dt_utc_for_event + timedelta(hours=getattr(config, 'FASTF1_ONGOING_SESSION_WINDOW_HOURS', 3))):
                event_status = "Ongoing"
                item_class_name += " event-ongoing"
                if first_upcoming_event_idx == -1:
                    first_upcoming_event_idx = i
            elif first_session_dt_utc_for_event > now_utc:
                event_status = "Upcoming"
                item_class_name += " event-upcoming"
                if first_upcoming_event_idx == -1:
                    first_upcoming_event_idx = i
        elif event.get('EventDate'):  # Fallback if no sessions, use EventDate
            event_date_utc = utils.parse_iso_timestamp_safe(
                event.get('EventDate'))
            if event_date_utc:
                event_header_date_str = format_session_time_local(
                    event_date_utc.isoformat(), user_timezone_str).split(' - ')[0]
                # Rough completed
                if event_date_utc < now_utc - timedelta(days=3):
                    event_status = "Completed"
                    item_class_name += " event-completed opacity-75"
                else:
                    event_status = "Upcoming"
                    item_class_name += " event-upcoming"
                    if first_upcoming_event_idx == -1:
                        first_upcoming_event_idx = i

        accordion_title_content = html.Div([
            html.Span(f"{round_number}: {event_name}",
                      className=title_class_name),
            html.Span(f" ({event_header_date_str})",
                      className="small text-muted ps-2"),
            dbc.Badge(event_status,
                      color=("light" if event_status == "Completed" else
                             "success" if event_status == "Ongoing" else
                             "primary"),
                      className="ms-auto float-end")
        ], className="d-flex w-100 align-items-center")

        sessions_content_list = []
        if event.get('Sessions'):
            sessions_content_list.append(html.Hr(className="my-2"))
            for session_item in sorted(event['Sessions'], key=lambda s: s.get('SessionDateUTC', '')):
                session_name_item = session_item.get('SessionName')
                session_time_utc_iso = session_item.get('SessionDateUTC')
                formatted_local_time = format_session_time_local(
                    session_time_utc_iso, user_timezone_str)
                session_style_dict = {'fontSize': '0.85rem'}

                s_item_dt_utc = utils.parse_iso_timestamp_safe(
                    session_time_utc_iso)
                if s_item_dt_utc and s_item_dt_utc < now_utc:  # Check if session is in the past
                    # Removed line-through for better readability
                    session_style_dict.update({'opacity': '0.6'})

                sessions_content_list.append(
                    dbc.Row([
                        dbc.Col(html.Strong(
                            f"{session_name_item}:"), md=4, style=session_style_dict),
                        dbc.Col(formatted_local_time, md=8,
                                style=session_style_dict)
                    ], className="mb-1")
                )
        else:
            sessions_content_list.append(html.P(
                "Session details currently unavailable.", className="small text-muted fst-italic mt-2"))

        accordion_items.append(
            dbc.AccordionItem(
                title=accordion_title_content,
                children=sessions_content_list,
                item_id=f"item-schedule-{i}",
                class_name=item_class_name
            )
        )

    active_item_to_open = f"item-schedule-{first_upcoming_event_idx}" if first_upcoming_event_idx != -1 else None
    if not accordion_items:  # Handle case where schedule_data was empty or resulted in no items
        return dbc.Alert("No schedule items to display.", color="info"), None

    # If no upcoming, but there are items, open the first one by default (most recent past)
    if not active_item_to_open and accordion_items:
        active_item_to_open = accordion_items[0].item_id if accordion_items else None

    return dbc.Accordion(accordion_items, active_item=active_item_to_open, flush=False, always_open=False),


@callback(
    [Output('next-session-countdown-name', 'children'),
     Output('next-session-countdown-text', 'children'),
     Output('next-session-countdown-datetime-local', 'children'),
     Output('next-race-countdown-name', 'children'),
     Output('next-race-countdown-text', 'children'),
     Output('next-race-countdown-datetime-local', 'children'),
     Output('schedule-page-interval-component', 'disabled')],  # To disable interval if no future events
    Input('schedule-page-interval-component', 'n_intervals'),
    State('f1-schedule-store-data', 'data'),
    State('user-timezone-store-data', 'data')
    # From main_app_layout
)
def update_countdowns_callback(n_intervals: int, schedule_data: Optional[List[Dict[str, Any]]], user_timezone_json: Optional[str]):
    # (Your existing update_countdowns_callback logic - ensure it uses the refined schedule_data structure
    #  and UTC dates from it for calculations. Seems mostly compatible.)
    logger.debug(
        f"Update Countdowns Callback triggered (n_intervals: {n_intervals})")

    user_timezone_str = "UTC"  # Default
    if user_timezone_json and isinstance(user_timezone_json, str):
        user_timezone_str = user_timezone_json

    loading_name = "Loading..."
    loading_text = "--d --h --m --s"
    loading_datetime = " "

    if not schedule_data:  # No data in store yet
        logger.debug(
            "Update Countdowns: Schedule data not yet available. Returning loading state.")
        return (loading_name, loading_text, loading_datetime,
                loading_name, loading_text, loading_datetime,
                False)  # Keep interval enabled

    now_utc = datetime.now(pytz.utc)
    next_overall_session_info = {'dt': datetime.max.replace(
        tzinfo=pytz.utc), 'name': None, 'event': None, 'iso': None}
    next_race_session_info = {'dt': datetime.max.replace(
        tzinfo=pytz.utc), 'name': None, 'event': None, 'iso': None}

    for event in schedule_data:
        event_name_cd = event.get(
            'OfficialEventName', event.get('EventName', 'Unknown Event'))
        for session in event.get('Sessions', []):
            session_time_utc_iso = session.get('SessionDateUTC')
            if session_time_utc_iso:
                session_dt_utc = utils.parse_iso_timestamp_safe(
                    session_time_utc_iso)
                if session_dt_utc and session_dt_utc > now_utc:
                    if session_dt_utc < next_overall_session_info['dt']:
                        next_overall_session_info = {
                            'dt': session_dt_utc,
                            'name': session.get('SessionName', 'Session'),
                            'event': event_name_cd,
                            'iso': session_time_utc_iso
                        }
                    if session.get('SessionName', '').strip().lower() == 'race':  # Check for "Race"
                        if session_dt_utc < next_race_session_info['dt']:
                            next_race_session_info = {
                                'dt': session_dt_utc,
                                'name': "Race",
                                'event': event_name_cd,
                                'iso': session_time_utc_iso
                            }

    session_name_display = "No upcoming sessions"
    session_countdown_str = "--:--:--:--"
    session_datetime_local_str = " "
    final_disable_interval = True  # Assume disabled unless a future event is found

    if next_overall_session_info['iso']:
        days, h, m, s, overall_past, _ = calculate_countdown(
            next_overall_session_info['iso'])
        session_name_display = f"{next_overall_session_info['event']} - {next_overall_session_info['name']}"
        if not overall_past:
            session_countdown_str = f"{days}d {h:02}h {m:02}m {s:02}s"
            session_datetime_local_str = format_session_time_local(
                next_overall_session_info['iso'], user_timezone_str)
            final_disable_interval = False  # Found a future session
        else:  # Should not happen due to `session_dt_utc > now_utc` check, but defensive
            session_countdown_str = "Started/Done"
            session_datetime_local_str = format_session_time_local(
                next_overall_session_info['iso'], user_timezone_str)
            # final_disable_interval might still be True if this is the only "future" found but it's past
            # The check `session_dt_utc > now_utc` should prevent this branch mostly

    race_name_display = "No upcoming race"
    race_countdown_str = "--:--:--:--"
    race_datetime_local_str = " "
    if next_race_session_info['iso']:
        days_r, h_r, m_r, s_r, race_past, _ = calculate_countdown(
            next_race_session_info['iso'])
        race_name_display = f"{next_race_session_info['event']} - Race"
        if not race_past:
            race_countdown_str = f"{days_r}d {h_r:02}h {m_r:02}m {s_r:02}s"
            race_datetime_local_str = format_session_time_local(
                next_race_session_info['iso'], user_timezone_str)
            final_disable_interval = False  # Found a future race, ensure interval active
        else:
            race_countdown_str = "Started/Done"
            race_datetime_local_str = format_session_time_local(
                next_race_session_info['iso'], user_timezone_str)
            # If overall was also past/none, final_disable_interval could be true.

    if next_overall_session_info['iso'] is None and next_race_session_info['iso'] is None:
        # Definitely disable if no future sessions at all were found
        final_disable_interval = True

    return (session_name_display, session_countdown_str, session_datetime_local_str,
            race_name_display, race_countdown_str, race_datetime_local_str,
            final_disable_interval)

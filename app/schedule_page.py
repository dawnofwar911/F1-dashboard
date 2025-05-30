# schedule_page.py
"""
Handles the layout and callbacks for the F1 Race Schedule page.
"""
import dash
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc
import pandas as pd
import fastf1
import datetime
import pytz  # For timezone handling
import logging # <<< ADDED logging import
from app_instance import app
import config
import utils
from pathlib import Path

# --- Setup Logger for this Module ---
logger = logging.getLogger("F1App.SchedulePage") # <<< ADDED logger instance

# --- Helper Functions ---

def get_current_year_schedule_with_sessions():
    logger.debug("Attempting to fetch F1 schedule with sessions...")
    schedule_data_list = []
    try:
        year = datetime.datetime.now().year
        
        cache_path_to_use = getattr(config, 'FASTF1_CACHE_DIR', Path(".fastf1_cache_schedule_default"))
        if not isinstance(cache_path_to_use, Path): 
            cache_path_to_use = Path(str(cache_path_to_use))
            logger.warning(f"FASTF1_CACHE_DIR was not a Path object, converted to: {cache_path_to_use}")

        try:
            cache_path_to_use.mkdir(parents=True, exist_ok=True)
            fastf1.Cache.enable_cache(cache_path_to_use)
            logger.info(f"FastF1 Cache using: {cache_path_to_use}")
        except Exception as e_cache:
            logger.error(f"Could not enable/create FastF1 cache at {cache_path_to_use}: {e_cache}", exc_info=True)

        schedule_df = fastf1.get_event_schedule(year, include_testing=False)
        logger.debug(f"Raw event schedule for {year} fetched. Rows: {len(schedule_df) if schedule_df is not None else 'None'}")

        if schedule_df is not None and not schedule_df.empty:
            for index, event_row in schedule_df.iterrows():
                event_data_dict = event_row.to_dict()
                event_name_for_logs = event_row.get('EventName', 'N/A')
                logger.debug(f"Processing event: {event_name_for_logs} (Round: {event_row.get('RoundNumber', 'N/A')})")

                for key, value in event_data_dict.items():
                    if isinstance(value, pd.Timestamp):
                        if value.tzinfo is None:
                            event_data_dict[key] = value.isoformat()
                        else:
                            event_data_dict[key] = value.tz_convert('UTC').isoformat()
                    elif isinstance(value, (datetime.date, datetime.datetime)):
                        event_data_dict[key] = value.isoformat()
                
                event_data_dict['RoundNumber'] = int(event_row['RoundNumber'])
                sessions_data_list_for_event = []
                try:
                    event_identifier = event_data_dict['RoundNumber']
                    full_event_obj = fastf1.get_event(year, event_identifier)
                    
                    if hasattr(full_event_obj, 'load_sessions') and not full_event_obj.sessions_loaded:
                         full_event_obj.load_sessions() 
                    elif hasattr(full_event_obj, 'load_session_info') and not full_event_obj.session_info_loaded: 
                         full_event_obj.load_session_info()
                    
                    logger.debug(f"  Fetched full event object for: {getattr(full_event_obj, 'EventName', event_name_for_logs)}")

                    # YOUR Corrected session_map logic:
                    # Keys are the display names you want.
                    # Values are the names FastF1's get_session_date() expects.
                    session_map_display_to_f1name = {
                        'FP1': 'Practice 1',
                        'FP2': 'Practice 2',
                        'FP3': 'Practice 3',
                        'Sprint Qualifying': 'Sprint Qualifying', # Official name F1 uses now
                        'Sprint': 'Sprint',                     # Official name F1 uses now
                        'Qualifying': 'Qualifying',          # To distinguish from Sprint Quali
                        'Race': 'Race'                    # For the main race
                    }
                    
                    # Iterate through your desired display names and try to fetch using the F1 official name
                    for display_name, f1_official_session_name in session_map_display_to_f1name.items():
                        try:
                            session_date_pd_ts = full_event_obj.get_session_date(f1_official_session_name, utc=True)
                            
                            if pd.notna(session_date_pd_ts):
                                if not isinstance(session_date_pd_ts, pd.Timestamp):
                                    session_date_pd_ts = pd.Timestamp(session_date_pd_ts)

                                session_date_utc_aware = None
                                if session_date_pd_ts.tzinfo is None:
                                    try:
                                        session_date_utc_aware = session_date_pd_ts.tz_localize('UTC')
                                    except Exception as e_localize: # Handle already localized if it somehow happens
                                        if hasattr(session_date_pd_ts, 'tzinfo') and session_date_pd_ts.tzinfo is not None:
                                            session_date_utc_aware = session_date_pd_ts.tz_convert('UTC')
                                        else:
                                            logger.error(f"    Error localizing naive timestamp for {display_name} ({session_date_pd_ts}): {e_localize}", exc_info=False)
                                            continue # Skip this session if error
                                else:
                                    session_date_utc_aware = session_date_pd_ts.tz_convert('UTC')
                                
                                if session_date_utc_aware:
                                    sessions_data_list_for_event.append({
                                        'SessionName': display_name, # Use YOUR desired display name
                                        'SessionDateUTC': session_date_utc_aware.isoformat()
                                    })
                                    logger.debug(f"    Added session: {display_name} (queried as '{f1_official_session_name}') at {session_date_utc_aware.isoformat()}")
                            # else:
                                # logger.debug(f"    Session '{f1_official_session_name}' date is NaT for {getattr(full_event_obj, 'EventName', event_name_for_logs)}")
                        
                        except ValueError: 
                            # logger.debug(f"    Session '{f1_official_session_name}' (for display as '{display_name}') not found for {getattr(full_event_obj, 'EventName', event_name_for_logs)}.")
                            pass
                        except Exception as e_session_date:
                            logger.warning(f"    Error getting date for session '{f1_official_session_name}' (display: '{display_name}') in {getattr(full_event_obj, 'EventName', event_name_for_logs)}: {e_session_date}", exc_info=False)
                    
                    if sessions_data_list_for_event:
                        sessions_data_list_for_event.sort(key=lambda s: s['SessionDateUTC'])
                        event_data_dict['Sessions'] = sessions_data_list_for_event
                        logger.debug(f"  Successfully processed {len(sessions_data_list_for_event)} sessions for {getattr(full_event_obj, 'EventName', event_name_for_logs)}")
                    else:
                        event_data_dict['Sessions'] = []
                        logger.warning(f"  No valid session dates could be retrieved for {getattr(full_event_obj, 'EventName', event_name_for_logs)} using predefined session map.")

                except Exception as e_event_load:
                    logger.error(f"  Error loading detailed event object or sessions for {event_name_for_logs} (Identifier: {event_identifier}): {e_event_load}", exc_info=True)
                    event_data_dict['Sessions'] = []
                
                schedule_data_list.append(event_data_dict)
        else:
            logger.warning(f"No events found in schedule for year {year}.")

    except Exception as e_main_fetch:
        logger.error(f"Major error fetching F1 schedule: {e_main_fetch}", exc_info=True)
        return [] 

    logger.info(f"Finished fetching schedule. Total events with some data: {len(schedule_data_list)}")
    return schedule_data_list

def format_session_time_local(utc_iso_str, user_timezone_str='UTC'):
    if not utc_iso_str:
        return "N/A"
    try:
        utc_dt = datetime.datetime.fromisoformat(utc_iso_str.replace('Z', '+00:00'))
        if utc_dt.tzinfo is None: 
            utc_dt = pytz.utc.localize(utc_dt)
        else: 
            utc_dt = utc_dt.astimezone(pytz.utc)
        
        user_tz = pytz.timezone(user_timezone_str)
        local_dt = utc_dt.astimezone(user_tz)
        return local_dt.strftime("%a, %d %b %Y - %H:%M (%Z)")
    except Exception as e:
        logger.error(f"Error converting time {utc_iso_str} to {user_timezone_str}: {e}", exc_info=True) # MODIFIED
        try: 
            dt_obj = datetime.datetime.fromisoformat(utc_iso_str.replace('Z', '+00:00'))
            return dt_obj.strftime("%a, %d %b %Y - %H:%M (UTC)")
        except:
            return "Invalid Date Data"

def calculate_countdown(target_utc_iso_str):
    if not target_utc_iso_str:
        return 0, 0, 0, 0, True, "N/A" 
    try:
        target_dt_utc = datetime.datetime.fromisoformat(target_utc_iso_str.replace('Z', '+00:00'))
        if target_dt_utc.tzinfo is None: target_dt_utc = pytz.utc.localize(target_dt_utc)
        else: target_dt_utc = target_dt_utc.astimezone(pytz.utc)

        now_utc = datetime.datetime.now(pytz.utc)
        delta = target_dt_utc - now_utc
        
        if delta.total_seconds() < 0:
            return 0, 0, 0, 0, True, target_dt_utc.strftime("%d %b, %H:%M UTC") 

        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return days, hours, minutes, seconds, False, target_dt_utc.strftime("%d %b %H:%M UTC")
    except Exception as e:
        logger.error(f"Error calculating countdown for {target_utc_iso_str}: {e}", exc_info=True) # MODIFIED
        return 0, 0, 0, 0, True, "Error in date"

# --- Layout Definition ---
# (Layout definition remains the same as in Response 4, Step 1)
schedule_page_layout = dbc.Container(fluid=True, className="pt-4 px-4", children=[
    dcc.Store(id='f1-schedule-store-data'), 
    dbc.Row(id='schedule-countdown-display-row', className="mb-4 text-center", children=[
        dbc.Col(lg=6, md=12, className="mb-3", children=[
            dbc.Card(className="h-100 shadow-sm", children=[dbc.CardBody([
                html.H5("Next Session", className="card-title text-light"),
                html.H4(id='next-session-countdown-name', children="Loading...", className="fw-normal"),
                html.P(id='next-session-countdown-text', className="display-5 fw-bold my-2", children="--d --h --m --s"),
                html.Small(id='next-session-countdown-datetime-local', children=" ", className="text-muted")
            ])])
        ]),
        dbc.Col(lg=6, md=12, children=[
            dbc.Card(className="h-100 shadow-sm", children=[dbc.CardBody([
                html.H5("Next Race", className="card-title text-danger"),
                html.H4(id='next-race-countdown-name', children="Loading...", className="fw-normal"),
                html.P(id='next-race-countdown-text', className="display-5 fw-bold my-2", children="--d --h --m --s"),
                html.Small(id='next-race-countdown-datetime-local', children=" ", className="text-muted")
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
    dcc.Interval(id='schedule-page-interval-component', interval=1*1000, n_intervals=0, disabled=False),
    dcc.Interval(id='schedule-page-fetch-interval-component', interval= 6 * 60 * 60 * 1000, n_intervals=0, disabled=False)
])

# --- Callbacks for Schedule Page ---
@callback(
    Output('f1-schedule-store-data', 'data'),
    Input('schedule-page-fetch-interval-component', 'n_intervals')
)
def fetch_f1_schedule_data_callback(n_intervals):
    logger.debug("Callback: Fetching F1 schedule data trigger...")
    schedule = get_current_year_schedule_with_sessions()
    logger.info(f"Callback: Fetched {len(schedule)} events for schedule store.")
    return schedule

@callback(
    Output('schedule-events-accordion', 'children'),
    Input('f1-schedule-store-data', 'data'),
    Input('user-timezone-store-data', 'data') 
)
def display_f1_schedule_callback(schedule_data, user_timezone):
    ctx = dash.callback_context
    triggered_id = ctx.triggered_id
    logger.debug(f"Callback: Display schedule triggered by {triggered_id or 'initial load/data change'}")

    if not user_timezone:
        logger.info("Display schedule: Waiting for user timezone.")
        return dbc.Alert("Detecting your timezone to display local times...", color="info", className="mt-3 text-center")
    if not schedule_data:
        logger.info("Display schedule: No schedule data available yet.")
        return dbc.Alert("Schedule data is loading or unavailable.", color="info", className="mt-3 text-center")

    accordion_items = []
    now_utc = datetime.datetime.now(pytz.utc)
    first_upcoming_event_idx = -1 

    def get_event_sort_key(event_dict):
        date_str = event_dict.get('EventDate') 
        if date_str:
            try:
                dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return dt.astimezone(pytz.utc) if dt.tzinfo else pytz.utc.localize(dt)
            except ValueError:
                try: 
                    return pytz.utc.localize(datetime.datetime.strptime(date_str, "%Y-%m-%d"))
                except ValueError:
                    logger.warning(f"Could not parse EventDate for sorting: {date_str}")
                    return pytz.utc.localize(datetime.datetime.max) 
        logger.warning(f"Event missing EventDate for sorting: {event_dict.get('EventName')}")
        return pytz.utc.localize(datetime.datetime.max) 

    try:
        sorted_schedule_data = sorted(schedule_data, key=get_event_sort_key)
    except Exception as e_sort:
        logger.error(f"Error sorting schedule data: {e_sort}", exc_info=True)
        sorted_schedule_data = schedule_data 

    for i, event in enumerate(sorted_schedule_data):
        event_name = event.get('OfficialEventName', event.get('EventName', 'Unknown Event'))
        round_number = event.get('RoundNumber', '')
        
        event_header_date_str = "Date TBC"
        event_status = "Status TBC"
        # --- Style Modifications for Accordion Item ---
        item_class_name = "mb-2 bg-dark text-light event-accordion-item" # Base class for all items
        title_class_name = "fw-bold" # Default title style

        first_session_for_date = event.get('Sessions')[0] if event.get('Sessions') else None
        date_source_for_header = None
        if first_session_for_date and first_session_for_date.get('SessionDateUTC'):
            date_source_for_header = first_session_for_date.get('SessionDateUTC')
        elif event.get('EventDate'):
             date_source_for_header = event.get('EventDate')
             if len(date_source_for_header) == 10: date_source_for_header += "T00:00:00Z" # Make it parsable as full ISO

        if date_source_for_header:
            try:
                event_header_date_str = format_session_time_local(date_source_for_header, user_timezone).split(' - ')[0]
            except Exception as e:
                logger.warning(f"Could not format header date {date_source_for_header}: {e}")
                event_header_date_str = "Date Error"


        all_session_dates_utc = []
        if event.get('Sessions'):
            for s_idx, s in enumerate(event['Sessions']):
                if s.get('SessionDateUTC'):
                    try:
                        s_dt = datetime.datetime.fromisoformat(s['SessionDateUTC'].replace('Z', '+00:00'))
                        all_session_dates_utc.append(s_dt.astimezone(pytz.utc) if s_dt.tzinfo else pytz.utc.localize(s_dt))
                    except ValueError: 
                        logger.warning(f"Malformed session date for event {event_name}, session index {s_idx}: {s.get('SessionDateUTC')}")
                        continue
        
        if not all_session_dates_utc and date_source_for_header: 
            try:
                main_event_dt_utc = datetime.datetime.fromisoformat(date_source_for_header.replace('Z', '+00:00'))
                main_event_dt_utc = main_event_dt_utc.astimezone(pytz.utc) if main_event_dt_utc.tzinfo else pytz.utc.localize(main_event_dt_utc)
                if main_event_dt_utc < now_utc - datetime.timedelta(days=event.get('SessionLengthDays', 3)): # Heuristic
                     event_status = "Completed"; item_class_name += " event-completed"
                else:
                     event_status = "Upcoming"
                     if first_upcoming_event_idx == -1: first_upcoming_event_idx = i
                     item_class_name += " event-upcoming"
            except ValueError: pass
        elif all_session_dates_utc:
            first_session_dt_utc = min(all_session_dates_utc)
            last_session_dt_utc = max(all_session_dates_utc)
            if last_session_dt_utc < now_utc:
                event_status = "Completed"; item_class_name += " event-completed"
            elif first_session_dt_utc <= now_utc <= last_session_dt_utc : 
                event_status = "Ongoing"; item_class_name += " event-ongoing"
                if first_upcoming_event_idx == -1: first_upcoming_event_idx = i
            elif first_session_dt_utc > now_utc: 
                event_status = "Upcoming"; item_class_name += " event-upcoming"
                if first_upcoming_event_idx == -1: first_upcoming_event_idx = i
        
        # Construct title with status for better visual cue
        accordion_title_content = html.Div([
            html.Span(f"R{round_number}: {event_name}", className=title_class_name),
            html.Span(f" ({event_header_date_str})", className="small text-muted ps-2"),
            dbc.Badge(event_status, 
                      color=("light" if event_status == "Completed" else 
                             "success" if event_status == "Ongoing" else 
                             "primary" if event_status == "Upcoming" else "secondary"), 
                      className="ms-auto float-end") # Use float-end to align badge to the right
        ], className="d-flex w-100 align-items-center") # Flexbox for title layout

        sessions_content_list = []
        if event.get('Sessions'):
            sessions_content_list.append(html.Hr(className="my-2")) # Separator
            for session_item in event['Sessions']: 
                session_name = session_item.get('SessionName')
                session_time_utc = session_item.get('SessionDateUTC')
                formatted_local_time = format_session_time_local(session_time_utc, user_timezone)
                session_style_dict = {'fontSize': '0.85rem'} # Make session text slightly smaller
                if session_time_utc:
                    try:
                        s_dt_utc = datetime.datetime.fromisoformat(session_time_utc.replace('Z', '+00:00'))
                        s_dt_utc = s_dt_utc.astimezone(pytz.utc) if s_dt_utc.tzinfo else pytz.utc.localize(s_dt_utc)
                        if s_dt_utc < now_utc:
                            session_style_dict.update({'textDecoration': 'line-through', 'opacity': '0.6'})
                    except ValueError: pass
                sessions_content_list.append(
                    dbc.Row([
                        dbc.Col(html.Strong(f"{session_name}:"), md=4, style=session_style_dict),
                        dbc.Col(formatted_local_time, md=8, style=session_style_dict)
                    ], className="mb-1")
                )
        else:
            sessions_content_list.append(html.P("Session details currently unavailable.", className="small text-muted fst-italic mt-2"))

        accordion_items.append(
            dbc.AccordionItem(
                title=accordion_title_content, # Use the div as title
                children=sessions_content_list, 
                item_id=f"item-schedule-{i}",
                class_name=item_class_name 
            )
        )
    
    active_item_to_open = f"item-schedule-{first_upcoming_event_idx}" if first_upcoming_event_idx != -1 else None
    if not active_item_to_open and accordion_items:
        active_item_to_open = accordion_items[0].item_id 

    return dbc.Accordion(accordion_items, active_item=active_item_to_open, flush=True, always_open=False)


@callback(
    [Output('next-session-countdown-name', 'children'),
     Output('next-session-countdown-text', 'children'),
     Output('next-session-countdown-datetime-local', 'children'),
     Output('next-race-countdown-name', 'children'),
     Output('next-race-countdown-text', 'children'),
     Output('next-race-countdown-datetime-local', 'children'),
     Output('schedule-page-interval-component', 'disabled')],
    Input('schedule-page-interval-component', 'n_intervals'),
    State('f1-schedule-store-data', 'data'),
    State('user-timezone-store-data', 'data')
)
def update_countdowns_callback(n_intervals, schedule_data, user_timezone):
    logger.debug(f"Update Countdowns Callback triggered (n_intervals: {n_intervals})")

    loading_name = "Loading..."
    loading_text = "--d --h --m --s"
    loading_datetime = " "
    
    # Default to keeping interval enabled unless explicitly disabled later
    # This 'current_disable_interval_state' is not actually used, the final_disable_interval is.
    # current_disable_interval_state = False 

    if not user_timezone: # Check if user_timezone is available
        logger.debug("Update Countdowns: User timezone not yet available. Returning loading state, interval enabled.")
        return (loading_name, loading_text, loading_datetime,
                loading_name, loading_text, loading_datetime,
                False) # KEEP INTERVAL ENABLED
    
    if not schedule_data: # Check if schedule_data is available
        logger.debug("Update Countdowns: Schedule data not yet available (timezone IS available). Returning loading state, interval enabled.")
        return (loading_name, loading_text, loading_datetime,
                loading_name, loading_text, loading_datetime,
                False) # KEEP INTERVAL ENABLED

    # If we reach here, both schedule_data and user_timezone should be available.
    logger.debug(f"Update Countdowns: Processing with schedule_data (Events: {len(schedule_data)}) and user_timezone ({user_timezone}).")
    now_utc = datetime.datetime.now(pytz.utc)
    logger.debug(f"Update Countdowns: Current UTC time: {now_utc.isoformat()}") # Log A
    
    next_overall_session_info = {'dt': datetime.datetime.max.replace(tzinfo=pytz.utc), 'name': None, 'event': None, 'iso': None}
    next_race_session_info = {'dt': datetime.datetime.max.replace(tzinfo=pytz.utc), 'name': None, 'event': None, 'iso': None}

    event_count_with_sessions = 0
    # --- Make sure ALL DEBUG logs inside this loop (D, E, F) are uncommented ---
    for event_idx, event in enumerate(schedule_data):
        # ... (loop as in Response 20, ensure logs D, E, F are active)
        event_name_for_countdown = event.get('OfficialEventName', event.get('EventName', f'Unknown Event {event_idx+1}'))
        if not event.get('Sessions'):
            # logger.debug(f"Event '{event_name_for_countdown}' has no 'Sessions' data for countdown.") # Log B
            continue
        
        event_count_with_sessions +=1
        for session_idx, session in enumerate(event.get('Sessions', [])):
            session_time_utc_iso = session.get('SessionDateUTC')
            session_name_log = session.get('SessionName', f'Unknown Session {session_idx+1}')
            # logger.debug(f"  Countdown Check: Event: {event_name_for_countdown}, Session: {session_name_log}, DateUTC: {session_time_utc_iso}") # Log C (Very verbose)

            if session_time_utc_iso:
                try:
                    session_dt_utc = datetime.datetime.fromisoformat(session_time_utc_iso.replace('Z', '+00:00'))
                    session_dt_utc = session_dt_utc.astimezone(pytz.utc) if session_dt_utc.tzinfo else pytz.utc.localize(session_dt_utc)

                    if session_dt_utc > now_utc: # Only consider future sessions
                        logger.debug(f"    Future session for countdown: {session_name_log} in {event_name_for_countdown} at {session_dt_utc.isoformat()}") # Log D (Make sure this one is active)
                        if session_dt_utc < next_overall_session_info['dt']:
                            next_overall_session_info = {
                                'dt': session_dt_utc,
                                'name': session.get('SessionName', 'Session'),
                                'event': event_name_for_countdown,
                                'iso': session_time_utc_iso
                            }
                            logger.debug(f"    New next_overall_session: {next_overall_session_info['event']} - {next_overall_session_info['name']}") # Log E (Make sure this is active)
                        
                        if session.get('SessionName') == 'Race':
                            if session_dt_utc < next_race_session_info['dt']:
                                next_race_session_info = {
                                    'dt': session_dt_utc,
                                    'name': "Race", 
                                    'event': event_name_for_countdown,
                                    'iso': session_time_utc_iso
                                }
                                logger.debug(f"    New next_race_session: {next_race_session_info['event']}") # Log F (Make sure this is active)
                except ValueError: 
                    logger.warning(f"Malformed date in countdown processing: Event '{event_name_for_countdown}', Session '{session_name_log}', DateStr '{session_time_utc_iso}'")
                    continue
    
    logger.debug(f"Total events with sessions considered for countdown: {event_count_with_sessions}") # Log G
    logger.debug(f"Final next_overall_session_info before formatting: ISO='{next_overall_session_info['iso']}', Event='{next_overall_session_info['event']}', Name='{next_overall_session_info['name']}'") # Log H
    logger.debug(f"Final next_race_session_info before formatting: ISO='{next_race_session_info['iso']}', Event='{next_race_session_info['event']}'") # Log I
                            
    session_name_display = "No upcoming sessions found"
    session_countdown_str = "All sessions complete"
    session_datetime_local_str = " "
    # Default to keeping interval enabled, only disable if truly no future events
    final_disable_interval = False 

    if next_overall_session_info['iso']:
        days, h, m, s, overall_past, target_time_utc_str_overall = calculate_countdown(next_overall_session_info['iso'])
        session_name_display = f"{next_overall_session_info['event']} - {next_overall_session_info['name']}"
        if not overall_past:
            session_countdown_str = f"{days}d {h:02}h {m:02}m {s:02}s"
            session_datetime_local_str = format_session_time_local(next_overall_session_info['iso'], user_timezone)
            # final_disable_interval remains False
        else: 
            session_countdown_str = "Event Started / Completed"
            session_datetime_local_str = format_session_time_local(next_overall_session_info['iso'], user_timezone)
    else: # No overall future session found
        final_disable_interval = True # No overall session, lean towards disabling unless a future race is found

    race_name_display = "No upcoming race found"
    race_countdown_str = "All races complete"
    race_datetime_local_str = " "
    if next_race_session_info['iso']:
        days, h, m, s, race_past, target_time_utc_str_race = calculate_countdown(next_race_session_info['iso'])
        race_name_display = f"{next_race_session_info['event']} - Race"
        if not race_past:
            race_countdown_str = f"{days}d {h:02}h {m:02}m {s:02}s"
            race_datetime_local_str = format_session_time_local(next_race_session_info['iso'], user_timezone)
            final_disable_interval = False # Future race found, keep interval active
        else:
            race_countdown_str = "Race Started / Completed"
            race_datetime_local_str = format_session_time_local(next_race_session_info['iso'], user_timezone)
            # If overall session was also past (or none), and race is past, then disable.
            # If overall session was none, final_disable_interval is already True here.
            # If overall session was past, final_disable_interval is still False from above.
            # This needs to be: disable only if BOTH are past or none.
            if next_overall_session_info['iso'] is None or overall_past: # If no future overall, and race is also past/none
                 final_disable_interval = True


    # If both are truly none, then disable.
    if next_overall_session_info['iso'] is None and next_race_session_info['iso'] is None:
        logger.info("No future sessions or races were identified by countdown logic. Disabling interval.")
        final_disable_interval = True

    logger.debug(f"Countdown outputs: SessionName='{session_name_display}', SessionText='{session_countdown_str}', RaceName='{race_name_display}', RaceText='{race_countdown_str}', IntervalDisabled={final_disable_interval}") # Log J
            
    return (session_name_display, session_countdown_str, session_datetime_local_str,
            race_name_display, race_countdown_str, race_datetime_local_str,
            final_disable_interval)
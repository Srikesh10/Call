"""
Calendar tool helper functions for the agent
"""
from datetime import datetime, timedelta
import re

async def parse_datetime(date_str: str, time_str: str) -> datetime:
    """Parse natural language date/time into ISO format"""
    # datetime and timedelta are already imported at module level
    
    # Handle relative dates
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    if date_str.lower() == 'today':
        target_date = today
    elif date_str.lower() == 'tomorrow':
        target_date = today + timedelta(days=1)
    elif date_str.lower().startswith('in ') and 'day' in date_str.lower():
        # "in 2 days", "in 3 days"
        days = int(re.search(r'\d+', date_str).group())
        target_date = today + timedelta(days=days)
    else:
        # Try to parse as YYYY-MM-DD
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
        except:
            # Fallback to tomorrow
            target_date = today + timedelta(days=1)
    
    # Parse time
    time_str = time_str.strip().upper()
    
    # Handle 12-hour format
    if 'PM' in time_str or 'AM' in time_str:
        time_str = time_str.replace('.', '').replace(' ', '')
        try:
            time_obj = datetime.strptime(time_str, '%I:%M%p')
        except:
            try:
                time_obj = datetime.strptime(time_str, '%I%p')
            except:
                time_obj = datetime.strptime('15:00', '%H:%M')  # Default 3 PM
    else:
        # 24-hour format
        try:
            time_obj = datetime.strptime(time_str, '%H:%M')
        except:
            time_obj = datetime.strptime('15:00', '%H:%M')  # Default 3 PM
    
    # Combine date and time
    start_datetime = target_date.replace(
        hour=time_obj.hour,
        minute=time_obj.minute
    )
    
    return start_datetime

def format_datetime_iso(dt: datetime) -> str:
    """Format datetime to ISO 8601 with timezone"""
    # Add IST timezone offset
    return dt.strftime('%Y-%m-%dT%H:%M:%S') + '+05:30'

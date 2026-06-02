"""Time range parsing utilities for converting LLM time expressions to date ranges."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def parse_time_range(
    time_range: dict[str, Any] | None,
    now: datetime | None = None,
    tz_name: str = "Asia/Jakarta",
) -> tuple[datetime, datetime]:
    """
    Parse LLM time_range output into (from_dt, to_dt) tuple.
    
    Args:
        time_range: The time_range dict from LLM proposal
        now: Current datetime (defaults to now in UTC)
        tz_name: Timezone name for local time calculations
        
    Returns:
        Tuple of (from_datetime, to_datetime) in UTC
        
    Raises:
        ValueError: If time_range format is invalid
    """
    if not now:
        now = datetime.now(timezone.utc)
    
    if not time_range:
        # Default: last 30 days
        from_dt = now - timedelta(days=30)
        to_dt = now
        return from_dt, to_dt
    
    range_type = time_range.get("type")
    value = time_range.get("value")
    
    if range_type == "hours":
        # "last N hours"
        try:
            hours = int(value or 24)
        except (TypeError, ValueError):
            hours = 24
        from_dt = now - timedelta(hours=hours)
        to_dt = now
        return from_dt, to_dt
    
    if range_type == "today":
        # Current day in user's timezone
        local_now = now.astimezone(timezone.utc)
        start_of_day = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1) - timedelta(microseconds=1)
        return start_of_day, end_of_day
    
    if range_type == "yesterday":
        # Previous day in user's timezone
        local_now = now.astimezone(timezone.utc)
        start_of_yesterday = (local_now - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_of_yesterday = start_of_yesterday + timedelta(days=1) - timedelta(microseconds=1)
        return start_of_yesterday, end_of_yesterday
    
    if range_type == "specific_date":
        # Specific date like "2026-06-01"
        try:
            date_str = str(value or "")
            date_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            start_of_day = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = start_of_day + timedelta(days=1) - timedelta(microseconds=1)
            return start_of_day, end_of_day
        except (ValueError, AttributeError):
            # Fallback to today
            return parse_time_range({"type": "today"}, now, tz_name)
    
    if range_type == "day_name":
        # "last friday", "last monday", etc.
        day_name = str(value or "").lower()
        day_map = {
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5,
            "sunday": 6,
        }
        target_weekday = day_map.get(day_name)
        if target_weekday is None:
            # Invalid day name, default to today
            return parse_time_range({"type": "today"}, now, tz_name)
        
        local_now = now.astimezone(timezone.utc)
        current_weekday = local_now.weekday()
        
        # Calculate days back to target weekday
        if current_weekday >= target_weekday:
            days_back = current_weekday - target_weekday
        else:
            days_back = 7 - (target_weekday - current_weekday)
        
        # If days_back is 0, it means today is that day, go back 7 days
        if days_back == 0:
            days_back = 7
        
        target_date = local_now - timedelta(days=days_back)
        start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1) - timedelta(microseconds=1)
        return start_of_day, end_of_day
    
    if range_type == "week":
        # "this week" or "last week"
        week_value = str(value or "this").lower()
        local_now = now.astimezone(timezone.utc)
        
        # Find start of current week (Monday)
        current_weekday = local_now.weekday()
        start_of_week = (local_now - timedelta(days=current_weekday)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        
        if week_value == "last":
            # Last week: 7 days before current week start
            start_of_week = start_of_week - timedelta(days=7)
        
        end_of_week = start_of_week + timedelta(days=7) - timedelta(microseconds=1)
        return start_of_week, end_of_week
    
    if range_type == "date_range":
        # Explicit date range with from_date and to_date
        try:
            from_str = time_range.get("from_date", "")
            to_str = time_range.get("to_date", "")
            
            from_dt = datetime.fromisoformat(from_str.replace("Z", "+00:00"))
            to_dt = datetime.fromisoformat(to_str.replace("Z", "+00:00"))
            
            # Ensure from_dt is start of day and to_dt is end of day
            from_dt = from_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            to_dt = to_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            return from_dt, to_dt
        except (ValueError, AttributeError):
            # Fallback to last 30 days
            from_dt = now - timedelta(days=30)
            to_dt = now
            return from_dt, to_dt
    
    # Unknown type, default to last 30 days
    from_dt = now - timedelta(days=30)
    to_dt = now
    return from_dt, to_dt


def format_date_range(from_dt: datetime, to_dt: datetime) -> str:
    """Format date range for display."""
    from_str = from_dt.strftime("%Y-%m-%d %H:%M")
    to_str = to_dt.strftime("%Y-%m-%d %H:%M")
    return f"{from_str} to {to_str}"

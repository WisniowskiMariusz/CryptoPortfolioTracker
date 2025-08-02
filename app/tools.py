from datetime import datetime, timezone, timedelta

from fastapi import HTTPException


def chunked(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    iterable = list(iterable)
    for i in range(0, len(iterable), n):
        yield iterable[i : i + n]


def datetime_from_str(date_str: str | None) -> datetime | None:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=(f"Invalid date format: {date_str}. " "Use YYYY-MM-DD."),
        )


def timestamp_from_str(date_str: str | None) -> int | None:
    if not date_str:
        return None
    return int(datetime_from_str(date_str=date_str).timestamp() * 1000)


def datetime_from_miliseconds(miliseconds: int | None) -> datetime | None:
    if not miliseconds:
        return None
    if miliseconds < 0:
        return datetime.min.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(miliseconds / 1000, tz=timezone.utc)


def convert_time_to_ms(time: str) -> int:
    print(f"Provided time string: {time}")
    if not isinstance(time, datetime):
        dt = datetime.strptime(time, "%Y-%m-%d %H:%M")
    else:
        dt = time
    dt = dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    print(f"Provided time: {dt}")
    print(f"Current time: {now}")
    if dt > now:
        dt = now  # Think about raising error here
    print(f"Used time: {dt}")
    timestamp_ms = int(dt.timestamp() * 1000)
    return timestamp_ms


def add_n_days_to_date(
    days: int, date: datetime = datetime.now(timezone.utc)
) -> datetime:
    """
    Move the date forward or back (if days number is negative)
    by a specified number of days. Default is now.
    """
    return date + timedelta(days=days)

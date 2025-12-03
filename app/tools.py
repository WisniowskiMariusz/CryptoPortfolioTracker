from decimal import Decimal
import hashlib
import json
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


def generate_hash(input_dict: dict[str, str]) -> str:
    """Generate a deterministic SHA-256 hash of the input dictionary."""
    sha256_hash = hashlib.sha256()
    serialized = json.dumps(input_dict, sort_keys=True, separators=(",", ":"))
    sha256_hash.update(serialized.encode("utf-8"))
    return sha256_hash.hexdigest()


def string(x: Decimal) -> str:
    """Convert Decimal to a clean string
    without trailing zeros or scientific notation."""
    if x.is_zero():
        return "0"
    s = format(x.normalize(), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s

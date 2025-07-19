from datetime import datetime

from fastapi import HTTPException


def chunked(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    iterable = list(iterable)
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]

def datetime_from_str(date_str:str | None) -> datetime | None:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {date_str}. Use YYYY-MM-DD.")


def timestamp_from_str(date_str: str | None) -> int | None:
    if not date_str:
        return None
    return int(datetime_from_str(date_str=date_str).timestamp() * 1000)   


def datetime_from_miliseconds(miliseconds: int | None) -> datetime | None:
    if not miliseconds:
        return None
    return datetime.fromtimestamp(miliseconds / 1000)

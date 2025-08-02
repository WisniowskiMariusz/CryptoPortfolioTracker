import pytest
from app import tools
from fastapi import HTTPException
from datetime import datetime, timedelta, timezone


def test_chunked_regular():
    data = [1, 2, 3, 4, 5]
    chunks = list(tools.chunked(data, 2))
    assert chunks == [[1, 2], [3, 4], [5]]


def test_chunked_exact():
    data = [1, 2, 3, 4]
    chunks = list(tools.chunked(data, 2))
    assert chunks == [[1, 2], [3, 4]]


def test_chunked_empty():
    data = []
    chunks = list(tools.chunked(data, 3))
    assert chunks == []


def test_to_datetime_valid():
    dt = tools.datetime_from_str("2024-06-25")
    assert dt.year == 2024 and dt.month == 6 and dt.day == 25


def test_to_datetime_none():
    assert tools.datetime_from_str(None) is None


def test_to_datetime_invalid():
    with pytest.raises(HTTPException) as exc:
        tools.datetime_from_str("25-06-2024")
    assert exc.value.status_code == 400
    assert "Invalid date format" in exc.value.detail


def test_to_timestamp_valid():
    ts = tools.timestamp_from_str("2024-06-25")
    assert isinstance(ts, int)
    # Check that the timestamp is for 2024-06-25 00:00:00
    assert ts == int(tools.datetime_from_str("2024-06-25").timestamp() * 1000)


def test_to_timestamp_none():
    assert tools.timestamp_from_str(None) is None


def test_convert_time_to_ms_from_string():
    time_str = "2024-01-01 12:00"
    ms = tools.convert_time_to_ms(time_str)
    expected = int(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert ms == expected


def test_convert_time_to_ms_from_datetime():
    dt = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    ms = tools.convert_time_to_ms(dt)
    expected = int(dt.timestamp() * 1000)
    assert ms == expected


def test_convert_time_to_ms_future_date():
    future_time = (datetime.now(timezone.utc) + timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M"
    )
    ms = tools.convert_time_to_ms(future_time)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    # Should not be greater than now
    assert ms <= now_ms


def test_convert_time_to_ms_now():
    now = datetime.now(timezone.utc)
    ms = tools.convert_time_to_ms(now)
    expected = int(now.timestamp() * 1000)
    assert ms == expected


def test_convert_time_to_ms_future_string_is_capped():
    # Should cap to now if future date is given as string
    future_time = (datetime.now(timezone.utc) + timedelta(days=10)).strftime(
        "%Y-%m-%d %H:%M"
    )
    ms = tools.convert_time_to_ms(future_time)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    assert ms <= now_ms


def test_datetime_from_milliseconds_valid():
    ms = 1609459200000  # 2021-01-01 00:00:00 UTC
    expected = datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc)
    result = tools.datetime_from_miliseconds(ms)
    assert result == expected


def test_datetime_from_milliseconds_zero():
    assert tools.datetime_from_miliseconds(0) is None


def test_datetime_from_milliseconds_none():
    assert tools.datetime_from_miliseconds(None) is None


def test_datetime_from_milliseconds_negative():
    ms = -62135596800000  # year 0001-01-01 00:00:00
    expected = datetime(1, 1, 1, 0, 0, tzinfo=timezone.utc)
    result = tools.datetime_from_miliseconds(ms)
    assert result == expected

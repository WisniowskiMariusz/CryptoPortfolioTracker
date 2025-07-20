import pytest
from app import tools
from fastapi import HTTPException


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

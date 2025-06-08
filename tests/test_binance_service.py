import pytest
from datetime import datetime, timezone
from app import binance_service as bs
from unittest.mock import patch, MagicMock


def test_convert_time_to_ms_from_string():
    time_str = "2024-01-01 12:00"
    ms = bs.convert_time_to_ms(time_str)
    expected = int(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert ms == expected


def test_convert_time_to_ms_from_datetime():
    dt = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    ms = bs.convert_time_to_ms(dt)
    expected = int(dt.timestamp() * 1000)
    assert ms == expected


@patch("app.binance_service.requests.get")
def test_get_klines_success(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [[1609459200000, "30000.0", "31000.0", "29500.0", "30500.0", "1000"]]
    mock_get.return_value = mock_response

    result = bs.get_klines("BTCUSDT", "1h")
    assert isinstance(result, list)
    assert result[0][1] == "30000.0"
    mock_get.assert_called_once()


@patch("app.binance_service.requests.get")
def test_get_klines_empty_result(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_get.return_value = mock_response

    result = bs.get_klines("BTCUSDT", "1h")
    assert result == []


@patch("app.binance_service.requests.get")
def test_get_klines_raises_on_http_error(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = Exception("Server error")
    mock_get.return_value = mock_response

    with pytest.raises(Exception):
        bs.get_klines("BTCUSDT", "1h")


def test_parse_klines_valid():
    data = [[1609459200000, "30000.0", "", "", "", ""]]
    parsed = bs.parse_klines(data, "BTCUSDT", "1h")
    assert parsed[0]["symbol"] == "BTCUSDT"
    assert parsed[0]["interval"] == "1h"
    assert parsed[0]["price"] == 30000.0
    assert "time" in parsed[0]


def test_parse_klines_empty():
    parsed = bs.parse_klines([], "BTCUSDT", "1h")
    assert parsed == []

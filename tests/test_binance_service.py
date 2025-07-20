import pytest
from datetime import datetime, timezone, timedelta
from app import binance_service as bs
from unittest.mock import patch, MagicMock
from requests.exceptions import (
    HTTPError,
    ConnectionError,
    Timeout,
    RequestException,
)


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


def test_convert_time_to_ms_future_date():
    future_time = (datetime.now(timezone.utc) + timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M"
    )
    ms = bs.convert_time_to_ms(future_time)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    # Should not be greater than now
    assert ms <= now_ms


def test_convert_time_to_ms_now():
    now = datetime.now(timezone.utc)
    ms = bs.convert_time_to_ms(now)
    expected = int(now.timestamp() * 1000)
    assert ms == expected


def test_convert_time_to_ms_future_string_is_capped():
    # Should cap to now if future date is given as string
    future_time = (datetime.now(timezone.utc) + timedelta(days=10)).strftime(
        "%Y-%m-%d %H:%M"
    )
    ms = bs.convert_time_to_ms(future_time)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    assert ms <= now_ms


@patch("app.binance_service.requests.get")
def test_get_klines_success(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        [1609459200000, "30000.0", "31000.0", "29500.0", "30500.0", "1000"]
    ]
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
    mock_response.raise_for_status.side_effect = HTTPError("http error")
    mock_get.return_value = mock_response
    with pytest.raises(HTTPError):
        bs.get_klines("BTCUSDT", "1h")


@patch("app.binance_service.requests.get")
@patch("time.sleep", return_value=None)
def test_get_klines_raises_connection_error(mock_sleep, mock_get):
    mock_get.side_effect = ConnectionError("conn error")
    with pytest.raises(RuntimeError):
        bs.get_klines("BTCUSDT", "1h")


@patch("app.binance_service.requests.get")
@patch("time.sleep", return_value=None)
def test_get_klines_raises_timeout(mock_sleep, mock_get):
    mock_get.side_effect = Timeout("timeout")
    with pytest.raises(RuntimeError):
        bs.get_klines("BTCUSDT", "1h")


@patch("app.binance_service.requests.get")
def test_get_klines_raises_request_exception(mock_get):
    mock_get.side_effect = RequestException("req exc")
    with pytest.raises(RequestException):
        bs.get_klines("BTCUSDT", "1h")


@patch("app.binance_service.requests.get")
@patch("time.sleep", return_value=None)
def test_get_klines_raises_runtime_error_on_max_attempts(mock_sleep, mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.raise_for_status.side_effect = None
    mock_response.json.return_value = []
    mock_get.return_value = mock_response
    with pytest.raises(RuntimeError):
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


def test_parse_klines_handles_empty_entry():
    # Should not fail if entry is empty or malformed
    data = [[]]
    result = bs.parse_klines(data, "BTCUSDT", "1h")
    assert isinstance(result, list)


def test_parse_klines_handles_non_numeric_price():
    # Should raise ValueError if price is not a float
    data = [[1609459200000, "not_a_number"]]
    with pytest.raises(ValueError):
        bs.parse_klines(data, "BTCUSDT", "1h")


@patch("app.binance_service.Spot")
def test_get_account_info_success(mock_spot_cls):
    mock_client = MagicMock()
    mock_client.account.return_value = {"balances": []}
    mock_spot_cls.return_value = mock_client
    result = bs.get_account_info()
    assert "balances" in result


@patch("app.binance_service.Spot")
def test_get_account_info_no_credentials(mock_spot_cls, monkeypatch):
    monkeypatch.setattr(bs, "BINANCE_API_KEY", None)
    monkeypatch.setattr(bs, "BINANCE_API_SECRET", None)
    with pytest.raises(Exception, match="Binance API credentials not set"):
        bs.get_account_info()


@patch("app.binance_service.requests.get")
def test_get_klines_rate_limit_retry(mock_get):
    # First call returns 429, second call returns 200
    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    mock_response_429.raise_for_status.side_effect = None

    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = [
        [1609459200000, "30000.0", "31000.0", "29500.0", "30500.0", "1000"]
    ]
    mock_response_200.raise_for_status.side_effect = None

    mock_get.side_effect = [mock_response_429, mock_response_200]

    result = bs.get_klines("BTCUSDT", "1h")
    assert isinstance(result, list)
    assert result[0][1] == "30000.0"
    assert mock_get.call_count == 2


def test_parse_klines_handles_multiple_entries():
    data = [
        [1609459200000, "30000.0", "", "", "", ""],
        [1609462800000, "31000.0", "", "", "", ""],
    ]
    parsed = bs.parse_klines(data, "BTCUSDT", "1h")
    assert len(parsed) == 2
    assert parsed[1]["price"] == 31000.0


@patch("app.binance_service.Spot")
def test_fetch_trades_returns_trades(mock_spot_cls):
    mock_client = MagicMock()
    batch1 = [
        {
            "id": 1,
            "symbol": "BTCUSDT",
            "orderId": 100,
            "price": "100.0",
            "qty": "0.1",
            "quoteQty": "10.0",
            "commission": "0.01",
            "commissionAsset": "BNB",
            "time": 1609459200000,
            "isBuyer": True,
            "isMaker": False,
            "isBestMatch": True,
        }
    ] * 2  # limit=2
    batch2 = [
        {
            "id": 2,
            "symbol": "BTCUSDT",
            "orderId": 101,
            "price": "200.0",
            "qty": "0.2",
            "quoteQty": "20.0",
            "commission": "0.02",
            "commissionAsset": "BNB",
            "time": 1609462800000,
            "isBuyer": False,
            "isMaker": True,
            "isBestMatch": False,
        }
    ] * 1
    mock_client.get_my_trades.side_effect = [batch1, batch2, []]
    mock_spot_cls.return_value = mock_client

    trades = bs.fetch_trades("BTCUSDT", limit=2)
    assert len(trades) == 3
    assert trades[0]["price"] == 100.0
    assert trades[2]["price"] == 200.0


@patch("app.binance_service.Spot")
def test_fetch_trades_returns_empty(mock_spot_cls):
    mock_client = MagicMock()
    mock_client.get_my_trades.return_value = []
    mock_spot_cls.return_value = mock_client
    trades = bs.fetch_trades("BTCUSDT")
    assert trades == []


@patch("app.binance_service.Spot")
def test_fetch_trades_with_start_and_end_time(mock_spot_cls):
    mock_client = MagicMock()
    batch = [
        {
            "id": 1,
            "symbol": "BTCUSDT",
            "orderId": 100,
            "price": "100.0",
            "qty": "0.1",
            "quoteQty": "10.0",
            "commission": "0.01",
            "commissionAsset": "BNB",
            "time": 1609459200000,
            "isBuyer": True,
            "isMaker": False,
            "isBestMatch": True,
        }
    ]
    mock_client.get_my_trades.side_effect = [batch, []]
    mock_spot_cls.return_value = mock_client
    trades = bs.fetch_trades("BTCUSDT", start_time=123, end_time=456)
    assert len(trades) == 1
    assert trades[0]["symbol"] == "BTCUSDT"


@patch("app.binance_service.Spot")
def test_fetch_trades_handles_non_bool_flags(mock_spot_cls):
    mock_client = MagicMock()
    batch = [
        {
            "id": 1,
            "symbol": "BTCUSDT",
            "orderId": 100,
            "price": "100.0",
            "qty": "0.1",
            "quoteQty": "10.0",
            "commission": "0.01",
            "commissionAsset": "BNB",
            "time": 1609459200000,
            "isBuyer": "1",
            "isMaker": "0",
            "isBestMatch": "1",
        }
    ]
    mock_client.get_my_trades.side_effect = [batch, []]
    mock_spot_cls.return_value = mock_client
    trades = bs.fetch_trades("BTCUSDT")
    assert trades[0]["isBuyer"] == 1
    assert trades[0]["isMaker"] == 0
    assert trades[0]["isBestMatch"] == 1


@patch("app.binance_service.Spot")
def test_fetch_trades_with_partial_batch(mock_spot_cls):
    mock_client = MagicMock()
    batch = [
        {
            "id": 1,
            "symbol": "BTCUSDT",
            "orderId": 100,
            "price": "100.0",
            "qty": "0.1",
            "quoteQty": "10.0",
            "commission": "0.01",
            "commissionAsset": "BNB",
            "time": 1609459200000,
            "isBuyer": True,
            "isMaker": False,
            "isBestMatch": True,
        }
    ]
    mock_client.get_my_trades.side_effect = [batch, []]
    mock_spot_cls.return_value = mock_client
    trades = bs.fetch_trades("BTCUSDT")
    assert len(trades) == 1


@patch("app.binance_service.Spot")
def test_fetch_trades_with_multiple_batches(mock_spot_cls):
    mock_client = MagicMock()
    batch1 = [
        {
            "id": 1,
            "symbol": "BTCUSDT",
            "orderId": 100,
            "price": "100.0",
            "qty": "0.1",
            "quoteQty": "10.0",
            "commission": "0.01",
            "commissionAsset": "BNB",
            "time": 1609459200000,
            "isBuyer": True,
            "isMaker": False,
            "isBestMatch": True,
        }
    ] * 1000
    batch2 = [
        {
            "id": 2,
            "symbol": "BTCUSDT",
            "orderId": 101,
            "price": "200.0",
            "qty": "0.2",
            "quoteQty": "20.0",
            "commission": "0.02",
            "commissionAsset": "BNB",
            "time": 1609462800000,
            "isBuyer": False,
            "isMaker": True,
            "isBestMatch": False,
        }
    ] * 500
    mock_client.get_my_trades.side_effect = [batch1, batch2, []]
    mock_spot_cls.return_value = mock_client
    trades = bs.fetch_trades("BTCUSDT")
    assert len(trades) == 1500
    assert trades[0]["price"] == 100.0
    assert trades[1000]["price"] == 200.0


@patch("app.binance_service.Spot")
def test_fetch_trades_binance_api_exception(mock_spot_cls):
    from binance.error import ClientError

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.text = "error"
    mock_client.get_my_trades.side_effect = ClientError(mock_response, 400, "error", {})
    mock_spot_cls.return_value = mock_client
    with pytest.raises(ClientError):
        bs.fetch_trades("BTCUSDT")


def test_fetch_prices_stream_empty(monkeypatch):
    def fake_get_klines(*args, **kwargs):
        return []

    monkeypatch.setattr(bs, "get_klines", fake_get_klines)
    result = list(bs.fetch_prices_stream("BTCUSDT", "1h"))
    assert result == []


def test_fetch_prices_stream_max_requests(monkeypatch):
    call_count = {"n": 0}

    def fake_get_klines(*args, **kwargs):
        call_count["n"] += 1
        return [[1609459200000, "30000.0", "", "", "", ""]]

    monkeypatch.setattr(bs, "get_klines", fake_get_klines)
    gen = bs.fetch_prices_stream("BTCUSDT", "1h", batch_size=1, max_requests=2)
    batches = list(gen)
    assert len(batches) == 2
    assert call_count["n"] == 2


def test_fetch_prices_stream_breaks_on_short_batch(monkeypatch):
    def fake_get_klines(*args, **kwargs):
        return [[1609459200000, "30000.0", "", "", "", ""]]

    monkeypatch.setattr(bs, "get_klines", fake_get_klines)
    gen = bs.fetch_prices_stream("BTCUSDT", "1h", batch_size=10)
    batches = list(gen)
    assert len(batches) == 1

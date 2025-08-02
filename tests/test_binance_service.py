import pytest
from binance.error import ClientError
from unittest.mock import patch, MagicMock
from requests.exceptions import (
    HTTPError,
    ConnectionError,
    Timeout,
    RequestException,
)
from app.binance_service import BinanceService
from app import tools


@patch("app.binance_service.requests.get")
def test_get_klines_success(mock_get, fake_binance_service):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        [1609459200000, "30000.0", "31000.0", "29500.0", "30500.0", "1000"]
    ]
    mock_get.return_value = mock_response

    result = fake_binance_service.get_klines("BTCUSDT", "1h")
    assert isinstance(result, list)
    assert result[0][1] == "30000.0"
    mock_get.assert_called_once()


@patch("app.binance_service.requests.get")
def test_get_klines_success_with_timing(mock_get, fake_binance_service):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        [1609459200000, "30000.0", "31000.0", "29500.0", "30500.0", "1000"]
    ]
    mock_get.return_value = mock_response

    result = fake_binance_service.get_klines(
        symbol="BTCUSDT",
        interval="1h",
        start_time="2021-01-01 00:00",
        end_time="2021-01-02 00:00",
    )
    assert isinstance(result, list)
    assert result[0][1] == "30000.0"
    mock_get.assert_called_once()


@patch("app.binance_service.requests.get")
def test_get_klines_raises_value_error(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = ValueError("Invalid JSON")

    mock_get.return_value = mock_response

    service = BinanceService(keyring_system_name="fake keyring system name")

    with pytest.raises(ValueError, match="Invalid JSON"):
        service.get_klines(symbol="BTCUSDT", interval="1h")


@patch("app.binance_service.requests.get")
def test_get_klines_empty_result(mock_get, fake_binance_service):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_get.return_value = mock_response

    result = fake_binance_service.get_klines("BTCUSDT", "1h")
    assert result == []


@patch("app.binance_service.requests.get")
def test_get_klines_raises_on_http_error(mock_get, fake_binance_service):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = HTTPError("http error")
    mock_get.return_value = mock_response
    with pytest.raises(HTTPError):
        fake_binance_service.get_klines("BTCUSDT", "1h")


@patch("app.binance_service.requests.get")
@patch("time.sleep", return_value=None)
def test_get_klines_raises_connection_error(mock_sleep, mock_get, fake_binance_service):
    mock_get.side_effect = ConnectionError("conn error")
    with pytest.raises(RuntimeError):
        fake_binance_service.get_klines("BTCUSDT", "1h")


@patch("app.binance_service.requests.get")
@patch("time.sleep", return_value=None)
def test_get_klines_raises_timeout(mock_sleep, mock_get, fake_binance_service):
    mock_get.side_effect = Timeout("timeout")
    with pytest.raises(RuntimeError):
        fake_binance_service.get_klines("BTCUSDT", "1h")


@patch("app.binance_service.requests.get")
def test_get_klines_raises_request_exception(mock_get, fake_binance_service):
    mock_get.side_effect = RequestException("req exc")
    with pytest.raises(RequestException):
        fake_binance_service.get_klines("BTCUSDT", "1h")


@patch("app.binance_service.requests.get")
@patch("time.sleep", return_value=None)
def test_get_klines_raises_runtime_error_on_max_attempts(
    mock_sleep, mock_get, fake_binance_service
):
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.raise_for_status.side_effect = None
    mock_response.json.return_value = []
    mock_get.return_value = mock_response
    with pytest.raises(RuntimeError):
        fake_binance_service.get_klines("BTCUSDT", "1h")


def test_parse_klines_valid(fake_binance_service):
    data = [[1609459200000, "30000.0", "", "", "", ""]]
    parsed = fake_binance_service.parse_klines(data, "BTCUSDT", "1h")
    assert parsed[0]["symbol"] == "BTCUSDT"
    assert parsed[0]["interval"] == "1h"
    assert parsed[0]["price"] == 30000.0
    assert "time" in parsed[0]


def test_parse_klines_empty(fake_binance_service):
    parsed = fake_binance_service.parse_klines([], "BTCUSDT", "1h")
    assert parsed == []


def test_parse_klines_handles_empty_entry(fake_binance_service):
    # Should not fail if entry is empty or malformed
    data = [[]]
    result = fake_binance_service.parse_klines(data, "BTCUSDT", "1h")
    assert isinstance(result, list)


def test_parse_klines_handles_non_numeric_price(fake_binance_service):
    # Should raise ValueError if price is not a float
    data = [[1609459200000, "not_a_number"]]
    with pytest.raises(ValueError):
        fake_binance_service.parse_klines(data, "BTCUSDT", "1h")


@patch("app.binance_service.requests.get")
def test_get_klines_rate_limit_retry(mock_get, fake_binance_service):
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

    result = fake_binance_service.get_klines("BTCUSDT", "1h")
    assert isinstance(result, list)
    assert result[0][1] == "30000.0"
    assert mock_get.call_count == 2


def test_parse_klines_handles_multiple_entries(fake_binance_service):
    data = [
        [1609459200000, "30000.0", "", "", "", ""],
        [1609462800000, "31000.0", "", "", "", ""],
    ]
    parsed = fake_binance_service.parse_klines(data, "BTCUSDT", "1h")
    assert len(parsed) == 2
    assert parsed[1]["price"] == 31000.0


def test_fetch_trades_returns_trades(mocked_binance_service):
    fake_binance_service, mock_client = mocked_binance_service
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
    mock_client.my_trades.side_effect = [batch1, batch2, []]
    trades = fake_binance_service.fetch_trades("BTCUSDT", limit=2)
    print(f"Fetched trades: {trades}")
    assert len(trades) == 3
    assert trades[0]["price"] == 100.0
    assert trades[2]["price"] == 200.0


def test_fetch_trades_returns_empty(mocked_binance_service):
    fake_binance_service, mock_client = mocked_binance_service
    mock_client.my_trades.return_value = []
    trades = fake_binance_service.fetch_trades("BTCUSDT")
    assert trades == []


def test_fetch_trades_with_start_and_end_time(mocked_binance_service):
    fake_binance_service, mock_client = mocked_binance_service
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
    mock_client.my_trades.side_effect = [batch, []]
    trades = fake_binance_service.fetch_trades("BTCUSDT", start_time=123, end_time=456)
    assert len(trades) == 1
    assert trades[0]["symbol"] == "BTCUSDT"


def test_fetch_trades_handles_non_bool_flags(mocked_binance_service):
    fake_binance_service, mock_client = mocked_binance_service
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
    mock_client.my_trades.side_effect = [batch, []]
    trades = fake_binance_service.fetch_trades("BTCUSDT")
    assert trades[0]["isBuyer"] == 1
    assert trades[0]["isMaker"] == 0
    assert trades[0]["isBestMatch"] == 1


def test_fetch_trades_with_partial_batch(mocked_binance_service):
    fake_binance_service, mock_client = mocked_binance_service
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
    mock_client.my_trades.side_effect = [batch, []]
    trades = fake_binance_service.fetch_trades("BTCUSDT")
    assert len(trades) == 1


def test_fetch_trades_with_multiple_batches(mocked_binance_service):
    fake_binance_service, mock_client = mocked_binance_service
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
    mock_client.my_trades.side_effect = [batch1, batch2, []]
    trades = fake_binance_service.fetch_trades("BTCUSDT")
    assert len(trades) == 1500
    assert trades[0]["price"] == 100.0
    assert trades[1000]["price"] == 200.0


def test_fetch_trades_binance_api_exception(mocked_binance_service):
    fake_binance_service, mock_client = mocked_binance_service
    mock_response = MagicMock()
    mock_response.text = "error"
    mock_client.my_trades.side_effect = ClientError(mock_response, 400, "error", {})
    with pytest.raises(ClientError):
        fake_binance_service.fetch_trades("BTCUSDT")


def test_fetch_prices_calls_dependencies_correctly(fake_binance_service):
    symbol = "BTCUSDT"
    interval = "1h"
    start_time = "2021-01-01 00:00:00"
    end_time = "2021-01-02 00:00:00"
    limit = 100

    # Mock internal dependencies
    fake_klines = [{"open": 1}, {"open": 2}]
    fake_parsed = [{"price": 100}, {"price": 110}]

    fake_binance_service.get_klines = MagicMock(return_value=fake_klines)
    fake_binance_service.parse_klines = MagicMock(return_value=fake_parsed)

    result = fake_binance_service.fetch_prices(
        symbol, interval, start_time, end_time, limit
    )

    fake_binance_service.get_klines.assert_called_once_with(
        symbol, interval, start_time, end_time, limit
    )
    fake_binance_service.parse_klines.assert_called_once_with(
        fake_klines, symbol, interval
    )
    assert result == fake_parsed


def test_fetch_prices_stream_empty(monkeypatch, fake_binance_service):
    def fake_get_klines(*args, **kwargs):
        return []

    monkeypatch.setattr(fake_binance_service, "get_klines", fake_get_klines)
    result = list(fake_binance_service.fetch_prices_stream("BTCUSDT", "1h"))
    assert result == []


def test_fetch_prices_stream_max_requests(monkeypatch, fake_binance_service):
    call_count = {"n": 0}

    def fake_get_klines(*args, **kwargs):
        call_count["n"] += 1
        return [[1609459200000, "30000.0", "", "", "", ""]]

    monkeypatch.setattr(fake_binance_service, "get_klines", fake_get_klines)
    gen = fake_binance_service.fetch_prices_stream(
        "BTCUSDT", "1h", batch_size=1, max_requests=2
    )
    batches = list(gen)
    assert len(batches) == 2
    assert call_count["n"] == 2


def test_fetch_prices_stream_breaks_on_short_batch(monkeypatch, fake_binance_service):
    def fake_get_klines(*args, **kwargs):
        return [[1609459200000, "30000.0", "", "", "", ""]]

    monkeypatch.setattr(fake_binance_service, "get_klines", fake_get_klines)
    gen = fake_binance_service.fetch_prices_stream("BTCUSDT", "1h", batch_size=10)
    batches = list(gen)
    assert len(batches) == 1


def test_get_dust_log_calls_client(fake_binance_service):
    fake_binance_service.client.dust_log = MagicMock(return_value={"dummy": True})
    result = fake_binance_service.get_dust_log()
    assert result == {"dummy": True}
    fake_binance_service.client.dust_log.assert_called_once()


def test_get_account_info(fake_binance_service):
    fake_binance_service.client.account = MagicMock(return_value={"balances": []})
    result = fake_binance_service.get_account_info()
    assert result == {"balances": []}
    fake_binance_service.client.account.assert_called_once()


def test_get_deposit_history(fake_binance_service):
    fake_binance_service.client.deposit_history = MagicMock(
        return_value=[{"asset": "BTC"}]
    )
    result = fake_binance_service.get_deposit_history(
        asset="BTC", start_time=1, end_time=2
    )
    assert result == [{"asset": "BTC"}]
    fake_binance_service.client.deposit_history.assert_called_once_with(
        asset="BTC", startTime=1, endTime=2
    )


def test_get_withdraw_history(fake_binance_service):
    fake_binance_service.client.withdraw_history = MagicMock(
        return_value=[{"asset": "ETH"}]
    )
    result = fake_binance_service.get_withdraw_history(
        asset="ETH", start_time=1, end_time=2
    )
    assert result == [{"asset": "ETH"}]
    fake_binance_service.client.withdraw_history.assert_called_once_with(
        asset="ETH", startTime=1, endTime=2
    )


def test_get_dust_log(fake_binance_service):
    fake_binance_service.client.dust_log = MagicMock(return_value={"total": 3})
    result = fake_binance_service.get_dust_log()
    assert result == {"total": 3}
    fake_binance_service.client.dust_log.assert_called_once()


def test_get_lending_interest_history(fake_binance_service):
    fake_binance_service.client.get_flexible_rewards_history = MagicMock(
        return_value=[{"interest": "1.0"}]
    )
    result = fake_binance_service.get_lending_interest_history(
        lending_type="DAILY", asset="USDT", start_time=1, end_time=2, limit=1000
    )
    assert result == [{"interest": "1.0"}]
    fake_binance_service.client.get_flexible_rewards_history.assert_called_once_with(
        lendingType="DAILY", asset="USDT", startTime=1, endTime=2, limit=1000
    )


def test_get_flexible_redemption_record(fake_binance_service):
    fake_binance_service.client.get_flexible_redemption_record = MagicMock(
        return_value=[{"id": 1}]
    )
    result = fake_binance_service.get_flexible_redemption_record(
        product_id="abc",
        redeem_id="r1",
        asset="BUSD",
        start_time=1,
        end_time=2,
        current=1,
        size=100,
    )
    assert result == [{"id": 1}]
    fake_binance_service.client.get_flexible_redemption_record.assert_called_once_with(
        productId="abc",
        redeemId="r1",
        asset="BUSD",
        startTime=1,
        endTime=2,
        current=1,
        size=100,
    )


def test_get_flexible_product_position(fake_binance_service):
    fake_binance_service.client.get_flexible_product_position = MagicMock(
        return_value=[{"asset": "BNB"}]
    )
    result = fake_binance_service.get_flexible_product_position(asset="BNB")
    assert result == [{"asset": "BNB"}]
    fake_binance_service.client.get_flexible_product_position.assert_called_once_with(
        asset="BNB"
    )


def test_get_api_key_success(fake_binance_service):
    assert fake_binance_service._get_api_key() == "fake_api_key"


def test_get_api_key_failure():
    with pytest.raises(Exception, match="not found in keyring"):
        BinanceService(keyring_system_name="wrong_system_name")


def test_get_api_secret_success(fake_binance_service):
    assert fake_binance_service._get_api_secret() == "fake_api_secret"


def test_get_api_secret_failure():
    with pytest.raises(Exception, match="not found in keyring"):
        BinanceService(keyring_system_name="only_api_key_correct")


def test_get_all_deposits_multiple_pages(fake_binance_service):
    page1 = [{"id": 1}]
    page2 = [{"id": 2}]
    page3 = []  # Stop

    fake_binance_service.client.deposit_history.side_effect = [page1, page2, page3]

    result = fake_binance_service.get_all_deposits(
        asset="BTC",
        earliest_date="2024-01-01",
        latest_date="2024-07-01",  # Covers 2 pages
    )

    assert result["count"] == 2
    assert result["data"] == page1 + page2
    assert fake_binance_service.client.deposit_history.call_count >= 2


def test_get_all_deposits_multiple_pages_no_latest_date(fake_binance_service):
    page1 = [{"id": 1}]
    page2 = [{"id": 2}]
    page3 = []  # Stop

    fake_binance_service.client.deposit_history.side_effect = [page1, page2, page3]
    earliest_date: str = tools.add_n_days_to_date(days=-180).strftime("%Y-%m-%d")
    result = fake_binance_service.get_all_deposits(
        asset="BTC", earliest_date=earliest_date
    )

    assert result["count"] == 2
    assert result["data"] == page1 + page2
    assert fake_binance_service.client.deposit_history.call_count >= 2


def test_get_all_withdrawals_multiple_pages(fake_binance_service):
    page1 = [{"id": 1, "amount": 0.2}]
    page2 = [{"id": 2, "amount": 0.3}]
    fake_binance_service.client.withdraw_history.side_effect = [page1, page2, []]

    result = fake_binance_service.get_all_withdrawals(
        asset="ETH", earliest_date="2024-01-01", latest_date="2024-07-01"
    )

    assert result["count"] == 2
    assert result["data"] == page1 + page2
    assert fake_binance_service.client.withdraw_history.call_count >= 2


def test_get_all_withdrawals_multiple_pages_no_latest_date(fake_binance_service):
    page1 = [{"id": 1, "amount": 0.2}]
    page2 = [{"id": 2, "amount": 0.3}]
    fake_binance_service.client.withdraw_history.side_effect = [page1, page2, []]
    earliest_date: str = tools.add_n_days_to_date(days=-180).strftime("%Y-%m-%d")
    result = fake_binance_service.get_all_withdrawals(
        asset="ETH", earliest_date=earliest_date
    )
    assert result["count"] == 2
    assert result["data"] == page1 + page2
    assert fake_binance_service.client.withdraw_history.call_count >= 2

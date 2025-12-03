import pytest
from unittest.mock import Mock
from decimal import Decimal
from app.kanga_service import KangaService


# ---------------------------------------------------------------------------
# 1. TEST: _create_start_end_time_strings
# ---------------------------------------------------------------------------
def test_create_start_end_time_strings():
    start, end = KangaService._create_start_end_time_strings("2024-01-05")

    assert start == "2024-01-05T00:00:00.000Z"
    assert end == "2024-01-05T23:59:59.999Z"


# ---------------------------------------------------------------------------
# 2. TEST: _create_dates_list
# ---------------------------------------------------------------------------
def test_create_dates_list():
    result = KangaService._create_dates_list("2024-01-01", "2024-01-03")

    assert result == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
    ]


def test_parse_trade_from_strings(monkeypatch):
    # ---- Mock keyring responses ----
    def fake_get_password(system, key):
        return {
            "api_key": "TEST_API_KEY",
            "api_secret": "TEST_API_SECRET",
            "user": "TEST_USER",
        }.get(key)

    monkeypatch.setattr("keyring.get_password", fake_get_password)

    # ---- Create service ----
    svc = KangaService()

    string_trade = {
        "utc_time": "2024-01-01 12:00:00",
        "bought_currency": "BTC",
        "sold_currency": "USDT",
        "price": "42000",
        "bought_amount": "1",
        "sold_amount": "42000",
        "fee_currency": "BTC",
        "fee_amount": "0.001",
        "original_id": "ABC123",
        "exchange": "Kanga",
        "user": "TEST_USER",
    }

    parsed = svc._parse_trade_from_strings(string_trade)

    assert parsed["bought_currency"] == "BTC"
    assert parsed["sold_currency"] == "USDT"
    assert parsed["price"] == 42000  # Decimal converted to float or Decimal
    assert parsed["original_id"] == "ABC123"
    assert "id" in parsed


def test_parse_trade_from_api(monkeypatch):
    # ---- Mock keyring responses ----
    def fake_get_password(system, key):
        return {
            "api_key": "TEST_API_KEY",
            "api_secret": "TEST_API_SECRET",
            "user": "TEST_USER",
        }.get(key)

    monkeypatch.setattr("keyring.get_password", fake_get_password)

    # ---- Create service ----
    svc = KangaService()
    svc.user = "test-user"

    monkeypatch.setattr("app.kanga_service.generate_hash", lambda input_dict: "APIHASH")

    kanga_trade = {
        "side": "BUYER",
        "buyingCurrency": "BTC",
        "payingCurrency": "USDT",
        "quantity": "0.2",
        "value": "9000",
        "price": "45000",
        "feeCurrency": "USDT",
        "fee": "2",
        "id": "555",
        "created": "2024-02-15T14:22:10Z",
    }

    parsed = svc._parse_trade_from_api(kanga_trade)

    assert parsed["original_id"] == "555"
    assert parsed["bought_currency"] == "BTC"
    assert parsed["sold_currency"] == "USDT"
    assert parsed["bought_amount"] == Decimal("0.2")
    assert parsed["price"] == Decimal("45000")
    assert parsed["id"] == "APIHASH"


def test_get_market_tickers_json_error(monkeypatch):
    # Create fake response with invalid JSON
    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.json.side_effect = ValueError("Invalid JSON")

    # Patch requests.get
    monkeypatch.setattr("app.kanga_service.requests.get", lambda url: mock_resp)

    # ---- Mock keyring responses ----
    def fake_get_password(system, key):
        return {
            "api_key": "TEST_API_KEY",
            "api_secret": "TEST_API_SECRET",
            "user": "TEST_USER",
        }.get(key)

    monkeypatch.setattr("keyring.get_password", fake_get_password)

    # ---- Create service ----
    svc = KangaService()

    with pytest.raises(Exception) as exc:
        svc.get_market_tickers()

    assert "Invalid JSON" in str(exc.value)

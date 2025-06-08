from fastapi.testclient import TestClient
from unittest.mock import patch
from app.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@patch("app.main.fetch_prices")
@patch("app.main.crud")
def test_fetch_prices_endpoint_success(mock_crud, mock_fetch_prices):
    mock_price = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "time": "2024-01-01T00:00:00",
        "price": 42000.0,
        "source": "binance"
    }
    mock_fetch_prices.return_value = [mock_price] * 2
    mock_crud.candle_exists.return_value = False
    mock_crud.create_candle.return_value = None

    response = client.post("/fetch_prices")
    assert response.status_code == 200
    assert "Fetched 2 prices, saved 2" in response.json()["message"]


@patch("app.main.fetch_prices")
def test_fetch_prices_endpoint_no_data(mock_fetch_prices):
    mock_fetch_prices.return_value = []
    response = client.post("/fetch_prices")
    assert response.status_code == 404
    assert response.json()["detail"] == "No prices found"


@patch("app.main.fetch_prices_stream")
@patch("app.main.crud")
def test_fetch_prices_stream_endpoint(mock_crud, mock_fetch_prices_stream):
    mock_price = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "time": "2024-01-01T00:00:00",
        "price": 42000.0,
        "source": "binance"
    }

    # Simulate one batch of 2 prices
    mock_fetch_prices_stream.return_value = iter([[mock_price, mock_price]])
    mock_crud.candle_exists.return_value = False
    mock_crud.create_candle.return_value = None

    response = client.post("/fetch_prices_stream")
    assert response.status_code == 200
    assert "Fetched 2 prices from stream, saved 2 to database" in response.json()["message"]

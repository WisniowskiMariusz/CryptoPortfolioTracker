import pytest
from datetime import datetime, UTC
from unittest.mock import MagicMock
from app import models
from app.crud import candle_exists, create_candle


@pytest.fixture
def mock_session():
    return MagicMock()


def test_candle_exists_true(mock_session):
    mock_query = MagicMock()
    mock_filter = MagicMock()
    mock_session.query.return_value = mock_query
    mock_query.filter.return_value = mock_filter
    mock_filter.first.return_value = models.PriceHistory()

    result = candle_exists(mock_session, "BTCUSDT", "1m", datetime.now(UTC))

    assert result is True
    mock_session.query.assert_called_once_with(models.PriceHistory)


def test_candle_exists_false(mock_session):
    mock_session.query().filter().first.return_value = None

    result = candle_exists(mock_session, "BTCUSDT", "1m", datetime.now(UTC))

    assert result is False


def test_create_candle(mock_session):
    # Arrange
    candle_data = {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "time": datetime.now(UTC),
        "price": 30000.5,
    }

    # Act
    result = create_candle(mock_session, candle_data)

    # Assert
    assert isinstance(result, models.PriceHistory)
    assert result.symbol == candle_data["symbol"]
    assert result.interval == candle_data["interval"]
    assert result.price == candle_data["price"]

    mock_session.add.assert_called_once()
    mock_session.commit.assert_called_once()
    mock_session.refresh.assert_called_once_with(result)

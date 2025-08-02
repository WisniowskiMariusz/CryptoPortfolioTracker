import os
import pytest
import keyring
from unittest.mock import patch, MagicMock
from app.binance_service import BinanceService

os.environ["USE_SQL_SERVER"] = "false"


@pytest.fixture(autouse=True)
def mock_keyring(monkeypatch):
    def fake_get_password(service_name, username):
        if service_name == "only_api_key_correct":
            if username == "api_secret":
                raise keyring.errors.KeyringError
        elif service_name != "fake keyring system name":
            raise keyring.errors.KeyringError
        if username == "api_key":
            return "fake_api_key"
        elif username == "api_secret":
            return "fake_api_secret"

    monkeypatch.setattr(keyring, "get_password", fake_get_password)


@pytest.fixture
def mocked_binance_service_instance():
    with patch("app.main.binance_service") as mocked_bs_instance:
        yield mocked_bs_instance


@pytest.fixture
def mocked_binance_service():
    mock_client = MagicMock()

    # This patch replaces the Spot class with your mock
    with patch("app.binance_service.Spot", return_value=mock_client):
        # Create the service with fake credentials (from mock_keyring)
        service = BinanceService("fake keyring system name")
        yield service, mock_client


@pytest.fixture
def fake_binance_service(mocked_binance_service):
    return mocked_binance_service[0]

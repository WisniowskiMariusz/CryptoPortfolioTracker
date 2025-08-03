import pytest
import keyring
from unittest.mock import patch, MagicMock
from app.binance_service import BinanceService
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from app.main import app
from app.dependencies import get_binance_service, get_db, get_db_session
from app.database import Database


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
    with patch("app.main.get_binance_service") as mocked_bs_instance:
        yield mocked_bs_instance


@pytest.fixture
def mocked_binance_service_tuple():
    mock_client = MagicMock()

    # This patch replaces the Spot class with your mock
    with patch("app.binance_service.Spot", return_value=mock_client):
        # Create the service with fake credentials (from mock_keyring)
        service = BinanceService("fake keyring system name")
        yield service, mock_client


@pytest.fixture
def fake_binance_service(mocked_binance_service_tuple):
    return mocked_binance_service_tuple[0]


@pytest.fixture
def mocked_binance_client(mocked_binance_service_tuple):
    return mocked_binance_service_tuple[1]


@pytest.fixture
def override_binance_dependency(mocked_binance_service_tuple):
    service_instance, _ = mocked_binance_service_tuple

    def _override():
        return service_instance

    app.dependency_overrides[get_binance_service] = _override
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def test_client(override_binance_dependency):
    return TestClient(app)


@pytest.fixture
def mocked_db_session():
    return MagicMock(spec=Session)


@pytest.fixture
def override_get_db_session(mocked_db_session):
    def _override():
        yield mocked_db_session

    app.dependency_overrides[get_db_session] = _override
    yield mocked_db_session
    app.dependency_overrides.pop(get_db_session, None)


@pytest.fixture
def mocked_db():
    return MagicMock(spec=Database)


@pytest.fixture
def override_get_db(mocked_db, override_get_db_session):
    app.dependency_overrides[get_db] = lambda: mocked_db
    yield mocked_db
    app.dependency_overrides.pop(get_db, None)

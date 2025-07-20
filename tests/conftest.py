import os
import pytest
import keyring

os.environ["USE_SQL_SERVER"] = "false"


@pytest.fixture(autouse=True)
def mock_keyring(monkeypatch):
    def fake_get_password(service_name, username):
        if username == "api_key":
            return "fake_api_key"
        elif username == "api_secret":
            return "fake_api_secret"
        return None

    monkeypatch.setattr(keyring, "get_password", fake_get_password)

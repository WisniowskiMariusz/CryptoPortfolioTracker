from fastapi import HTTPException
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.exc import SQLAlchemyError
from app.database import Base, Database
from app import models

# Use in-memory SQLite for isolated testing
TEST_DATABASE_URL = "sqlite://"


@pytest.fixture
def database():
    return Database()


@pytest.fixture(scope="module")
def test_engine():
    engine = create_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def test_session(test_engine):
    connection = test_engine.connect()
    transaction = connection.begin()
    session = sessionmaker(bind=connection)()
    yield session
    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
def sample_1_trade():
    return [
        {"id": 1, "symbol": "BTCUSDT", "price": 100},
    ]


@pytest.fixture
def sample_2_trades():
    return [
        {"id": 1, "symbol": "BTCUSDT", "price": 100},
        {"id": 2, "symbol": "BTCUSDT", "price": 200},
    ]


@pytest.fixture
def sample_1_deposit():
    return [
        {
            "id": "1",
            "amount": 10,
            "coin": "BTC",
            "network": "btc",
            "status": "done",
            "address": "addr1",
            "addressTag": None,
            "txId": "tx1",
            "insertTime": 1650000000000,
            "transferType": None,
            "confirmTimes": None,
            "unlockConfirm": None,
            "walletType": None,
        }
    ]


@pytest.fixture
def sample_2_deposits():
    return [
        {
            "id": "1",
            "amount": 10,
            "coin": "BTC",
            "network": "btc",
            "status": "done",
            "address": "addr1",
            "addressTag": None,
            "txId": "tx1",
            "insertTime": 1650000000000,
            "transferType": None,
            "confirmTimes": None,
            "unlockConfirm": None,
            "walletType": None,
        },
        {
            "id": "2",
            "amount": 5,
            "coin": "ETH",
            "network": "eth",
            "status": "done",
            "address": "addr2",
            "addressTag": None,
            "txId": "tx2",
            "insertTime": 1650000000000,
            "transferType": None,
            "confirmTimes": None,
            "unlockConfirm": None,
            "walletType": None,
        },
    ]


@pytest.fixture
def sample_1_withdrawal():
    return [
        {
            "id": "1",
            "amount": 10,
            "coin": "BTC",
            "network": "btc",
            "status": "done",
            "address": "addr1",
            "addressTag": None,
            "txId": "tx1",
            "applyTime": None,
            "completeTime": None,
            "transferType": None,
            "walletType": None,
            "transactionFee": None,
            "info": None,
            "confirmNo": None,
            "txKey": None,
        }
    ]


@pytest.fixture
def sample_2_withdrawals():
    return [
        {
            "id": "1",
            "amount": 10,
            "coin": "BTC",
            "network": "btc",
            "status": "done",
            "address": "addr1",
            "addressTag": None,
            "txId": "tx1",
            "applyTime": None,
            "completeTime": None,
            "transferType": None,
            "walletType": None,
            "transactionFee": None,
            "info": None,
            "confirmNo": None,
            "txKey": None,
        },
        {
            "id": "2",
            "amount": 5,
            "coin": "ETH",
            "network": "eth",
            "status": "done",
            "address": "addr2",
            "addressTag": None,
            "txId": "tx2",
            "applyTime": None,
            "completeTime": None,
            "transferType": None,
            "walletType": None,
            "transactionFee": None,
            "info": None,
            "confirmNo": None,
            "txKey": None,
        },
    ]


def test_session_runs_query(test_session):
    result = test_session.execute(text("SELECT 1")).scalar()
    assert result == 1


def test_store_trades_inserts_unique_trades(test_session, database, sample_2_trades):
    inserted = database.store_trades(test_session, sample_2_trades)
    assert len(inserted) == 2

    rows = test_session.query(models.TradesFromApi).all()
    assert len(rows) == 2


def test_store_trades_skips_existing_trades(test_session, database, sample_1_trade):
    # Insert first time
    database.store_trades(test_session, sample_1_trade)
    # Try inserting duplicate
    inserted = database.store_trades(test_session, sample_1_trade)
    assert inserted == []


def test_store_trades_raises_http_exception_on_db_error(
    database, mocked_db_session, sample_1_trade
):
    mocked_db_session.bulk_save_objects.side_effect = SQLAlchemyError("fail")
    with pytest.raises(HTTPException) as exc_info:
        database.store_trades(db_session=mocked_db_session, trades=sample_1_trade)
    assert "DB error" in str(exc_info.value.detail)
    mocked_db_session.rollback.assert_called_once()


def test_store_deposits_success(database, override_get_db_session, sample_1_deposit):
    session = override_get_db_session
    # Simulate no existing deposits (empty query result)
    session.query().filter().all.return_value = []
    count = database.store_deposits(session, sample_1_deposit)
    assert count == 1
    session.bulk_save_objects.assert_called_once()
    session.commit.assert_called_once()


def test_store_deposits_duplicates_skipped(
    database, override_get_db_session, sample_2_deposits
):
    session = override_get_db_session
    # Simulate first deposit already exists
    session.query().filter().all.return_value = [("1",)]

    count = database.store_deposits(session, sample_2_deposits)
    assert count == 1  # only second deposit inserted
    session.bulk_save_objects.assert_called_once()
    session.commit.assert_called_once()


def test_store_deposits_db_error(database, override_get_db_session, sample_1_deposit):
    session = override_get_db_session
    session.bulk_save_objects.side_effect = SQLAlchemyError("DB fail")
    with pytest.raises(HTTPException) as exc_info:
        database.store_deposits(session, sample_1_deposit)
    session.rollback.assert_called_once()
    assert exc_info.value.status_code == 500
    assert "DB error" in exc_info.value.detail


def test_store_withdrawals_success(
    database, override_get_db_session, sample_1_withdrawal
):
    session = override_get_db_session
    session.query().filter().all.return_value = []
    count = database.store_withdrawals(session, sample_1_withdrawal)
    assert count == 1
    session.bulk_save_objects.assert_called_once()
    session.commit.assert_called_once()


def test_store_withdrawals_duplicates_skipped(
    database, override_get_db_session, sample_2_withdrawals
):
    session = override_get_db_session
    session.query().filter().all.return_value = [("1",)]
    count = database.store_withdrawals(session, sample_2_withdrawals)
    assert count == 1
    session.bulk_save_objects.assert_called_once()
    session.commit.assert_called_once()


def test_store_withdrawals_db_error(database, override_get_db_session):
    session = override_get_db_session
    withdrawals = [
        {
            "id": "1",
            "amount": 10,
            "coin": "BTC",
            "network": "btc",
            "status": "done",
            "address": "addr1",
            "addressTag": None,
            "txId": "tx1",
            "applyTime": None,
            "completeTime": None,
            "transferType": None,
            "walletType": None,
            "transactionFee": None,
            "info": None,
            "confirmNo": None,
            "txKey": None,
        }
    ]

    session.bulk_save_objects.side_effect = SQLAlchemyError("DB fail")

    with pytest.raises(HTTPException) as exc_info:
        database.store_withdrawals(session, withdrawals)

    session.rollback.assert_called_once()
    assert exc_info.value.status_code == 500
    assert "DB error" in exc_info.value.detail

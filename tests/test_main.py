from fastapi.testclient import TestClient
from unittest.mock import patch
from app.main import app
import io
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

client = TestClient(app)
MIME_TYPE_EXCEL_XLSX = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_check_wrong_method():
    response = client.post("/health")
    assert response.status_code == 405


@patch("app.main.crud")
def test_fetch_prices_endpoint_success(mock_crud, test_client, fake_binance_service):
    mock_price = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "time": "2024-01-01T00:00:00",
        "price": 42000.0,
        "source": "binance",
    }
    with patch.object(
        fake_binance_service, "fetch_prices", return_value=[mock_price] * 2
    ):
        mock_crud.candle_exists.return_value = False
        mock_crud.create_candle.return_value = None
        response = test_client.post("/fetch_and_store_prices")
    assert response.status_code == 200
    assert "Fetched 2 prices, saved 2" in response.json()["message"]


def test_fetch_prices_endpoint_no_data(test_client, fake_binance_service):
    with patch.object(fake_binance_service, "fetch_prices", return_value=[]):
        response = test_client.post("/fetch_and_store_prices")
        assert response.status_code == 404
        assert response.json()["detail"] == "No prices found"


@patch("app.main.crud")
def test_fetch_prices_stream_endpoint(mock_crud, test_client, fake_binance_service):
    mock_price = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "time": "2024-01-01T00:00:00",
        "price": 42000.0,
        "source": "binance",
    }
    # Simulate one batch of 2 prices
    with patch.object(
        fake_binance_service,
        "fetch_prices_stream",
        return_value=iter([[mock_price, mock_price]]),
    ):
        mock_crud.candle_exists.return_value = False
        mock_crud.create_candle.return_value = None
        response = test_client.post("/fetch_and_store_prices_stream")
    assert response.status_code == 200
    assert (
        "Fetched 2 prices from stream, saved 2 to database"
        in response.json()["message"]
    )


def test_get_deposits_success(test_client, mocked_binance_client):
    mock_response = {"status": "success", "data": [], "count": 0}
    mocked_binance_client.deposit_history.return_value = mock_response
    response = test_client.get(
        "/get_deposits",
        params={
            "asset": "BTC",
            "start_time": 1625090462000,
            "end_time": 1627761600000,
        },
    )
    assert response.status_code == 200
    assert response.json() == mock_response


def test_get_deposits_exception(test_client, mocked_binance_client):
    mocked_binance_client.deposit_history.side_effect = Exception(
        "Something went wrong"
    )
    response = test_client.get("/get_deposits")
    assert response.status_code == 500
    assert response.json() == {"detail": "Something went wrong"}


def test_get_withdrawals_success(
    test_client, mocked_binance_client, fake_binance_service
):
    mock_response = {"status": "success", "data": [], "count": 0}
    mocked_binance_client.withdraw_history.return_value = mock_response
    response = test_client.get(
        "/get_withdrawals",
        params={
            "asset": "BTC",
            "start_time": 1625090462000,
            "end_time": 1627761600000,
        },
    )
    assert response.status_code == 200
    assert response.json() == mock_response
    # fake_binance_service.get_withdraw_history.assert_called_once_with(
    #     "BTC", 1625090462000, 1627761600000
    # )


def test_get_withrawals_exception(test_client, mocked_binance_client):
    mocked_binance_client.withdraw_history.side_effect = Exception(
        "Something went wrong"
    )
    response = test_client.get("/get_withdrawals")
    assert response.status_code == 500
    assert response.json() == {"detail": "Something went wrong"}


def test_get_account_success(test_client, mocked_binance_client):
    mocked_binance_client.account.return_value = {"balances": []}
    response = test_client.get("/get_account")
    assert response.status_code == 200
    assert response.json() == {"balances": []}


def test_get_account_error(test_client, mocked_binance_client):
    mocked_binance_client.account.side_effect = Exception("fail")
    response = test_client.get("/get_account")
    assert response.status_code == 500
    assert response.json()["detail"] == "fail"


def test_get_earnings_success(test_client, mocked_binance_client):
    mock_response = {
        "status": "success",
        "count": 1,
        "data": [{"asset": "BTC", "interest": "0.0001"}],
    }
    mocked_binance_client.get_flexible_rewards_history.return_value = mock_response
    response = test_client.get(
        "/get_earnings",
        params={
            "lending_type": "DAILY",
            "asset": "BTC",
            "start_time": 1609459200000,
            "end_time": 1612137600000,
            "limit": 500,
        },
    )
    assert response.status_code == 200
    assert response.json() == mock_response


def test_get_earnings_error(test_client, mocked_binance_client):
    mocked_binance_client.get_flexible_rewards_history.side_effect = Exception(
        "Simulated failure"
    )
    response = test_client.get("/get_earnings")
    assert response.status_code == 500
    assert "Simulated failure" in response.json()["detail"]


def test_get_dust_conversion_history_success(test_client, mocked_binance_client):
    mock_response = {
        "status": "success",
        "total_converted": "0.01",
        "logs": [{"asset": "LTC", "amount": "0.1"}],
    }
    mocked_binance_client.dust_log.return_value = mock_response
    response = test_client.get("/get_dust_conversion_history")
    assert response.status_code == 200
    assert response.json() == mock_response


def test_get_dust_conversion_history_error(test_client, mocked_binance_client):
    mocked_binance_client.dust_log.side_effect = Exception("Simulated failure")
    response = test_client.get("/get_dust_conversion_history")
    assert response.status_code == 500
    assert "Simulated failure" in response.json()["detail"]


def test_fetch_and_store_trades_success(
    test_client, fake_binance_service, override_get_db
):
    override_get_db.store_trades.return_value = [object()]
    with patch.object(
        fake_binance_service,
        "fetch_all_trades_for_symbol",
        return_value=[{"id": 1, "symbol": "BTCUSDT"}],
    ):
        response = test_client.post("/fetch_and_store_trades")
    assert response.status_code == 200
    assert response.json()["Stored trades"] == 1
    assert response.json()["Fetched trades"] == 1


def test_fetch_and_store_trades_no_trades(test_client, fake_binance_service):
    with patch.object(
        fake_binance_service, "fetch_all_trades_for_symbol", return_value=[]
    ):
        response = test_client.post("/fetch_and_store_trades")
    assert response.status_code == 404
    assert "No trades found" in response.json()["detail"]


def test_fetch_and_store_trades_exception(test_client, fake_binance_service):
    with patch.object(
        fake_binance_service,
        "fetch_all_trades_for_symbol",
        side_effect=Exception("fail"),
    ):
        response = test_client.post("/fetch_and_store_trades")
    assert response.status_code == 500
    assert response.json()["detail"] == "fail"


@patch("app.main.datetime_from_str")
def test_fetch_and_store_trades_for_all_symbols_db_error(mock_to_datetime, test_client):
    mock_to_datetime.side_effect = Exception("fail")
    response = test_client.post("/fetch_and_store_trades_for_all_symbols")
    assert response.status_code == 500
    assert "DB error" in response.json()["detail"]


def test_fetch_and_store_trades_for_all_symbols_no_pairs(test_client):
    response = test_client.post("/fetch_and_store_trades_for_all_symbols")
    # Depends on implementation can be 404 lub 200 with info about no pairs
    assert response.status_code in (404, 200)


def test_fetch_and_store_trades_for_all_symbols_db_exception(
    test_client, override_get_db_session
):
    override_get_db_session.query.side_effect = Exception("fail")
    response = test_client.post("/fetch_and_store_trades_for_all_symbols")
    assert response.status_code == 500
    assert "DB error" in response.json()["detail"]


def test_upload_xlsx_missing_column(override_get_db_session):
    df = pd.DataFrame({"A": [1]})
    file_bytes = io.BytesIO()
    df.to_excel(file_bytes, index=False, engine="openpyxl")
    file_bytes.seek(0)
    response = client.post(
        "/upload-xlsx",
        files={
            "file": (
                "test.xlsx",
                file_bytes,
                MIME_TYPE_EXCEL_XLSX,
            )
        },
    )
    assert response.status_code == 400
    assert "Missing required columns" in response.json()["detail"]


def test_upload_xlsx_wrong_filetype():
    response = client.post(
        "/upload-xlsx", files={"file": ("test.txt", b"abc", "text/plain")}
    )
    assert response.status_code == 400
    assert "Only .xlsx files are supported." in response.json()["detail"]


def test_upload_xlsx_success(override_get_db_session):
    df = pd.DataFrame(
        {
            "Date(UTC)": ["2025-06-14 09:26:02"],
            "Pair": ["RENDEREUR"],
            "Base Asset": ["RENDER"],
            "Quote Asset": ["EUR"],
            "Type": ["buy"],
            "Price": [2.971],
            "Amount": [5.942],
            "Total": [17.64],
            "Fee": [0.0000079],
            "Fee Coin": ["BNB"],
        }
    )
    file_bytes = io.BytesIO()
    df.to_excel(file_bytes, index=False, engine="openpyxl")
    file_bytes.seek(0)
    response = client.post(
        "/upload-xlsx",
        files={
            "file": (
                "test.xlsx",
                file_bytes,
                MIME_TYPE_EXCEL_XLSX,
            )
        },
    )
    assert response.status_code == 200
    assert "inserted" in response.json()


def test_upload_xlsx_row_parse_error(override_get_db_session):
    df = pd.DataFrame(
        {
            "Date(UTC)": ["2025-06-14 09:26:02"],
            "Pair": ["RENDEREUR"],
            "Base Asset": ["RENDER"],
            "Quote Asset": ["EUR"],
            "Type": ["buy"],
            "Price": ["not_a_number"],  # Błąd!
            "Amount": [5.942],
            "Total": [17.64],
            "Fee": [0.0000079],
            "Fee Coin": ["BNB"],
        }
    )
    file_bytes = io.BytesIO()
    df.to_excel(file_bytes, index=False, engine="openpyxl")
    file_bytes.seek(0)
    response = client.post(
        "/upload-xlsx",
        files={
            "file": (
                "test.xlsx",
                file_bytes,
                MIME_TYPE_EXCEL_XLSX,
            )
        },
    )
    assert response.status_code == 400
    assert "Error parsing row" in response.json()["detail"]


def test_upload_xlsx_db_error(override_get_db_session):
    df = pd.DataFrame(
        {
            "Date(UTC)": ["2025-06-14 09:26:02"],
            "Pair": ["RENDEREUR"],
            "Base Asset": ["RENDER"],
            "Quote Asset": ["EUR"],
            "Type": ["buy"],
            "Price": [2.971],
            "Amount": [5.942],
            "Total": [17.64],
            "Fee": [0.0000079],
            "Fee Coin": ["BNB"],
        }
    )
    file_bytes = io.BytesIO()
    df.to_excel(file_bytes, index=False, engine="openpyxl")
    file_bytes.seek(0)
    override_get_db_session.bulk_save_objects.side_effect = SQLAlchemyError("fail")
    response = client.post(
        "/upload-xlsx",
        files={
            "file": (
                "test.xlsx",
                file_bytes,
                MIME_TYPE_EXCEL_XLSX,
            )
        },
    )
    assert response.status_code == 500
    assert "Database error" in response.json()["detail"]


def test_upload_csv_missing_column(override_get_db_session):
    df = pd.DataFrame({"A": [1]})
    file_bytes = io.StringIO()
    df.to_csv(file_bytes, index=False)
    file_bytes.seek(0)
    response = client.post(
        "/upload-csv",
        files={"file": ("test.csv", file_bytes.getvalue(), "text/csv")},
    )
    assert response.status_code == 400
    assert "Missing required columns" in response.json()["detail"]


def test_upload_csv_wrong_filetype():
    response = client.post(
        "/upload-csv", files={"file": ("test.txt", "abc", "text/plain")}
    )
    assert response.status_code == 400
    assert "Only .csv files are supported." in response.json()["detail"]


def test_upload_csv_success(override_get_db_session):
    df = pd.DataFrame(
        {
            "Date(UTC)": ["2025-06-14 09:26:02"],
            "Pair": ["RENDEREUR"],
            "Side": ["BUY"],
            "Price": ["2.971"],
            "Executed": ["2RENDER"],
            "Amount": ["5.942EUR"],
            "Fee": ["0.0000079BNB"],
        }
    )
    file_str = io.StringIO()
    df.to_csv(file_str, index=False)
    file_bytes = io.BytesIO(file_str.getvalue().encode("utf-8"))
    file_bytes.seek(0)
    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code == 200
    assert "inserted" in response.json()


def test_upload_csv_row_parse_error(override_get_db_session):
    df = pd.DataFrame(
        {
            "Date(UTC)": ["2025-06-14 09:26:02"],
            "Pair": ["RENDEREUR"],
            "Side": ["BUY"],
            "Price": ["not_a_number"],
            "Executed": ["2RENDER"],
            "Amount": ["5.942EUR"],
            "Fee": ["0.0000079BNB"],
        }
    )
    file_str = io.StringIO()
    df.to_csv(file_str, index=False)
    file_bytes = io.BytesIO(file_str.getvalue().encode("utf-8"))
    file_bytes.seek(0)
    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code == 400
    assert "Error parsing row" in response.json()["detail"]


def test_upload_csv_db_error(override_get_db_session):
    df = pd.DataFrame(
        {
            "Date(UTC)": ["2025-06-14 09:26:02"],
            "Pair": ["RENDEREUR"],
            "Side": ["BUY"],
            "Price": ["2.971"],
            "Executed": ["2RENDER"],
            "Amount": ["5.942EUR"],
            "Fee": ["0.0000079BNB"],
        }
    )
    file_str = io.StringIO()
    df.to_csv(file_str, index=False)
    file_bytes = io.BytesIO(file_str.getvalue().encode("utf-8"))
    file_bytes.seek(0)
    override_get_db_session.bulk_save_objects.side_effect = SQLAlchemyError("fail")
    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code == 500
    assert "Database error" in response.json()["detail"]


def test_upload_xlsx_read_error(override_get_db_session):
    # Corrupted XLSX file
    response = client.post(
        "/upload-xlsx",
        files={
            "file": (
                "test.xlsx",
                b"not an excel file",
                MIME_TYPE_EXCEL_XLSX,
            )
        },
    )
    assert response.status_code == 400
    assert "Error reading Excel file" in response.json()["detail"]


def test_upload_csv_read_error(override_get_db_session):
    # Corrupted CSV file
    response = client.post(
        "/upload-csv",
        files={"file": ("test.csv", b"\x00\x01\x02", "text/csv")},
    )
    assert response.status_code == 400
    assert "Missing required columns" in response.json()["detail"]


def test_upload_xlsx_empty_file(override_get_db_session):
    # Empty XLSX file
    file_bytes = io.BytesIO()
    file_bytes.seek(0)
    response = client.post(
        "/upload-xlsx",
        files={
            "file": (
                "test.xlsx",
                file_bytes,
                MIME_TYPE_EXCEL_XLSX,
            )
        },
    )
    # Can be 400 or 422 depending on validation logic
    assert response.status_code in (400, 422)


def test_upload_csv_empty_file(override_get_db_session):
    # Empty CSV file
    file_bytes = io.BytesIO()
    file_bytes.seek(0)
    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code in (400, 422)


def test_upload_xlsx_missing_one_column(override_get_db_session):
    df = pd.DataFrame(
        {
            "Date(UTC)": ["2025-06-14 09:26:02"],
            # "Pair" intentionally missing
            "Base Asset": ["RENDER"],
            "Quote Asset": ["EUR"],
            "Type": ["buy"],
            "Price": [2.971],
            "Amount": [5.942],
            "Total": [17.64],
            "Fee": [0.0000079],
            "Fee Coin": ["BNB"],
        }
    )
    file_bytes = io.BytesIO()
    df.to_excel(file_bytes, index=False, engine="openpyxl")
    file_bytes.seek(0)
    response = client.post(
        "/upload-xlsx",
        files={
            "file": (
                "test.xlsx",
                file_bytes,
                MIME_TYPE_EXCEL_XLSX,
            )
        },
    )
    assert response.status_code == 400
    assert "Missing required columns" in response.json()["detail"]


def test_upload_csv_missing_one_column(override_get_db_session):
    df = pd.DataFrame(
        {
            "Date(UTC)": ["2025-06-14 09:26:02"],
            # "Pair" intentionally missing
            "Side": ["BUY"],
            "Price": ["2.971"],
            "Executed": ["2RENDER"],
            "Amount": ["5.942EUR"],
            "Fee": ["0.0000079BNB"],
        }
    )
    file_str = io.StringIO()
    df.to_csv(file_str, index=False)
    file_bytes = io.BytesIO(file_str.getvalue().encode("utf-8"))
    file_bytes.seek(0)
    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code == 400
    assert "Missing required columns" in response.json()["detail"]


def test_upload_csv_with_duplicate_rows(override_get_db_session):
    df = pd.DataFrame(
        {
            "Date(UTC)": ["2025-06-14 09:26:02", "2025-06-14 09:26:02"],
            "Pair": ["RENDEREUR", "RENDEREUR"],
            "Side": ["BUY", "BUY"],
            "Price": ["2.971", "2.971"],
            "Executed": ["2RENDER", "2RENDER"],
            "Amount": ["5.942EUR", "5.942EUR"],
            "Fee": ["0.0000079BNB", "0.0000079BNB"],
        }
    )
    file_str = io.StringIO()
    df.to_csv(file_str, index=False)
    file_bytes = io.BytesIO(file_str.getvalue().encode("utf-8"))
    file_bytes.seek(0)
    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code == 200
    assert "inserted" in response.json()
    assert response.json()["inserted"] == 2


def test_upload_xlsx_with_duplicate_rows(override_get_db_session):
    df = pd.DataFrame(
        {
            "Date(UTC)": ["2025-06-14 09:26:02", "2025-06-14 09:26:02"],
            "Pair": ["RENDEREUR", "RENDEREUR"],
            "Base Asset": ["RENDER", "RENDER"],
            "Quote Asset": ["EUR", "EUR"],
            "Type": ["buy", "buy"],
            "Price": [2.971, 2.971],
            "Amount": [5.942, 5.942],
            "Total": [17.64, 17.64],
            "Fee": [0.0000079, 0.0000079],
            "Fee Coin": ["BNB", "BNB"],
        }
    )
    file_bytes = io.BytesIO()
    df.to_excel(file_bytes, index=False, engine="openpyxl")
    file_bytes.seek(0)
    response = client.post(
        "/upload-xlsx",
        files={
            "file": (
                "test.xlsx",
                file_bytes,
                MIME_TYPE_EXCEL_XLSX,
            )
        },
    )
    assert response.status_code == 200
    assert "inserted" in response.json()
    assert response.json()["inserted"] == 2

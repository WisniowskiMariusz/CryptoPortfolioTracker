from fastapi.testclient import TestClient
from unittest.mock import patch
from app.main import app
import io
import pandas as pd
from app.database import database
from sqlalchemy.exc import SQLAlchemyError

client = TestClient(app)
MIME_TYPE_EXCEL_XLSX = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)


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
        "source": "binance",
    }
    mock_fetch_prices.return_value = [mock_price] * 2
    mock_crud.candle_exists.return_value = False
    mock_crud.create_candle.return_value = None

    response = client.post("/fetch_and_store_prices")
    assert response.status_code == 200
    assert "Fetched 2 prices, saved 2" in response.json()["message"]


@patch("app.main.fetch_prices")
def test_fetch_prices_endpoint_no_data(mock_fetch_prices):
    mock_fetch_prices.return_value = []
    response = client.post("/fetch_and_store_prices")
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
        "source": "binance",
    }

    # Simulate one batch of 2 prices
    mock_fetch_prices_stream.return_value = iter([[mock_price, mock_price]])
    mock_crud.candle_exists.return_value = False
    mock_crud.create_candle.return_value = None

    response = client.post("/fetch_and_store_prices_stream")
    assert response.status_code == 200
    assert (
        "Fetched 2 prices from stream, saved 2 to database"
        in response.json()["message"]
    )


@patch("app.main.get_account_info")
def test_get_account_success(mock_get_account_info):
    mock_get_account_info.return_value = {"balances": []}
    response = client.get("/get_account")
    assert response.status_code == 200
    assert response.json() == {"balances": []}


@patch("app.main.get_account_info")
def test_get_account_error(mock_get_account_info):
    mock_get_account_info.side_effect = Exception("fail")
    response = client.get("/get_account")
    assert response.status_code == 500
    assert response.json()["detail"] == "fail"


@patch("app.main.fetch_trades")
@patch("app.main.database")
def test_fetch_and_store_trades_success(mock_database, mock_fetch_trades):
    mock_fetch_trades.return_value = [{"id": 1, "symbol": "BTCUSDT"}]
    mock_database.store_trades.return_value = [object()]
    response = client.post("/fetch_and_store_trades")
    assert response.status_code == 200
    assert response.json()["Stored trades"] == 1
    assert response.json()["Fetched trades"] == 1


@patch("app.main.fetch_trades")
def test_fetch_and_store_trades_no_trades(mock_fetch_trades):
    mock_fetch_trades.return_value = []
    response = client.post("/fetch_and_store_trades")
    assert response.status_code == 404
    assert "No trades found" in response.json()["detail"]


@patch("app.main.fetch_trades")
def test_fetch_and_store_trades_exception(mock_fetch_trades):
    mock_fetch_trades.side_effect = Exception("fail")
    response = client.post("/fetch_and_store_trades")
    assert response.status_code == 500
    assert response.json()["detail"] == "fail"


def test_upload_xlsx_missing_column():
    df = pd.DataFrame({"A": [1]})
    file_bytes = io.BytesIO()
    df.to_excel(file_bytes, index=False, engine="openpyxl")
    file_bytes.seek(0)

    def override_get_db_session():
        class DummySession:
            def bulk_save_objects(self, records):
                pass

            def commit(self):
                pass

        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session
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


def test_upload_xlsx_success():
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

    class DummySession:
        def bulk_save_objects(self, records):
            pass

        def commit(self):
            pass

    def override_get_db_session():
        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session

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


def test_upload_xlsx_row_parse_error():
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

    class DummySession:
        def bulk_save_objects(self, records):
            pass

        def commit(self):
            pass

    def override_get_db_session():
        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session

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


def test_upload_xlsx_db_error():
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

    class DummySession:
        def bulk_save_objects(self, records):
            raise SQLAlchemyError("fail")

        def commit(self):
            pass

        def rollback(self):
            pass

    def override_get_db_session():
        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session

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


def test_upload_csv_missing_column():
    df = pd.DataFrame({"A": [1]})
    file_bytes = io.StringIO()
    df.to_csv(file_bytes, index=False)
    file_bytes.seek(0)

    def override_get_db_session():
        class DummySession:
            def bulk_save_objects(self, records):
                pass

            def commit(self):
                pass

        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session
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


def test_upload_csv_success():
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

    class DummySession:
        def bulk_save_objects(self, records):
            pass

        def commit(self):
            pass

    def override_get_db_session():
        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session

    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code == 200
    assert "inserted" in response.json()


def test_upload_csv_row_parse_error():
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

    class DummySession:
        def bulk_save_objects(self, records):
            pass

        def commit(self):
            pass

    def override_get_db_session():
        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session

    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code == 400
    assert "Error parsing row" in response.json()["detail"]


def test_upload_csv_db_error():
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

    class DummySession:
        def bulk_save_objects(self, records):
            raise SQLAlchemyError("fail")

        def commit(self):
            pass

        def rollback(self):
            pass

    def override_get_db_session():
        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session

    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code == 500
    assert "Database error" in response.json()["detail"]


@patch("app.main.datetime_from_str")
def test_fetch_and_store_trades_for_all_symbols_db_error(mock_to_datetime):
    mock_to_datetime.side_effect = Exception("fail")
    response = client.post("/fetch_and_store_trades_for_all_symbols")
    assert response.status_code == 500
    assert "DB error" in response.json()["detail"]


def test_upload_xlsx_read_error():
    def override_get_db_session():
        class DummySession:
            def bulk_save_objects(self, records):
                pass

            def commit(self):
                pass

        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session

    # Uszkodzony plik xlsx
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


def test_upload_csv_read_error():
    def override_get_db_session():
        class DummySession:
            def bulk_save_objects(self, records):
                pass

            def commit(self):
                pass

        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session

    # Uszkodzony plik csv
    response = client.post(
        "/upload-csv",
        files={"file": ("test.csv", b"\x00\x01\x02", "text/csv")},
    )
    assert response.status_code == 400
    assert "Missing required columns" in response.json()["detail"]


def test_upload_xlsx_empty_file():
    def override_get_db_session():
        class DummySession:
            def bulk_save_objects(self, records):
                pass

            def commit(self):
                pass

        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session

    # Pusty plik xlsx
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
    # Może być 400 lub 422 w zależności od walidacji
    assert response.status_code in (400, 422)


def test_upload_csv_empty_file():
    def override_get_db_session():
        class DummySession:
            def bulk_save_objects(self, records):
                pass

            def commit(self):
                pass

        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session

    # Pusty plik csv
    file_bytes = io.BytesIO()
    file_bytes.seek(0)
    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code in (400, 422)


def test_upload_xlsx_missing_one_column():
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

    def override_get_db_session():
        class DummySession:
            def bulk_save_objects(self, records):
                pass

            def commit(self):
                pass

        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session
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


def test_upload_csv_missing_one_column():
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

    def override_get_db_session():
        class DummySession:
            def bulk_save_objects(self, records):
                pass

            def commit(self):
                pass

        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session
    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code == 400
    assert "Missing required columns" in response.json()["detail"]


def test_upload_csv_with_duplicate_rows():
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

    class DummySession:
        def bulk_save_objects(self, records):
            pass

        def commit(self):
            pass

    def override_get_db_session():
        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session
    response = client.post(
        "/upload-csv", files={"file": ("test.csv", file_bytes, "text/csv")}
    )
    assert response.status_code == 200
    assert "inserted" in response.json()
    assert response.json()["inserted"] == 2


def test_upload_xlsx_with_duplicate_rows():
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

    class DummySession:
        def bulk_save_objects(self, records):
            pass

        def commit(self):
            pass

    def override_get_db_session():
        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session
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


def test_fetch_and_store_trades_for_all_symbols_no_pairs():
    class DummyQuery:
        def filter(self, *a, **kw):
            return self

        def distinct(self):
            return self

        def all(self):
            return []

    class DummySession:
        def bulk_save_objects(self, records):
            pass

        def commit(self):
            pass

        def query(self, *a, **kw):
            return DummyQuery()

    def override_get_db_session():
        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session
    response = client.post("/fetch_and_store_trades_for_all_symbols")
    # Zależnie od implementacji może być 404 lub 200 z info o braku
    assert response.status_code in (404, 200)


def test_fetch_and_store_trades_for_all_symbols_db_exception():
    class DummySession:
        def query(self, *a, **kw):
            raise Exception("fail")

    def override_get_db_session():
        yield DummySession()

    app.dependency_overrides[database.get_db_session] = override_get_db_session
    response = client.post("/fetch_and_store_trades_for_all_symbols")
    assert response.status_code == 500
    assert "DB error" in response.json()["detail"]


def test_health_check_wrong_method():
    response = client.post("/health")
    assert response.status_code == 405

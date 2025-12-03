import asyncio
import time
import pandas as pd
from typing import Annotated
from dotenv import load_dotenv
from io import BytesIO, StringIO
from fastapi import FastAPI, Depends, Query, HTTPException, UploadFile, File
from fastapi.routing import APIRoute
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.database import Database
from app import models, crud
from app.binance_service import BinanceService
from app.tools import datetime_from_str, timestamp_from_str
from app.dependencies import get_binance_service, get_db_session, get_db
from app.nbp_router import router as nbp_router
from app.binance_router import router as binance_router
from app.kanga_router import router as kanga_router
from app.users_router import router as users_router

load_dotenv()


app = FastAPI(
    title="CryptoPortfolioTracker API",
    description="API to track your crypto portfolio and transactions.",
    version="0.1.0",
)

app.include_router(nbp_router)
app.include_router(binance_router)
app.include_router(kanga_router)
app.include_router(users_router)


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/routes", tags=["Utility"], summary="List all API routes")
def show_routes() -> list[dict]:
    """
    Show all routes in the application."""
    response: list = []
    try:
        for route in app.routes:
            if isinstance(route, APIRoute):
                response.append(
                    {
                        "path": route.path,
                        "name": route.name,
                        "methods": list(route.methods),
                        "operation_id": route.operation_id,
                    }
                )
        return response
    except Exception as e:
        return {"error": str(e)}


@app.get("/get_account")
def get_account(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
):
    try:
        return binance_service.get_account_info()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get_deposits")
def get_deposits(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    asset: str = None,
    start_time: int = 1625090462000,
    end_time: int = 1627761600000,
):
    try:
        return binance_service.get_deposit_history(asset, start_time, end_time)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get_withdrawals")
def get_withdrawals(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    asset: str = None,
    start_time: int = 1679180400000,
    end_time: int = 1683756000000,
):
    try:
        return binance_service.get_withdraw_history(asset, start_time, end_time)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get_earnings")
def get_earnings(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    lending_type: str = "DAILY",
    asset: str = None,
    start_time: int = None,
    end_time: int = None,
    limit: int = 1000,
):
    """
    Returns earned interest history from Binance Flexible Savings.
    If union=True, uses the union interest history endpoint.
    """
    try:
        return binance_service.get_lending_interest_history(
            lending_type=lending_type,
            asset=asset,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get_dust_conversion_history")
def get_dust_conversion_history(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
):
    """
    Returns small-balance (dust) conversion history from Binance.
    """
    try:
        return binance_service.get_dust_log()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------- POST endpoints below -------------------


@app.post("/fetch_and_store_prices")
def fetch_prices_endpoint(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    symbol: str = Query(default="BTCUSDT", description="Trading symbol, e.g. BTCUSDT"),
    interval: str = Query(
        default="1d",
        description=("Price interval: (1,3,5,15,30)m, (1,2,4,6,8,12)h, (1,3)d, 1w, 1M"),
    ),
    start_time: str | None = Query(
        default=None,
        description="YYYY-MM-DD HH:MM format or UNIX timestamp in ms",
    ),
    end_time: str | None = Query(
        default="2025-05-01 00:00",
        description="YYYY-MM-DD HH:MM format or UNIX timestamp in ms",
    ),
    limit: int = Query(
        1000, ge=1, le=1000, description="Number of prices to fetch (max 1000)"
    ),
):
    prices = binance_service.fetch_prices(symbol, interval, start_time, end_time, limit)
    if not prices:
        raise HTTPException(status_code=404, detail="No prices found")

    saved_count = 0
    if len(prices):
        print(f"prices[0]: {prices[0]}")
    if len(prices) == 1000:
        print(f"prices[999]: {prices[999]}")
    else:
        print("prices[999] not exist")

    for price in prices:
        if not crud.candle_exists(
            db_session, price["symbol"], price["interval"], price["time"]
        ):
            crud.create_candle(db_session, price)
            saved_count += 1

    return {"message": f"Fetched {len(prices)} prices, saved {saved_count}"}


@app.post("/fetch_and_store_prices_stream")
def fetch_prices_stream_endpoint(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    symbol: str = Query("BTCUSDT", description="Trading symbol, e.g. BTCUSDT"),
    interval: str = Query("1d", description="Price interval, e.g. 1m, 1h, 1d"),
    end_time: str | None = Query(
        default=None, description="ISO8601 timestamp or UNIX timestamp in ms"
    ),
    max_requests: int = Query(
        0,
        ge=0,
        description=(
            "Number of requests (batches) to execute; "
            "0 = until the end of available data"
        ),
    ),
):
    start = time.perf_counter()
    total_saved = 0
    total_fetched = 0
    for prices_batch in binance_service.fetch_prices_stream(
        symbol,
        interval,
        batch_size=1000,
        end_time=end_time,
        max_requests=max_requests,
    ):
        for price in prices_batch:
            if not crud.candle_exists(
                db_session, price["symbol"], price["interval"], price["time"]
            ):
                crud.create_candle(db_session, price)
                total_saved += 1
        total_fetched += len(prices_batch)
    elapsed = time.perf_counter() - start
    return {
        "message": (
            f"Fetched {total_fetched} prices from stream, "
            f"saved {total_saved} to database. "
            f"Elapsed time: "
            f"{int(elapsed) // 3600:02d}:"
            f"{int(elapsed) % 3600 // 60:02d}:"
            f"{int(elapsed) % 60:02d}."
        )
    }


@app.post("/fetch_and_store_trades")
async def get_binance_trades(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    database: Annotated[Database, Depends(get_db)],
    symbol: str = Query(default="BTCUSDT", description="Trading symbol, e.g. BTCUSDT"),
    start_time: str = Query(None, description="Start date in YYYY-MM-DD format"),
    end_time: str = Query(None, description="End date in YYYY-MM-DD format"),
):
    start_ts = timestamp_from_str(start_time)
    end_ts = timestamp_from_str(end_time)
    try:
        trades = binance_service.fetch_all_trades_for_symbol(symbol, start_ts, end_ts)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    print(f"Fetched {len(trades)} trades for symbol {symbol}.")
    if not trades:
        raise HTTPException(
            status_code=404, detail="No trades found for the specified symbol."
        )
    stored_trades = database.store_trades(db_session=db_session, trades=trades)
    print(f"Stored {len(stored_trades)} trades for symbol {symbol}.")
    return {
        "Stored trades": len(stored_trades),
        "Fetched trades": len(trades),
        "symbol": symbol,
    }


@app.post("/fetch_and_store_trades_for_all_symbols")
async def fetch_and_store_trades_for_all_symbols(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    database: Annotated[Database, Depends(get_db)],
    start_time: str = Query(None, description="Start date in YYYY-MM-DD format"),
    end_time: str = Query(None, description="End date in YYYY-MM-DD format"),
):
    try:
        start_dt = datetime_from_str(start_time)
        end_dt = datetime_from_str(end_time)
        filters_for_xlsx = []
        filters_for_csv = []
        if start_dt:
            filters_for_xlsx.append(models.TradesFromXlsx.date_utc >= start_dt)
            filters_for_csv.append(models.TradesFromCsv.date_utc >= start_dt)
        if end_dt:
            filters_for_xlsx.append(models.TradesFromXlsx.date_utc <= end_dt)
            filters_for_csv.append(models.TradesFromCsv.date_utc <= end_dt)
        pairs_from_xlsx = (
            db_session.query(models.TradesFromXlsx.pair)
            .filter(*filters_for_xlsx)
            .distinct()
            .all()
        )
        symbols_from_xlsx = set(row[0].replace("/", "") for row in pairs_from_xlsx)
        print(f"Unique pairs found in XLSX: {symbols_from_xlsx}")
        pairs_from_csv = (
            db_session.query(models.TradesFromCsv.pair)
            .filter(*filters_for_csv)
            .distinct()
            .all()
        )
        symbols_from_csv = set(row[0] for row in pairs_from_csv)
        print(f"Unique pairs found in CSV: {symbols_from_csv}")
        symbols = list(symbols_from_xlsx | symbols_from_csv)
        print(f"Unique pairs found in database: {symbols}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    results = []
    for symbol in symbols:
        data = binance_service.fetch_all_trades_for_symbol(
            symbol=symbol,
            start_time=timestamp_from_str(start_time),
            end_time=timestamp_from_str(end_time),
        )
        results.extend(data)
        await asyncio.sleep(0.5)
    stored_trades = database.store_trades(db_session=db_session, trades=results)
    if not results:
        raise HTTPException(status_code=404, detail="No trades found for any symbol.")
    return {
        "Stored trades": len(stored_trades),
        "Fetched trades": len(results),
    }


@app.post("/fetch_and_store_all_deposits")
def fetch_and_store_all_deposits(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    database: Annotated[Database, Depends(get_db)],
    db_session: Annotated[Session, Depends(get_db_session)],
    asset: str | None = None,
    earliest_date: str = "2017-07-01",
    latest_date: str | None = None,
):
    """
    Fetches all deposits from Binance (with pagination)
    and stores unique ones in the database.
    Returns both the number fetched and the number actually stored.
    """
    result = binance_service.get_all_deposits(
        asset=asset, earliest_date=earliest_date, latest_date=latest_date
    )
    if result["status"] != "success":
        return result
    fetched_count = result["count"]
    stored_count = database.store_deposits(db_session, result["data"])
    return {"fetched": fetched_count, "stored": stored_count}


@app.post("/fetch_and_store_all_withdrawals")
def fetch_and_store_all_withdrawals(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    database: Annotated[Database, Depends(get_db)],
    db_session: Annotated[Session, Depends(get_db_session)],
    asset: str | None = None,
    earliest_date: str = "2017-07-01",
    latest_date: str | None = None,
):
    """
    Fetches all withdrawals from Binance (with pagination) and stores unique
    ones in the database. Returns both the number fetched and the number
    actually stored.
    """
    result = binance_service.get_all_withdrawals(
        asset=asset, earliest_date=earliest_date, latest_date=latest_date
    )
    if result["status"] != "success":
        return result
    fetched_count = result["count"]
    stored_count = database.store_withdrawals(db_session, result["data"])
    return {"fetched": fetched_count, "stored": stored_count}


@app.post("/upload-xlsx")
async def upload_xlsx(
    db_session: Annotated[Session, Depends(get_db_session)],
    file: UploadFile = File(...),
):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported.")
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents), engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading Excel file: {e}")
    # Adjust these columns to match your Transaction model
    required_columns = {
        "Date(UTC)",
        "Pair",
        "Base Asset",
        "Quote Asset",
        "Type",
        "Price",
        "Amount",
        "Total",
        "Fee",
        "Fee Coin",
    }
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {missing}",
        )
    records = []
    for _, row in df.iterrows():
        try:
            trade = models.TradesFromXlsx(
                date_utc=row["Date(UTC)"],
                pair=row["Pair"],
                base_asset=row["Base Asset"],
                quote_asset=row["Quote Asset"],
                type=row["Type"].lower(),
                price=float(row["Price"]),
                amount=float(row["Amount"]),
                total=float(row["Total"]),
                fee=float(row["Fee"]),
                fee_coin=row["Fee Coin"],
            )
            records.append(trade)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error parsing row: {e}")
    try:
        db_session.bulk_save_objects(records)
        db_session.commit()
    except SQLAlchemyError as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    return {"inserted": len(records)}


@app.post("/upload-csv")
async def upload_csv(
    db_session: Annotated[Session, Depends(get_db_session)],
    file: UploadFile = File(...),
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are supported.")
    try:
        contents = await file.read()
        df = pd.read_csv(StringIO(contents.decode("utf-8")))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading CSV file: {e}")
    required_columns = {
        "Date(UTC)",
        "Pair",
        "Side",
        "Price",
        "Executed",
        "Amount",
        "Fee",
    }
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise HTTPException(
            status_code=400, detail=f"Missing required columns: {missing}"
        )

    records = []
    for _, row in df.iterrows():
        try:
            trade = models.TradesFromCsv(
                date_utc=pd.to_datetime(row["Date(UTC)"]),
                pair=str(row["Pair"]),
                side=str(row["Side"]),
                price=float(row["Price"]),
                executed=str(row["Executed"]),
                amount=str(row["Amount"]),
                fee=str(row["Fee"]),
            )
            records.append(trade)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error parsing row: {e}")
    try:
        db_session.bulk_save_objects(records)
        db_session.commit()
    except SQLAlchemyError as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    return {"inserted": len(records)}


@app.get("/simple_earn/flexible/redemption_record")
def flexible_redemption_record(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    product_id: str = None,
    redeem_id: str = None,
    asset: str = None,
    start_time: int = None,
    end_time: int = None,
    current: int = 1,
    size: int = 10,
):
    try:
        return binance_service.get_flexible_redemption_record(
            product_id=product_id,
            redeem_id=redeem_id,
            asset=asset,
            start_time=start_time,
            end_time=end_time,
            current=current,
            size=size,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/simple_earn/flexible/position")
def simple_earn_flexible_position(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    asset: str = None,
):
    """
    Returns current Simple Earn Flexible positions from Binance.
    """
    try:
        return binance_service.get_flexible_product_position(asset=asset)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

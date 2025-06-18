import asyncio
import time
from typing import Annotated
from fastapi import FastAPI, Depends, Query, HTTPException, UploadFile, File
from sqlalchemy import or_, and_
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.database import Base, Database, get_db, database
from app import models, crud
from app.binance_service import fetch_prices, fetch_prices_stream, fetch_trades, get_account_info
from dotenv import load_dotenv
import pandas as pd
from io import BytesIO, StringIO
from datetime import datetime

load_dotenv()

app = FastAPI(
    title="CryptoPortfolioTracker API",
    description="API to track your crypto portfolio and transactions.",
    version="0.1.0"
)


@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/get_account")
def get_account():
    try:
        return get_account_info()        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/fetch_and_store_prices")
def fetch_prices_endpoint(
    db_session: Annotated[Session, Depends(database.get_db_session)],
    symbol: str = Query(default="BTCUSDT", description="Trading symbol, e.g. BTCUSDT"),
    interval: str = Query(default="1d", description="Price interval: (1,3,5,15,30)m, (1,2,4,6,8,12)h, (1,3)d, 1w, 1M"),
    start_time: str | None = Query(default=None, description="YYYY-MM-DD HH:MM format or UNIX timestamp in ms"),
    end_time: str | None = Query(default="2025-05-01 00:00", description="YYYY-MM-DD HH:MM format or UNIX timestamp in ms"),
    limit: int = Query(1000, ge=1, le=1000, description="Number of prices to fetch (max 1000)"),        
):    
    prices = fetch_prices(symbol, interval, start_time, end_time, limit)
    if not prices:
        raise HTTPException(status_code=404, detail="No prices found")

    saved_count = 0
    if len(prices):
        print(f"prices[0]: {prices[0]}")
    if len(prices)==1000:
        print(f"prices[999]: {prices[999]}")
    else:
        print("prices[999] not exist")

    for price in prices:
        if not crud.candle_exists(db_session, price["symbol"], price["interval"], price["time"]):
            crud.create_candle(db_session, price)
            saved_count += 1

    return {"message": f"Fetched {len(prices)} prices, saved {saved_count}"}

@app.post("/fetch_and_store_prices_stream")
def fetch_prices_stream_endpoint(
    db_session: Annotated[Session, Depends(database.get_db_session)],
    symbol: str = Query("BTCUSDT", description="Trading symbol, e.g. BTCUSDT"),
    interval: str = Query("1d", description="Price interval, e.g. 1m, 1h, 1d"),    
    end_time: str | None = Query(default=None, description="ISO8601 timestamp or UNIX timestamp in ms"),    
    max_requests: int = Query(0, ge=0, description="Number of requests (batchów) do wykonania; 0 = do końca dostępnych danych"),    
):    
    start = time.perf_counter()
    total_saved = 0
    total_fetched = 0
    for prices_batch in fetch_prices_stream(symbol, interval, batch_size=1000, end_time=end_time, max_requests=max_requests):
        for price in prices_batch:
            if not crud.candle_exists(db_session, price["symbol"], price["interval"], price["time"]):
                crud.create_candle(db_session, price)
                total_saved += 1
        total_fetched += len(prices_batch)
    elapsed = time.perf_counter() - start        
    return {"message": f"Fetched {total_fetched} prices from stream, saved {total_saved} to database. Elapsed time: {int(elapsed) // 3600:02d}:{int(elapsed) % 3600 // 60:02d}:{int(elapsed) % 60:02d}."}



def chunked(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    iterable = list(iterable)
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]


def store_trades(db: Session, trades: list) -> int:
    try:
        fields = trades[0].keys() if trades else []
        print(f"Fields in trades: {fields}")
        # Get all order_ids (or composite keys) from incoming trades
        incoming_trades = set((trade.get("id"), trade.get("symbol")) for trade in trades)
        print(f"Incoming keys: {list(incoming_trades)[:5]}... (total {len(incoming_trades)})")
        existing_trades = set()
        batch_size = 50  # lub 100, zależnie od wydajności
        for batch in chunked(incoming_trades, batch_size):
            conditions = [and_(models.TradesFromApi.id == id_, models.TradesFromApi.symbol == symbol) for id_, symbol in batch]
            rows = db.query(models.TradesFromApi.id, models.TradesFromApi.symbol).filter(or_(*conditions)).all()
            existing_trades.update(rows)
        print(f"Existing keys: {list(existing_trades)[:5]}... (total {len(existing_trades)})")

        
        # Filter out trades that already exist
        unique_trades = [models.create_model_instance_from_dict(model_class=models.TradesFromApi, data=trade) for trade in trades if (trade.get("id"), trade.get("symbol")) not in existing_trades]
        print(f"Unique trades to insert: {unique_trades[:5]}... (total {len(unique_trades)})")

        # Bulk insert only unique trades
        db.bulk_save_objects(unique_trades)
        db.commit()
        return unique_trades
    except SQLAlchemyError as e:
        db.rollback()
        print(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"DB error: {str(e)}")
    

def to_datetime(date_str:str | None) -> datetime | None:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {date_str}. Use YYYY-MM-DD.")


def to_timestamp(date_str: str | None) -> int | None:
    if not date_str:
        return None
    return int(to_datetime(date_str=date_str).timestamp() * 1000)
    

@app.post("/fetch_and_store_trades")
async def get_binance_trades(
    db_session: Annotated[Session, Depends(database.get_db_session)],
    symbol: str = Query(default="BTCUSDT", description="Trading symbol, e.g. BTCUSDT"),
    start_time: str = Query(None, description="Start date in YYYY-MM-DD format"),
    end_time: str = Query(None, description="End date in YYYY-MM-DD format"),    
):    
    start_ts = to_timestamp(start_time)
    end_ts = to_timestamp(end_time)
    try:
        trades = fetch_trades(symbol, start_ts, end_ts)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    print(f"Fetched {len(trades)} trades for symbol {symbol}.")
    # print(f"Trades: {trades}")
    if not trades:
        raise HTTPException(status_code=404, detail="No trades found for the specified symbol.")    
    # Store trades in the database       
    stored_trades = store_trades(db=db_session, trades=trades)
    print(f"Stored {len(stored_trades)} trades for symbol {symbol}.")
    # Optionally, return the stored trades or the fetched trades
    return {"Stored trades": len(stored_trades), "Fetched trades": len(trades), "symbol": symbol}


@app.post("/upload-xlsx")
async def upload_xlsx(db_session: Annotated[Session, Depends(database.get_db_session)], file: UploadFile = File(...)):    
    if not file.filename.endswith('.xlsx'):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported.")
    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents), engine='openpyxl')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading Excel file: {e}")
    # Adjust these columns to match your Transaction model
    required_columns = {"Date(UTC)", "Pair", "Base Asset", "Quote Asset", "Type", "Price", "Amount", "Total", "Fee", "Fee Coin"}
    if not required_columns.issubset(df.columns):
        raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")
    records = []
    for _, row in df.iterrows():
        try:
            trade = models.TradesFromXlsx(
                date_utc=row["Date(UTC)"],
                pair=row["Pair"],
                base_asset=row["Base Asset"],
                quote_asset=row["Quote Asset"],
                type=row["Type"].lower(),  # Ensure type is lowercase
                price=float(row["Price"]),
                amount=float(row["Amount"]),
                total=float(row["Total"]),
                fee=float(row["Fee"]),
                fee_coin=row["Fee Coin"]
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
async def upload_csv(db_session: Annotated[Session, Depends(database.get_db_session)], file: UploadFile = File(...)):    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only .csv files are supported.")
    try:
        contents = await file.read()
        df = pd.read_csv(StringIO(contents.decode('utf-8')))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading CSV file: {e}")
    required_columns = {"Date(UTC)", "Pair", "Side", "Price", "Executed", "Amount", "Fee"}
    if not required_columns.issubset(df.columns):
        raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")

    records = []
    for _, row in df.iterrows():
        try:
            trade = models.TradesFromCsv(
                date_utc=pd.to_datetime(row["Date(UTC)"]),
                pair=row["Pair"],
                side=row["Side"],
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


@app.post("/fetch_and_store_trades_for_all_symbols")
async def fetch_and_store_trades_for_all_symbols(
    db_session: Annotated[Session, Depends(database.get_db_session)],
    start_time: str = Query(None, description="Start date in YYYY-MM-DD format"),
    end_time: str = Query(None, description="End date in YYYY-MM-DD format"),    
    ):
    try:        
        # Pobierz unikalne pary z bazy
        start_dt = to_datetime(start_time)
        end_dt = to_datetime(end_time)
        filters_for_xlsx = []
        filters_for_csv = []
        if start_dt:
            filters_for_xlsx.append(models.TradesFromXlsx.date_utc >= start_dt)
            filters_for_csv.append(models.TradesFromCsv.date_utc >= start_dt)            
        if end_dt:            
            filters_for_xlsx.append(models.TradesFromXlsx.date_utc <= end_dt)        
            filters_for_csv.append(models.TradesFromCsv.date_utc <= end_dt)        
        pairs_from_xlsx = db_session.query(models.TradesFromXlsx.pair).filter(*filters_for_xlsx).distinct().all()
        symbols_from_xlsx: set = set([row[0].replace("/","") for row in pairs_from_xlsx])
        print(f"Unique pairs found in XLSX: {symbols_from_xlsx}")
        pairs_from_csv = db_session.query(models.TradesFromCsv.pair).filter(*filters_for_csv).distinct().all()
        symbols_from_csv: set = set([row[0] for row in pairs_from_csv])
        print(f"Unique pairs found in CSV: {symbols_from_csv}")
        symbols = list(symbols_from_xlsx | symbols_from_csv)
        print(f"Unique pairs found in database: {symbols}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    results = []
    for symbol in symbols:
        data = fetch_trades(symbol=symbol, start_time=to_timestamp(start_time), end_time=to_timestamp(end_time))
        results.extend(data)
        await asyncio.sleep(0.5)
    stored_trades = store_trades(db=db_session, trades=results)
    if not results:
        raise HTTPException(status_code=404, detail="No trades found for any symbol.")
    return {"Stored trades": len(stored_trades), "Fetched trades": len(results)}



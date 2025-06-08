import time
from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from app.database import SessionLocal, engine, Base
from app import models, crud
from app.binance_service import fetch_prices, fetch_prices_stream

app = FastAPI(
    title="CryptoPortfolioTracker API",
    description="API to track your crypto portfolio and transactions.",
    version="0.1.0"
)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/fetch_prices")
def fetch_prices_endpoint(
    symbol: str = Query(default="BTCUSDT", description="Trading symbol, e.g. BTCUSDT"),
    interval: str = Query(default="1d", description="Price interval: (1,3,5,15,30)m, (1,2,4,6,8,12)h, (1,3)d, 1w, 1M"),
    start_time: str | None = Query(default=None, description="YYYY-MM-DD HH:MM format or UNIX timestamp in ms"),
    end_time: str | None = Query(default="2025-05-01 00:00", description="YYYY-MM-DD HH:MM format or UNIX timestamp in ms"),
    limit: int = Query(1000, ge=1, le=1000, description="Number of prices to fetch (max 1000)"),
    db: Session = Depends(get_db)
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
        if not crud.candle_exists(db, price["symbol"], price["interval"], price["time"]):
            crud.create_candle(db, price)
            saved_count += 1

    return {"message": f"Fetched {len(prices)} prices, saved {saved_count}"}

@app.post("/fetch_prices_stream")
def fetch_prices_stream_endpoint(
    symbol: str = Query("BTCUSDT", description="Trading symbol, e.g. BTCUSDT"),
    interval: str = Query("1d", description="Price interval, e.g. 1m, 1h, 1d"),    
    end_time: str | None = Query(default=None, description="ISO8601 timestamp or UNIX timestamp in ms"),    
    max_requests: int = Query(0, ge=0, description="Number of requests (batchów) do wykonania; 0 = do końca dostępnych danych"),
    db: Session = Depends(get_db)
):
    start = time.perf_counter()
    total_saved = 0
    total_fetched = 0
    for prices_batch in fetch_prices_stream(symbol, interval, batch_size=1000, end_time=end_time, max_requests=max_requests):
        for price in prices_batch:
            if not crud.candle_exists(db, price["symbol"], price["interval"], price["time"]):
                crud.create_candle(db, price)
                total_saved += 1
        total_fetched += len(prices_batch)
    elapsed = time.perf_counter() - start        
    return {"message": f"Fetched {total_fetched} prices from stream, saved {total_saved} to database. Elapsed time: {int(elapsed) // 3600:02d}:{int(elapsed) % 3600 // 60:02d}:{int(elapsed) % 60:02d}."}

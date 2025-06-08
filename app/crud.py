from sqlalchemy.orm import Session
from app import models
from datetime import datetime

def candle_exists(db: Session, symbol: str, interval: str, time: datetime) -> bool:
    return db.query(models.PriceHistory).filter(
        models.PriceHistory.symbol == symbol,
        models.PriceHistory.interval == interval,
        models.PriceHistory.time == time
    ).first() is not None

def create_candle(db: Session, candle: dict):
    db_candle = models.PriceHistory(
        symbol=candle["symbol"],
        interval=candle["interval"],
        time=candle["time"],
        price=candle["price"],
        source=candle.get("source", "binance")
    )
    db.add(db_candle)
    db.commit()
    db.refresh(db_candle)
    return db_candle


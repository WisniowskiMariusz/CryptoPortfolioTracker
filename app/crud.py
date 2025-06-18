from sqlalchemy.orm import Session
from app import models
from datetime import datetime

def candle_exists(db_session: Session, symbol: str, interval: str, time: datetime) -> bool:
    return db_session.query(models.PriceHistory).filter(
        models.PriceHistory.symbol == symbol,
        models.PriceHistory.interval == interval,
        models.PriceHistory.time == time
    ).first() is not None

def create_candle(db_session: Session, candle: dict):
    db_candle = models.PriceHistory(
        symbol=candle["symbol"],
        interval=candle["interval"],
        time=candle["time"],
        price=candle["price"],
        source=candle.get("source", "binance")
    )
    db_session.add(db_candle)
    db_session.commit()
    db_session.refresh(db_candle)
    return db_candle


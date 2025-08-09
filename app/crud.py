from sqlalchemy.orm import Session
from app import models
from datetime import datetime, date


def candle_exists(
    db_session: Session, symbol: str, interval: str, time: datetime
) -> bool:
    return (
        db_session.query(models.PriceHistory)
        .filter(
            models.PriceHistory.symbol == symbol,
            models.PriceHistory.interval == interval,
            models.PriceHistory.time == time,
        )
        .first()
        is not None
    )


def create_candle(db_session: Session, candle: dict):
    db_candle = models.PriceHistory(
        symbol=candle["symbol"],
        interval=candle["interval"],
        time=candle["time"],
        price=candle["price"],
        source=candle.get("source", "binance"),
    )
    db_session.add(db_candle)
    db_session.commit()
    db_session.refresh(db_candle)
    return db_candle


def rate_exists(
    db_session: Session, base_currency: str, quote_currency: str, date: date
) -> bool:
    return (
        db_session.query(models.DailyPriceHistory)
        .filter(
            models.DailyPriceHistory.base_currency == base_currency,
            models.DailyPriceHistory.quote_currency == quote_currency,
            models.DailyPriceHistory.date == date,
        )
        .first()
        is not None
    )


def create_rate(db_session: Session, rate: dict):
    db_rate = models.DailyPriceHistory(
        base_currency=rate["base_currency"],
        quote_currency=rate["quote_currency"],
        date=rate["date"],
        price=rate["price"],
        source=rate["source"],
    )
    db_session.add(db_rate)
    db_session.commit()
    db_session.refresh(db_rate)
    return db_rate

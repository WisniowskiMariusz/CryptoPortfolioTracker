from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, select
from app import models
from app.tools import chunked
from datetime import datetime, date
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import IntegrityError


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


def binance_symbol_exists(db_session: Session, symbol: str) -> bool:
    return (
        db_session.query(models.BinanceSymbols)
        .filter(models.BinanceSymbols.symbol == symbol)
        .first()
        is not None
    )


def get_binance_symbol_dict(db_session: Session, symbol: str) -> dict:
    return row_to_dict(
        db_session.execute(
            select(models.BinanceSymbols).where(models.BinanceSymbols.symbol == symbol)
        ).scalar_one_or_none()
    )


def create_binance_symbol(db_session: Session, symbol_data: dict):
    db_symbol = models.BinanceSymbols(
        symbol=symbol_data["symbol"],
        status=symbol_data["status"],
        base_currency=symbol_data["baseAsset"],
        quote_currency=symbol_data["quoteAsset"],
    )
    db_session.add(db_symbol)
    db_session.commit()
    db_session.refresh(db_symbol)
    return db_symbol


def upsert_binance_symbols(db_session: Session, symbols_data: list[dict]) -> dict:
    """Store the rates in the database."""
    saved_count = 0
    updated_count = 0
    print(f"Storing {len(symbols_data)} Binance symbols in the database...")
    for symbol_data in symbols_data:
        if not binance_symbol_exists(db_session, symbol_data["symbol"]):
            create_binance_symbol(db_session, symbol_data)
            saved_count += 1
        else:
            existing_symbol = (
                db_session.query(models.BinanceSymbols)
                .filter(models.BinanceSymbols.symbol == symbol_data["symbol"])
                .first()
            )
            for key, value in symbol_data.items():
                setattr(existing_symbol, key, value)
            db_session.commit()
            updated_count += 1
    return {
        "saved_symbols": saved_count,
        "updated_symbols": updated_count,
    }


def get_ticker(db_session: Session, ticker: str, venue: str) -> models.Tickers | None:
    return db_session.execute(
        select(models.Tickers).where(
            models.Tickers.ticker == ticker, models.Tickers.venue == venue
        )
    ).scalar_one_or_none()


def ticker_exists(db_session: Session, ticker: str, venue: str) -> bool:
    return get_ticker(db_session, ticker, venue) is not None


def create_ticker_record(db_session: Session, ticker: str, venue: str):
    ticker_record = models.Tickers(
        ticker=ticker,
        venue=venue,
        base_asset=ticker.split("-")[0] if "-" in ticker else None,
        quote_asset=ticker.split("-")[1] if "-" in ticker else None,
    )
    db_session.add(ticker_record)
    db_session.commit()
    db_session.refresh(ticker_record)
    return ticker_record


def upsert_tickers(db_session: Session, tickers: list, venue: str) -> dict:
    """Store the tickers in the database."""
    saved_count = 0
    updated_count = 0
    print(f"Storing {len(tickers)} tickers in the database...")
    for ticker in tickers:
        existing_ticker = get_ticker(db_session, ticker, venue)
        if existing_ticker is None:
            create_ticker_record(db_session, ticker, venue)
            saved_count += 1
            continue
        existing_ticker.base_asset = ticker.split("-")[0] if "-" in ticker else None
        existing_ticker.quote_asset = ticker.split("-")[1] if "-" in ticker else None
        db_session.commit()
        updated_count += 1
    return {
        "saved_tickers": saved_count,
        "updated_tickers": updated_count,
    }


def get_user(db_session: Session, name: str) -> models.Users | None:
    return db_session.execute(
        select(models.Users).where(models.Users.name == name)
    ).scalar_one_or_none()


def get_user_id(db_session: Session, name: str) -> str:
    user = get_user(db_session, name)
    if user:
        return str(user.id)
    raise ValueError(f"User with name '{name}' does not exist.")


def get_exchange(db_session: Session, name: str) -> models.Exchanges | None:
    return db_session.execute(
        select(models.Exchanges).where(models.Exchanges.name == name)
    ).scalar_one_or_none()


def get_exchange_id(db_session: Session, name: str) -> str:
    exchange = get_exchange(db_session, name)
    if exchange:
        return str(exchange.id)
    raise ValueError(f"Exchange with name '{name}' does not exist.")


def get_all_users(db_session: Session) -> list[dict]:
    """Return a list of all users from the Users table."""
    users = db_session.execute(select(models.Users)).scalars().all()
    return [
        {
            "user_id": user.id,
            "user_name": user.name,
        }
        for user in users
    ]


def get_all_exchanges(db_session: Session) -> list[dict]:
    """Return a list of all exchanges from the Exchanges table."""
    exchanges = db_session.execute(select(models.Exchanges)).scalars().all()
    return [
        {
            "exchange_id": exchange.id,
            "exchange_name": exchange.name,
        }
        for exchange in exchanges
    ]


def user_exists(db_session: Session, name: str) -> bool:
    return get_user(db_session=db_session, name=name) is not None


def exchange_exists(db_session: Session, name: str) -> bool:
    return get_exchange(db_session=db_session, name=name) is not None


def create_user_record(db_session: Session, name: str):
    user_record = models.Users(name=name)
    db_session.add(user_record)
    db_session.commit()
    db_session.refresh(user_record)
    return user_record


def create_exchange_record(db_session: Session, name: str):
    exchange_record = models.Exchanges(name=name)
    db_session.add(exchange_record)
    db_session.commit()
    db_session.refresh(exchange_record)
    return exchange_record


def upsert_user(db_session: Session, name: str) -> int:
    """Store the tickers in the database."""
    print(f"Adding {name} user to the database...")
    existing_user = get_user(db_session=db_session, name=name)
    if existing_user is None:
        create_user_record(db_session=db_session, name=name)
        db_session.commit()
        return get_user_id(db_session, name)
    return existing_user.id


def upsert_exchange(db_session: Session, name: str) -> int:
    """Store the exchange in the database."""
    print(f"Adding {name} exchange to the database...")
    existing_exchange = get_exchange(db_session=db_session, name=name)
    if existing_exchange is None:
        create_exchange_record(db_session=db_session, name=name)
        db_session.commit()
        return get_exchange_id(db_session, name)
    return existing_exchange.id


def get_trade_record(
    db_session: Session,
    exchange_id: int,
    user_id: int,
    trade_id: str,
    utc_time: datetime,
) -> models.Trades | None:
    return db_session.execute(
        select(models.Trades).where(
            models.Trades.exchange_id == exchange_id,
            models.Trades.user_id == user_id,
            models.Trades.id == trade_id,
            models.Trades.utc_time == utc_time,
        )
    ).scalar_one_or_none()


def trade_record_exists(
    db_session: Session,
    trade_id: str,
) -> bool:
    return (
        get_trade_record(
            db_session=db_session,
            trade_id=trade_id,
        )
        is not None
    )


def create_trade_record(db_session: Session, trade_data: dict):
    trade_record = models.Trades(
        time=trade_data["time"],
        id=trade_data["id"],
        price=trade_data["price"],
        bought_currency=trade_data["bought_currency"],
        bought_amount=trade_data["bought_amount"],
        sold_currency=trade_data["sold_currency"],
        sold_amount=trade_data["sold_amount"],
        fee_amount=trade_data["fee_amount"],
        fee_currency=trade_data["fee_currency"],
        exchange_id=trade_data["exchange"],
        user_id=trade_data["user_id"],
    )
    db_session.add(trade_record)
    db_session.commit()
    db_session.refresh(trade_record)
    return trade_record


def upsert_trade_records(
    db_session: Session, user: str, exchange: str, trades_data: list[dict]
) -> dict:
    """Store trades in the database using a single bulk insert for new records."""
    print(f"Storing {len(trades_data)} trades in the database...")

    # Build set of keys to check existing records in one query

    try:
        user_id = get_user_id(db_session, user)
    except ValueError:
        user_id = upsert_user(db_session, user)
    try:
        exchange_id = get_exchange_id(db_session, exchange)
    except ValueError:
        exchange_id = upsert_exchange(db_session, exchange)

    # print(f"trades_data[0]: {trades_data[0]}")
    for trade in trades_data:
        if "id" not in trade or "original_id" not in trade:
            print(trade)
            if "message" in trade:
                return {"message": trade["message"]}
            raise ValueError(f"Trade data missing 'id' or 'original_id': {trade}")
    keys = [(trade["id"], trade["original_id"]) for trade in trades_data]
    existing_keys = set()
    existing_ids_empty_original = set()
    existing_ids_not_empty_original = set()
    print(f"Checking existing trade records for {len(keys)} keys...")
    if keys:
        batch_size = 300
        for keys_chunk in chunked(keys, batch_size):
            conditions = [
                and_(
                    models.Trades.id == key[0],
                    models.Trades.original_id == key[1],
                    models.Trades.exchange_id == exchange_id,
                    models.Trades.user_id == user_id,
                )
                for key in keys_chunk
            ]

            stmt_tuple = select(
                models.Trades.id,
                models.Trades.original_id,
                models.Trades.user_id,
                models.Trades.exchange_id,
            ).where(or_(*conditions))
            rows = db_session.execute(stmt_tuple).tuples().all()
            existing_keys.update(set(rows))
            stmt_ids = select(models.Trades.id, models.Trades.original_id).where(
                models.Trades.id.in_([key[0] for key in keys_chunk]),
                models.Trades.exchange_id == exchange_id,
                models.Trades.user_id == user_id,
            )
            rows_ids = db_session.execute(stmt_ids).tuples().all()
            rows_ids_empty_original = [row[0] for row in rows_ids if row[1] == ""]
            rows_ids_not_empty_original = [row[0] for row in rows_ids if row[1] != ""]
            existing_ids_empty_original.update(set(rows_ids_empty_original))
            existing_ids_not_empty_original.update(set(rows_ids_not_empty_original))

    print(f"Found {len(existing_keys)} existing trade records in the database.")
    print(
        f"Found {len(existing_ids_empty_original)} existing trade IDs "
        "with empty original_id."
    )
    print(
        f"Found {len(existing_ids_not_empty_original)} existing trade IDs "
        "with non-empty original_id."
    )
    # Prepare mappings for insertion (skip existing)
    to_insert = []
    to_update = []
    duplicate = []
    seen_insert_keys = set()
    for trade in trades_data:
        if (trade["id"], trade["original_id"]) in existing_keys:
            duplicate.append(trade)
            continue
        if trade["id"] in existing_ids_empty_original:
            if trade["original_id"] == "":
                duplicate.append(trade)
                continue
            to_update.append(trade)
            continue
        trade.update(
            {
                "exchange_id": exchange_id,
                "user_id": user_id,
            }
        )
        insert_key = (trade["id"], trade["utc_time"])
        if insert_key in seen_insert_keys:
            duplicate.append(trade)
            continue
        seen_insert_keys.add(insert_key)
        if trade["id"] in existing_ids_not_empty_original:
            duplicate.append(trade)
            continue
        to_insert.append(trade)
    try:
        if duplicate:
            print(f"Found {len(duplicate)} duplicate trades.")
            # print(f"Duplicate trades: {duplicate}")
        if to_insert:
            print(f"Inserting {len(to_insert)} new trades...")
            # print(f"Trades to insert: {to_insert}")
            db_session.bulk_insert_mappings(models.Trades, to_insert)
        if to_update:
            print(f"Updating {len(to_update)} existing trades...")
            # print(f"Trades to update: {to_update}")
            for trade in to_update:
                row = (
                    db_session.query(models.Trades)
                    .filter(
                        models.Trades.id == trade["id"], models.Trades.original_id == ""
                    )
                    .first()
                )
                if row:
                    row.utc_time = trade["utc_time"]
                    row.original_id = trade["original_id"]
                    db_session.commit()
        db_session.commit()
    except IntegrityError as ie:
        db_session.rollback()
        print(f"IntegrityError during upsert_trade_records: {ie}")
        if duplicate:
            print(f"Example of duplicate trade: {duplicate[0]}")
        else:
            print("No duplicate trades found.")
        return {
            "fetched_trades": len(trades_data),
            "to_insert_trades": len(to_insert),
            "to_update_trades": len(to_update),
            "duplicate_trades": len(duplicate),
            "sum_check": len(to_insert) + len(to_update) + len(duplicate),
            "message": ie.args[0],
        }

    return {
        "fetched_trades": len(trades_data),
        "inserted_trades": len(to_insert),
        "updated_trades": len(to_update),
        "duplicate_trades": len(duplicate),
        "sum_check": len(to_insert) + len(to_update) + len(duplicate),
    }


def get_first_trade_for_date_with_no_empty_original_id(
    db_session: Session,
    exchange: str,
    user: str,
    date: str,
) -> bool:
    date_obj = datetime.fromisoformat(date).date()
    start_datetime = datetime.combine(date_obj, datetime.min.time())
    end_datetime = datetime.combine(date_obj, datetime.max.time())
    exchange_id = get_exchange_id(db_session, exchange)
    user_id = get_user_id(db_session, user)
    return (
        db_session.query(models.Trades)
        .filter(
            models.Trades.exchange_id == exchange_id,
            models.Trades.user_id == user_id,
            models.Trades.utc_time >= start_datetime,
            models.Trades.utc_time <= end_datetime,
            models.Trades.original_id != "",
        )
        .first()
    )


def trade_exists_for_date_no_empty_original_id(
    db_session: Session,
    exchange: str,
    user: str,
    date: str,
) -> bool:
    trade: models.Trades = get_first_trade_for_date_with_no_empty_original_id(
        db_session=db_session, exchange=exchange, user=user, date=date
    )
    # if trade:
    #     print(trade.to_dict())
    # print(trade is not None)
    return trade is not None


def get_trades_for_date_with_empty_original_id(
    db_session: Session,
    exchange: str,
    user: str,
    date: str,
) -> list[models.Trades] | None:
    date_obj = datetime.fromisoformat(date).date()
    start_datetime = datetime.combine(date_obj, datetime.min.time())
    end_datetime = datetime.combine(date_obj, datetime.max.time())
    exchange_id = get_exchange_id(db_session, exchange)
    user_id = get_user_id(db_session, user)
    return (
        db_session.query(models.Trades)
        .filter(
            models.Trades.exchange_id == exchange_id,
            models.Trades.user_id == user_id,
            models.Trades.utc_time >= start_datetime,
            models.Trades.utc_time <= end_datetime,
            models.Trades.original_id == "",
        )
        .all()
    )


def row_to_dict(row):
    return {c.key: getattr(row, c.key) for c in inspect(row).mapper.column_attrs}

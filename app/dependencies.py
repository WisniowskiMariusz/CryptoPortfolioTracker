from typing import Annotated
from fastapi import Depends
from functools import lru_cache
from app.binance_service import BinanceService
from app.database import Database


def get_binance_service():
    return BinanceService()


@lru_cache()
def get_db() -> Database:
    return Database()


def get_db_session(database: Annotated[Database, Depends(get_db)]):
    yield from database.get_db_session()

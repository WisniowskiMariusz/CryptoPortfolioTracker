from typing import Annotated
from fastapi import Depends
from functools import lru_cache
from app.binance_service import BinanceService
from app.kanga_service import KangaService
from app.database import Database
from app.nbp_service import NbpService


def get_binance_service():
    return BinanceService()


def get_kanga_service():
    return KangaService()


@lru_cache()
def get_nbp_service():
    return NbpService()


@lru_cache()
def get_db() -> Database:
    return Database()


def get_db_session(database: Annotated[Database, Depends(get_db)]):
    yield from database.get_db_session()

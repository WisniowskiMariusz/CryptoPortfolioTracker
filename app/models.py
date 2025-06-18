import os
from sqlalchemy import Column, Integer, BigInteger, VARCHAR, String, Float, DateTime, PrimaryKeyConstraint
from sqlalchemy.dialects.mssql import SMALLDATETIME
from sqlalchemy.inspection import inspect 
from app.database import Base

# Use SMALLDATETIME only if SQL Server is being used
transaction_time_type = SMALLDATETIME if os.getenv("USE_SQL_SERVER", "true").lower() == "true" else DateTime

class TradedSymbols(Base):
    __tablename__ = "traded_symbols"    
    symbol = Column(VARCHAR(20), primary_key=True, unique=True, index=True)


class TradesFromApi(Base):
    __tablename__ = "trades_from_api"

    id = Column(BigInteger, name="id", index=True)
    symbol = Column(VARCHAR(20), name="symbol", index=True)    
    price = Column(Float, name="price")
    qty = Column(Float, name="qty")
    quote_qty = Column(Float, name="quoteQty")
    commission = Column(Float, name="commission")
    commission_asset = Column(String, name="commissionAsset")
    time = Column(DateTime, name="time", index=True)  
    is_buyer = Column(Integer, name="isBuyer")  # 0 or 1
    is_maker = Column(Integer, name="isMaker")  # 0 or 1    
    is_best_match = Column(Integer, name="isBestMatch")  # 0 or 1
    __table_args__ = (
        PrimaryKeyConstraint('id', 'symbol', name='pk_id_symbol'),
    )


class TradesFromXlsx(Base):
    __tablename__ = "trades_from_xlsx"

    id = Column(Integer, primary_key=True, index=True)
    date_utc = Column(DateTime, name="Date(UTC)", index=True)  # Date(UTC)
    pair = Column(String(20), name="Pair", index=True)  # Pair
    base_asset = Column(String(20), name="Base Asset")  # Base Asset
    quote_asset = Column(String(20), name="Quote Asset")  # Quote Asset
    type = Column(String(10), name="Type")  # Type (buy/sell)
    price = Column(Float, name="Price")  # Price
    amount = Column(Float, name="Amount")  # Amount
    total = Column(Float, name="Total")  # Total
    fee = Column(Float, name="Fee")  # Fee
    fee_coin = Column(String(20), name="Fee Coin")  # Fee Coin


class TradesFromCsv(Base):
    __tablename__ = "trades_from_csv"

    id = Column(Integer, primary_key=True, index=True)
    date_utc = Column(DateTime, name="Date(UTC)", index=True)
    pair = Column(String(20), name="Pair", index=True)
    side = Column(String(10), name="Side")
    price = Column(Float, name="Price")
    executed = Column(String(20), name="Executed")  
    amount = Column(String(20), name="Amount")    
    fee = Column(String(20), name="Fee")


class PriceHistory(Base):
    __tablename__ = "price_history"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), index=True)
    interval = Column(String(5), index=True)
    time = Column(transaction_time_type, index=True)          
    price = Column(Float)                      
    source = Column(String(20), default="binance")

def create_model_instance_from_dict(model_class, data: dict, key_map: dict | None=None):
    """
    Tworzy instancję modelu SQLAlchemy na podstawie słownika danych.
    key_map: opcjonalny słownik mapujący klucze z danych na pola modelu.
    """    
    key_map = key_map or {}
    db_column_names = [column.name for column in inspect(model_class).mapper.columns]    
    py_attr_names = [c.key for c in inspect(model_class).mapper.column_attrs]    
    for name, key in zip(py_attr_names, db_column_names):
        if name != key:
            key_map[key] = name    
    filtered_data = {}
    for k, v in data.items():        
        model_key = key_map.get(k, k)
        if model_key in py_attr_names:
            filtered_data[model_key] = v
    return model_class(**filtered_data)
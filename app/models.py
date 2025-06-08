import os
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.dialects.mssql import SMALLDATETIME 
from app.database import Base

# Use SMALLDATETIME only if SQL Server is being used
transaction_time_type = SMALLDATETIME if os.getenv("USE_SQL_SERVER", "true").lower() == "true" else DateTime

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), index=True)  # np. 'BTCUSDT'
    transaction_time = Column(transaction_time_type, index=True)
    transaction_type = Column(String(10))  # np. 'buy', 'sell'
    quantity = Column(Float)
    price = Column(Float)  # cena za 1 jednostkę
    fee = Column(Float, nullable=True)  # prowizja

class PriceHistory(Base):
    __tablename__ = "price_history"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), index=True)
    interval = Column(String(5), index=True)
    time = Column(transaction_time_type, index=True)          
    price = Column(Float)                      
    source = Column(String(20), default="binance")

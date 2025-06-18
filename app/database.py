import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect

use_sql = os.getenv("USE_SQL_SERVER", "true").lower() == "true"
if use_sql:    
    engine = create_engine(url="mssql+pyodbc://localhost/crypto-tracker?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes")
else:
    engine = create_engine(url="sqlite:///./crypto-tracker.db", connect_args={"check_same_thread": False})  # potrzebne dla SQLite

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class Database:
    def __init__(self):
        use_sql = os.getenv("USE_SQL_SERVER", "true").lower() == "true"
        if use_sql:
            self.engine = create_engine(
                url="mssql+pyodbc://localhost/crypto-tracker?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes"
            )
        else:
            self.engine = create_engine(
                url="sqlite:///./crypto-tracker.db",
                connect_args={"check_same_thread": False}
            )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_db_session(self):
        db_session = self.SessionLocal()
        try:
            yield db_session
        finally:
            db_session.close()

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
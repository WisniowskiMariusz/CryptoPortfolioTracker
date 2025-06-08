import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

# SQLALCHEMY_DATABASE_URL = "sqlite:///./crypto_tracker.db"

# SQLALCHEMY_DATABASE_URL = (
#     "mssql+pyodbc://localhost/crypto_tracker?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes"    
# )
use_sql = os.getenv("USE_SQL_SERVER", "true").lower() == "true"
if use_sql:    
    engine = create_engine(url="mssql+pyodbc://localhost/crypto-tracker?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes")
else:
    engine = create_engine(url="sqlite:///./crypto-tracker.db", connect_args={"check_same_thread": False})  # potrzebne dla SQLite

# SQLALCHEMY_DATABASE_URL = (
#     "mssql+pyodbc://localhost/crypto-tracker?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes"    
# )
# engine = create_engine(
#     SQLALCHEMY_DATABASE_URL, # connect_args={"check_same_thread": False}  # potrzebne dla SQLite
# )
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
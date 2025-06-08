from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# SQLALCHEMY_DATABASE_URL = "sqlite:///./crypto_tracker.db"

# SQLALCHEMY_DATABASE_URL = (
#     "mssql+pyodbc://localhost/crypto_tracker?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes"    
# )
SQLALCHEMY_DATABASE_URL = (
    "mssql+pyodbc://localhost/crypto-tracker?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes"    
)



engine = create_engine(
    SQLALCHEMY_DATABASE_URL, # connect_args={"check_same_thread": False}  # potrzebne dla SQLite
)


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
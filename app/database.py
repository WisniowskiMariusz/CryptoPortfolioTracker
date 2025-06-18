import os
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Database:    
    def __init__(self):
        use_sql = os.getenv("USE_SQL_SERVER", "true").lower() == "true"
        if use_sql:
            self.engine: Engine = create_engine(
                url="mssql+pyodbc://localhost/crypto-tracker?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes"
            )
        else:
            self.engine: Engine = create_engine(
                url="sqlite:///./crypto-tracker.db",
                connect_args={"check_same_thread": False}
            )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        Base.metadata.create_all(bind=self.engine)

    def get_db_session(self):
        db_session = self.SessionLocal()
        try:
            yield db_session
        finally:
            db_session.close()

# Singleton instance for the whole app
database = Database()

def get_db():
    return database
            

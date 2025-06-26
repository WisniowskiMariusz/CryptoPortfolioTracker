import os
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

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

    def store_trades(self, db_session: Session, trades: list) -> int:
        from sqlalchemy import or_, and_
        from sqlalchemy.exc import SQLAlchemyError
        from fastapi import HTTPException
        from app import models
        from app.tools import chunked

        try:
            fields = trades[0].keys() if trades else []
            print(f"Fields in trades: {fields}")
            incoming_trades = set((trade.get("id"), trade.get("symbol")) for trade in trades)
            print(f"Incoming keys: {list(incoming_trades)[:5]}... (total {len(incoming_trades)})")
            existing_trades = set()
            batch_size = 50
            for batch in chunked(incoming_trades, batch_size):
                conditions = [and_(models.TradesFromApi.id == id_, models.TradesFromApi.symbol == symbol) for id_, symbol in batch]
                rows = db_session.query(models.TradesFromApi.id, models.TradesFromApi.symbol).filter(or_(*conditions)).all()
                existing_trades.update(rows)
            print(f"Existing keys: {list(existing_trades)[:5]}... (total {len(existing_trades)})")
            unique_trades = [
                models.create_model_instance_from_dict(model_class=models.TradesFromApi, data=trade)
                for trade in trades
                if (trade.get("id"), trade.get("symbol")) not in existing_trades
            ]
            print(f"Unique trades to insert: {unique_trades[:5]}... (total {len(unique_trades)})")
            db_session.bulk_save_objects(unique_trades)
            db_session.commit()
            return unique_trades
        except SQLAlchemyError as e:
            db_session.rollback()
            print(f"Database error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"DB error: {str(e)}")

# Singleton instance for the whole app
database = Database()

def get_db():
    return database


import os
from sqlalchemy import Engine, create_engine, or_, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
from fastapi import HTTPException
from app import models
from app.tools import chunked, datetime_from_miliseconds
from app.base import Base


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
                connect_args={"check_same_thread": False},
            )
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        Base.metadata.create_all(bind=self.engine)

    def get_db_session(self):
        db_session = self.SessionLocal()
        try:
            yield db_session
        finally:
            db_session.close()

    def store_trades(self, db_session: Session, trades: list) -> int:
        try:
            fields = trades[0].keys() if trades else []
            print(f"Fields in trades: {fields}")
            incoming_trades = set(
                (trade.get("id"), trade.get("symbol")) for trade in trades
            )
            print(
                f"Incoming keys: {list(incoming_trades)[:5]}... (total {len(incoming_trades)})"
            )
            existing_trades = set()
            batch_size = 50
            for batch in chunked(incoming_trades, batch_size):
                conditions = [
                    and_(
                        models.TradesFromApi.id == id_,
                        models.TradesFromApi.symbol == symbol,
                    )
                    for id_, symbol in batch
                ]
                rows = (
                    db_session.query(
                        models.TradesFromApi.id, models.TradesFromApi.symbol
                    )
                    .filter(or_(*conditions))
                    .all()
                )
                existing_trades.update(rows)
            print(
                f"Existing keys: {list(existing_trades)[:5]}... (total {len(existing_trades)})"
            )
            unique_trades = [
                models.create_model_instance_from_dict(
                    model_class=models.TradesFromApi, data=trade
                )
                for trade in trades
                if (trade.get("id"), trade.get("symbol")) not in existing_trades
            ]
            print(
                f"Unique trades to insert: {unique_trades[:5]}... (total {len(unique_trades)})"
            )
            db_session.bulk_save_objects(unique_trades)
            db_session.commit()
            return unique_trades
        except SQLAlchemyError as e:
            db_session.rollback()
            print(f"Database error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"DB error: {str(e)}")

    def store_deposits(self, db_session: Session, deposits: list) -> int:
        try:
            # Optionally, deduplicate by id if needed
            deposit_ids = set(dep.get("id") for dep in deposits)
            existing_ids = set()
            if deposit_ids:
                rows = (
                    db_session.query(models.Deposit.id)
                    .filter(models.Deposit.id.in_(deposit_ids))
                    .all()
                )
                existing_ids = set(row[0] for row in rows)
            unique_deposits = [
                models.Deposit(
                    id=str(dep.get("id")),
                    amount=dep.get("amount"),
                    coin=dep.get("coin"),
                    network=dep.get("network"),
                    status=dep.get("status"),
                    address=dep.get("address"),
                    address_tag=dep.get("addressTag"),
                    tx_id=dep.get("txId"),
                    insert_time=datetime_from_miliseconds(dep.get("insertTime")),
                    transfer_type=dep.get("transferType"),
                    confirm_times=dep.get("confirmTimes"),
                    unlock_confirm=dep.get("unlockConfirm"),
                    wallet_type=dep.get("walletType"),
                )
                for dep in deposits
                if dep.get("id") not in existing_ids
            ]
            db_session.bulk_save_objects(unique_deposits)
            db_session.commit()
            return len(unique_deposits)
        except SQLAlchemyError as e:
            db_session.rollback()
            print(f"Database error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"DB error: {str(e)}")

    def store_withdrawals(self, db_session: Session, withdrawals: list) -> int:
        try:
            withdrawal_ids = set(w.get("id") for w in withdrawals)
            existing_ids = set()
            if withdrawal_ids:
                rows = (
                    db_session.query(models.Withdrawal.id)
                    .filter(models.Withdrawal.id.in_(withdrawal_ids))
                    .all()
                )
                existing_ids = set(row[0] for row in rows)
            unique_withdrawals = [
                models.Withdrawal(
                    id=str(w.get("id")),
                    amount=w.get("amount"),
                    coin=w.get("coin"),
                    network=w.get("network"),
                    status=w.get("status"),
                    address=w.get("address"),
                    address_tag=w.get("addressTag"),
                    tx_id=w.get("txId"),
                    apply_time=w.get("applyTime"),
                    success_time=w.get("completeTime"),
                    transfer_type=w.get("transferType"),
                    wallet_type=w.get("walletType"),
                    transaction_fee=w.get("transactionFee"),
                    info=w.get("info"),
                    confirm_no=w.get("confirmNo"),
                    tx_key=w.get("txKey"),
                )
                for w in withdrawals
                if w.get("id") not in existing_ids
            ]
            db_session.bulk_save_objects(unique_withdrawals)
            db_session.commit()
            print(f"Stored {len(unique_withdrawals)} withdrawals in the database.")
            return len(unique_withdrawals)
        except SQLAlchemyError as e:
            db_session.rollback()
            print(f"Database error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"DB error: {str(e)}")


# Singleton instance for the whole app
database = Database()


def get_db():
    return database

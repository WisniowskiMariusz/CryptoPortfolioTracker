from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Query
from typing import Annotated
from app.dependencies import get_kanga_service, get_db_session
from app.kanga_service import KangaService
from sqlalchemy.orm import Session
from app import crud
from app.users_enum import UsersEnum


router = APIRouter(prefix="/kanga", tags=["Kanga"])


@router.get("/get_main_account_balances")
def get_main_account_balances(
    kanga_service: Annotated[KangaService, Depends(get_kanga_service)],
) -> dict:
    """
    Get exchange rate for a given currency code and table.
    Example: /nbp/rates/A/USD
    """
    try:
        return {"wallet": kanga_service.get_main_account_balances()}
    except HTTPException as e:
        raise e


@router.get("/get_active_order_list")
def get_active_order_list(
    kanga_service: Annotated[KangaService, Depends(get_kanga_service)],
    market: str = "BTC-PLN",
) -> dict:
    """
    Get active orders for a specific market.
    Example: /kanga/get_active_order_list?market=BTC-PLN
    """
    try:
        response = kanga_service.get_active_order_list(market)
        if response is None:
            raise HTTPException(status_code=404, detail="No active orders found.")
        return response
    except HTTPException as e:
        raise e


@router.get("/get_and_store_trades")
def get_and_store_trades(
    kanga_service: Annotated[KangaService, Depends(get_kanga_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    start_time: str = "2025-04-14T00:00:00.000Z",
    end_time: str = "2025-04-14T23:59:59.999Z",
) -> dict:
    """
    Get and store trades in db for a specific time range.
    Example:
    /kanga/get_transaction_history_list?start_time=2023-01-01T00:00:00.000Z&end_time=2023-01-312025-04-14T23:59:59.999Z
    """
    try:
        response = kanga_service._get_transaction_history_list(start_time, end_time)
        if response is None:
            raise HTTPException(status_code=404, detail="No transactions found.")
        if "list" in response:
            # print(f"Response {response['list']}")
            trades = [
                kanga_service._parse_trade_from_api(kanga_trade)
                for kanga_trade in response["list"]
            ]
            return crud.upsert_trade_records(
                db_session=db_session,
                user=kanga_service.user,
                exchange="Kanga",
                trades_data=trades,
            )
    except HTTPException as e:
        raise e


@router.get("/get_and_store_trades_list_for_date")
def get_and_store_trades_list_for_date(
    kanga_service: Annotated[KangaService, Depends(get_kanga_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    date: str = "2025-04-14",
) -> dict:
    """
    Get and store trades in db for a specific date.
    Example:
    /kanga/get_transaction_history_list?start_time=2023-01-01&end_time=2023-01-31
    """
    try:
        return crud.upsert_trade_records(
            db_session=db_session,
            user=kanga_service.user,
            exchange="Kanga",
            trades_data=kanga_service.get_trades_for_date(
                db_session=db_session, date=date
            ),
        )
    except HTTPException as e:
        raise e


@router.get("/get_and_store_trades_list_for_time_period")
def get_trades_list_for_time_period(
    kanga_service: Annotated[KangaService, Depends(get_kanga_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    start_date: str = "2025-04-13",
    end_date: str = "2025-04-14",
) -> dict:
    """
    Get and store trades in db for a time period.
    """
    try:
        return crud.upsert_trade_records(
            db_session=db_session,
            user=kanga_service.user,
            exchange="Kanga",
            trades_data=kanga_service.get_trades_for_time_period(
                db_session=db_session, start_date=start_date, end_date=end_date
            ),
        )
    except HTTPException as e:
        raise e
    except ValueError as ve:
        return {"status": "ValueError", "message": ve.args[0]}


@router.get("/get_market_list")
def get_market_list(
    kanga_service: Annotated[KangaService, Depends(get_kanga_service)],
) -> dict:
    """
    Get the list of markets available on Kanga exchange.
    Example: /kanga/get_market_list
    """
    try:
        return kanga_service.get_market_list()
    except HTTPException as e:
        raise e


@router.get("/get_market_tickers")
def get_market_tickers(
    kanga_service: Annotated[KangaService, Depends(get_kanga_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
) -> dict:
    """Update Kanga symbols in the database."""
    tickers: list = kanga_service.get_market_tickers()
    if not tickers:
        raise HTTPException(
            status_code=404, detail="No tickers found in Kanga API response."
        )
    try:
        return crud.upsert_tickers(
            db_session=db_session, tickers=tickers, venue="Kanga"
        )
    except Exception as e:
        print(f"Error storing Kanga symbols: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error storing Kanga symbols: {str(e)}",
        )


@router.get("/get_user")
def get_user(
    kanga_service: Annotated[KangaService, Depends(get_kanga_service)],
) -> dict:
    """
    Return the authenticated Kanga user information.

    Retrieves and returns the current user's data from the injected KangaService.

    Parameters
    ----------
    kanga_service : KangaService
        Injected dependency (via Depends) that provides access to the current user's
        information through its `user` attribute.

    Returns
    -------
    dict
        A dictionary containing the authenticated user's data as provided by the
        service.
    Raises
    ------
    HTTPException
        Raised with status code 500 if an unexpected error occurs while accessing the
        KangaService.
    """
    try:
        return {"User": kanga_service.user}
    except Exception as e:
        print(f"Error storing Kanga symbols: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error storing Kanga symbols: {str(e)}",
        )


@router.post("/upload-csv")
async def upload_csv(
    kanga_service: Annotated[KangaService, Depends(get_kanga_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    user: UsersEnum | None = None,
    file: UploadFile = File(...),
    timezone: str = Query(
        default="Europe/Warsaw",
        description="""Timezone string.
        Examples: Europe/Warsaw, Europe/Berlin, America/New_York""",
    ),
):
    if not user:
        raise HTTPException(status_code=400, detail="Provide user.")
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are supported.")
    try:
        contents: bytes = await file.read()
        trades_data = kanga_service.parse_trades_from_csv(
            csv_file=contents, timezone=timezone, user=user.value
        )
    except Exception as e:
        print(f"Error processing uploaded CSV file: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing uploaded CSV file: {str(e)}",
        )
    try:
        return crud.upsert_trade_records(
            db_session=db_session,
            user=user.value,
            exchange="Kanga",
            trades_data=trades_data,
        )
    except HTTPException as e:
        raise e

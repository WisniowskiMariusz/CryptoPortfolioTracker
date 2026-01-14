from fastapi import APIRouter, File, HTTPException, Depends, Query, UploadFile
from typing import Annotated
from sqlalchemy.orm import Session
from app.binance_service import BinanceService
from app.dependencies import get_db_session, get_binance_service
from app import crud, tools
from app.users_enum import UsersEnum
from app.binance_raw import get_my_trades, snapshot, get_all_order_list
from app.config import NUMBER_OF_MILISECONDS_IN_A_DAY


router = APIRouter(prefix="/binance", tags=["Binance"])


@router.get("/get_binance_exchange_info")
def get_binance_exchange_info(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
) -> dict:
    """
    Get exchange rate for a given currency code and table.
    Example: /nbp/rates/A/USD
    """
    response: dict = binance_service.get_exchange_info()
    try:
        return {
            "timezone": response["timezone"],
            "serverTime": response["serverTime"],
            "rateLimits": response["rateLimits"],
            "exchangeFilters": response["exchangeFilters"],
            "number of symbols": len(
                response["symbols"] if "symbols" in response else 0
            ),
            "symbols keys": (
                str(response["symbols"][0].keys()) if response["symbols"] else []
            ),
        }
    except KeyError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected response format from Binance API: {e}",
        )


@router.post("/update_symbols")
def update_symbols(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
) -> dict:
    """Update Binance symbols in the database."""
    symbols_data: list[dict] = binance_service.get_symbols()
    if not symbols_data:
        raise HTTPException(
            status_code=404, detail="No symbols found in Binance API response."
        )
    try:
        return crud.upsert_binance_symbols(
            db_session=db_session, symbols_data=symbols_data
        )
    except Exception as e:
        print(f"Error storing Binance symbols: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error storing Binance symbols: {str(e)}",
        )


@router.post("/get_currencies")
def get_currencies(
    db_session: Annotated[Session, Depends(get_db_session)],
    symbol: str = Query(default="USTBTC", description="Symbol to filter currencies"),
) -> dict:
    """Get Binance currencies from the database."""
    try:
        return crud.get_binance_symbol_dict(db_session=db_session, symbol=symbol)
    except Exception as e:
        print(f"Error storing Binance symbols: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error storing Binance symbols: {str(e)}",
        )


@router.post("/upload-csv")
async def upload_csv(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    user: UsersEnum | None = None,
    file: UploadFile = File(...),
):
    if not user:
        raise HTTPException(status_code=400, detail="Provide user.")
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are supported.")
    try:
        contents: bytes = await file.read()
        trades_data: list[list[str]] = binance_service.parse_trades_from_csv(
            db_session=db_session, csv_file=contents, user=user.value
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
            exchange="Binance",
            trades_data=trades_data,
        )
    except HTTPException as e:
        raise e


@router.post("/upload-xlsx")
async def upload_xlsx(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    user: UsersEnum | None = None,
    file: UploadFile = File(...),
):
    if not user:
        raise HTTPException(status_code=400, detail="Provide user.")
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported.")
    try:
        contents: bytes = await file.read()
        trades_data: list[list[str]] = binance_service.parse_trades_from_xlsx(
            db_session=db_session, xlsx_file=contents, user=user.value
        )
    except Exception as e:
        print(f"Error processing uploaded XLSX file: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing uploaded XLSX file: {str(e)}",
        )
    # print(f"Parsed trades data: {trades_data}")
    try:
        return crud.upsert_trade_records(
            db_session=db_session,
            user=user.value,
            exchange="Binance",
            trades_data=trades_data,
        )
    except HTTPException as e:
        raise e


@router.post("/fetch_and_store_trades_24h")
async def fetch_trades_24h(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    symbol: str = Query(default="BTCUSDT", description="Trading symbol, e.g. BTCUSDT"),
    start_time: str = Query(None, description="Start date in YYYY-MM-DD format"),
    end_time: str = Query(None, description="End date in YYYY-MM-DD format"),
):
    start_ts = tools.timestamp_from_str(start_time)
    # To include the whole end day
    end_ts = tools.timestamp_from_str(end_time) + NUMBER_OF_MILISECONDS_IN_A_DAY - 1
    try:
        api_trades = binance_service.fetch_all_trades_for_symbol(
            symbol, start_ts, end_ts
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    print(f"Fetched trades for symbol {symbol}: {api_trades}")
    if not api_trades:
        raise HTTPException(
            status_code=404, detail="No trades found for the specified symbol."
        )
    stored_trades = binance_service.parse_trades_from_api(
        db_session=db_session, api_trades=api_trades, user=binance_service.user
    )
    # stored_trades = database.store_trades(db_session=db_session, trades=trades)
    # print(f"Stored {len(stored_trades)} trades for symbol {symbol}.")
    return {
        "Stored trades": len(stored_trades),
        "Fetched trades": len(api_trades),
        "symbol": symbol,
    }


@router.post("/fetch_trades_raw_24h")
async def fetch_trades_raw_24h(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    symbol: str = Query(default="BTCUSDT", description="Trading symbol, e.g. BTCUSDT"),
    start_time: str = Query(None, description="Start date in YYYY-MM-DD format"),
    end_time: str = Query(None, description="End date in YYYY-MM-DD format"),
):
    try:
        response = get_my_trades(
            api_key=binance_service.api_key,
            secret_key=binance_service.api_secret,
            base_url=binance_service.api_url,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
        )
        response_json = response.json()
    except Exception as e:
        print(f"Error fetching trades from Binance API: {str(e)}")
        return {"error": str(e)}
    # if not response:
    #     return {"message": "No response for the specified symbol."}
    return {"response": response_json}


@router.post("/snapshot")
async def account_snapshot(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
    omitZeroBalances: bool = Query(
        True, description="Omit zero balances from the snapshot"
    ),
) -> dict:
    try:
        response = snapshot(
            api_key=binance_service.api_key,
            secret_key=binance_service.api_secret,
            base_url=binance_service.api_url,
        )
        response_json = response.json()
    except Exception as e:
        print(f"Error fetching trades from Binance API: {str(e)}")
        return {"error": str(e)}
    return {"response": response_json}


@router.get("/get_user")
def get_user(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
) -> dict:
    try:
        return {"User": binance_service.user}
    except Exception as e:
        print(f"Error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error: {str(e)}",
        )


@router.post("/get_all_order_list")
async def get_all_order_list_endpoint(
    binance_service: Annotated[BinanceService, Depends(get_binance_service)],
) -> dict:
    try:
        response = get_all_order_list(
            api_key=binance_service.api_key,
            secret_key=binance_service.api_secret,
            base_url=binance_service.api_url,
        )
        response_json = response.json()
    except Exception as e:
        print(f"Error fetching trades from Binance API: {str(e)}")
        return {"error": str(e)}
    return {"response": response_json}

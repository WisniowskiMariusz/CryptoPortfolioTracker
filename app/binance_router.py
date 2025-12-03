from fastapi import APIRouter, File, HTTPException, Depends, Query, UploadFile
from typing import Annotated
from sqlalchemy.orm import Session
from app.binance_service import BinanceService
from app.dependencies import get_db_session, get_binance_service
from app import crud


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
    file: UploadFile = File(...),
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are supported.")
    try:
        contents: bytes = await file.read()
        trades: list[list[str]] = binance_service.parse_trades_from_csv(
            file_content=contents
        )
        for trade in trades:
            print(str(trade))
        return {
            "trades": len(binance_service.parse_trades_from_csv(file_content=contents))
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading CSV file: {e}")

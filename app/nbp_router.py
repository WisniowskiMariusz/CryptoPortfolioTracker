from fastapi import APIRouter, HTTPException, Depends
from typing import Annotated
from sqlalchemy.orm import Session
from app.nbp_service import NbpService
from app.dependencies import get_nbp_service, get_db_session

import requests

router = APIRouter(prefix="/nbp", tags=["NBP"])


@router.get("/rates")
def get_exchange_rate_with_dates(
    nbp_service: Annotated[NbpService, Depends(get_nbp_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    table: str = "a",
    code: str = "eur",
    start_date: str = "2022-01-01",
    end_date: str = "2022-12-31",
) -> dict:
    """
    Get exchange rate for a given currency code and table.
    Example: /nbp/rates/A/USD
    """
    response: requests.Response = nbp_service.get_exchange_rate_with_dates(
        table=table, code=code, start_date=start_date, end_date=end_date
    )
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="NBP API error")
    return {
        "stored_rates": nbp_service.store_rates(
            db_session=db_session, rates=nbp_service.parse_rates(response=response)
        )
    }

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Annotated
from sqlalchemy.orm import Session
from app.nbp_service import NbpService
from app.dependencies import get_nbp_service, get_db_session

import requests

router = APIRouter(prefix="/nbp", tags=["NBP"])


@router.get("/fetch_rates")
def get_exchange_rate_with_dates(
    nbp_service: Annotated[NbpService, Depends(get_nbp_service)],
    db_session: Annotated[Session, Depends(get_db_session)],
    table: str = "a",
    code: str = "eur",
    start_date: str = Query(
        ...,
        description=(
            "Start date in YYYY-MM-DD format; cannot be a future date "
            "and must be before end_date."
        ),
    ),
    end_date: str = Query(
        ...,
        description=(
            "End date in YYYY-MM-DD format; cannot be a future date "
            "and must be within 367 days of start_date."
        ),
    ),
) -> dict:
    """
    Get exchange rate for a given currency code and table.
    Example: /nbp/rates/A/USD
    """
    response: requests.Response = nbp_service.get_exchange_rate_with_dates(
        table=table, code=code, start_date=start_date, end_date=end_date
    )
    if response.status_code != 200:
        message = (
            f"Error fetching data from NBP API: {response.status_code} - "
            f"{response.text}"
        )
        print(message)
        raise HTTPException(status_code=response.status_code, detail=message)
    return {
        "stored_rates": nbp_service.store_rates(
            db_session=db_session, rates=nbp_service.parse_rates(response=response)
        )
    }

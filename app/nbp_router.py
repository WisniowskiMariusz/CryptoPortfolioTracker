from fastapi import APIRouter, HTTPException
import requests

router = APIRouter(prefix="/nbp", tags=["NBP"])


@router.get("/rates")
def get_exchange_rate_with_dates(
    table: str = "a",
    code: str = "eur",
    start_date: str = "2022-01-01",
    end_date: str = "2022-12-31",
):
    """
    Get exchange rate for a given currency code and table.
    Example: /nbp/rates/A/USD
    """
    url = f"https://api.nbp.pl/api/exchangerates/rates/{table}/?format=json"
    if code != "x":
        url = url.replace("?", f"{code}/?")
    else:
        url = url.replace("/rates/", "/tables/")
        if start_date != "x" and end_date != "x":
            url = url.replace("?", f"{start_date}/{end_date}/?")
    print(f"Fetching NBP rates from: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="NBP API error")
    return response.json()

import requests
from typing import Dict
from app import crud


NBP_API_URL = "https://api.nbp.pl/api/"


class NbpService:

    def __init__(self):
        self.base_url = NBP_API_URL

    def get_exchange_rate_with_dates(
        self, table: str, code: str, start_date: str, end_date: str
    ) -> Dict:
        """
        Get exchange rate for a given currency code and table.
        Example: /nbp/rates/A/USD
        """
        base_url = f"{self.base_url}exchangerates/rates/{table}/?format=json"
        if code != "x":
            url = base_url.replace("?", f"{code}/?")
            if start_date != "x" and end_date != "x":
                url = url.replace("?", f"{start_date}/{end_date}/?")
        else:
            url = base_url.replace("/rates/", "/tables/")
        print(f"Fetching NBP rates from: {url}")
        return requests.get(url)

    def parse_rates(self, response: requests.Response) -> list[Dict]:
        """
        Parse the response from NBP API and return the rates.
        """
        if response.status_code != 200:
            raise ValueError(f"Error fetching data: {response.status_code}")
        data = response.json()
        if "rates" in data:
            return [
                {
                    "base_currency": data["code"],
                    "quote_currency": "PLN",
                    "date": rate["effectiveDate"],
                    "price": float(rate["mid"]),
                    "source": "NBP",
                }
                for rate in data["rates"]
            ]
        else:
            raise ValueError("Unexpected response format")

    def store_rates(self, db_session, rates: list[Dict]) -> int:
        """Store the rates in the database."""
        saved_count = 0
        for rate in rates:
            if not crud.rate_exists(
                db_session, rate["base_currency"], rate["quote_currency"], rate["date"]
            ):
                crud.create_rate(db_session, rate)
                saved_count += 1
        return saved_count

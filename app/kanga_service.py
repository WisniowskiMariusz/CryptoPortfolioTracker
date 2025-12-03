import keyring.errors
import requests
import keyring
import hashlib
import hmac
import json
import time as time_mod
import pandas as pd
from decimal import Decimal
from time import time
from fastapi import HTTPException
from datetime import datetime, timezone, timedelta
from io import StringIO
from app.crud import (
    trade_exists_for_date_no_empty_original_id,
    get_trades_for_date_with_empty_original_id,
    Session,
)
from app.tools import generate_hash, string


KANGA_API_URL = "https://api.kanga.exchange"
# in {https://api.kanga.exchange, https://trade.kanga.exchange/api/v2/}
# RETRY_ATTEMPTS = 5
KEYRING_SYSTEM_NAME = "kanga_marcelina_api_wallet_and_history"
# in {
# "kanga_wallet_api",
# "kanga_trading_api", (Mariusz)
# "kanga_history_api",
# "kanga_marcelina_trading"
# "kanga_mariusz_api_portfel_and_history"
# "kanga_marcelina_api_wallet_and_history"
# }
PAUSE_SECONDS = 1.0
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.0


class KangaService:
    keyring_system_name: str
    api_url: str
    api_key: str
    api_secret: str
    user: str

    def __init__(
        self,
        keyring_system_name: str = KEYRING_SYSTEM_NAME,
        pause_seconds: float = PAUSE_SECONDS,
        max_retries: int = MAX_RETRIES,
        backoff_factor: float = BACKOFF_FACTOR,
    ):
        self.keyring_system_name = keyring_system_name
        self.api_url = KANGA_API_URL
        self.api_key: str = self._get_api_key()
        self.api_secret: str = self._get_api_secret()
        self.user: str = self._get_user()
        if not self.user:
            raise Exception("Username not found in keyring.")
        # rate limiting / retry configuration
        self.pause_seconds = float(pause_seconds)
        self.max_retries = int(max_retries)
        self.backoff_factor = float(backoff_factor)

    def _get_api_key(self) -> str:
        """
        Fetches API key from keyring.
        """
        try:
            return keyring.get_password(self.keyring_system_name, "api_key")
        except keyring.errors.KeyringError:
            raise Exception("API key not found in keyring.")

    def _get_api_secret(self) -> str:
        """
        Fetches API secret from keyring.
        """
        try:
            return keyring.get_password(self.keyring_system_name, "api_secret")
        except keyring.errors.KeyringError:
            raise Exception("API secret not found in keyring.")

    def _get_user(self) -> str | None:
        """
        Fetches username from keyring.
        """
        try:
            return keyring.get_password(self.keyring_system_name, "user")
        except keyring.errors.KeyringError:
            raise Exception("Username not found in keyring.")

    def get_main_account_balances(self) -> list[dict]:
        payload = {"nonce": int(time() * 1000), "appId": self.api_key}
        data_json = json.dumps(payload)
        sign = hmac.new(
            bytes(self.api_secret, "utf-8"),
            data_json.encode("utf8"),
            hashlib.sha512,
        ).hexdigest()
        headers = {
            "api-sig": sign,
        }
        try:
            response: requests.Response = requests.post(
                self.api_url + "/api/v2/wallet/list", headers=headers, data=data_json
            )
            print(f"Response status code: {response.status_code}")
            if response.status_code != 200 or response.json().get("result") != "ok":
                print(f"Error: {response.status_code} - {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Error fetching main account balances: {response.text}",
                )
            return response.json().get("wallets")
        except requests.JSONDecodeError as error:
            print(error)

    def get_orderbook_raw(self, market) -> dict | None:
        try:
            response = requests.get(
                self.api_url + f"api/v2/market/orderbook/raw?market={market}"
            )
            return response.json()
        except requests.JSONDecodeError as error:
            print(response)
            print(f"Get orderbook_raw failed: {error}")

    def get_orderbook(self, market) -> dict | None:
        try:
            response = requests.get(
                self.api_url + f"api/v2/market/depth?market={market}"
            )
            return response.json()
        except requests.JSONDecodeError as error:
            print(response)
            print(f"Get orderbook failed: {error}")

    def get_active_order_list(self, market) -> requests.Response | None:
        payload = {
            "nonce": int(time() * 1000),
            "appId": self.api_key,
            "market": market,
        }
        data_json = json.dumps(payload)
        sign = hmac.new(
            bytes(self.api_secret, "utf-8"),
            data_json.encode("utf8"),
            hashlib.sha512,
        ).hexdigest()
        headers = {
            "api-sig": sign,
        }
        response = requests.post(
            self.api_url + "/api/v2/market/order/list", headers=headers, data=data_json
        )
        try:
            return response.json()
        except requests.JSONDecodeError as error:
            print(f"Bad request: {error}")
            return {"result": {error}}

    def get_market_list(self) -> requests.Response:
        payload = {"nonce": int(time() * 1000), "appId": self.api_key}
        data_json = json.dumps(payload)
        sign = hmac.new(
            bytes(self.api_secret, "utf-8"),
            data_json.encode("utf8"),
            hashlib.sha512,
        ).hexdigest()
        headers = {
            "api-sig": sign,
        }
        response = requests.post(
            self.api_url + "/api/markets", headers=headers, data=data_json
        )
        try:
            print(response.json().keys())
            return response.json()
        except Exception as error:
            print(error)

    def get_order(self, order_id) -> requests.Response:
        payload = {
            "nonce": int(time() * 1000),
            "appId": self.api_key,
            "orderId": order_id,
        }
        data_json = json.dumps(payload)
        sign = hmac.new(
            bytes(self.api_secret, "utf-8"),
            data_json.encode("utf8"),
            hashlib.sha512,
        ).hexdigest()
        headers = {
            "api-sig": sign,
        }
        return requests.post(
            self.api_url + "/api/v2/market/order/get", headers=headers, data=data_json
        )

    def _get_transaction_history_list(
        self, start_time: str, end_time: str
    ) -> dict | None:
        payload = {
            "nonce": int(time() * 1000),
            "appId": self.api_key,
            "startTime": start_time,
            "endTime": end_time,
            "limit": 500,
        }
        data_json = json.dumps(payload)
        sign = hmac.new(
            bytes(self.api_secret, "utf-8"),
            data_json.encode("utf8"),
            hashlib.sha512,
        ).hexdigest()
        headers = {"api-sig": sign}

        attempt = 0
        while attempt <= self.max_retries:
            try:
                response: requests.Response = requests.post(
                    self.api_url + "/api/v2/market/transactions/history/list",
                    headers=headers,
                    data=data_json,
                    timeout=30,
                )
            except requests.RequestException as exc:
                # network error -> retry with backoff
                if attempt == self.max_retries:
                    raise HTTPException(
                        status_code=502,
                        detail=f"Network error fetching transaction history: {exc}",
                    )
                wait = (2**attempt) * self.backoff_factor
                time_mod.sleep(wait)
                attempt += 1
                continue

            # handle rate limit / transient server responses
            if response.status_code == 200:
                try:
                    return response.json()
                except requests.JSONDecodeError:
                    # treat as transient and retry
                    if attempt == self.max_retries:
                        raise HTTPException(
                            status_code=502,
                            detail="Invalid JSON received from Kanga API.",
                        )
                    wait = (2**attempt) * self.backoff_factor
                    time_mod.sleep(wait)
                    attempt += 1
                    continue

            if response.status_code == 429:
                # rate limited -> exponential backoff then retry
                wait = (2**attempt) * self.backoff_factor
                time_mod.sleep(wait)
                attempt += 1
                continue

            # non-200 non-429 -> raise
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Error fetching transaction history: {response.text}",
            )

        # exhausted retries
        raise HTTPException(
            status_code=503,
            detail="Exceeded retries when fetching transaction history.",
        )

    def get_market_tickers(self) -> list:
        """
        Fetches market tickers from Kanga API.
        """
        response = requests.get(self.api_url + "/api/v2/market/ticker")
        try:
            return response.json().keys()
        except requests.JSONDecodeError as error:
            print(f"Error fetching market tickers: {error}")
            raise HTTPException(
                status_code=500,
                detail=f"Error fetching market tickers: {error}",
            )

    def _parse_trade_from_strings(self, string_trade: dict[str, str]) -> dict:
        parsed_trade: dict = string_trade.copy()
        parsed_trade.update(
            {"utc_time": string_trade["utc_time"][:-3], "original_id": ""}
        )
        print(f"Trade source for hash generation: {string_trade}")
        print(f"Trade source for hash generation: {parsed_trade}")
        parsed_trade: dict = string_trade | {
            "utc_time": datetime.strptime(
                string_trade["utc_time"], "%Y-%m-%d %H:%M:%S"
            ),
            "price": Decimal(string_trade["price"]),
            "bought_amount": Decimal(string_trade["bought_amount"]),
            "sold_amount": Decimal(string_trade["sold_amount"]),
            "fee_amount": Decimal(string_trade["fee_amount"]),
            "original_id": str(string_trade["original_id"]),
            "id": generate_hash(input_dict=parsed_trade),
        }
        del parsed_trade["exchange"]
        del parsed_trade["user"]
        print(f"Updated trade after hash generation: {parsed_trade}")
        return parsed_trade

    def _parse_trade_from_api(self, kanga_trade: dict) -> dict:
        """
        Parses a Kanga trade dictionary into a standardized format.
        The returned 'time' field is a timezone-aware datetime (UTC) or None.
        """
        if kanga_trade["side"] == "BUYER":
            bought_currency: str = kanga_trade["buyingCurrency"]
            sold_currency: str = kanga_trade["payingCurrency"]
            bought_amount: Decimal = Decimal(kanga_trade["quantity"])
            sold_amount: Decimal = Decimal(kanga_trade["value"])
        else:
            bought_currency: str = kanga_trade["payingCurrency"]
            sold_currency: str = kanga_trade["buyingCurrency"]
            bought_amount: Decimal = Decimal(kanga_trade["value"])
            sold_amount: Decimal = Decimal(kanga_trade["quantity"])

        utc_time_str: str = (
            str(kanga_trade["created"]).replace("T", " ").replace("Z", "")
        )
        print(f"Parsing trade with time: {utc_time_str}")
        trade_str = {
            "utc_time": utc_time_str,
            "bought_currency": bought_currency,
            "sold_currency": sold_currency,
            "price": str(kanga_trade["price"]),
            "bought_amount": string(bought_amount),
            "sold_amount": string(sold_amount),
            "fee_currency": str(kanga_trade["feeCurrency"]),
            "fee_amount": str(kanga_trade["fee"]),
            "original_id": str(kanga_trade["id"]),
            "id": "",
            "exchange": "Kanga",
            "user": self.user,
        }
        # print(f"Trade source for hash generation: {trade_str}")
        # parsed_trade: dict = trade_str | {
        #     "utc_time": datetime.strptime(utc_time_str[:-3], "%Y-%m-%d %H:%M:%S"),
        #     "price": Decimal(trade_str["price"]),
        #     "bought_amount": Decimal(trade_str["bought_amount"]),
        #     "sold_amount": Decimal(trade_str["sold_amount"]),
        #     "fee_amount": Decimal(trade_str["fee_amount"]),
        #     "original_id": str(kanga_trade["id"]),
        #     "id": generate_hash(input_dict=trade_str),
        # }
        # del parsed_trade["exchange"]
        # del parsed_trade["user"]
        # print(f"Updated trade after hash generation: {parsed_trade}")
        # return parsed_trade
        return self._parse_trade_from_strings(string_trade=trade_str)

    def get_trades_for_date(self, db_session: Session, date: str) -> list[dict]:
        """
        Fetches transaction history for a specific date.
        date: string in 'YYYY-MM-DD' format.
        """

        def _get_fake_trade(date: datetime, trade_id) -> dict:
            return {
                "utc_time": date.replace(
                    hour=12, minute=0, second=0, microsecond=0
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "bought_currency": "ALL",
                "sold_currency": "ALL",
                "price": 0.0,
                "bought_amount": 0.0,
                "sold_amount": 0.0,
                "fee_currency": "Not applicable",
                "fee_amount": 0.0,
                "original_id": trade_id,
                "id": "",
                "exchange": "Kanga",
                "user": self.user,
            }

        start_time, end_time = self._create_start_end_time_strings(date)
        end_time_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
        print(f"End time: {end_time_dt}")
        if trade_exists_for_date_no_empty_original_id(
            db_session=db_session, exchange="Kanga", user=self.user, date=date
        ) and end_time_dt < datetime.now(timezone.utc):
            already_checked_message: str = (
                f"Past date: {date} was already checked for Kanga Exchange"
                f" for user: {self.user}. It means no new trades can appear"
                f" and no new api request needed. Date: {date} is skipped."
            )
            print(already_checked_message)
            return [{"message": already_checked_message}]
        no_trades_original_id: str = (
            f"No trades for user: {self.user}, exchange: Kanga, date: {date}"
        )
        no_data_original_id: str = (
            f"Data for {date} are unavailable ({date} < 2023-03-15)."
        )
        print(datetime(year=2023, month=3, day=15, tzinfo=timezone.utc))
        if end_time_dt < datetime(year=2023, month=3, day=15, tzinfo=timezone.utc):
            trades_empty_original = get_trades_for_date_with_empty_original_id(
                db_session=db_session, exchange="Kanga", user=self.user, date=date
            )
            print(f"len(trades_empty_original: {len(trades_empty_original)}")
            if len(trades_empty_original):
                trades_empty_original2 = [
                    trade.to_dict() for trade in trades_empty_original
                ]
                for trade in trades_empty_original2:
                    trade.update(
                        {"original_id": "Not available for dates before 2023-03-15"}
                    )
                print(trades_empty_original2)
                return trades_empty_original2
            print(no_data_original_id)
            return [
                self._parse_trade_from_strings(
                    _get_fake_trade(end_time_dt, no_data_original_id)
                )
            ]
        response = self._get_transaction_history_list(start_time, end_time)
        # print(f"Transaction history response for date {date}: {response}")
        if response is None:
            raise HTTPException(status_code=404, detail="No transactions found.")
        if "message" in response:
            return [
                self._parse_trade_from_strings(
                    _get_fake_trade(end_time_dt, no_data_original_id)
                )
            ]
        if "list" in response:
            if len(response["list"]) == 0 and end_time_dt < datetime.now(timezone.utc):
                return [
                    self._parse_trade_from_strings(
                        _get_fake_trade(end_time_dt, no_trades_original_id)
                    )
                ]
            return [
                self._parse_trade_from_api(kanga_trade)
                for kanga_trade in response["list"]
            ]
        if "result" in response and "code" in response:
            if response["result"] == "fail" and response["code"] == 429:
                message_429: str = "Too many calls."
                print(message_429)
                return [{"message": message_429}]
        print("Response in unexpected format, returning empty list")
        print(f"Response {response}")
        # Response {'result': 'fail', 'code': 429}
        return []

    def get_trades_for_time_period(
        self, db_session: Session, start_date: str, end_date: str
    ) -> list[dict]:
        """
        Fetches transaction history for a specific time period.
        start_time and end_time: strings in 'YYYY-MM-DD' format.
        """
        trades = []
        dates = self._create_dates_list(start_date, end_date)
        for date in dates:
            if not isinstance(date, str) or len(date) != 10:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid date format. Expected 'YYYY-MM-DD'.",
                )
            # if if_exist_trade_record_for_date(
            #     db_session=db_session, exchange="Kanga", user=self.user, date=date
            # ):
            #     print(f"Trades for date: {date} already exist in DB. Skipping ...")
            #     continue
            print(f"Fetching trades for date: {date} ...")
            trades_for_date = self.get_trades_for_date(db_session=db_session, date=date)
            no_request_message = (
                f"Last date: {date} didn't send request, so pause is not needed."
            )
            too_many_calls_message = (
                f"For {date} request call results with api limit breach error.\n",
                "So far fetched trades will be added to database, but fetching\n",
                "process is stopped now.",
            )
            if len(trades_for_date) > 0:
                if "message" in trades_for_date[0]:
                    if "was already checked" in trades_for_date[0]["message"]:
                        print(no_request_message)
                        continue
                    if "Too many calls." in trades_for_date[0]["message"]:
                        print(too_many_calls_message)
                        return trades
                if "original_id" in trades_for_date[0]:
                    if "< 2023-03-15)" in trades_for_date[0]["original_id"]:
                        # print(trades_for_date[0]["original_id"])
                        print(f"Found {len(trades_for_date)} trades for date: {date}.")
                        trades.extend(trades_for_date)
                        continue
                print(f"Found {len(trades_for_date)} trades for date: {date}.")
                trades.extend(trades_for_date)
                # if trades_for_date[0]["fee_currency"] == "Not applicable":
                #     print(no_request_message)
                #     continue
            # pause between requests to avoid being rate-limited / banned
            # It looks like Kanga limit is 60 requests per minute.
            print(f"Pausing for {self.pause_seconds} seconds ...")
            if self.pause_seconds > 0:
                time_mod.sleep(self.pause_seconds)
        return trades

    @staticmethod
    def _create_start_end_time_strings(date: str) -> tuple[str, str]:
        """
        Creates ISO 8601 formatted start and end time strings for Kanga API.
        The returned strings are in the format 'YYYY-MM-DDTHH:MM:SS.sssZ'.
        """
        return f"{date}T00:00:00.000Z", f"{date}T23:59:59.999Z"

    @staticmethod
    def _create_dates_list(start_date: str, end_date: str) -> list[str]:
        """
        Creates a list of date strings in 'YYYY-MM-DD'
        format between start_date and end_date.
        Both start_date and end_date should be in 'YYYY-MM-DD' format.
        """
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        if end_dt > datetime.now():
            print(
                f"end_date must be before or equal to today but it is {end_date}.\n"
                "Instead today date will be taken as end_date."
            )
            end_dt = datetime.now()
        if start_dt > end_dt:
            raise ValueError("start_date must be before or equal to end_date.")
        dates_list = []
        current_dt = start_dt
        while current_dt <= end_dt:
            dates_list.append(current_dt.strftime("%Y-%m-%d"))
            current_dt += timedelta(days=1)
        return dates_list

    @staticmethod
    def alias_currencies(pair: str) -> str:
        """
        Aliases certain currency codes to standard ones.
        E.g., 'XBT' to 'BTC'.
        """
        alias_map = {
            "PLN°": "oPLN",
            "EUR°": "oEUR",
            "USD°": "oUSD",
            # Add more aliases as needed
        }
        for currency in alias_map:
            pair = pair.replace(currency, alias_map[currency])
        return pair

    def parse_trades_from_csv(
        self, csv_file: bytes, timezone: str, user: str
    ) -> list[dict]:
        """Imports trades from a Kanga CSV file into the database."""
        try:
            df = pd.read_csv(StringIO(csv_file.decode("utf-8")))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error reading CSV file: {e}")
        required_columns = {
            "Data",
            "Para",
            "Strona",
            "Ilość",
            "Cena",
            "Opłata",
            "Suma",
        }
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise HTTPException(
                status_code=400, detail=f"Missing required columns: {missing}"
            )

        trades_data = []
        for _, row in df.iterrows():
            try:
                base_amount = Decimal(str(row["Ilość"]).split(" ")[0])
                print(f"Initial base amount: {base_amount}")
                quote_amount = Decimal(str(row["Suma"]).split(" ")[0])
                fee, fee_currency = row["Opłata"].split(" ")
                base_currency, quote_currency = self.alias_currencies(
                    row["Para"]
                ).split("/")
                if row["Strona"] == "Kupujący":
                    bought_currency: str = base_currency
                    sold_currency: str = quote_currency
                    bought_amount: Decimal = base_amount
                    print(f"Initial bought amount: {bought_amount}")
                    sold_amount: Decimal = -1 * quote_amount
                if row["Strona"] == "Sprzedający":
                    bought_currency: str = quote_currency
                    sold_currency: str = base_currency
                    bought_amount: Decimal = quote_amount
                    sold_amount: Decimal = -1 * base_amount
                if fee_currency == sold_currency:
                    # In this case in Kanga csv files fee is counted twice
                    # (one added to sold amount, one as separate fee amount)
                    sold_amount -= Decimal(fee)
                if fee_currency == bought_currency:
                    # In this case in Kanga csv files fee is deducted twice
                    # (one from bought amount, one as separate fee amount)
                    bought_amount += Decimal(fee)
                    print(
                        f"Adjusted bought amount for fee: {bought_amount}, Fee: {fee}"
                    )
                    trade_time: pd.Timestamp = pd.to_datetime(row["Data"])
                    trade_time_utc: pd.Timestamp = trade_time.tz_localize(
                        timezone
                    ).tz_convert("UTC")
                    print(f"Parsing trade with time: {trade_time}")
                trade_str = {
                    "utc_time": trade_time_utc.strftime("%Y-%m-%d %H:%M"),
                    "bought_currency": str(bought_currency),
                    "sold_currency": str(sold_currency),
                    "price": str(row["Cena"]).split(" ")[0],
                    "bought_amount": string(bought_amount),
                    "sold_amount": string(sold_amount),
                    "fee_currency": str(fee_currency),
                    "fee_amount": str(fee),
                    "original_id": "",
                    "id": "",
                    "exchange": "Kanga",
                    "user": user,
                }
                print(f"Trade source for hash generation: {trade_str}")
                trade_hash = generate_hash(input_dict=trade_str)
                for trade in trades_data:
                    if trade["id"] == trade_hash:
                        trade_time_utc = trade_time_utc + pd.Timedelta(seconds=1)
                        print(
                            """
                            WARNING!!! Duplicate trade time in CSV detected.
                            Adjusted time:
                            """
                        )
                        print(f"trade: {trade}")
                parsed_trade: dict = trade_str | {
                    "utc_time": trade_time_utc,
                    "price": Decimal(trade_str["price"]),
                    "bought_amount": Decimal(trade_str["bought_amount"]),
                    "sold_amount": Decimal(trade_str["sold_amount"]),
                    "fee_amount": Decimal(trade_str["fee_amount"]),
                    "id": trade_hash,
                }
                del parsed_trade["exchange"]
                del parsed_trade["user"]

                trades_data.append(parsed_trade)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Error parsing row: {e}")
        print(f"First 5: {trades_data[:5]}")
        print(f"Last 5: {trades_data[-5:]}")
        return trades_data

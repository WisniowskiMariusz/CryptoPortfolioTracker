import keyring.errors
import requests
import time
import keyring
from datetime import datetime, timedelta, timezone
from requests.exceptions import (
    RequestException,
    HTTPError,
    Timeout,
    ConnectionError,
)
from typing import List, Dict, Generator
from binance.spot import Spot
from binance.error import ClientError
from app import tools


BINANCE_API_URL = "https://api.binance.com/api/v3/"
RETRY_ATTEMPTS = 5
KEYRING_SYSTEM_NAME = "binance_CherryWallet_api"


class BinanceService:
    keyring_system_name: str

    def __init__(self, keyring_system_name=KEYRING_SYSTEM_NAME):
        self.keyring_system_name = keyring_system_name
        api_key: str = self._get_api_key()
        api_secret: str = self._get_api_secret()
        self.client: Spot = self._get_client(api_key, api_secret)

    def _get_api_key(self) -> str:
        """
        Fetches Binance API key from keyring.
        """
        try:
            return keyring.get_password(self.keyring_system_name, "api_key")
        except keyring.errors.KeyringError:
            raise Exception("Binance API key not found in keyring.")

    def _get_api_secret(self) -> str:
        """
        Fetches Binance API secret from keyring.
        """
        try:
            return keyring.get_password(self.keyring_system_name, "api_secret")
        except keyring.errors.KeyringError:
            raise Exception("Binance API secret not found in keyring.")

    def _get_client(self, api_key: str, api_secret: str) -> Spot:
        return Spot(api_key=api_key, api_secret=api_secret)

    def get_account_info(self):
        return self.client.account()

    def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 1000,
    ) -> List[Dict]:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time:
            params["startTime"] = tools.convert_time_to_ms(start_time)
        if end_time:
            params["endTime"] = tools.convert_time_to_ms(end_time)

        for attempt in range(RETRY_ATTEMPTS):
            try:
                response = requests.get(
                    url=f"{BINANCE_API_URL}klines", params=params, timeout=10
                )
                if response.status_code == 429:
                    print("Rate limit exceeded (429). Waiting before retry...")
                    time.sleep(2**attempt)  # exponential backoff
                    continue
                response.raise_for_status()
                data = response.json()
                print(f"Type of response.json(): {type(data)}")
                return data
            except (ConnectionError, Timeout) as e:
                print(f"Network error: {e}. Retrying...")
                time.sleep(2**attempt)
            except HTTPError as e:
                print(f"HTTP error: {e}")
                raise
            except ValueError as e:
                print(f"Error parsing response JSON: {e}")
                raise
            except RequestException as e:
                print(f"Unexpected request error: {e}")
                raise
        raise RuntimeError(
            f"Failed to fetch data from Binance after " f"{RETRY_ATTEMPTS} attempts."
        )

    def parse_klines(self, data: List[Dict], symbol: str, interval: str) -> List[Dict]:
        prices: list = []
        for entry in data:
            if not entry or len(entry) < 2:
                continue  # skip empty or malformed entries
            open_time_ms = entry[0]
            open_price_str = entry[1]
            prices.append(
                {
                    "symbol": symbol,
                    "interval": interval,
                    "time": datetime.fromtimestamp(
                        open_time_ms / 1000, timezone.utc
                    ).replace(second=0, microsecond=0, tzinfo=None),
                    "price": float(open_price_str),
                    "source": "binance",
                }
            )
        return prices

    def fetch_prices(
        self,
        symbol: str,
        interval: str,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 1000,
    ) -> List[Dict]:
        data = self.get_klines(symbol, interval, start_time, end_time, limit)
        return self.parse_klines(data, symbol, interval)

    def fetch_prices_stream(
        self,
        symbol: str,
        interval: str,
        batch_size: int = 1000,
        end_time: str | None = None,
        max_requests: None | int = 0,
    ) -> Generator[List[Dict], None, None]:
        requests_made = 0

        while True:
            if max_requests and requests_made >= max_requests:
                break
            data = self.get_klines(
                symbol=symbol,
                interval=interval,
                start_time=None,
                end_time=end_time,
                limit=batch_size,
            )
            if not data:
                break
            yield self.parse_klines(data, symbol, interval)
            if len(data) < batch_size:
                break
            last_open_time_ms = data[0][0]
            print(f"len(data): {len(data)}")
            print(f"data[0]: {data[0]}")
            print(f"data[-1]: {data[-1]}")
            print(f"Last open time in ms: {last_open_time_ms}")
            end_time = datetime.fromtimestamp(
                last_open_time_ms / 1000, timezone.utc
            ) - timedelta(minutes=1)
            requests_made += 1
            time.sleep(0.5)

    def fetch_trades(self, symbol, start_time=None, end_time=None, limit=1000) -> list:
        try:
            trades = []
            from_id = None
            while True:
                params = {"symbol": symbol, "limit": limit}
                if from_id:
                    params["fromId"] = from_id
                if start_time:
                    params["startTime"] = start_time
                if end_time:
                    params["endTime"] = end_time
                batch = self.client.my_trades(**params)
                if not batch:
                    break
                for trade in batch:
                    trades.append(
                        {
                            "id": trade.get("id"),
                            "symbol": trade.get("symbol"),
                            "orderId": trade.get("orderId"),
                            "price": float(trade.get("price")),
                            "qty": float(
                                trade.get("qty"),
                            ),
                            "quoteQty": float(trade.get("quoteQty")),
                            "commission": float(trade.get("commission")),
                            "commissionAsset": trade.get("commissionAsset"),
                            "time": datetime.fromtimestamp(
                                trade["time"] / 1000.0, tz=timezone.utc
                            ).replace(tzinfo=None),
                            "isBuyer": int(trade.get("isBuyer", False)),
                            "isMaker": int(trade.get("isMaker", False)),
                            "isBestMatch": int(trade.get("isBestMatch", False)),
                        }
                    )
                if len(batch) < limit:
                    break
                from_id = batch[-1]["id"] + 1
            print(f"Fetched {len(trades)} trades for symbol {symbol}.")
            if not trades:
                print(f"No trades found for symbol {symbol}.")
                return []
            return trades
        except ClientError as e:
            print(f"Binance API error: {str(e)}")
            raise e

    def get_deposit_history(
        self, asset: str = None, start_time: int = None, end_time: int = None
    ):
        return self.client.deposit_history(
            asset=asset, startTime=start_time, endTime=end_time
        )

    def get_withdraw_history(
        self, asset: str = None, start_time: int = None, end_time: int = None
    ):
        return self.client.withdraw_history(
            asset=asset, startTime=start_time, endTime=end_time
        )

    def get_all_deposits(
        self,
        asset: str = None,
        earliest_date: str = "2017-07-01",
        latest_date: str = None,
    ) -> dict:
        """
        Fetches all deposits from Binance, paginating 90-day windows,
        and returns all results in one call.
        Iterates through all windows until earliest_date,
        even if some windows are empty.
        If latest_date is provided, starts from that date instead of now.
        """
        if latest_date:
            latest_dt = datetime.strptime(latest_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        else:
            latest_dt = datetime.now(timezone.utc)
        earliest_dt = datetime.strptime(earliest_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        all_deposits = []
        page = 1

        while True:
            end_time = tools.add_n_days_to_date(date=latest_dt, days=-90 * (page - 1))
            start_time = tools.add_n_days_to_date(date=end_time, days=-90)
            if start_time < earliest_dt:
                start_time = earliest_dt
            end_time_ms = int(end_time.timestamp() * 1000)
            start_time_ms = int(start_time.timestamp() * 1000)
            print(
                f"Fetching deposits: page {page}, "
                f"{start_time.date()} to {end_time.date()} ..."
            )
            deposits = self.client.deposit_history(
                asset=asset, startTime=start_time_ms, endTime=end_time_ms
            )
            print(
                f"  Found {len(deposits) if deposits else 0} "
                f"deposits in this window."
            )
            if deposits:
                all_deposits.extend(deposits)
            if start_time == earliest_dt:
                print("Reached earliest date, stopping.")
                break
            page += 1
            time.sleep(0.5)
        print(f"Total deposits fetched: {len(all_deposits)}")
        return {
            "status": "success",
            "count": len(all_deposits),
            "data": all_deposits,
        }

    def get_all_withdrawals(
        self,
        asset: str = None,
        earliest_date: str = "2017-07-01",
        latest_date: str = None,
    ) -> dict:
        if latest_date:
            latest_dt = datetime.strptime(latest_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        else:
            latest_dt = datetime.now(timezone.utc)
        earliest_dt = datetime.strptime(earliest_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        all_withdrawals = []
        page = 1

        while True:
            end_time = tools.add_n_days_to_date(date=latest_dt, days=-90 * (page - 1))
            start_time = tools.add_n_days_to_date(date=end_time, days=-90)
            if start_time < earliest_dt:
                start_time = earliest_dt
            end_time_ms = int(end_time.timestamp() * 1000)
            start_time_ms = int(start_time.timestamp() * 1000)
            print(
                f"Fetching withdrawals: page {page}, "
                f"{start_time.date()} to {end_time.date()} ..."
            )
            withdrawals = self.client.withdraw_history(
                asset=asset, startTime=start_time_ms, endTime=end_time_ms
            )
            print(
                f"  Found {len(withdrawals) if withdrawals else 0} "
                "withdrawals in this window."
            )
            if withdrawals:
                all_withdrawals.extend(withdrawals)
            if start_time == earliest_dt:
                print("Reached earliest date, stopping.")
                break
            page += 1
            time.sleep(0.5)
        print(f"Total withdrawals fetched: {len(all_withdrawals)}")
        return {
            "status": "success",
            "count": len(all_withdrawals),
            "data": all_withdrawals,
        }

    def get_dust_log(self):
        """
        Fetches small-balance (dust) conversion history from Binance.
        """
        return self.client.dust_log()

    def get_lending_interest_history(
        self,
        # lending_type can be "DAILY", "ACTIVITY", or "CUSTOMIZED_FIXED
        lending_type: str = "DAILY",
        asset: str = None,
        start_time: int = None,
        end_time: int = None,
        limit: int = 1000,
    ):
        """
        Fetches interest history from Binance Savings
        (Flexible, Activity, or Customized Fixed).
        """
        params = {"lendingType": lending_type, "limit": limit}
        if asset:
            params["asset"] = asset
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        return self.client.get_flexible_rewards_history(**params)

    def get_flexible_redemption_record(
        self,
        # product_id is the ID of the Flexible Savings product
        product_id: str = None,
        redeem_id: str = None,
        asset: str = None,
        start_time: int = None,
        end_time: int = None,
        current: int = 1,
        size: int = 100,
    ):
        """
        Fetches Flexible Redemption Record from
        Binance Simple Earn using binance-connector.
        """
        params = {"current": current, "size": size}
        if product_id:
            params["productId"] = product_id
        if redeem_id:
            params["redeemId"] = redeem_id
        if asset:
            params["asset"] = asset
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        return self.client.get_flexible_redemption_record(**params)

    def get_flexible_product_position(self, asset: str = None):
        """
        Fetches current Simple Earn Flexible positions from Binance.
        """
        params = {}
        if asset:
            params["asset"] = asset
        return self.client.get_flexible_product_position(**params)

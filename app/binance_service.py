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


BINANCE_API_URL = "https://api.binance.com/api/v3/"
BINANCE_API_KEY = keyring.get_password("binance_CherryWallet_api", "api_key")
BINANCE_API_SECRET = keyring.get_password("binance_CherryWallet_api", "api_secret")


def get_binance_client() -> Spot:
    return Spot(BINANCE_API_KEY, BINANCE_API_SECRET)


def convert_time_to_ms(time: str) -> int:
    print(f"Provided time string: {time}")
    if not isinstance(time, datetime):
        dt = datetime.strptime(time, "%Y-%m-%d %H:%M")
    else:
        dt = time
    dt = dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    print(f"Provided time: {dt}")
    print(f"Current time: {now}")
    if dt > now:
        dt = now  # albo rzuć błąd, jeśli wolisz
    print(f"Used time: {dt}")
    timestamp_ms = int(dt.timestamp() * 1000)
    return timestamp_ms


def get_account_info():
    api_key = BINANCE_API_KEY
    api_secret = BINANCE_API_SECRET
    if not api_key or not api_secret:
        raise Exception("Binance API credentials not set")
    client = get_binance_client()
    return client.account()


def get_klines(
    symbol: str,
    interval: str,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 1000,
) -> List[Dict]:
    NUMBER_OF_ATTEMPTS = 5
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_time:
        params["startTime"] = convert_time_to_ms(start_time)
    if end_time:
        params["endTime"] = convert_time_to_ms(end_time)

    for attempt in range(NUMBER_OF_ATTEMPTS):
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
        f"Failed to fetch data from Binance after " f"{NUMBER_OF_ATTEMPTS} attempts."
    )


def parse_klines(data: List[Dict], symbol: str, interval: str) -> List[Dict]:
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
    symbol: str,
    interval: str,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 1000,
) -> List[Dict]:
    data = get_klines(symbol, interval, start_time, end_time, limit)
    return parse_klines(data, symbol, interval)


def fetch_prices_stream(
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
        data = get_klines(
            symbol=symbol,
            interval=interval,
            start_time=None,  # zawsze zaczynamy od najnowszych danych
            end_time=end_time,
            limit=batch_size,
        )
        if not data:
            break
        yield parse_klines(data, symbol, interval)
        if len(data) < batch_size:
            # osiągnęliśmy najdawniejsze dostępne dane
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
        time.sleep(0.5)  # aby nie przekroczyć limitu API


def fetch_trades(symbol, start_time=None, end_time=None, limit=1000) -> list:
    client = get_binance_client()
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
            batch = client.get_my_trades(**params)
            if not batch:
                break
            for trade in batch:
                # Normalize trade data to match your Transaction model fields
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
    asset: str = None, start_time: int = None, end_time: int = None
):
    client = get_binance_client()
    return client.deposit_history(asset=asset, startTime=start_time, endTime=end_time)


def get_withdraw_history(
    asset: str = None, start_time: int = None, end_time: int = None
):
    client = get_binance_client()
    return client.withdraw_history(asset=asset, startTime=start_time, endTime=end_time)


def get_all_deposits(
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
    client = get_binance_client()
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
        end_time = latest_dt - timedelta(days=90 * (page - 1))
        start_time = end_time - timedelta(days=90)
        if start_time < earliest_dt:
            start_time = earliest_dt
        end_time_ms = int(end_time.timestamp() * 1000)
        start_time_ms = int(start_time.timestamp() * 1000)
        print(
            f"Fetching deposits: page {page}, "
            f"{start_time.date()} to {end_time.date()} ..."
        )
        deposits = client.deposit_history(
            asset=asset, startTime=start_time_ms, endTime=end_time_ms
        )
        print(
            f"  Found {len(deposits) if deposits else 0} " f"deposits in this window."
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
    asset: str = None,
    earliest_date: str = "2017-07-01",
    latest_date: str = None,
) -> dict:
    client = get_binance_client()
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
        end_time = latest_dt - timedelta(days=90 * (page - 1))
        start_time = end_time - timedelta(days=90)
        if start_time < earliest_dt:
            start_time = earliest_dt
        end_time_ms = int(end_time.timestamp() * 1000)
        start_time_ms = int(start_time.timestamp() * 1000)
        print(
            f"Fetching withdrawals: page {page}, "
            f"{start_time.date()} to {end_time.date()} ..."
        )
        withdrawals = client.get_withdraw_history(
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


def get_dust_log():
    """
    Fetches small-balance (dust) conversion history from Binance.
    """
    client: Spot = get_binance_client()
    return client.get_dust_log()


def get_lending_interest_history(
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
    client: Spot = get_binance_client()
    params = {"lendingType": lending_type, "limit": limit}
    if asset:
        params["asset"] = asset
    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time
    return client.get_flexible_rewards_history(**params)


def get_flexible_redemption_record(
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
    client = get_binance_client()
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

    return client.get_flexible_redemption_record(**params)


def get_flexible_product_position(asset: str = None):
    """
    Fetches current Simple Earn Flexible positions from Binance.
    """
    client = get_binance_client()
    params = {}
    if asset:
        params["asset"] = asset
    return client.get_flexible_product_position(**params)

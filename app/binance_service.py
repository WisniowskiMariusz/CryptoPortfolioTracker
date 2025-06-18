import requests
import time
import keyring
from datetime import datetime, timezone, timedelta
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError
from typing import List, Dict, Generator
from binance.client import Client
from binance.exceptions import BinanceAPIException


BINANCE_API_URL = "https://api.binance.com/api/v3/"
BINANCE_API_KEY = keyring.get_password('binance_CherryWallet_api', 'api_key')
BINANCE_API_SECRET = keyring.get_password('binance_CherryWallet_api', 'api_secret')

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
    client = Client(api_key, api_secret)
    return client.get_account()


def get_klines(symbol: str, interval: str, start_time: str | None = None, end_time: str | None = None, limit: int = 1000) -> List[Dict]:
    NUMBER_OF_ATTEMPTS = 5
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    if start_time:
        params["startTime"] = convert_time_to_ms(start_time)
    if end_time:
        params["endTime"] = convert_time_to_ms(end_time)   

    for attempt in range(NUMBER_OF_ATTEMPTS):
        try:
            response = requests.get(url=f"{BINANCE_API_URL}klines", params=params, timeout=10)
            if response.status_code == 429:
                print("Rate limit exceeded (429). Waiting before retry...")
                time.sleep(2 ** attempt)  # exponential backoff
                continue
            response.raise_for_status()
            data = response.json()
            print(f"Type of response.json(): {type(data)}")
            return data
        except (ConnectionError, Timeout) as e:
            print(f"Network error: {e}. Retrying...")
            time.sleep(2 ** attempt)
        except HTTPError as e:
            print(f"HTTP error: {e}")
            raise
        except ValueError as e:
            print(f"Error parsing response JSON: {e}")
            raise
        except RequestException as e:
            print(f"Unexpected request error: {e}")
            raise
    raise RuntimeError(f"Failed to fetch data from Binance after {NUMBER_OF_ATTEMPTS} attempts.")


def parse_klines(data: List[Dict], symbol: str, interval: str) -> List[Dict]:
    prices: list = []
    for entry in data:
        open_time_ms = entry[0]
        open_price_str = entry[1]
        prices.append({
            "symbol": symbol,
            "interval": interval,
            "time": datetime.fromtimestamp(open_time_ms / 1000, timezone.utc).replace(second=0, microsecond=0, tzinfo=None),  
            "price": float(open_price_str),                          
            "source": "binance"
        })
    return prices

def fetch_prices(symbol: str, interval: str, start_time: str | None = None, end_time: str | None = None, limit: int = 1000) -> List[Dict]:
    data = get_klines(symbol, interval, start_time, end_time, limit)    
    return parse_klines(data, symbol, interval)

def fetch_prices_stream(
    symbol: str,
    interval: str,    
    batch_size: int = 1000,    
    end_time: str | None = None, 
    max_requests: None | int = 0
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
            limit=batch_size
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
        end_time = datetime.fromtimestamp(last_open_time_ms / 1000, timezone.utc) - timedelta(minutes=1)
        requests_made += 1
        time.sleep(0.5)  # aby nie przekroczyć limitu API

def get_binance_client() -> Client:
    return Client(BINANCE_API_KEY, BINANCE_API_SECRET)

def fetch_trades(symbol, start_time=None, end_time=None) -> list:
    client = get_binance_client()
    try:
        trades = []        
        # all_symbols = [s['symbol'] for s in client.get_exchange_info().get("symbols")]
        # print(f"Available symbols: {len(all_symbols)}")
        limit = 1000
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
                    "qty": float(trade.get("qty"),),
                    "quoteQty": float(trade.get("quoteQty")),
                    "commission": float(trade.get("commission")),
                    "commissionAsset": trade.get("commissionAsset"),
                    "time": datetime.fromtimestamp(trade['time'] / 1000.0, tz=timezone.utc).replace(tzinfo=None),
                    "isBuyer": int(trade.get("isBuyer", False)),
                    "isMaker": int(trade.get("isMaker", False)),
                    "isBestMatch": int(trade.get("isBestMatch", False))
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
    except BinanceAPIException as e:
        print(f"Binance API error: {e.message}")
        raise e

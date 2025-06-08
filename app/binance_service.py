import requests
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Generator

BINANCE_API_URL = "https://api.binance.com/api/v3/"

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


def get_klines(symbol: str, interval: str, start_time: str | None = None, end_time: str | None = None, limit: int = 1000) -> List[Dict]:
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    if start_time:
        params["startTime"] = convert_time_to_ms(start_time)
    if end_time:
        params["endTime"] = convert_time_to_ms(end_time)
    response: requests.Response = requests.get(f"{BINANCE_API_URL}klines", params=params)
    response.raise_for_status()
    data: List[Dict] = response.json()
    print(f"Type of response.json(): {type(data)}")
    return data

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

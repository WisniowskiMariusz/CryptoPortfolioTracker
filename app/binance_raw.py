import time
import hmac
import hashlib
import urllib.parse
import requests
from app.tools import timestamp_from_str


def sign_query(query: str, secret_key: str) -> str:
    return hmac.new(
        secret_key.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def create_params(secret_key: str, symbol: str, start_time: str, end_time: str) -> str:

    # Required
    params = {
        "symbol": symbol,
        "timestamp": int(time.time() * 1000),
        "startTime": timestamp_from_str(start_time),
        "endTime": timestamp_from_str(end_time),
    }

    # Build querystring
    query_string = urllib.parse.urlencode(params)

    # Sign
    signature = sign_query(query=query_string, secret_key=secret_key)
    params["signature"] = signature
    print(f"Created params: {params}")
    return params


def get_my_trades(
    api_key: str,
    secret_key: str,
    base_url: str,
    symbol: str,
    start_time: str,
    end_time: str,
) -> requests.Response:

    headers = {"X-MBX-APIKEY": api_key}

    # Make GET request to Binance
    url: str = f"{base_url}myTrades"
    print(f"Request URL: {url}")
    response = requests.get(
        url=url,
        params=create_params(
            secret_key=secret_key,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
        ),
        headers=headers,
    )

    return response


# 2025-12-06 19:10 CET:
# On symbol USDCUSDT I have got trades for both dates 2024-07-22 and 2024-07-23 but:
# request for USDCUSDT 2024-07-23 giving response and for 2024-07-22 not.
# Possibly because of trades being too old?
# It looks like Binance API does not return trades older than 500 days.

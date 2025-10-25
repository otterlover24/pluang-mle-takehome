"""
CoinCap API utilities for cryptocurrency data fetching
"""

import os
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Literal, Optional, Any
import time
from functools import lru_cache
import pandas as pd


base_url = "https://rest.coincap.io"
symbol_to_slug = {
    "btc": "bitcoin",
    "eth": "ethereum",
}


def get_historical_quotes(
    symbol: str,
    start_date: str,
    end_date: str,
    interval: Optional[
        Literal["m1", "m5", "m15", "m30", "h1", "h2", "h6", "h12", "d1"]
    ] = "h1",
) -> pd.DataFrame:
    """
    Get historical OHLCV data for a cryptocurrency using CoinCap API

    Args:
        symbol: Cryptocurrency symbol (e.g., 'bitcoin', 'ethereum')
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with OHLCV data
    """
    # Convert dates to millisecond timestamps for CoinCap API
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)

    # CoinCap uses asset IDs (lowercase names like 'bitcoin', 'ethereum')
    asset_id = symbol.lower()

    # Build the URL and parameters
    endpoint = f"/v3/assets/{symbol_to_slug.get(asset_id, asset_id)}/history"
    url = f"{base_url}{endpoint}"

    params = {"interval": interval, "start": start_ts, "end": end_ts}  # Daily interval

    # Set up headers (optional API key for higher rate limits)
    headers = {}
    api_key = os.getenv("COINCAP_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    # Make the request with error handling
    response = None
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response: {response.text if response is not None else 'No response'}")
        return pd.DataFrame()

    except requests.exceptions.Timeout:
        print(f"Request timeout for {symbol}")
        return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"Request failed for {symbol}: {str(e)}")
        return pd.DataFrame()

    # Parse response data
    if "data" not in data or not data["data"]:
        print(f"No historical data available for {symbol}")
        return pd.DataFrame()

    # Convert to DataFrame
    quotes = data["data"]
    df_data = []

    for quote in quotes:
        df_data.append(
            {
                "Date": datetime.fromisoformat(quote["date"]),
                "Price USD": float(quote["priceUsd"]),
            }
        )

    df = pd.DataFrame(df_data)
    df.set_index("Date", inplace=True)

    return df


def get_macd(symbol: str):
    """
    Gets MACD technical indicator for a cryptocurrency using CoinCap API
    """
    slug = symbol_to_slug.get(symbol.lower(), symbol.lower())
    url = f"{base_url}/v3/ta/{slug}/macd"

    # Set up headers (optional API key for higher rate limits)
    headers = {}
    api_key = os.getenv("COINCAP_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    params = {}
    # Make the request with error handling
    response = None
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response: {response.text if response is not None else 'No response'}")
        return pd.DataFrame()

    except requests.exceptions.Timeout:
        print(f"Request timeout for {symbol}")
        return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"Request failed for {symbol}: {str(e)}")
        return pd.DataFrame()

    # Parse response data
    if "data" not in data or not data["data"]:
        print(f"No historical data available for {symbol}")
        return pd.DataFrame()

    # Convert to DataFrame
    macd_array = data["macd"]
    df_data = []

    for macd_entry in macd_array:
        df_data.append(
            {
                "Date": datetime.fromisoformat(macd_entry["date"]),
                "MACD": float(macd_entry["macd"]),
                "Signal": float(macd_entry["signal"]),
                "Histogram": float(macd_entry["histogram"]),
            }
        )

    df = pd.DataFrame(df_data)
    df.set_index("Date", inplace=True)

    return df

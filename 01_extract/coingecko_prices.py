"""
Extract SOL price + FDV from CoinGecko (last 365 days, free Demo key).

Output columns:
  date, sol_price_usd, fdv_usd

Note: CoinGecko Demo key limits to 365 days of history.
For older data, we fall back to Dune-derived prices (total_reward_usd / total_reward).
"""

import csv
import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import COINGECKO_BASE_URL, SOL_COIN_ID, DATA_RAW, CSV_DELIMITER, CSV_ENCODING


COLUMNS = ["date", "sol_price_usd", "fdv_usd"]
OUTPUT_FILE = "coingecko_sol_price.csv"


def fetch_market_chart(days: int = 365) -> dict:
    """Fetch SOL market chart data from CoinGecko."""
    url = (
        f"{COINGECKO_BASE_URL}/coins/{SOL_COIN_ID}/market_chart"
        f"?vs_currency=usd&days={days}&interval=daily"
    )
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def fetch_coin_data() -> dict:
    """Fetch current SOL coin data (for FDV)."""
    url = f"{COINGECKO_BASE_URL}/coins/{SOL_COIN_ID}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def extract():
    print("  Fetching CoinGecko market chart (365 days)...")
    chart = fetch_market_chart(365)

    prices = chart.get("prices", [])
    print(f"  Got {len(prices)} daily price points")

    # Build rows
    rows = []
    for ts_ms, price in prices:
        date = datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")
        rows.append({"date": date, "sol_price_usd": round(price, 4), "fdv_usd": ""})

    # Try to get current FDV
    try:
        coin = fetch_coin_data()
        fdv = coin.get("market_data", {}).get("fully_diluted_valuation", {}).get("usd")
        if fdv and rows:
            rows[-1]["fdv_usd"] = fdv
            print(f"  Current FDV: ${fdv:,.0f}")
    except Exception as e:
        print(f"  Warning: Could not fetch FDV: {e}")

    # Save
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    filepath = DATA_RAW / OUTPUT_FILE
    with open(filepath, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, delimiter=CSV_DELIMITER)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Saved: {filepath} ({len(rows)} rows)")
    return rows


if __name__ == "__main__":
    print("\n=== Extracting CoinGecko SOL Prices ===")
    extract()
    print("Done.")

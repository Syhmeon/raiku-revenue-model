"""
Merge all raw data sources into a single epoch-level database.

Inputs (from data/raw/):
  - dune_epoch_data_v2.csv       → epoch economics
  - dune_commission_validators_v2.csv → validator metrics
  - dune_active_stake_v1.csv     → active stake
  - coingecko_sol_price.csv      → SOL prices

Output (to data/processed/):
  - solana_epoch_database.csv    → merged, validated dataset

The output contains ONLY raw data columns.
All derived columns (APY, ratios, etc.) should be computed via Excel formulas
or in the model layer — NOT here.
"""

import csv
import sys
from datetime import datetime
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_RAW, DATA_PROCESSED, CSV_DELIMITER, CSV_ENCODING


OUTPUT_FILE = "solana_epoch_database.csv"

# Final columns in the merged output (RAW data only)
OUTPUT_COLUMNS = [
    # Identity
    "epoch", "block_time", "epoch_time",
    # Rewards (SOL)
    "inflationary_reward", "fee_reward", "mev_reward",
    # Commission & Validators
    "voting_rewards_sol", "avg_commission_rate", "validator_count", "stake_account_count",
    # Stake
    "active_stake_sol",
    # APY (from Dune, NULL for recent epochs)
    "issue_apy",
    # Price
    "sol_price_usd", "fdv_usd",
]


def load_csv(filename: str) -> dict[int, dict]:
    """Load a CSV keyed by epoch number."""
    filepath = DATA_RAW / filename
    if not filepath.exists():
        print(f"  WARNING: {filename} not found, skipping")
        return {}

    data = {}
    with open(filepath, encoding=CSV_ENCODING) as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        for row in reader:
            epoch = row.get("epoch", "")
            if epoch and epoch.strip():
                try:
                    epoch_num = int(float(epoch))
                    data[epoch_num] = row
                except ValueError:
                    continue
    print(f"  Loaded {filename}: {len(data)} epochs")
    return data


def match_price_to_epoch(epoch_date: str, prices: list[dict]) -> dict:
    """Find the CoinGecko price closest to an epoch date."""
    if not epoch_date or not prices:
        return {}

    try:
        target = datetime.strptime(epoch_date[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return {}

    best = None
    best_diff = float("inf")
    for p in prices:
        try:
            d = datetime.strptime(p["date"], "%Y-%m-%d")
            diff = abs((d - target).total_seconds())
            if diff < best_diff:
                best_diff = diff
                best = p
        except (ValueError, KeyError):
            continue

    return best or {}


def build():
    print("\n=== Building Merged Epoch Database ===")

    # Load all sources
    epochs = load_csv("dune_epoch_data_v2.csv")
    validators = load_csv("dune_commission_validators_v2.csv")
    stakes = load_csv("dune_active_stake_v1.csv")

    # Load CoinGecko prices as a list (not epoch-keyed)
    prices_list = []
    prices_path = DATA_RAW / "coingecko_sol_price.csv"
    if prices_path.exists():
        with open(prices_path, encoding=CSV_ENCODING) as f:
            prices_list = list(csv.DictReader(f, delimiter=CSV_DELIMITER))
        print(f"  Loaded coingecko_sol_price.csv: {len(prices_list)} days")

    # Determine epoch range
    all_epochs = sorted(set(epochs.keys()) | set(validators.keys()))
    print(f"  Epoch range: {min(all_epochs)} to {max(all_epochs)} ({len(all_epochs)} epochs)")

    # Merge
    merged = []
    for epoch_num in all_epochs:
        e = epochs.get(epoch_num, {})
        v = validators.get(epoch_num, {})
        s = stakes.get(epoch_num, {})

        # Match price by date (CoinGecko, last 365 days only)
        block_time = e.get("block_time", "")
        price_match = match_price_to_epoch(block_time, prices_list)

        # CoinGecko price if available, else derive from Dune (total_reward_usd / total_reward)
        sol_price = price_match.get("sol_price_usd", "")
        if not sol_price:
            try:
                reward_usd = float(e.get("total_reward_usd", 0) or 0)
                reward_sol = float(e.get("total_reward", 0) or 0)
                if reward_sol > 0 and reward_usd > 0:
                    sol_price = round(reward_usd / reward_sol, 4)
            except (ValueError, TypeError):
                sol_price = ""

        row = {
            "epoch": epoch_num,
            "block_time": block_time,
            "epoch_time": e.get("epoch_time", ""),
            "inflationary_reward": e.get("inflationary_reward", ""),
            "fee_reward": e.get("fee_reward", ""),
            "mev_reward": e.get("mev_reward", ""),
            "voting_rewards_sol": v.get("voting_rewards_sol", ""),
            "avg_commission_rate": v.get("avg_commission_rate", ""),
            "validator_count": v.get("validator_count", ""),
            "stake_account_count": v.get("stake_account_count", ""),
            "active_stake_sol": s.get("active_stake_sol", ""),
            "issue_apy": e.get("issue_apy", ""),
            "sol_price_usd": sol_price,
            "fdv_usd": price_match.get("fdv_usd", ""),
        }
        merged.append(row)

    # Save
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    filepath = DATA_PROCESSED / OUTPUT_FILE
    with open(filepath, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, delimiter=CSV_DELIMITER)
        writer.writeheader()
        writer.writerows(merged)

    print(f"\n  Output: {filepath}")
    print(f"  Rows: {len(merged)} epochs")
    print(f"  Columns: {len(OUTPUT_COLUMNS)}")

    # Validate
    nulls = {col: sum(1 for r in merged if not r.get(col)) for col in OUTPUT_COLUMNS}
    print("\n  Null counts:")
    for col, n in nulls.items():
        if n > 0:
            print(f"    {col}: {n}/{len(merged)} ({n/len(merged)*100:.0f}%)")

    return merged


if __name__ == "__main__":
    build()
    print("\nDone.")

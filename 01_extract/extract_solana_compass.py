"""
Extract epoch performance data from Solana Compass API (free, no auth).

Fetches /api/epoch-performance/{epoch} which returns PER-VALIDATOR data.
We aggregate across all validators to get epoch-level totals.

All fee/tip values from the API are in LAMPORTS (÷1e9 for SOL).

Output: data/raw/solana_compass_epochs.csv (semicolon-delimited)
"""

import argparse
import csv
import json
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Import config from parent
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import SOLANA_COMPASS_BASE_URL, SOLANA_COMPASS_FIRST_EPOCH, DATA_RAW, CSV_DELIMITER, CSV_ENCODING

OUTPUT_FILE = "solana_compass_epochs.csv"
LAMPORTS_PER_SOL = 1_000_000_000

# Browser-like headers (some APIs block default Python User-Agent)
HTTP_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

# Output columns — aggregated across all validators for each epoch
COLUMNS = [
    "epoch",
    # Transactions
    "total_txns",
    "total_vote_txns",
    "total_non_vote_txns",
    "total_success_txns",
    "total_failed_txns",
    # Compute Units
    "total_cu",
    # Fees (in SOL, converted from lamports)
    "total_all_fees_sol",
    "total_base_fees_sol",
    "total_priority_fees_sol",
    "total_jito_tips_sol",
    # Slots
    "total_slots",
    "total_skipped_slots",
    "total_packed_slots",
    # Jito-specific
    "total_jito_transactions",
    "total_priority_txns",
    # Validator count (number of validators in response)
    "validator_count",
]


def fetch_epoch(epoch: int, max_retries: int = 3) -> list | None:
    """Fetch per-validator performance data for a single epoch.

    Retries on timeout/5xx errors. Returns None for 422 (not available).
    """
    for attempt in range(max_retries):
        try:
            url = f"{SOLANA_COMPASS_BASE_URL}/epoch-performance/{epoch}"
            req = urllib.request.Request(url, headers=HTTP_HEADERS)
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())
                # API returns {"data": [...validators...], "meta": {...}}
                if isinstance(data, dict) and "data" in data:
                    return data["data"]
                # Or it might return a list directly
                if isinstance(data, list):
                    return data
                return None
        except urllib.error.HTTPError as e:
            if e.code in (404, 422):
                return None  # Epoch not available
            if attempt < max_retries - 1:
                time.sleep(2)
        except (urllib.error.URLError, TimeoutError):
            if attempt < max_retries - 1:
                time.sleep(2)
    return None


def aggregate_validators(validators: list) -> dict:
    """Sum per-validator data to get epoch totals.

    All fee fields from the API are in lamports — we convert to SOL.
    """
    agg = {
        "total_txns": 0,
        "total_vote_txns": 0,
        "total_non_vote_txns": 0,
        "total_success_txns": 0,
        "total_failed_txns": 0,
        "total_cu": 0,
        "total_all_fees_lamports": 0,
        "total_base_fees_lamports": 0,
        "total_priority_fees_lamports": 0,
        "total_jito_tips_lamports": 0,
        "total_slots": 0,
        "total_skipped_slots": 0,
        "total_packed_slots": 0,
        "total_jito_transactions": 0,
        "total_priority_txns": 0,
    }

    for v in validators:
        agg["total_txns"] += _int(v.get("txns"))
        agg["total_vote_txns"] += _int(v.get("vote_txns"))
        agg["total_non_vote_txns"] += _int(v.get("non_vote_txns"))
        agg["total_success_txns"] += _int(v.get("success"))
        agg["total_failed_txns"] += _int(v.get("failed"))
        agg["total_cu"] += _int(v.get("cu"))
        agg["total_all_fees_lamports"] += _int(v.get("all_fees"))
        agg["total_base_fees_lamports"] += _int(v.get("base_fees"))
        agg["total_priority_fees_lamports"] += _int(v.get("priority_fees"))
        agg["total_jito_tips_lamports"] += _int(v.get("jito_total"))
        agg["total_slots"] += _int(v.get("num_slots"))
        agg["total_skipped_slots"] += _int(v.get("skipped"))
        agg["total_packed_slots"] += _int(v.get("packed_slots"))
        agg["total_jito_transactions"] += _int(v.get("jito_transactions"))
        agg["total_priority_txns"] += _int(v.get("priority_txns"))

    # Convert lamports to SOL
    return {
        "total_txns": agg["total_txns"],
        "total_vote_txns": agg["total_vote_txns"],
        "total_non_vote_txns": agg["total_non_vote_txns"],
        "total_success_txns": agg["total_success_txns"],
        "total_failed_txns": agg["total_failed_txns"],
        "total_cu": agg["total_cu"],
        "total_all_fees_sol": round(agg["total_all_fees_lamports"] / LAMPORTS_PER_SOL, 6),
        "total_base_fees_sol": round(agg["total_base_fees_lamports"] / LAMPORTS_PER_SOL, 6),
        "total_priority_fees_sol": round(agg["total_priority_fees_lamports"] / LAMPORTS_PER_SOL, 6),
        "total_jito_tips_sol": round(agg["total_jito_tips_lamports"] / LAMPORTS_PER_SOL, 6),
        "total_slots": agg["total_slots"],
        "total_skipped_slots": agg["total_skipped_slots"],
        "total_packed_slots": agg["total_packed_slots"],
        "total_jito_transactions": agg["total_jito_transactions"],
        "total_priority_txns": agg["total_priority_txns"],
        "validator_count": len(validators),
    }


def _int(val) -> int:
    """Safely convert to int (handle None, empty string, float)."""
    if val is None or val == "":
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def get_current_epoch() -> int:
    """Detect the latest available epoch by binary search."""
    # Start from a known-good recent epoch and go up
    low, high = 930, 960
    best = low
    while low <= high:
        mid = (low + high) // 2
        validators = fetch_epoch(mid, max_retries=1)
        if validators is not None and len(validators) > 0:
            best = mid
            low = mid + 1
        else:
            high = mid - 1
        time.sleep(0.3)
    return best


def load_existing_epochs() -> set:
    """Load already-extracted epoch numbers from the output CSV."""
    filepath = DATA_RAW / OUTPUT_FILE
    if not filepath.exists():
        return set()
    epochs = set()
    with open(filepath, "r", encoding=CSV_ENCODING) as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        for row in reader:
            try:
                epochs.add(int(row["epoch"]))
            except (ValueError, KeyError):
                pass
    return epochs


def extract(full: bool = False):
    """Main extraction logic."""
    print(f"  Solana Compass API: {SOLANA_COMPASS_BASE_URL}")
    print(f"  First epoch: {SOLANA_COMPASS_FIRST_EPOCH}")

    # Determine current epoch
    print("  Detecting current epoch...")
    current_epoch = get_current_epoch()
    print(f"  Current epoch: {current_epoch}")

    total_epochs = current_epoch - SOLANA_COMPASS_FIRST_EPOCH + 1
    print(f"  Total available: {total_epochs} epochs ({SOLANA_COMPASS_FIRST_EPOCH}-{current_epoch})")

    # Determine which epochs to fetch
    if full:
        epochs_to_fetch = list(range(SOLANA_COMPASS_FIRST_EPOCH, current_epoch + 1))
        existing_rows = []
        print(f"  Mode: FULL re-extraction ({len(epochs_to_fetch)} epochs)")
    else:
        existing_epochs = load_existing_epochs()
        all_epochs = set(range(SOLANA_COMPASS_FIRST_EPOCH, current_epoch + 1))
        missing = sorted(all_epochs - existing_epochs)
        epochs_to_fetch = missing

        # Load existing data to merge later
        filepath = DATA_RAW / OUTPUT_FILE
        existing_rows = []
        if filepath.exists():
            with open(filepath, "r", encoding=CSV_ENCODING) as f:
                reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
                existing_rows = list(reader)

        print(f"  Mode: INCREMENTAL ({len(existing_epochs)} existing, {len(missing)} new)")

    if not epochs_to_fetch:
        print("  Nothing to fetch — already up to date!")
        return

    # Fetch and aggregate epochs
    new_rows = []
    errors = []
    for i, epoch in enumerate(epochs_to_fetch):
        validators = fetch_epoch(epoch)
        if validators is not None and len(validators) > 0:
            agg = aggregate_validators(validators)
            row = {"epoch": epoch}
            row.update(agg)
            new_rows.append(row)
        else:
            errors.append(epoch)

        # Progress
        if (i + 1) % 10 == 0 or (i + 1) == len(epochs_to_fetch):
            pct = (i + 1) / len(epochs_to_fetch) * 100
            print(f"    [{i+1}/{len(epochs_to_fetch)}] {pct:.0f}% — last: epoch {epoch}")

        # Rate limit: 1.0s between requests (large per-validator responses)
        time.sleep(1.0)

    print(f"  Fetched: {len(new_rows)} epochs OK, {len(errors)} errors")
    if errors:
        print(f"  Failed epochs: {errors[:20]}{'...' if len(errors) > 20 else ''}")

    # Merge with existing data (if incremental)
    all_rows = existing_rows + new_rows

    # Sort by epoch
    all_rows.sort(key=lambda r: int(r.get("epoch", 0)))

    # Save CSV
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    filepath = DATA_RAW / OUTPUT_FILE
    with open(filepath, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, delimiter=CSV_DELIMITER)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print(f"  Saved: {filepath} ({len(all_rows)} rows)")

    # Summary stats from last epoch
    if new_rows:
        last = new_rows[-1]
        print(f"\n  Latest epoch ({last.get('epoch')}) sample:")
        print(f"    validator_count: {last.get('validator_count')}")
        print(f"    total_non_vote_txns: {last.get('total_non_vote_txns'):,}")
        print(f"    total_priority_fees_sol: {last.get('total_priority_fees_sol')} SOL")
        print(f"    total_jito_tips_sol: {last.get('total_jito_tips_sol')} SOL")
        print(f"    total_all_fees_sol: {last.get('total_all_fees_sol')} SOL")
        print(f"    total_cu: {last.get('total_cu'):,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract Solana Compass epoch data")
    parser.add_argument("--full", action="store_true", help="Re-extract ALL epochs (ignores existing)")
    args = parser.parse_args()

    print("\n=== Extracting Solana Compass Epoch Data ===")
    extract(full=args.full)
    print("Done.")

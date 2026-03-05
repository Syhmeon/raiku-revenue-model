"""
Extract intra-day peak metrics from Trillium epoch_timeseries API.

Fetches /epoch_timeseries/{epoch} for volatile epochs (extreme/elevated tags)
and computes peak 15-minute metrics per epoch:
  - Peak priority fees per block (SOL) and multiple vs epoch baseline
  - Peak user transactions per block and multiple vs epoch baseline
  - Peak MEV/total fees per block (SOL)
  - Skip rate during peak window

These metrics capture intra-day congestion spikes that epoch-level data misses
(e.g. Oct 10, 2025: 32x priority fee spike at peak hour, only 2x at epoch level).

Output: data/raw/trillium_intraday_peaks.csv (semicolon-delimited)
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

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import TRILLIUM_BASE_URL, DATA_RAW, DATA_PROCESSED, CSV_DELIMITER, CSV_ENCODING

OUTPUT_FILE = "trillium_intraday_peaks.csv"
LAMPORTS_PER_SOL = 1_000_000_000

# Columns for the output CSV
COLUMNS = [
    "epoch",
    "volatility_tag",              # From epoch database: extreme/elevated/normal
    "total_buckets",               # Number of 15-min buckets in epoch
    "total_validators",            # Validators observed
    # Baseline metrics (epoch-wide average, per block)
    "baseline_pf_per_block_sol",   # Baseline priority fee per block (SOL)
    "baseline_tx_per_block",       # Baseline user TX per block
    "baseline_mev_per_block_sol",  # Baseline total fees per block (SOL) - proxy for MEV
    # Peak 15-min bucket metrics
    "peak_pf_per_block_sol",       # Peak priority fee per block (SOL)
    "peak_pf_time",                # UTC timestamp of peak PF bucket
    "peak_pf_multiple",            # Peak PF / baseline PF
    "peak_tx_per_block",           # Peak user TX per block
    "peak_tx_time",                # UTC timestamp of peak TX bucket
    "peak_tx_multiple",            # Peak TX / baseline TX
    "peak_tf_per_block_sol",       # Peak total fees per block (SOL) - proxy for MEV
    "peak_tf_time",                # UTC timestamp of peak TF bucket
    "peak_tf_multiple",            # Peak TF / baseline TF
    # Hourly peak (aggregated across 4 consecutive 15-min buckets)
    "peak_hour_pf_sol",            # Peak hourly total priority fees (SOL)
    "peak_hour_pf_time",           # UTC timestamp of peak hour start
    "peak_hour_tx_avg_per_block",  # Peak hourly avg user TX per block
    # Skip rate
    "total_skipped_slots",         # Total skipped slots in epoch
    "total_blocks",                # Total blocks produced
    "skip_rate_pct",               # Skip rate %
]

# Browser-like headers (Trillium blocks default Python User-Agent)
HTTP_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def fetch_timeseries(epoch, retries=3, timeout=60):
    """Fetch epoch_timeseries data from Trillium API."""
    url = f"{TRILLIUM_BASE_URL}/epoch_timeseries/{epoch}"
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HTTP_HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except (urllib.error.HTTPError, urllib.error.URLError, Exception) as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"    Retry {attempt + 1}/{retries} for epoch {epoch} ({e}), waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"    FAILED epoch {epoch} after {retries} attempts: {e}")
                return None


def aggregate_timeseries(data):
    """
    Aggregate per-validator per-bucket data into network-wide per-bucket metrics.
    Returns list of dicts sorted by time_bucket.
    """
    cols = data["columns"]
    rows = data["rows"]

    # Build column index
    ci = {c: i for i, c in enumerate(cols)}

    # Aggregate across validators per time_bucket
    buckets = {}
    validators = set()
    for r in rows:
        tb = r[ci["time_bucket"]]
        pk = r[ci["pk_idx"]]
        validators.add(pk)
        if tb not in buckets:
            buckets[tb] = {
                "time_bucket": tb,
                "blocks": 0, "user_tx": 0, "vote_tx": 0,
                "priority_fees": 0, "cu_used": 0,
                "total_fees": 0, "rewards": 0,
                "skipped_slots": 0,
            }
        b = buckets[tb]
        b["blocks"] += r[ci["block_count"]] or 0
        b["user_tx"] += r[ci["user_tx"]] or 0
        b["vote_tx"] += r[ci["vote_tx"]] or 0
        b["priority_fees"] += r[ci["priority_fees"]] or 0
        b["cu_used"] += r[ci["cu_used"]] or 0
        b["total_fees"] += r[ci["total_fees"]] or 0
        b["rewards"] += r[ci["rewards"]] or 0
        b["skipped_slots"] += r[ci["skipped_slots"]] or 0

    sorted_buckets = sorted(buckets.values(), key=lambda x: x["time_bucket"])
    return sorted_buckets, len(validators)


def compute_peaks(sorted_buckets, n_validators):
    """
    Compute peak metrics from aggregated 15-min buckets.
    Returns a dict of peak metrics ready for CSV.
    """
    from datetime import datetime, timezone

    n = len(sorted_buckets)
    if n == 0:
        return None

    # Per-block metrics for each bucket
    for b in sorted_buckets:
        blocks = b["blocks"]
        b["pf_per_block"] = b["priority_fees"] / blocks if blocks > 0 else 0
        b["tx_per_block"] = b["user_tx"] / blocks if blocks > 0 else 0
        b["tf_per_block"] = b["total_fees"] / blocks if blocks > 0 else 0
        b["time_str"] = datetime.fromtimestamp(
            b["time_bucket"], tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M")

    # Baseline: epoch-wide average (all buckets)
    total_blocks = sum(b["blocks"] for b in sorted_buckets)
    total_pf = sum(b["priority_fees"] for b in sorted_buckets)
    total_tx = sum(b["user_tx"] for b in sorted_buckets)
    total_tf = sum(b["total_fees"] for b in sorted_buckets)
    total_skip = sum(b["skipped_slots"] for b in sorted_buckets)

    bl_pf = total_pf / total_blocks if total_blocks > 0 else 0
    bl_tx = total_tx / total_blocks if total_blocks > 0 else 0
    bl_tf = total_tf / total_blocks if total_blocks > 0 else 0

    # Peak 15-min bucket for each metric
    peak_pf_bucket = max(sorted_buckets, key=lambda b: b["pf_per_block"])
    peak_tx_bucket = max(sorted_buckets, key=lambda b: b["tx_per_block"])
    peak_tf_bucket = max(sorted_buckets, key=lambda b: b["tf_per_block"])

    # Peak hour: sliding window of 4 consecutive 15-min buckets
    peak_hour_pf = 0
    peak_hour_idx = 0
    peak_hour_tx_blocks = 0
    peak_hour_tx_total = 0
    for i in range(max(1, n - 3)):
        window = sorted_buckets[i:i + 4]
        hour_pf = sum(b["priority_fees"] for b in window)
        if hour_pf > peak_hour_pf:
            peak_hour_pf = hour_pf
            peak_hour_idx = i
            peak_hour_tx_blocks = sum(b["blocks"] for b in window)
            peak_hour_tx_total = sum(b["user_tx"] for b in window)

    peak_hour_time = sorted_buckets[peak_hour_idx]["time_str"] if sorted_buckets else ""
    peak_hour_tx_avg = peak_hour_tx_total / peak_hour_tx_blocks if peak_hour_tx_blocks > 0 else 0

    return {
        "total_buckets": n,
        "total_validators": n_validators,
        "baseline_pf_per_block_sol": round(bl_pf / LAMPORTS_PER_SOL, 6),
        "baseline_tx_per_block": round(bl_tx, 1),
        "baseline_mev_per_block_sol": round(bl_tf / LAMPORTS_PER_SOL, 6),
        "peak_pf_per_block_sol": round(peak_pf_bucket["pf_per_block"] / LAMPORTS_PER_SOL, 6),
        "peak_pf_time": peak_pf_bucket["time_str"],
        "peak_pf_multiple": round(peak_pf_bucket["pf_per_block"] / bl_pf, 1) if bl_pf > 0 else 0,
        "peak_tx_per_block": round(peak_tx_bucket["tx_per_block"], 1),
        "peak_tx_time": peak_tx_bucket["time_str"],
        "peak_tx_multiple": round(peak_tx_bucket["tx_per_block"] / bl_tx, 1) if bl_tx > 0 else 0,
        "peak_tf_per_block_sol": round(peak_tf_bucket["tf_per_block"] / LAMPORTS_PER_SOL, 6),
        "peak_tf_time": peak_tf_bucket["time_str"],
        "peak_tf_multiple": round(peak_tf_bucket["tf_per_block"] / bl_tf, 1) if bl_tf > 0 else 0,
        "peak_hour_pf_sol": round(peak_hour_pf / LAMPORTS_PER_SOL, 2),
        "peak_hour_pf_time": peak_hour_time,
        "peak_hour_tx_avg_per_block": round(peak_hour_tx_avg, 1),
        "total_skipped_slots": total_skip,
        "total_blocks": total_blocks,
        "skip_rate_pct": round(total_skip / (total_blocks + total_skip) * 100, 3) if (total_blocks + total_skip) > 0 else 0,
    }


def load_volatile_epochs():
    """Load epochs tagged as extreme or elevated from the epoch database."""
    db_file = DATA_PROCESSED / "solana_epoch_database.csv"
    if not db_file.exists():
        print(f"  ERROR: {db_file} not found. Run build_database.py first.")
        sys.exit(1)

    epochs = []
    with open(db_file, "r", encoding=CSV_ENCODING) as f:
        for r in csv.DictReader(f, delimiter=CSV_DELIMITER):
            tag = r.get("volatility_tag", "normal")
            ep = int(r["epoch"])
            # Only fetch timeseries for Trillium-covered epochs (552+)
            if tag in ("extreme", "elevated") and ep >= 552:
                epochs.append((ep, tag))

    return sorted(epochs)


def load_existing():
    """Load already-extracted epochs from output CSV."""
    outpath = DATA_RAW / OUTPUT_FILE
    if not outpath.exists():
        return set()
    with open(outpath, "r", encoding=CSV_ENCODING) as f:
        return {int(r["epoch"]) for r in csv.DictReader(f, delimiter=CSV_DELIMITER)}


def main():
    parser = argparse.ArgumentParser(description="Extract intra-day peak metrics from Trillium")
    parser.add_argument("--full", action="store_true", help="Re-extract all volatile epochs")
    parser.add_argument("--epochs", type=str, help="Comma-separated epoch list (overrides auto-detect)")
    parser.add_argument("--tag", type=str, default="extreme,elevated",
                        help="Volatility tags to fetch (default: extreme,elevated)")
    args = parser.parse_args()

    print("\n=== Extract Intra-Day Peak Metrics (Trillium epoch_timeseries) ===\n")

    # Determine which epochs to fetch
    if args.epochs:
        epochs = [(int(e.strip()), "manual") for e in args.epochs.split(",")]
        print(f"  Manual epoch list: {len(epochs)} epochs")
    else:
        all_volatile = load_volatile_epochs()
        tags = set(args.tag.split(","))
        epochs = [(ep, tag) for ep, tag in all_volatile if tag in tags]
        print(f"  Auto-detected: {len(all_volatile)} volatile epochs (552+)")
        print(f"  Filtering by tags {tags}: {len(epochs)} epochs to fetch")

    # Incremental mode
    if not args.full:
        existing = load_existing()
        epochs = [(ep, tag) for ep, tag in epochs if ep not in existing]
        print(f"  After incremental filter: {len(epochs)} new epochs")

    if not epochs:
        print("\n  Nothing to extract. Use --full to re-extract all.")
        return

    # Load existing data for appending
    outpath = DATA_RAW / OUTPUT_FILE
    existing_rows = []
    if outpath.exists() and not args.full:
        with open(outpath, "r", encoding=CSV_ENCODING) as f:
            existing_rows = list(csv.DictReader(f, delimiter=CSV_DELIMITER))

    # Fetch and process
    results = list(existing_rows)
    success = 0
    failed = 0

    for i, (epoch, tag) in enumerate(epochs):
        print(f"\n  [{i + 1}/{len(epochs)}] Epoch {epoch} ({tag})...", end=" ", flush=True)

        data = fetch_timeseries(epoch)
        if data is None:
            failed += 1
            continue

        try:
            sorted_buckets, n_validators = aggregate_timeseries(data)
            peaks = compute_peaks(sorted_buckets, n_validators)
            if peaks is None:
                print("EMPTY (no buckets)")
                failed += 1
                continue

            row = {"epoch": epoch, "volatility_tag": tag}
            row.update(peaks)
            results.append(row)
            success += 1

            pf_mult = peaks["peak_pf_multiple"]
            tx_mult = peaks["peak_tx_multiple"]
            print(f"OK — PF peak {pf_mult}x @ {peaks['peak_pf_time']}, TX peak {tx_mult}x @ {peaks['peak_tx_time']}")

        except Exception as e:
            print(f"ERROR: {e}")
            failed += 1

        # Rate limiting: ~1 request per 2 seconds
        if i < len(epochs) - 1:
            time.sleep(2)

    # Save
    results.sort(key=lambda r: int(r["epoch"]))
    with open(outpath, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, delimiter=CSV_DELIMITER)
        writer.writeheader()
        for row in results:
            writer.writerow({k: row.get(k, "") for k in COLUMNS})

    print(f"\n  Saved: {outpath}")
    print(f"  Total: {len(results)} epochs ({success} new, {failed} failed)")
    print(f"  Columns: {len(COLUMNS)}")


if __name__ == "__main__":
    main()

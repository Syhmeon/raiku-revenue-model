"""
Download Query C daily per-program fee data from Dune Analytics.
Uses 6 pre-executed batch queries (5-day windows) and merges into one CSV.

Execution IDs from completed Dune queries:
  C1 (Feb 04-08): 01KKAA9B6QQSM5CJF2PPKRP8Q7  (238 rows)
  C2 (Feb 09-13): 01KKAADX5N0RMSZFRFNQX6PSWH  (228 rows)
  C3 (Feb 14-18): 01KKAAGFDYJ3Y7J2D0SH898EKH  (227 rows)
  C4 (Feb 19-23): 01KKAAKXWJSJNGFWVVFPB1SQB6  (233 rows)
  C5 (Feb 24-28): 01KKAAMM95BMKCYBMGNT6B2E62  (243 rows)
  C6 (Mar 01-05): 01KKAAMMAZS837JW0KJ137E75B  (231 rows)
  Total expected: ~1400 rows
"""

import os
import csv
import json
import time
import urllib.request
import urllib.error
from pathlib import Path
from dotenv import load_dotenv

# ── Paths ──────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
load_dotenv(PROJECT_ROOT / ".env")
API_KEY = os.getenv("DUNE_API_KEY")

# ── Batch definitions ──────────────────────────────────────────────
BATCHES = [
    {"name": "C1", "label": "feb04_08", "exec_id": "01KKAA9B6QQSM5CJF2PPKRP8Q7", "expected": 238},
    {"name": "C2", "label": "feb09_13", "exec_id": "01KKAADX5N0RMSZFRFNQX6PSWH", "expected": 228},
    {"name": "C3", "label": "feb14_18", "exec_id": "01KKAAGFDYJ3Y7J2D0SH898EKH", "expected": 227},
    {"name": "C4", "label": "feb19_23", "exec_id": "01KKAAKXWJSJNGFWVVFPB1SQB6", "expected": 233},
    {"name": "C5", "label": "feb24_28", "exec_id": "01KKAAMM95BMKCYBMGNT6B2E62", "expected": 243},
    {"name": "C6", "label": "mar01_05", "exec_id": "01KKAAMMAZS837JW0KJ137E75B", "expected": 231},
]

COLUMNS = ["program_id", "day", "tx_count", "success_count",
           "base_fees_sol", "priority_fees_sol", "total_fees_sol", "total_cu"]

DUNE_API_BASE = "https://api.dune.com/api/v1"
PAGE_SIZE = 1000  # Dune API max per page


def fetch_execution_results(exec_id: str) -> list[dict]:
    """Fetch all rows from a completed Dune execution via REST API with pagination."""
    all_rows = []
    offset = 0

    while True:
        url = f"{DUNE_API_BASE}/execution/{exec_id}/results?limit={PAGE_SIZE}&offset={offset}"
        req = urllib.request.Request(url, headers={"X-Dune-API-Key": API_KEY})

        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code} fetching {exec_id} offset={offset}: {e.reason}")
            break
        except urllib.error.URLError as e:
            print(f"  URL error fetching {exec_id}: {e.reason}")
            break

        state = data.get("state", "UNKNOWN")
        if state != "QUERY_STATE_COMPLETED":
            print(f"  Execution {exec_id} not completed (state={state}), skipping")
            break

        rows = data.get("result", {}).get("rows", [])
        all_rows.extend(rows)

        # Check if more pages
        metadata = data.get("result", {}).get("metadata", {})
        total_rows = metadata.get("total_row_count", len(all_rows))

        if len(all_rows) >= total_rows or len(rows) == 0:
            break
        offset += len(rows)

    return all_rows


def save_batch_csv(rows: list[dict], filename: str) -> Path:
    """Save rows to semicolon-delimited CSV."""
    filepath = DATA_RAW / filename
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in COLUMNS})
    return filepath


def merge_all_batches(batch_files: list[Path], output_name: str) -> Path:
    """Merge all batch CSVs into one sorted file."""
    all_rows = []
    for bf in batch_files:
        with open(bf, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                all_rows.append(row)

    # Sort by day ASC, total_fees_sol DESC
    all_rows.sort(key=lambda r: (r["day"], -float(r["total_fees_sol"] or 0)))

    output_path = DATA_RAW / output_name
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, delimiter=";")
        writer.writeheader()
        writer.writerows(all_rows)

    return output_path


def validate(filepath: Path):
    """Run validation checks on the merged CSV."""
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        rows = list(reader)

    print(f"\n{'='*60}")
    print(f"VALIDATION: {filepath.name}")
    print(f"{'='*60}")

    # 1. Row count
    n = len(rows)
    print(f"  Row count: {n}  (expected 1200-1590)")
    assert 1000 <= n <= 1700, f"Row count {n} outside expected range"

    # 2. Unique programs
    programs = set(r["program_id"] for r in rows)
    print(f"  Unique programs: {len(programs)}  (expected ~53)")

    # 3. Date range
    days = sorted(set(r["day"] for r in rows))
    print(f"  Date range: {days[0]} to {days[-1]}  (expected 2026-02-04 to 2026-03-05)")
    print(f"  Unique days: {len(days)}")

    # 4. Arithmetic check: base + priority ≈ total
    errors = 0
    for r in rows:
        base = float(r["base_fees_sol"])
        prio = float(r["priority_fees_sol"])
        total = float(r["total_fees_sol"])
        diff = abs((base + prio) - total)
        if diff > 0.001:  # tolerance for rounding
            errors += 1
    print(f"  Arithmetic (base+prio~=total): {errors} mismatches  (should be 0 or near 0)")

    # 5. No negative values
    neg = 0
    for r in rows:
        for col in ["tx_count", "success_count", "base_fees_sol", "priority_fees_sol", "total_fees_sol", "total_cu"]:
            if float(r[col]) < 0:
                neg += 1
    print(f"  Negative values: {neg}  (should be 0)")

    # 6. Top programs by total fees
    from collections import defaultdict
    prog_fees = defaultdict(float)
    for r in rows:
        prog_fees[r["program_id"]] += float(r["total_fees_sol"])
    top5 = sorted(prog_fees.items(), key=lambda x: -x[1])[:5]
    print(f"\n  Top 5 programs by total fees (SOL):")
    for pid, fees in top5:
        print(f"    {pid[:12]}...  {fees:.4f} SOL")

    print(f"\n  VALIDATION {'PASSED' if errors == 0 and neg == 0 else 'WARNINGS'}")


def main():
    if not API_KEY:
        print("ERROR: DUNE_API_KEY not found in .env")
        return

    print(f"Dune API Key: {API_KEY[:8]}...")
    print(f"Output dir: {DATA_RAW}")
    print()

    batch_files = []

    for batch in BATCHES:
        filename = f"dune_daily_{batch['name']}_{batch['label']}.csv"
        print(f"[{batch['name']}] Fetching execution {batch['exec_id'][:16]}...")

        rows = fetch_execution_results(batch["exec_id"])
        print(f"  Got {len(rows)} rows (expected {batch['expected']})")

        if len(rows) == 0:
            print(f"  WARNING: No rows! Skipping.")
            continue

        filepath = save_batch_csv(rows, filename)
        batch_files.append(filepath)
        print(f"  Saved to {filepath.name}")

    if not batch_files:
        print("ERROR: No batch files created!")
        return

    # Merge
    print(f"\nMerging {len(batch_files)} batches...")
    merged = merge_all_batches(batch_files, "dune_daily_program_fees_30d.csv")
    print(f"Merged file: {merged}")

    # Validate
    validate(merged)


if __name__ == "__main__":
    main()

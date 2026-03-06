"""
Extract per-program data by market condition from Dune Analytics.
================================================================
Generates and executes a SQL query that classifies each day (2024+)
as normal/elevated/extreme based on the epoch volatility tags,
then aggregates per-program stats by condition.

This is the KEY query for Point 11: connecting market conditions to program behavior.

Flow:
  1. Read solana_epoch_database.csv → build date→condition mapping
  2. Generate SQL with embedded CASE WHEN for date→condition classification
  3. Execute on Dune (or print SQL for manual execution)
  4. Save results to data/raw/dune_program_conditions.csv

Usage:
  python 01_extract/extract_program_conditions.py --print-sql     # Print SQL to review
  python 01_extract/extract_program_conditions.py --create-query  # Create on Dune + execute
  python 01_extract/extract_program_conditions.py                 # Execute existing query

Output:
  data/raw/dune_program_conditions.csv
  Columns: market_condition;program_id;tx_count;success_count;total_fees_sol;
           total_cu;avg_fee_per_cu_lamports;median_fee_per_cu_lamports;days_observed
"""

import csv
import sys
from pathlib import Path
from datetime import datetime, timedelta

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_RAW, DATA_PROCESSED, DUNE_QUERIES, CSV_DELIMITER, CSV_ENCODING

# ── Config ────────────────────────────────────────────────
EPOCH_DB_FILE = DATA_PROCESSED / "solana_epoch_database.csv"
OUTPUT_FILE = DATA_RAW / "dune_program_conditions.csv"
QUERY_KEY = "program_conditions"  # Key in DUNE_QUERIES (to be added to config.py)
START_DATE = "2024-01-01"  # Only 2024+ (current ecosystem, relevant programs)

OUTPUT_COLUMNS = [
    "market_condition", "program_id", "tx_count", "success_count",
    "total_fees_sol", "total_cu", "avg_fee_per_cu_lamports",
    "median_fee_per_cu_lamports", "days_observed",
]


def build_date_condition_map():
    """Read epoch database and build day→condition mapping for 2024+."""
    if not EPOCH_DB_FILE.exists():
        print(f"  ERROR: {EPOCH_DB_FILE} not found. Run build_database.py first.")
        sys.exit(1)

    with open(EPOCH_DB_FILE, "r", encoding=CSV_ENCODING) as f:
        content = f.read()
        if content.startswith("\ufeff"):
            content = content[1:]
        reader = csv.DictReader(content.splitlines(), delimiter=CSV_DELIMITER)
        rows = list(reader)

    day_cond = {}
    for r in rows:
        date_str = r.get("date", "")
        tag = r.get("volatility_tag", "")
        dur_str = r.get("duration_days", "")
        dur = float(dur_str) if dur_str else 2.0

        if not date_str or not tag or date_str < START_DATE:
            continue

        start = datetime.strptime(date_str, "%Y-%m-%d")
        day = start
        while day < start + timedelta(days=dur):
            day_cond[day.strftime("%Y-%m-%d")] = tag
            day += timedelta(days=1)

    return day_cond


def build_case_when_sql(day_cond):
    """Generate CASE WHEN SQL fragment from day→condition mapping."""
    # Group into contiguous date ranges per condition (skip 'normal' = ELSE)
    sorted_days = sorted((d, t) for d, t in day_cond.items() if t != "normal")

    ranges = []
    for d, t in sorted_days:
        if ranges and ranges[-1]["tag"] == t:
            prev_end = datetime.strptime(ranges[-1]["end"], "%Y-%m-%d")
            curr = datetime.strptime(d, "%Y-%m-%d")
            if (curr - prev_end).days <= 1:
                ranges[-1]["end"] = d
                continue
        ranges.append({"start": d, "end": d, "tag": t})

    # Generate SQL
    lines = []
    for r in ranges:
        if r["start"] == r["end"]:
            lines.append(f"      WHEN block_date = DATE '{r['start']}' THEN '{r['tag']}'")
        else:
            lines.append(
                f"      WHEN block_date BETWEEN DATE '{r['start']}' AND DATE '{r['end']}' THEN '{r['tag']}'"
            )
    lines.append("      ELSE 'normal'")

    case_sql = "    CASE\n" + "\n".join(lines) + "\n    END"

    n_extreme = sum(1 for r in ranges if r["tag"] == "extreme")
    n_elevated = sum(1 for r in ranges if r["tag"] == "elevated")
    print(f"  Generated {len(ranges)} CASE WHEN clauses ({n_extreme} extreme, {n_elevated} elevated)")

    return case_sql


def generate_full_sql(case_when_sql):
    """Generate the complete Dune SQL query."""
    sql = f"""-- RAIKU: Per-program stats by market condition (2024+)
-- Point 11: Which programs pay more during congestion?
-- Market conditions from epoch volatility_tag classification
-- Scans 2024+ with partition pruning on block_date
WITH tx_with_program AS (
  SELECT
    block_date,
    fee,
    compute_units_consumed,
    instructions[1].executing_account AS program_id,
    success,
{case_when_sql} AS market_condition
  FROM solana.transactions
  WHERE block_date >= DATE '{START_DATE}'
    AND compute_units_consumed > 0
    AND fee > 0
)
SELECT
  market_condition,
  program_id,
  COUNT(*) AS tx_count,
  SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
  ROUND(SUM(CAST(fee AS double)) / 1e9, 4) AS total_fees_sol,
  SUM(compute_units_consumed) AS total_cu,
  ROUND(AVG(CAST(fee AS double) / CAST(compute_units_consumed AS double)), 4)
    AS avg_fee_per_cu_lamports,
  ROUND(APPROX_PERCENTILE(
    CAST(fee AS double) / CAST(compute_units_consumed AS double), 0.5
  ), 4) AS median_fee_per_cu_lamports,
  COUNT(DISTINCT block_date) AS days_observed
FROM tx_with_program
GROUP BY market_condition, program_id
HAVING SUM(compute_units_consumed) > 1000000
ORDER BY market_condition, total_fees_sol DESC"""
    return sql


def print_sql():
    """Print the SQL for review."""
    day_cond = build_date_condition_map()
    case_sql = build_case_when_sql(day_cond)
    sql = generate_full_sql(case_sql)

    print("\n" + "=" * 70)
    print("  SQL: Per-program stats by market condition (2024+)")
    print("  → Create on dune.com, set ID in config.py → DUNE_QUERIES['program_conditions']")
    print("=" * 70)
    print(sql)

    # Save SQL to file for easy copy
    sql_file = Path(__file__).parent.parent / "data" / "sql_program_conditions.sql"
    sql_file.parent.mkdir(parents=True, exist_ok=True)
    sql_file.write_text(sql, encoding="utf-8")
    print(f"\n  SQL saved to: {sql_file}")


def create_query():
    """Create the query on Dune via API."""
    from dune_client import DuneClient

    day_cond = build_date_condition_map()
    case_sql = build_case_when_sql(day_cond)
    sql = generate_full_sql(case_sql)

    client = DuneClient()

    query_id = DUNE_QUERIES.get(QUERY_KEY)
    if query_id:
        print(f"  Query already exists: {query_id}")
        print(f"  Updating SQL...")
        client.update_query(query_id, sql)
    else:
        print("  Creating new query on Dune...")
        query_id = client.create_query(
            name="RAIKU: Per-program stats by market condition (2024+)",
            query_sql=sql,
        )
        # Update config.py
        _update_config(query_id)

    return query_id, sql


def _update_config(query_id):
    """Add/update DUNE_QUERIES entry in config.py."""
    import re
    config_path = Path(__file__).parent.parent / "config.py"
    content = config_path.read_text(encoding="utf-8")

    # Check if key already exists
    if f'"{QUERY_KEY}"' in content:
        pattern = rf'("{QUERY_KEY}":\s*)(None|\d+)'
        content = re.sub(pattern, rf'\g<1>{query_id}', content)
    else:
        # Add before the closing brace of DUNE_QUERIES
        content = content.replace(
            '"program_fees_aggregate": ',
            f'"{QUERY_KEY}": {query_id},       # Per-program by market condition (2024+)\n    "program_fees_aggregate": ',
        )

    config_path.write_text(content, encoding="utf-8")
    print(f"  → Updated config.py: DUNE_QUERIES['{QUERY_KEY}'] = {query_id}")


def extract():
    """Execute the query and save results."""
    query_id = DUNE_QUERIES.get(QUERY_KEY)
    if not query_id:
        print(f"  ERROR: Query ID not set for '{QUERY_KEY}'")
        print("  Run with --create-query first")
        return False

    from dune_client import DuneClient
    client = DuneClient()

    print(f"  Executing query {query_id}...")
    print("  (This may take 10-15 minutes — scanning all 2024+ transactions)")
    rows = client.execute_and_fetch(query_id)

    if not rows:
        print("  ERROR: No results returned")
        return False

    # Save
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, delimiter=CSV_DELIMITER,
                                extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"  Saved: {OUTPUT_FILE}")
    print(f"  {len(rows)} rows (program × condition)")

    # Quick stats
    from collections import Counter
    conds = Counter(r.get("market_condition", "") for r in rows)
    for c, n in sorted(conds.items()):
        print(f"    {c}: {n} programs")

    return True


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Extract per-program data by market condition")
    parser.add_argument("--print-sql", action="store_true", help="Print SQL query")
    parser.add_argument("--create-query", action="store_true", help="Create/update query on Dune")
    args = parser.parse_args()

    print("\n=== Extract Program × Condition Data (Dune) ===")

    if args.print_sql:
        print_sql()
        return

    if args.create_query:
        query_id, sql = create_query()
        print(f"\n  Now execute: python {__file__} (to run the query)")
        return

    extract()
    print("\nDone.")


if __name__ == "__main__":
    main()

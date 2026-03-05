"""
Extract per-program fee/CU data from Dune Analytics.
=====================================================
Two modes:
  1. Aggregate: Top 500 programs by priority fees (30-day, one row per program)
  2. Daily: Per-program daily breakdown (30-day, one row per program per day)

The aggregate query is the primary output for the revenue model.
The daily query is optional (for trend analysis).

Usage:
  python 01_extract/extract_dune_programs.py                     # Extract aggregate
  python 01_extract/extract_dune_programs.py --daily              # Also extract daily
  python 01_extract/extract_dune_programs.py --print-sql          # Print SQL to create on Dune
  python 01_extract/extract_dune_programs.py --create-queries     # Create queries on Dune via API + execute

Output:
  data/raw/dune_program_fees_aggregate.csv  (top 500 programs, 30-day)
  data/raw/dune_program_fees_daily.csv      (daily breakdown, optional)
"""

import argparse
import re
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DUNE_QUERIES, DATA_RAW

# ── SQL Queries (to create on Dune) ──────────────────────

SQL_AGGREGATE = """
-- RAIKU: Top programs by priority fees (30-day aggregate)
-- Created via extract_dune_programs.py --create-queries
SELECT
    t.executing_account AS program_id,
    COUNT(*) AS tx_count,
    COUNT(CASE WHEN t.success THEN 1 END) AS success_count,
    SUM(t.fee) / 1e9 AS total_fees_sol,
    SUM(COALESCE(t.priority_fee, 0)) / 1e9 AS priority_fees_sol,
    SUM(t.compute_units_consumed) AS total_cu,
    AVG(t.compute_units_consumed) AS avg_cu_consumed,
    -- Fee per CU distribution (in lamports)
    APPROX_PERCENTILE(
        CASE WHEN t.compute_units_consumed > 0
        THEN CAST(COALESCE(t.priority_fee, 0) AS DOUBLE) / CAST(t.compute_units_consumed AS DOUBLE)
        END, 0.5
    ) AS median_fee_per_cu,
    APPROX_PERCENTILE(
        CASE WHEN t.compute_units_consumed > 0
        THEN CAST(COALESCE(t.priority_fee, 0) AS DOUBLE) / CAST(t.compute_units_consumed AS DOUBLE)
        END, 0.25
    ) AS p25_fee_per_cu,
    APPROX_PERCENTILE(
        CASE WHEN t.compute_units_consumed > 0
        THEN CAST(COALESCE(t.priority_fee, 0) AS DOUBLE) / CAST(t.compute_units_consumed AS DOUBLE)
        END, 0.75
    ) AS p75_fee_per_cu,
    -- Avg fee per CU (for aggregates)
    CASE
        WHEN SUM(t.compute_units_consumed) > 0
        THEN SUM(CAST(COALESCE(t.priority_fee, 0) AS DOUBLE)) / SUM(CAST(t.compute_units_consumed AS DOUBLE))
        ELSE 0
    END AS avg_fee_per_cu_lamports
FROM solana.transactions t
WHERE t.block_time >= NOW() - INTERVAL '30' DAY
    AND t.success = true
    AND t.compute_units_consumed > 0
GROUP BY 1
HAVING SUM(COALESCE(t.priority_fee, 0)) > 0
ORDER BY priority_fees_sol DESC
LIMIT 500
""".strip()

SQL_DAILY = """
-- RAIKU: Per-program daily fee/CU breakdown (30-day)
-- Created via extract_dune_programs.py --create-queries
SELECT
    DATE_TRUNC('day', t.block_time) AS day,
    t.executing_account AS program_id,
    COUNT(*) AS tx_count,
    COUNT(CASE WHEN t.success THEN 1 END) AS success_count,
    SUM(t.fee) / 1e9 AS total_fees_sol,
    SUM(COALESCE(t.priority_fee, 0)) / 1e9 AS priority_fees_sol,
    SUM(t.compute_units_consumed) AS total_cu,
    AVG(t.compute_units_consumed) AS avg_cu_consumed,
    CASE
        WHEN SUM(t.compute_units_consumed) > 0
        THEN SUM(CAST(COALESCE(t.priority_fee, 0) AS DOUBLE)) / SUM(CAST(t.compute_units_consumed AS DOUBLE))
        ELSE 0
    END AS fee_per_cu_lamports
FROM solana.transactions t
WHERE t.block_time >= NOW() - INTERVAL '30' DAY
    AND t.success = true
    AND t.compute_units_consumed > 0
    -- Only top programs (by total priority fees) to limit result size
    AND t.executing_account IN (
        SELECT t2.executing_account
        FROM solana.transactions t2
        WHERE t2.block_time >= NOW() - INTERVAL '30' DAY
            AND t2.success = true
        GROUP BY t2.executing_account
        HAVING SUM(COALESCE(t2.priority_fee, 0)) / 1e9 > 1.0  -- >1 SOL in priority fees
    )
GROUP BY 1, 2
ORDER BY day DESC, priority_fees_sol DESC
""".strip()

# ── Output columns ────────────────────────────────────────

AGGREGATE_COLUMNS = [
    "program_id", "tx_count", "success_count",
    "total_fees_sol", "priority_fees_sol", "total_cu", "avg_cu_consumed",
    "median_fee_per_cu", "p25_fee_per_cu", "p75_fee_per_cu", "avg_fee_per_cu_lamports",
]

DAILY_COLUMNS = [
    "day", "program_id", "tx_count", "success_count",
    "total_fees_sol", "priority_fees_sol", "total_cu", "avg_cu_consumed",
    "fee_per_cu_lamports",
]

AGGREGATE_FILE = "dune_program_fees_aggregate.csv"
DAILY_FILE = "dune_program_fees_daily.csv"


def print_sql():
    """Print the SQL queries to create on Dune."""
    print("\n" + "=" * 70)
    print("  SQL QUERY 1: Aggregate (top 500 programs, 30-day)")
    print("  → Create on dune.com, set ID in config.py → DUNE_QUERIES['program_fees_aggregate']")
    print("=" * 70)
    print(SQL_AGGREGATE)
    print("\n" + "=" * 70)
    print("  SQL QUERY 2: Daily breakdown (30-day, programs > 1 SOL priority fees)")
    print("  → Create on dune.com, set ID in config.py → DUNE_QUERIES['program_fees_30d']")
    print("=" * 70)
    print(SQL_DAILY)


def update_config_query_id(key: str, query_id: int):
    """Update a DUNE_QUERIES entry in config.py with the new query ID."""
    config_path = Path(__file__).parent.parent / "config.py"
    content = config_path.read_text(encoding="utf-8")

    # Match the pattern: "key": None, or "key": 12345,
    pattern = rf'("{key}":\s*)(None|\d+)'
    replacement = rf'\g<1>{query_id}'
    new_content, count = re.subn(pattern, replacement, content)

    if count == 0:
        print(f"  WARNING: Could not find '{key}' in config.py — update manually")
        print(f"  Set DUNE_QUERIES['{key}'] = {query_id}")
        return False

    config_path.write_text(new_content, encoding="utf-8")
    print(f"  → Updated config.py: DUNE_QUERIES['{key}'] = {query_id}")
    return True


def create_queries():
    """Create both queries on Dune via API and update config.py."""
    from dune_client import DuneClient
    client = DuneClient()

    print("\n--- Creating queries on Dune ---")

    # Aggregate query
    agg_id = DUNE_QUERIES.get("program_fees_aggregate")
    if agg_id:
        print(f"  Aggregate query already exists: {agg_id}")
    else:
        agg_id = client.create_query(
            name="RAIKU: Top 500 programs by priority fees (30d)",
            query_sql=SQL_AGGREGATE,
        )
        update_config_query_id("program_fees_aggregate", agg_id)

    # Daily query
    daily_id = DUNE_QUERIES.get("program_fees_30d")
    if daily_id:
        print(f"  Daily query already exists: {daily_id}")
    else:
        daily_id = client.create_query(
            name="RAIKU: Per-program daily fees (30d)",
            query_sql=SQL_DAILY,
        )
        update_config_query_id("program_fees_30d", daily_id)

    return agg_id, daily_id


def extract_aggregate():
    """Extract aggregate per-program data from Dune."""
    query_id = DUNE_QUERIES.get("program_fees_aggregate")
    if not query_id:
        print("\n  ERROR: Dune query ID not set for 'program_fees_aggregate'")
        print("  Run with --create-queries to create it automatically,")
        print("  or create manually on dune.com (use --print-sql to see the SQL)")
        return False

    from dune_client import DuneClient
    client = DuneClient()
    rows = client.execute_and_fetch(query_id)
    client.save_csv(rows, AGGREGATE_FILE, AGGREGATE_COLUMNS)
    return True


def extract_daily():
    """Extract daily per-program data from Dune."""
    query_id = DUNE_QUERIES.get("program_fees_30d")
    if not query_id:
        print("\n  ERROR: Dune query ID not set for 'program_fees_30d'")
        print("  Run with --create-queries to create it automatically,")
        print("  or create manually on dune.com (use --print-sql to see the SQL)")
        return False

    from dune_client import DuneClient
    client = DuneClient()
    rows = client.execute_and_fetch(query_id)
    client.save_csv(rows, DAILY_FILE, DAILY_COLUMNS)
    return True


def main():
    parser = argparse.ArgumentParser(description="Extract per-program fee/CU data from Dune")
    parser.add_argument("--daily", action="store_true", help="Also extract daily breakdown")
    parser.add_argument("--print-sql", action="store_true", help="Print SQL queries to create on Dune")
    parser.add_argument("--create-queries", action="store_true",
                        help="Create queries on Dune via API and update config.py")
    args = parser.parse_args()

    print("\n=== Extract Per-Program Fee/CU Data (Dune) ===")

    if args.print_sql:
        print_sql()
        return

    # Create queries if needed
    if args.create_queries:
        agg_id, daily_id = create_queries()
        # Reload config after update
        import importlib
        import config
        importlib.reload(config)
        # Update local references
        if agg_id:
            DUNE_QUERIES["program_fees_aggregate"] = agg_id
        if daily_id:
            DUNE_QUERIES["program_fees_30d"] = daily_id

    # Aggregate (always)
    print("\n--- Aggregate (top 500 programs, 30-day) ---")
    ok = extract_aggregate()
    if not ok:
        print("\n  Falling back to existing 7-day data in dune_fee_per_cu_by_program.csv")
        existing = DATA_RAW / "dune_fee_per_cu_by_program.csv"
        if existing.exists():
            print(f"  ✓ {existing.name} is available as fallback")
        else:
            print(f"  ✗ No fallback available")

    # Daily (optional)
    if args.daily:
        print("\n--- Daily (per-program, 30-day) ---")
        extract_daily()

    print("\nDone.")


if __name__ == "__main__":
    main()

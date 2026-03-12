"""
Build Program Database — Merge Dune per-program data + manual mapping
=====================================================================
Merges per-program analytics from Dune with manual RAIKU classifications.
Optionally enriches with market condition analysis (from build_program_conditions.py).

Sources:
  1. Dune aggregate (30-day): fee/CU, priority fees, CU, fail rate — primary
  2. Dune 7-day snapshot: fee/CU by program — fallback if 30-day not yet available
  3. program_categories.csv: manual classification into RAIKU archetypes
  4. program_conditions.csv (optional): per-condition fee/CU from daily Dune data

Output: data/processed/program_database.csv (semicolon-delimited)

Changes from v2:
  - Added condition enrichment: fee_multiplier_elevated, fee_multiplier_extreme,
    congestion_sensitivity columns (from program_conditions.csv if available)
  - These columns show how programs behave under different market conditions,
    directly sizing the AOT opportunity.
"""

import csv
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_RAW, DATA_PROCESSED, DATA_MAPPING, CSV_DELIMITER, CSV_ENCODING

# ── Input files ────────────────────────────────────────────
DUNE_AGGREGATE_FILE = DATA_RAW / "dune_program_fees_v3.csv"
DUNE_7DAY_FILE = DATA_RAW / "dune_fee_per_cu_by_program.csv"
MAPPING_FILE = DATA_MAPPING / "program_categories.csv"
CONDITIONS_FILE = DATA_PROCESSED / "program_conditions.csv"

# ── Output ─────────────────────────────────────────────────
OUTPUT_FILE = DATA_PROCESSED / "program_database.csv"

OUTPUT_COLUMNS = [
    "program_id",                            # A - Address (PK)
    "program_name",                          # B - Human-readable name
    "raiku_category",                        # C - RAIKU archetype category
    "raiku_subcategory",                     # D - Subcategory (amm, orderbook, prop_amm, etc.)
    "raiku_product",                         # E - aot / jit / both / potential / neither / unknown
    "period_days",                           # F - Data period in days (30 or 7)
    "tx_count",                              # G - Total transactions (success + failed)
    "success_count",                         # H - Successful transactions
    "fail_rate",                             # I - Failure rate (0-1), from SQL including failed txns
    "base_plus_priority_fees_sol",           # J - Total on-chain fees (base + priority) in SOL
    "priority_fees_sol",                     # K - Priority fees only in SOL
    "total_cu",                              # L - Total CU consumed (all txns)
    "avg_cu_per_tx",                         # M - Average CU per transaction
    "avg_cu_per_block",                      # N - Avg CU per block (blockspace perspective)
    "blocks_touched",                        # O - Number of distinct blocks this program appeared in
    "median_priority_fee_per_cu_lamports",   # P - Median priority fee/CU (lamports)
    "p25_priority_fee_per_cu_lamports",      # Q - P25 priority fee/CU (lamports)
    "p75_priority_fee_per_cu_lamports",      # R - P75 priority fee/CU (lamports)
    "avg_priority_fee_per_cu_lamports",      # S - Average priority fee/CU (lamports)
    "pct_of_total_priority",                 # T - % of total priority fees (COMPUTED, not raw)
    # Condition enrichment (from program_conditions.csv, Point 11)
    "fee_multiplier_elevated",               # U - Fee/CU ratio: elevated ÷ normal
    "fee_multiplier_extreme",                # V - Fee/CU ratio: extreme ÷ normal
    "congestion_sensitivity",                # W - high / medium / low / unknown
]

# ── Column mapping for 7-day fallback ─────────────────────
# The 7-day CSV has different column names — map them to the new schema
FALLBACK_COL_MAP = {
    "primary_program": "program_id",
    "program_name": "program_name",
    "tx_count": "tx_count",
    "success_count": "success_count",
    "total_fees_sol": "base_plus_priority_fees_sol",
    "avg_fee_per_cu_lamports": "avg_priority_fee_per_cu_lamports",
    "median_fee_per_cu": "median_priority_fee_per_cu_lamports",
    "p25_fee_per_cu": "p25_priority_fee_per_cu_lamports",
    "p75_fee_per_cu": "p75_priority_fee_per_cu_lamports",
    "avg_cu_consumed": "avg_cu_per_tx",
    "total_cu_billions": "_total_cu_billions",  # Needs ×1e9 conversion
}


def safe_float(val, default=None):
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def load_csv(filepath):
    if not filepath.exists():
        print(f"  WARNING: {filepath.name} not found")
        return []
    with open(filepath, "r", encoding=CSV_ENCODING) as f:
        content = f.read()
        if content.startswith("\ufeff"):
            content = content[1:]
        reader = csv.DictReader(content.splitlines(), delimiter=CSV_DELIMITER)
        return list(reader)


def load_mapping():
    """Load program → category mapping."""
    if not MAPPING_FILE.exists():
        print(f"  WARNING: Mapping file not found ({MAPPING_FILE.name})")
        return {}
    rows = load_csv(MAPPING_FILE)
    mapping = {}
    for r in rows:
        pid = r.get("program_id", "").strip()
        if pid:
            mapping[pid] = {
                "name": r.get("program_name", ""),
                "category": r.get("raiku_category", "unknown"),
                "subcategory": r.get("subcategory", ""),
                "product": r.get("raiku_product", "unknown"),
            }
    print(f"    Mapping: {len(mapping)} programs classified")
    return mapping


def build_from_30d(dune_rows, mapping):
    """Build program records from 30-day Dune aggregate data."""
    programs = {}
    for r in dune_rows:
        pid = r.get("program_id", "").strip()
        if not pid:
            continue

        # Name and classification from mapping
        name = mapping.get(pid, {}).get("name") or r.get("program_name", "")
        cat = mapping.get(pid, {}).get("category", "unknown")
        subcat = mapping.get(pid, {}).get("subcategory", "")
        product = mapping.get(pid, {}).get("product", "unknown")

        tx = safe_float(r.get("tx_count"), 0)
        success = safe_float(r.get("success_count"), 0)

        programs[pid] = {
            "program_id": pid,
            "program_name": name,
            "raiku_category": cat,
            "raiku_subcategory": subcat,
            "raiku_product": product,
            "period_days": 30,
            "tx_count": int(tx) if tx else "",
            "success_count": int(success) if success else "",
            "fail_rate": round(1 - success / tx, 4) if tx > 0 else "",
            "base_plus_priority_fees_sol": safe_float(r.get("total_fees_sol")),
            "priority_fees_sol": safe_float(r.get("priority_fees_sol")),
            "total_cu": safe_float(r.get("total_cu")),
            "avg_cu_per_tx": safe_float(r.get("avg_cu_per_tx", r.get("avg_cu_consumed"))),
            "avg_cu_per_block": safe_float(r.get("avg_cu_per_block")),
            "blocks_touched": safe_float(r.get("blocks_touched")),
            "median_priority_fee_per_cu_lamports": safe_float(r.get("median_priority_fee_per_cu_lamports",
                                                                     r.get("median_fee_per_cu"))),
            "p25_priority_fee_per_cu_lamports": safe_float(r.get("p25_priority_fee_per_cu_lamports",
                                                                  r.get("p25_fee_per_cu"))),
            "p75_priority_fee_per_cu_lamports": safe_float(r.get("p75_priority_fee_per_cu_lamports",
                                                                  r.get("p75_fee_per_cu"))),
            "avg_priority_fee_per_cu_lamports": safe_float(r.get("avg_priority_fee_per_cu_lamports",
                                                                  r.get("avg_fee_per_cu_lamports"))),
            "pct_of_total_priority": None,  # Computed below
        }
    return programs


def build_from_7d(dune_rows, mapping):
    """Build program records from 7-day fallback data (different column names)."""
    programs = {}
    for r in dune_rows:
        pid = r.get("primary_program", "").strip()
        if not pid:
            continue

        name = mapping.get(pid, {}).get("name") or r.get("program_name", "")
        cat = mapping.get(pid, {}).get("category", "unknown")
        subcat = mapping.get(pid, {}).get("subcategory", "")
        product = mapping.get(pid, {}).get("product", "unknown")

        tx = safe_float(r.get("tx_count"), 0)
        success = safe_float(r.get("success_count"), 0)

        # 7-day file has total_cu_billions — convert to raw CU
        total_cu_b = safe_float(r.get("total_cu_billions"))
        total_cu = total_cu_b * 1e9 if total_cu_b is not None else None

        programs[pid] = {
            "program_id": pid,
            "program_name": name,
            "raiku_category": cat,
            "raiku_subcategory": subcat,
            "raiku_product": product,
            "period_days": 7,
            "tx_count": int(tx) if tx else "",
            "success_count": int(success) if success else "",
            "fail_rate": round(1 - success / tx, 4) if tx > 0 else "",
            "base_plus_priority_fees_sol": safe_float(r.get("total_fees_sol")),
            "priority_fees_sol": None,  # Not available in 7-day file
            "total_cu": total_cu,
            "avg_cu_per_tx": safe_float(r.get("avg_cu_consumed")),
            "avg_cu_per_block": None,  # Not available in 7-day file
            "blocks_touched": None,  # Not available in 7-day file
            "median_priority_fee_per_cu_lamports": safe_float(r.get("median_fee_per_cu")),
            "p25_priority_fee_per_cu_lamports": safe_float(r.get("p25_fee_per_cu")),
            "p75_priority_fee_per_cu_lamports": safe_float(r.get("p75_fee_per_cu")),
            "avg_priority_fee_per_cu_lamports": safe_float(r.get("avg_fee_per_cu_lamports")),
            "pct_of_total_priority": None,  # Cannot compute without priority_fees_sol
        }
    return programs


def load_conditions():
    """Load condition enrichment data (fee multipliers, sensitivity) if available."""
    if not CONDITIONS_FILE.exists():
        print(f"    Conditions: not available (run build_program_conditions.py first)")
        return {}
    rows = load_csv(CONDITIONS_FILE)
    cond_map = {}
    for r in rows:
        pid = r.get("program_id", "").strip()
        if pid:
            cond_map[pid] = {
                "fee_multiplier_elevated": safe_float(r.get("fee_multiplier_elevated")),
                "fee_multiplier_extreme": safe_float(r.get("fee_multiplier_extreme")),
                "congestion_sensitivity": r.get("congestion_sensitivity", "unknown"),
            }
    print(f"    Conditions: {len(cond_map)} programs with condition data")
    enriched = sum(1 for v in cond_map.values() if v.get("congestion_sensitivity") != "unknown")
    print(f"      {enriched} with known sensitivity (high/medium/low)")
    return cond_map


def build():
    print("  Loading sources...")

    # 1. Load Dune aggregate (preferred) or 7-day fallback
    dune_rows = load_csv(DUNE_AGGREGATE_FILE)
    is_30d = bool(dune_rows)

    if not dune_rows:
        print("  → Falling back to 7-day Dune data")
        dune_rows = load_csv(DUNE_7DAY_FILE)

    if dune_rows:
        label = "30d" if is_30d else "7d"
        print(f"    Dune ({label}): {len(dune_rows)} programs")
    else:
        print("    Dune: no data available")
        return

    # 2. Load mapping
    mapping = load_mapping()

    # 3. Build merged database
    print("\n  Merging...")
    if is_30d:
        programs = build_from_30d(dune_rows, mapping)
    else:
        programs = build_from_7d(dune_rows, mapping)

    # 4. Compute % of total priority fees (DERIVED column)
    total_pf = sum(safe_float(p.get("priority_fees_sol"), 0) for p in programs.values())
    if total_pf > 0:
        for p in programs.values():
            pf = safe_float(p.get("priority_fees_sol"), 0)
            p["pct_of_total_priority"] = round(pf / total_pf, 6) if pf > 0 else 0

    # 5. Enrich with condition data (Point 11 — market condition × programs)
    conditions = load_conditions()
    enriched_count = 0
    for pid, p in programs.items():
        cond = conditions.get(pid, {})
        p["fee_multiplier_elevated"] = cond.get("fee_multiplier_elevated")
        p["fee_multiplier_extreme"] = cond.get("fee_multiplier_extreme")
        p["congestion_sensitivity"] = cond.get("congestion_sensitivity", "unknown")
        if cond:
            enriched_count += 1
    if conditions:
        print(f"    Enriched {enriched_count}/{len(programs)} programs with condition data")

    # Sort by priority fees descending (fallback: total fees)
    def sort_key(p):
        return safe_float(p.get("priority_fees_sol"), 0) or safe_float(p.get("base_plus_priority_fees_sol"), 0)

    sorted_programs = sorted(programs.values(), key=sort_key, reverse=True)

    # Save
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, delimiter=CSV_DELIMITER,
                                extrasaction="ignore")
        writer.writeheader()
        for p in sorted_programs:
            clean = {k: ("" if v is None else v) for k, v in p.items()}
            writer.writerow(clean)

    print(f"\n  Saved: {OUTPUT_FILE}")
    print(f"  {len(sorted_programs)} programs × {len(OUTPUT_COLUMNS)} columns")

    # Stats
    classified = sum(1 for p in sorted_programs if p.get("raiku_category") not in ("unknown", ""))
    unclassified = len(sorted_programs) - classified
    print(f"\n  Classification: {classified}/{len(sorted_programs)} programs mapped ({unclassified} unclassified)")

    # Fail rate stats (only meaningful if SQL includes failed txns)
    fail_rates = [safe_float(p.get("fail_rate"), 0) for p in sorted_programs if p.get("fail_rate") != ""]
    non_zero_fails = [f for f in fail_rates if f > 0]
    if fail_rates:
        print(f"  Fail rates: {len(non_zero_fails)}/{len(fail_rates)} programs have non-zero fail rate")
        if non_zero_fails:
            avg_fail = sum(non_zero_fails) / len(non_zero_fails)
            max_fail = max(non_zero_fails)
            print(f"    Avg (non-zero): {avg_fail:.2%}, Max: {max_fail:.2%}")

    if total_pf > 0:
        top10_pf = sum(safe_float(p.get("priority_fees_sol"), 0) for p in sorted_programs[:10])
        print(f"  Top 10 = {top10_pf/total_pf*100:.1f}% of total priority fees")

    # Show top 10
    label = "30d" if is_30d else "7d"
    print(f"\n  Top 10 programs by priority fees ({label}):")
    for i, p in enumerate(sorted_programs[:10], 1):
        name = p.get("program_name") or p["program_id"][:12] + "..."
        pf = safe_float(p.get("priority_fees_sol"), 0) or safe_float(p.get("base_plus_priority_fees_sol"), 0)
        cat = p.get("raiku_category", "?")
        prod = p.get("raiku_product", "?")
        fail = safe_float(p.get("fail_rate"), 0)
        med = safe_float(p.get("median_priority_fee_per_cu_lamports"))
        med_str = f"{med:.4f}" if med is not None else "?"
        print(f"    {i:>2}. {name:<30} | {pf:>10.2f} SOL | med: {med_str} lam/CU"
              f" | fail: {fail:.1%} | {cat}/{prod}")


if __name__ == "__main__":
    print("\n=== Building Program Database ===")
    build()
    print("\nDone.")

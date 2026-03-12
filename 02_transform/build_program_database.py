"""
Build Program Database — canonical merge for fixed batch window
===============================================================

Canonical batch window:
  [2026-02-04, 2026-03-05)  (end date exclusive)

Canonical sources:
  1) dune_program_fees_v3.csv  (Query 6817783)    -> canonical fee/CU columns
  2) dune_jito_tips_qc_20260204_20260305_excl.csv (Query 6818065) -> Jito-only fields
  3) program_categories.csv    -> manual taxonomy mapping
  4) program_conditions.csv    -> optional condition enrichment

Output:
  data/processed/program_database.csv (semicolon-delimited)
"""

import csv
import sys
from collections import Counter
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_RAW, DATA_PROCESSED, DATA_MAPPING, CSV_DELIMITER, CSV_ENCODING


# ── Canonical inputs ───────────────────────────────────────
DUNE_BASE_FILE = DATA_RAW / "dune_program_fees_v3.csv"
DUNE_JITO_FILE = DATA_RAW / "dune_jito_tips_qc_20260204_20260305_excl.csv"
MAPPING_FILE = DATA_MAPPING / "program_categories.csv"
CONDITIONS_FILE = DATA_PROCESSED / "program_conditions.csv"

# ── Canonical batch metadata ───────────────────────────────
CANONICAL_BATCH_WINDOW = "[2026-02-04, 2026-03-05)"
PERIOD_DAYS = 30
LAMPORTS_PER_SOL = 1_000_000_000

# ── Validation policy ──────────────────────────────────────
SHARED_COLUMNS = [
    "tx_count",
    "priority_fees_sol",
    "total_fees_sol",
    "total_cu",
    "avg_cu_per_tx",
    "blocks_touched",
    "avg_cu_per_block",
]

EXACT_COLUMNS = {"tx_count", "total_cu", "blocks_touched"}
TOLERANCES = {
    "priority_fees_sol": 0.001,
    "total_fees_sol": 0.001,
    "avg_cu_per_tx": 1.0,
    "avg_cu_per_block": 1.0,
}

# Blocking threshold if shared-column mismatch is materially high.
MATERIAL_MISMATCH_RATE = 0.20
MATERIAL_MISMATCH_ABS = 25

# ── Required columns ───────────────────────────────────────
REQUIRED_BASE_COLUMNS = [
    "program_id",
    "tx_count",
    "success_count",
    "base_fees_sol",
    "priority_fees_sol",
    "total_fees_sol",
    "total_cu",
    "avg_cu_per_tx",
    "blocks_touched",
    "avg_cu_per_block",
    "median_priority_fee_per_cu_lamports",
    "p25_priority_fee_per_cu_lamports",
    "p75_priority_fee_per_cu_lamports",
    "avg_priority_fee_per_cu_lamports",
]

REQUIRED_JITO_COLUMNS = [
    "program_id",
    "tx_count",
    "jito_tx_count",
    "jito_tips_sol",
    "priority_fees_sol",
    "total_fees_sol",
    "total_cu",
    "avg_cu_per_tx",
    "blocks_touched",
    "avg_cu_per_block",
    "jito_tips_per_cu_lamports",
    "priority_fees_per_cu_lamports",
]

# ── Output ─────────────────────────────────────────────────
OUTPUT_FILE = DATA_PROCESSED / "program_database.csv"

OUTPUT_COLUMNS = [
    "program_id",
    "program_name",
    "raiku_category",
    "raiku_subcategory",
    "raiku_product",
    "period_days",
    "batch_window",
    "tx_count",
    "success_count",
    "fail_rate",
    "base_fees_sol",
    "priority_fees_sol",
    "total_fees_sol",
    "base_plus_priority_fees_sol",
    "jito_tx_count",
    "jito_mev_fees_sol",
    "jito_tips_per_cu_lamports",
    "total_cu",
    "avg_cu_per_tx",
    "blocks_touched",
    "avg_cu_per_block",
    "median_priority_fee_per_cu_lamports",
    "p25_priority_fee_per_cu_lamports",
    "p75_priority_fee_per_cu_lamports",
    "avg_priority_fee_per_cu_lamports",
    "total_fees_incl_jito",
    "non_base_fees_incl_jito",
    "non_base_fees_per_cu_incl_jito",
    "total_fees_per_cu_incl_jito",
    "avg_base_fee_per_tx",
    "avg_total_fee_per_tx_excl_jito",
    "avg_jito_fee_per_tx",
    "avg_tx_per_block",
    "pct_of_total_priority",
    "fee_multiplier_elevated",
    "fee_multiplier_extreme",
    "congestion_sensitivity",
]


def safe_float(val, default=None):
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        pass

    s = str(val).strip()
    if not s:
        return default

    # Accept both dot and comma decimal formats from CSV exports.
    s = s.replace("\u00a0", "").replace(" ", "")
    if "," in s and "." in s:
        # Use the right-most separator as decimal; strip the other as thousands.
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    num = safe_float(val, None)
    if num is None:
        return default
    return int(round(num))


def safe_div(numerator, denominator):
    if numerator is None:
        numerator = 0.0
    if denominator in (None, 0, 0.0):
        return 0.0
    return numerator / denominator


def load_csv_strict(filepath, required_columns, label):
    if not filepath.exists():
        raise RuntimeError(f"{label} missing: {filepath}")

    with open(filepath, "r", encoding=CSV_ENCODING) as f:
        content = f.read()
        if content.startswith("\ufeff"):
            content = content[1:]
        reader = csv.DictReader(content.splitlines(), delimiter=CSV_DELIMITER)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    missing = [c for c in required_columns if c not in fieldnames]
    if missing:
        raise RuntimeError(
            f"{label} schema mismatch. Missing columns: {', '.join(missing)}"
        )

    if not rows:
        raise RuntimeError(f"{label} has no rows: {filepath}")

    print(f"    {label}: {len(rows)} rows, {len(fieldnames)} columns")
    return rows


def assert_unique_program_id(rows, label):
    counts = Counter(r.get("program_id", "").strip() for r in rows)
    duplicates = [(pid, n) for pid, n in counts.items() if pid and n > 1]
    if duplicates:
        preview = ", ".join(f"{pid} ({n})" for pid, n in duplicates[:8])
        raise RuntimeError(f"{label} has duplicate program_id values: {preview}")
    print(f"    {label}: program_id uniqueness OK ({len(counts)} unique IDs)")


def index_by_program_id(rows):
    indexed = {}
    for row in rows:
        pid = row.get("program_id", "").strip()
        if pid:
            indexed[pid] = row
    return indexed


def validate_shared_columns(base_map, jito_map):
    base_ids = set(base_map.keys())
    jito_ids = set(jito_map.keys())
    matched_ids = base_ids & jito_ids
    only_base = len(base_ids - jito_ids)
    only_jito = len(jito_ids - base_ids)

    print("\n  Shared-column validation (base vs jito)")
    print(f"    Matched program_ids: {len(matched_ids)}")
    print(f"    Base-only program_ids: {only_base}")
    print(f"    Jito-only program_ids: {only_jito}")

    if not matched_ids:
        raise RuntimeError("No overlapping program_id rows between base and Jito files.")

    mismatch_counts = Counter()
    mismatch_rows = []

    for pid in matched_ids:
        base = base_map[pid]
        jito = jito_map[pid]
        row_diffs = {}

        for col in SHARED_COLUMNS:
            b = safe_float(base.get(col), None)
            j = safe_float(jito.get(col), None)
            if b is None or j is None:
                continue

            if col in EXACT_COLUMNS:
                is_match = int(round(b)) == int(round(j))
                delta = abs(int(round(b)) - int(round(j)))
            else:
                tol = TOLERANCES.get(col, 1e-9)
                delta = abs(b - j)
                is_match = delta <= tol

            if not is_match:
                mismatch_counts[col] += 1
                row_diffs[col] = {"base": b, "jito": j, "delta": delta}

        if row_diffs:
            score = row_diffs.get("total_fees_sol", {"delta": 0})["delta"]
            mismatch_rows.append({"program_id": pid, "score": score, "diffs": row_diffs})

    mismatch_rows_count = len(mismatch_rows)
    mismatch_rate = mismatch_rows_count / len(matched_ids) if matched_ids else 0.0

    print(f"    Rows with >=1 mismatch: {mismatch_rows_count} ({mismatch_rate:.2%})")
    for col in SHARED_COLUMNS:
        print(f"      {col:<18} mismatches: {mismatch_counts[col]}")

    if mismatch_rows:
        print("\n    Top mismatches (first 10):")
        top = sorted(mismatch_rows, key=lambda x: x["score"], reverse=True)[:10]
        for item in top:
            pid = item["program_id"]
            diffs = ", ".join(
                f"{k}:base={v['base']} jito={v['jito']} Δ={v['delta']}"
                for k, v in item["diffs"].items()
            )
            print(f"      {pid[:12]}... | {diffs}")

    if (
        mismatch_rows_count >= MATERIAL_MISMATCH_ABS
        and mismatch_rate > MATERIAL_MISMATCH_RATE
    ):
        raise RuntimeError(
            "Blocking error: shared-column mismatch rate is materially high "
            f"({mismatch_rows_count}/{len(matched_ids)} = {mismatch_rate:.2%}). "
            "Check that both inputs use the same canonical window and query definitions."
        )


def load_mapping():
    if not MAPPING_FILE.exists():
        print(f"  WARNING: Mapping file not found ({MAPPING_FILE.name})")
        return {}

    with open(MAPPING_FILE, "r", encoding=CSV_ENCODING) as f:
        content = f.read()
        if content.startswith("\ufeff"):
            content = content[1:]
        reader = csv.DictReader(content.splitlines(), delimiter=CSV_DELIMITER)
        rows = list(reader)

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


def load_conditions():
    if not CONDITIONS_FILE.exists():
        print("    Conditions: not available (run build_program_conditions.py first)")
        return {}

    with open(CONDITIONS_FILE, "r", encoding=CSV_ENCODING) as f:
        content = f.read()
        if content.startswith("\ufeff"):
            content = content[1:]
        reader = csv.DictReader(content.splitlines(), delimiter=CSV_DELIMITER)
        rows = list(reader)

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
    return cond_map


def build_records(base_rows, jito_map, mapping):
    programs = {}
    for row in base_rows:
        pid = row.get("program_id", "").strip()
        if not pid:
            continue

        jito = jito_map.get(pid, {})

        tx_count = safe_int(row.get("tx_count"), 0)
        success_count = safe_int(row.get("success_count"), 0)
        base_fees_sol = safe_float(row.get("base_fees_sol"), 0.0) or 0.0
        priority_fees_sol = safe_float(row.get("priority_fees_sol"), 0.0) or 0.0
        total_fees_sol = safe_float(row.get("total_fees_sol"), 0.0) or 0.0
        total_cu = safe_float(row.get("total_cu"), 0.0) or 0.0
        blocks_touched = safe_int(row.get("blocks_touched"), 0)

        jito_mev_fees_sol = safe_float(jito.get("jito_tips_sol"), 0.0) or 0.0
        jito_tx_count = safe_int(jito.get("jito_tx_count"), 0)
        jito_tips_per_cu_lamports = safe_float(jito.get("jito_tips_per_cu_lamports"))

        total_fees_incl_jito = total_fees_sol + jito_mev_fees_sol
        non_base_fees_incl_jito = priority_fees_sol + jito_mev_fees_sol

        programs[pid] = {
            "program_id": pid,
            "program_name": mapping.get(pid, {}).get("name") or row.get("program_name", ""),
            "raiku_category": mapping.get(pid, {}).get("category", "unknown"),
            "raiku_subcategory": mapping.get(pid, {}).get("subcategory", ""),
            "raiku_product": mapping.get(pid, {}).get("product", "unknown"),
            "period_days": PERIOD_DAYS,
            "batch_window": CANONICAL_BATCH_WINDOW,
            "tx_count": tx_count,
            "success_count": success_count,
            "fail_rate": round(safe_div(tx_count - success_count, tx_count), 4),
            "base_fees_sol": base_fees_sol,
            "priority_fees_sol": priority_fees_sol,
            "total_fees_sol": total_fees_sol,
            "base_plus_priority_fees_sol": total_fees_sol,
            "jito_tx_count": jito_tx_count,
            "jito_mev_fees_sol": jito_mev_fees_sol,
            "jito_tips_per_cu_lamports": jito_tips_per_cu_lamports if jito_tips_per_cu_lamports is not None else "",
            "total_cu": total_cu,
            "avg_cu_per_tx": safe_float(row.get("avg_cu_per_tx"), 0.0) or 0.0,
            "blocks_touched": blocks_touched,
            "avg_cu_per_block": safe_float(row.get("avg_cu_per_block"), 0.0) or 0.0,
            "median_priority_fee_per_cu_lamports": safe_float(row.get("median_priority_fee_per_cu_lamports")),
            "p25_priority_fee_per_cu_lamports": safe_float(row.get("p25_priority_fee_per_cu_lamports")),
            "p75_priority_fee_per_cu_lamports": safe_float(row.get("p75_priority_fee_per_cu_lamports")),
            "avg_priority_fee_per_cu_lamports": safe_float(row.get("avg_priority_fee_per_cu_lamports")),
            "total_fees_incl_jito": round(total_fees_incl_jito, 6),
            "non_base_fees_incl_jito": round(non_base_fees_incl_jito, 6),
            # Aggregate-period ratios from totals (lamports/CU), not tx-level averages.
            "non_base_fees_per_cu_incl_jito": safe_div(non_base_fees_incl_jito * LAMPORTS_PER_SOL, total_cu),
            "total_fees_per_cu_incl_jito": safe_div(total_fees_incl_jito * LAMPORTS_PER_SOL, total_cu),
            "avg_base_fee_per_tx": safe_div(base_fees_sol, tx_count),
            "avg_total_fee_per_tx_excl_jito": safe_div(total_fees_sol, tx_count),
            "avg_jito_fee_per_tx": safe_div(jito_mev_fees_sol, tx_count),
            "avg_tx_per_block": safe_div(tx_count, blocks_touched),
            "pct_of_total_priority": 0.0,
        }

    # % of total priority fees (canonical base metric)
    total_priority = sum(
        safe_float(p.get("priority_fees_sol"), 0.0) or 0.0 for p in programs.values()
    )
    if total_priority > 0:
        for p in programs.values():
            pf = safe_float(p.get("priority_fees_sol"), 0.0) or 0.0
            p["pct_of_total_priority"] = round(pf / total_priority, 6)

    return programs


def build():
    print("  Loading canonical sources...")
    print(f"    Batch window: {CANONICAL_BATCH_WINDOW}")

    base_rows = load_csv_strict(DUNE_BASE_FILE, REQUIRED_BASE_COLUMNS, "Base (Query 6817783)")
    jito_rows = load_csv_strict(DUNE_JITO_FILE, REQUIRED_JITO_COLUMNS, "Jito (Query 6818065)")

    assert_unique_program_id(base_rows, "Base (Query 6817783)")
    assert_unique_program_id(jito_rows, "Jito (Query 6818065)")

    base_map = index_by_program_id(base_rows)
    jito_map = index_by_program_id(jito_rows)
    validate_shared_columns(base_map, jito_map)

    print("\n  Loading mapping and conditions...")
    mapping = load_mapping()
    conditions = load_conditions()

    print("\n  Building merged program records...")
    programs = build_records(base_rows, jito_map, mapping)

    # Condition enrichment
    enriched_count = 0
    for pid, p in programs.items():
        cond = conditions.get(pid, {})
        p["fee_multiplier_elevated"] = cond.get("fee_multiplier_elevated")
        p["fee_multiplier_extreme"] = cond.get("fee_multiplier_extreme")
        p["congestion_sensitivity"] = cond.get("congestion_sensitivity", "unknown")
        if cond:
            enriched_count += 1
    if conditions:
        print(f"    Conditions enriched: {enriched_count}/{len(programs)}")

    sorted_programs = sorted(
        programs.values(),
        key=lambda p: safe_float(p.get("priority_fees_sol"), 0.0) or 0.0,
        reverse=True,
    )

    # Save output
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=OUTPUT_COLUMNS,
            delimiter=CSV_DELIMITER,
            extrasaction="ignore",
        )
        writer.writeheader()
        for program in sorted_programs:
            cleaned = {k: ("" if v is None else v) for k, v in program.items()}
            writer.writerow(cleaned)

    print(f"\n  Saved: {OUTPUT_FILE}")
    print(f"  {len(sorted_programs)} programs × {len(OUTPUT_COLUMNS)} columns")

    # Quick stats
    classified = sum(
        1 for p in sorted_programs if p.get("raiku_category") not in ("unknown", "")
    )
    with_jito = sum(
        1
        for p in sorted_programs
        if safe_float(p.get("jito_mev_fees_sol"), 0.0)
        and safe_float(p.get("jito_mev_fees_sol"), 0.0) > 0
    )
    print(f"  Classification: {classified}/{len(sorted_programs)} mapped")
    print(f"  Programs with Jito fees > 0: {with_jito}/{len(sorted_programs)}")


if __name__ == "__main__":
    print("\n=== Building Program Database (Canonical Batch Merge) ===")
    build()
    print("\nDone.")

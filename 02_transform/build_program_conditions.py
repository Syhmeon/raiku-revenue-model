"""
Build Program × Market Condition Analysis
==========================================
Reads per-program × per-condition data from Dune (extracted by extract_program_conditions.py)
and computes fee multipliers and congestion sensitivity classification.

Source: data/raw/dune_program_conditions.csv
  → Already aggregated by Dune SQL as: market_condition × program_id
  → Conditions (normal/elevated/extreme) derived from epoch volatility_tag
  → Covers all 2024+ data (not just 30 days — captures all historical congestion events)

Output: data/processed/program_conditions.csv (semicolon-delimited)

Columns per condition (suffixed _normal, _elevated, _extreme):
  - fee_per_cu_lamports_{cond}: avg fee/CU under that condition
  - median_fee_per_cu_lamports_{cond}: median fee/CU
  - tx_count_{cond}: total transactions
  - fail_rate_{cond}: failure rate
  - total_fees_sol_{cond}: total fees in SOL
  - days_{cond}: number of days observed under that condition

Derived:
  - fee_multiplier_elevated: avg fee/CU elevated ÷ normal
  - fee_multiplier_extreme: avg fee/CU extreme ÷ normal
  - congestion_sensitivity: high (≥3x) / medium (1.5-3x) / low (<1.5x)

This is the KEY table for RAIKU AOT sizing: which programs pay MORE during congestion.
"""

import csv
import sys
from pathlib import Path
from collections import defaultdict

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_RAW, DATA_PROCESSED, DATA_MAPPING, CSV_DELIMITER, CSV_ENCODING

# ── Input files ────────────────────────────────────────────
CONDITIONS_RAW_FILE = DATA_RAW / "dune_program_conditions.csv"
MAPPING_FILE = DATA_MAPPING / "program_categories.csv"

# ── Output ─────────────────────────────────────────────────
OUTPUT_FILE = DATA_PROCESSED / "program_conditions.csv"

CONDITIONS = ["normal", "elevated", "extreme"]

OUTPUT_COLUMNS = [
    "program_id",
    "program_name",
    "raiku_category",
    "raiku_product",
    # Per-condition metrics
    *[f"fee_per_cu_lamports_{c}" for c in CONDITIONS],
    *[f"median_fee_per_cu_lamports_{c}" for c in CONDITIONS],
    *[f"tx_count_{c}" for c in CONDITIONS],
    *[f"fail_rate_{c}" for c in CONDITIONS],
    *[f"total_fees_sol_{c}" for c in CONDITIONS],
    *[f"days_{c}" for c in CONDITIONS],
    # Derived: multipliers vs normal baseline
    "fee_multiplier_elevated",   # fee/CU elevated ÷ fee/CU normal
    "fee_multiplier_extreme",    # fee/CU extreme ÷ fee/CU normal
    # Summary
    "total_days",
    "conditions_observed",       # e.g. "normal,elevated,extreme"
    "congestion_sensitivity",    # high/medium/low/unknown
]


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
        return {}
    rows = load_csv(MAPPING_FILE)
    mapping = {}
    for r in rows:
        pid = r.get("program_id", "").strip()
        if pid:
            mapping[pid] = {
                "name": r.get("program_name", ""),
                "category": r.get("raiku_category", "unknown"),
                "product": r.get("raiku_product", "unknown"),
            }
    return mapping


def classify_sensitivity(fee_normal, fee_elevated, fee_extreme):
    """
    Classify congestion sensitivity based on fee multipliers.
    High = programs that pay significantly more during congestion (prime AOT targets).
    """
    if fee_normal is None or fee_normal <= 0:
        return "unknown"

    mult_elev = (fee_elevated / fee_normal) if fee_elevated and fee_elevated > 0 else None
    mult_extr = (fee_extreme / fee_normal) if fee_extreme and fee_extreme > 0 else None

    max_mult = max(filter(None, [mult_elev, mult_extr, 1.0]))

    if max_mult >= 3.0:
        return "high"
    elif max_mult >= 1.5:
        return "medium"
    else:
        return "low"


def build():
    print("  Loading sources...")

    # 1. Load raw condition data (already aggregated by Dune)
    raw_rows = load_csv(CONDITIONS_RAW_FILE)
    if not raw_rows:
        print(f"  ERROR: No condition data found at {CONDITIONS_RAW_FILE}")
        print("  Run: python 01_extract/extract_program_conditions.py")
        return

    print(f"    Raw condition data: {len(raw_rows)} rows (program × condition)")

    # 2. Load mapping
    mapping = load_mapping()
    print(f"    Mapping: {len(mapping)} programs classified")

    # 3. Pivot: group by program_id, with one sub-dict per condition
    programs = defaultdict(dict)
    for r in raw_rows:
        pid = r.get("program_id", "").strip()
        cond = r.get("market_condition", "").strip()
        if not pid or cond not in CONDITIONS:
            continue

        tx = safe_float(r.get("tx_count"), 0)
        success = safe_float(r.get("success_count"), 0)

        programs[pid][cond] = {
            "fee_per_cu": safe_float(r.get("avg_fee_per_cu_lamports")),
            "median_fee_per_cu": safe_float(r.get("median_fee_per_cu_lamports")),
            "tx_count": int(tx) if tx else 0,
            "success_count": int(success) if success else 0,
            "fail_rate": round(1 - success / tx, 4) if tx > 0 else None,
            "total_fees_sol": safe_float(r.get("total_fees_sol"), 0),
            "days": int(safe_float(r.get("days_observed"), 0)),
        }

    print(f"    Distinct programs: {len(programs)}")

    # Show condition coverage
    for c in CONDITIONS:
        n = sum(1 for pid, conds in programs.items() if c in conds)
        print(f"      {c}: {n} programs have data")

    # 4. Build output rows
    results = []
    for pid, cond_data in programs.items():
        row = {
            "program_id": pid,
            "program_name": mapping.get(pid, {}).get("name", ""),
            "raiku_category": mapping.get(pid, {}).get("category", "unknown"),
            "raiku_product": mapping.get(pid, {}).get("product", "unknown"),
        }

        conditions_seen = []
        fee_per_cu_by_cond = {}

        for c in CONDITIONS:
            d = cond_data.get(c)
            if d and d["tx_count"] > 0:
                conditions_seen.append(c)
                fee_per_cu_by_cond[c] = d["fee_per_cu"]

                row[f"fee_per_cu_lamports_{c}"] = d["fee_per_cu"] if d["fee_per_cu"] is not None else ""
                row[f"median_fee_per_cu_lamports_{c}"] = d["median_fee_per_cu"] if d["median_fee_per_cu"] is not None else ""
                row[f"tx_count_{c}"] = d["tx_count"]
                row[f"fail_rate_{c}"] = d["fail_rate"] if d["fail_rate"] is not None else ""
                row[f"total_fees_sol_{c}"] = round(d["total_fees_sol"], 4)
                row[f"days_{c}"] = d["days"]
            else:
                row[f"fee_per_cu_lamports_{c}"] = ""
                row[f"median_fee_per_cu_lamports_{c}"] = ""
                row[f"tx_count_{c}"] = ""
                row[f"fail_rate_{c}"] = ""
                row[f"total_fees_sol_{c}"] = ""
                row[f"days_{c}"] = 0

        # Fee multipliers vs normal
        normal_fpc = fee_per_cu_by_cond.get("normal")
        for c in ["elevated", "extreme"]:
            c_fpc = fee_per_cu_by_cond.get(c)
            if normal_fpc and normal_fpc > 0 and c_fpc and c_fpc > 0:
                row[f"fee_multiplier_{c}"] = round(c_fpc / normal_fpc, 2)
            else:
                row[f"fee_multiplier_{c}"] = ""

        # Summary
        total_days = sum(cond_data.get(c, {}).get("days", 0) for c in CONDITIONS)
        row["total_days"] = total_days
        row["conditions_observed"] = ",".join(conditions_seen)
        row["congestion_sensitivity"] = classify_sensitivity(
            fee_per_cu_by_cond.get("normal"),
            fee_per_cu_by_cond.get("elevated"),
            fee_per_cu_by_cond.get("extreme"),
        )

        results.append(row)

    # Sort by total fees (all conditions combined) descending
    def sort_key(r):
        return sum(safe_float(r.get(f"total_fees_sol_{c}"), 0) for c in CONDITIONS)

    results.sort(key=sort_key, reverse=True)

    # 5. Save
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, delimiter=CSV_DELIMITER,
                                extrasaction="ignore")
        writer.writeheader()
        for r in results:
            clean = {k: ("" if v is None else v) for k, v in r.items()}
            writer.writerow(clean)

    print(f"\n  Saved: {OUTPUT_FILE}")
    print(f"  {len(results)} programs × {len(OUTPUT_COLUMNS)} columns")

    # 6. Stats
    sensitivity_counts = defaultdict(int)
    for r in results:
        sensitivity_counts[r.get("congestion_sensitivity", "unknown")] += 1

    print(f"\n  Congestion sensitivity classification:")
    print(f"    High (fee ≥3x during congestion):  {sensitivity_counts.get('high', 0)} programs")
    print(f"    Medium (fee 1.5-3x):               {sensitivity_counts.get('medium', 0)} programs")
    print(f"    Low (fee <1.5x):                   {sensitivity_counts.get('low', 0)} programs")
    print(f"    Unknown (no normal baseline):       {sensitivity_counts.get('unknown', 0)} programs")

    # Top 10 most congestion-sensitive
    sens_programs = []
    for r in results:
        if r.get("congestion_sensitivity") in ("high", "medium"):
            mult = safe_float(r.get("fee_multiplier_extreme"),
                              safe_float(r.get("fee_multiplier_elevated"), 0))
            sens_programs.append((r, mult))
    sens_programs.sort(key=lambda x: x[1], reverse=True)

    if sens_programs:
        print(f"\n  Top 10 most congestion-sensitive programs:")
        for r, mult in sens_programs[:10]:
            name = r.get("program_name") or r["program_id"][:16] + "..."
            cat = r.get("raiku_category", "?")
            normal = safe_float(r.get("fee_per_cu_lamports_normal"), 0)
            elev = safe_float(r.get("fee_per_cu_lamports_elevated"), 0)
            extr = safe_float(r.get("fee_per_cu_lamports_extreme"), 0)
            print(f"    {name:<35} | {cat:<12} | n:{normal:.2f} e:{elev:.2f} x:{extr:.2f} | {mult:.1f}x")

    # Top 10 by total fees during extreme conditions
    extreme_fees = [(r, safe_float(r.get("total_fees_sol_extreme"), 0)) for r in results]
    extreme_fees.sort(key=lambda x: x[1], reverse=True)
    top_extreme = [x for x in extreme_fees if x[1] > 0][:10]
    if top_extreme:
        print(f"\n  Top 10 by fees during EXTREME conditions:")
        for r, fees in top_extreme:
            name = r.get("program_name") or r["program_id"][:16] + "..."
            print(f"    {name:<35} | {fees:>10.2f} SOL extreme")


if __name__ == "__main__":
    print("\n=== Building Program × Condition Analysis ===")
    build()
    print("\nDone.")

"""
Build simulator-facing AOT artifact (aot_programs.v1) from canonical upstream outputs.

Inputs (source of truth):
  - data/processed/program_database.csv
  - data/mapping/program_categories.csv

Output:
  - data/processed/aot_programs.v1.js
"""

from __future__ import annotations

import csv
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "processed" / "program_database.csv"
CAT_PATH = PROJECT_ROOT / "data" / "mapping" / "program_categories.csv"
OUT_PATH = PROJECT_ROOT / "data" / "processed" / "aot_programs.v1.js"

DB_REQUIRED = {
    "program_id",
    "program_name",
    "batch_window",
    "total_cu",
    "base_fees_sol",
    "priority_fees_sol",
    "jito_mev_fees_sol",
}

CAT_REQUIRED = {
    "program_id",
    "program_name",
    "raiku_category",
    "subcategory",
    "raiku_product",
}

HIGH_TIER_SEGMENTS = {"prop_amm", "orderbook"}
CORE_PRODUCT_SCOPES = {"aot", "both"}
BENCHMARK_PRODUCT_SCOPE = "jit"
BENCHMARK_CATEGORY = "arb_bot"
EXCLUDED_CATEGORIES = {"other", "unknown"}


def read_csv_semicolon(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter=";"))


def ensure_required_columns(rows: list[dict], required: set[str], label: str) -> None:
    if not rows:
        raise ValueError(f"{label} is empty: {required}")
    cols = set(rows[0].keys())
    missing = sorted(required - cols)
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def to_float(value: str | None) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)


def parse_batch_window(window: str | None) -> tuple[str | None, str | None]:
    if not window:
        return None, None
    text = window.strip()
    if not (text.startswith("[") and text.endswith(")")):
        return None, None
    core = text[1:-1]
    parts = [p.strip() for p in core.split(",")]
    if len(parts) != 2:
        return None, None
    return parts[0], parts[1]


def derive_segment_key(raiku_category: str, subcategory: str) -> str:
    cat = (raiku_category or "").strip().lower()
    sub = (subcategory or "").strip().lower()

    if cat == BENCHMARK_CATEGORY:
        return BENCHMARK_CATEGORY
    if cat == "prop_amm":
        return "prop_amm"
    if cat == "dex":
        if sub == "orderbook":
            return "orderbook"
        if sub == "aggregator":
            return "aggregator"
        return "amm_pools"
    if cat in {"lending", "perps", "oracle", "bridge", "cranker", "depin", "payments", "nft", "gaming"}:
        return cat
    return "other"


def derive_tier_key(segment_key: str) -> str:
    return "high" if segment_key in HIGH_TIER_SEGMENTS else "standard"


def git_revision() -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PROJECT_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        )
        return out.strip()
    except Exception:
        return "unknown"


def main() -> None:
    db_rows = read_csv_semicolon(DB_PATH)
    cat_rows = read_csv_semicolon(CAT_PATH)
    ensure_required_columns(db_rows, DB_REQUIRED, "program_database.csv")
    ensure_required_columns(cat_rows, CAT_REQUIRED, "program_categories.csv")

    cat_by_program_id: dict[str, dict] = {}
    for row in cat_rows:
        pid = row["program_id"]
        if pid in cat_by_program_id:
            raise ValueError(f"Duplicate program_id in program_categories.csv: {pid}")
        cat_by_program_id[pid] = row

    programs: list[dict] = []
    batch_windows: set[str] = set()
    core_rows = 0
    benchmark_rows = 0

    for db in db_rows:
        pid = db["program_id"]
        cat = cat_by_program_id.get(pid)
        if cat is None:
            raise ValueError(f"Missing taxonomy row for program_id: {pid}")

        product_scope = (cat["raiku_product"] or "").strip().lower()
        raiku_category = (cat["raiku_category"] or "").strip().lower()
        is_core = product_scope in CORE_PRODUCT_SCOPES
        is_benchmark = product_scope == BENCHMARK_PRODUCT_SCOPE and raiku_category == BENCHMARK_CATEGORY
        if not (is_core or is_benchmark):
            continue
        if raiku_category in EXCLUDED_CATEGORIES:
            continue

        total_cu = to_float(db["total_cu"])
        if total_cu <= 0:
            continue

        segment_key = derive_segment_key(raiku_category, cat["subcategory"])
        if segment_key == "other":
            # Do not emit rows that collapse into unknown/other taxonomy segments.
            continue

        tier_key = derive_tier_key(segment_key)
        is_aot_relevant = is_core

        if is_core:
            core_rows += 1
        elif is_benchmark:
            benchmark_rows += 1

        programs.append(
            {
                "program_id": pid,
                "program_name": (cat["program_name"] or db["program_name"] or "").strip(),
                "product_scope": product_scope,
                "is_aot_relevant": True,
                "segment_key": segment_key,
                "tier_key": tier_key,
                "cu_b_30d": total_cu / 1_000_000_000,
                "fee_base_sol_30d": to_float(db["base_fees_sol"]),
                "fee_priority_sol_30d": to_float(db["priority_fees_sol"]),
                "fee_jito_sol_30d": to_float(db["jito_mev_fees_sol"]),
            }
        )
        if db.get("batch_window"):
            batch_windows.add(db["batch_window"])

    programs.sort(key=lambda r: r["program_id"])

    start_date = None
    end_date = None
    if batch_windows:
        parsed = [parse_batch_window(w) for w in sorted(batch_windows)]
        starts = sorted({s for s, _ in parsed if s})
        ends = sorted({e for _, e in parsed if e})
        start_date = starts[0] if starts else None
        end_date = ends[-1] if ends else None

    payload = {
        "schema_version": "aot_programs.v1",
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_file": "data/processed/program_database.csv + data/mapping/program_categories.csv",
        "source_revision": git_revision(),
        "window_start_date": start_date,
        "window_end_date": end_date,
        "program_count": len(programs),
        "programs": programs,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    artifact_js = "window.RAIKU_AOT_DATA = " + json.dumps(payload, indent=2) + ";\n"
    OUT_PATH.write_text(artifact_js, encoding="utf-8")

    print(f"Wrote: {OUT_PATH}")
    print(f"program_count: {len(programs)}")
    print(f"core_rows (aot|both): {core_rows}")
    print(f"benchmark_rows (jit+arb_bot): {benchmark_rows}")
    print(f"window_start_date: {start_date}")
    print(f"window_end_date: {end_date}")


if __name__ == "__main__":
    main()

"""
Build D.daily temporal data for the RAIKU Revenue Simulator.

Reads:
  - data/raw/dune_daily_program_fees_30d.csv  (1400 rows: daily per-program data)
  - data/mapping/program_categories.csv        (program → category mapping)

Produces:
  - data/processed/daily_temporal_payload.js   (D.daily JS snippet for embedding)

The output is a pre-aggregated dataset:
  10 business categories x 30 days ≈ 300 entries,
  each with CU-weighted avg fee/CU, distribution percentiles (p25/med/p75),
  and per-program breakdowns.
"""

import csv
import json
import math
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_MAP = PROJECT_ROOT / "data" / "mapping"
DATA_OUT = PROJECT_ROOT / "data" / "processed"
DATA_OUT.mkdir(parents=True, exist_ok=True)

# ── Business category mapping (mirrors JS BIZ_CATEGORIES) ──
BIZ_CATEGORIES = {
    "prop_amm":     {"cat": "dex",         "subs": ["prop_amm"]},
    "orderbook":    {"cat": "dex",         "subs": ["orderbook"]},
    "amm_pools":    {"cat": "dex",         "subs": ["amm", "clmm", "dlmm", "bonding_curve"]},
    "perps":        {"cat": "perps",       "subs": ["perpetuals"]},
    "lending":      {"cat": "lending",     "subs": ["pool", "flash_loan", "yield"]},
    "aggregator":   {"cat": "dex",         "subs": ["aggregator"]},
    "oracle":       {"cat": "oracle",      "subs": ["price_feed"]},
    "bridge":       {"cat": "bridge",      "subs": ["cross_chain"]},
    "trading_bots": {"cat": "trading_bot", "subs": ["keeper"]},
    "infra":        {"cat": ["defi", "payments", "staking"], "subs": None},  # matches any sub
}

LS = 1e9  # lamports per SOL


def classify_program(cat: str, sub: str) -> str:
    """Map (raiku_category, subcategory) to business category ID."""
    for biz_id, rule in BIZ_CATEGORIES.items():
        if biz_id == "infra":
            # Special: matches multiple categories, any subcategory
            if cat in rule["cat"]:
                return biz_id
        else:
            if cat == rule["cat"] and sub in rule["subs"]:
                return biz_id
    return None  # not mapped


def percentile(arr, pct):
    """Compute percentile from sorted array."""
    if not arr:
        return 0
    s = sorted(arr)
    idx = (pct / 100) * (len(s) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return s[lo]
    return s[lo] + (s[hi] - s[lo]) * (idx - lo)


def main():
    # 1. Load program → category mapping
    prog_map = {}  # program_id → {cat, sub, prod, name}
    with open(DATA_MAP / "program_categories.csv", "r", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter=";"):
            prog_map[row["program_id"]] = {
                "cat": row["raiku_category"],
                "sub": row["subcategory"],
                "prod": row["raiku_product"],
                "name": row["program_name"],
            }

    # 2. Load daily data
    daily_rows = []
    with open(DATA_RAW / "dune_daily_program_fees_30d.csv", "r", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter=";"):
            pid = row["program_id"]
            info = prog_map.get(pid)
            if not info:
                continue  # skip unmapped programs

            biz_cat = classify_program(info["cat"], info["sub"])
            if not biz_cat:
                continue  # not in AOT business categories

            # Only AOT or Both products
            if info["prod"] not in ("aot", "both"):
                continue

            cu = int(row["total_cu"])
            tf_sol = float(row["total_fees_sol"])
            pf_sol = float(row["priority_fees_sol"])
            bf_sol = float(row["base_fees_sol"])

            if cu <= 0:
                continue

            # Fee/CU in lamports
            fpc = (tf_sol * LS) / cu

            daily_rows.append({
                "pid": pid,
                "name": info["name"],
                "biz_cat": biz_cat,
                "day": row["day"],
                "cu": cu,
                "tf": tf_sol,
                "pf": pf_sol,
                "bf": bf_sol,
                "fpc": fpc,
                "tx": int(row["tx_count"]),
                "sx": int(row["success_count"]),
            })

    print(f"Loaded {len(daily_rows)} classified daily rows")

    # 3. Aggregate by (biz_cat, day)
    cat_day = defaultdict(list)
    for r in daily_rows:
        cat_day[(r["biz_cat"], r["day"])].append(r)

    # Also aggregate network-level daily
    day_net = defaultdict(list)
    for r in daily_rows:
        day_net[r["day"]].append(r)

    # 4. Build output structure
    daily_entries = []
    for (biz_cat, day), progs in sorted(cat_day.items(), key=lambda x: (x[0][1], x[0][0])):
        total_cu = sum(p["cu"] for p in progs)
        total_tf = sum(p["tf"] for p in progs)
        total_pf = sum(p["pf"] for p in progs)

        # CU-weighted fee/CU
        fpc = (total_tf * LS / total_cu) if total_cu > 0 else 0

        # Per-program fee/CU distribution
        prog_fpcs = [p["fpc"] for p in progs]
        p25 = percentile(prog_fpcs, 25)
        med = percentile(prog_fpcs, 50)
        p75 = percentile(prog_fpcs, 75)

        daily_entries.append({
            "c": biz_cat,
            "d": day,
            "cu": round(total_cu / 1e9, 2),    # billions
            "tf": round(total_tf, 4),            # SOL
            "pf": round(total_pf, 4),            # SOL
            "fpc": round(fpc, 4),                # lamports
            "n": len(progs),
            "p25": round(p25, 4),
            "med": round(med, 4),
            "p75": round(p75, 4),
        })

    # Network-level daily
    net_entries = []
    for day, progs in sorted(day_net.items()):
        total_cu = sum(p["cu"] for p in progs)
        total_tf = sum(p["tf"] for p in progs)
        fpc = (total_tf * LS / total_cu) if total_cu > 0 else 0
        net_entries.append({
            "d": day,
            "cu": round(total_cu / 1e9, 2),
            "tf": round(total_tf, 4),
            "fpc": round(fpc, 4),
            "n": len(progs),
        })

    print(f"Category-day entries: {len(daily_entries)}")
    print(f"Network-day entries: {len(net_entries)}")

    # Category summary
    cats_seen = defaultdict(set)
    for e in daily_entries:
        cats_seen[e["c"]].add(e["d"])
    print("\nCategory coverage:")
    for cat, days in sorted(cats_seen.items()):
        print(f"  {cat:15s}: {len(days)} days")

    # 5. Generate JS snippet
    js_daily = json.dumps(daily_entries, separators=(",", ":"))
    js_net = json.dumps(net_entries, separators=(",", ":"))

    payload = f"D.daily={js_daily};\nD.dailyNet={js_net};\n"

    out_path = DATA_OUT / "daily_temporal_payload.js"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(payload)

    print(f"\nJS payload: {len(payload):,} chars")
    print(f"Saved to: {out_path}")

    # Also save as CSV for debugging
    csv_path = DATA_OUT / "daily_category_aggregates.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["c", "d", "cu", "tf", "pf", "fpc", "n", "p25", "med", "p75"], delimiter=";")
        w.writeheader()
        w.writerows(daily_entries)
    print(f"CSV debug: {csv_path}")


if __name__ == "__main__":
    main()

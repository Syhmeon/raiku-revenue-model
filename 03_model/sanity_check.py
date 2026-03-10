"""
Sanity Check: Top-Down vs Bottom-Up AOT + JIT Summary
=======================================================
Compares the two AOT approaches, explains divergences,
and produces a unified revenue summary across all models.

Output: data/processed/sanity_check_report.csv
        + console report with full analysis
"""

import csv
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    DATA_RAW, DATA_PROCESSED, CSV_DELIMITER, CSV_ENCODING,
    PROTOCOL_TAKE_RATE,
)

DATABASE_FILE = DATA_PROCESSED / "solana_epoch_database.csv"
AOT_FILE = DATA_PROCESSED / "aot_revenue_scenarios.csv"
JIT_FILE = DATA_PROCESSED / "jit_revenue_scenarios.csv"
OUTPUT_FILE = DATA_PROCESSED / "sanity_check_report.csv"


def safe_float(val, default=None):
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def load_csv(path):
    with open(path, "r", encoding=CSV_ENCODING) as f:
        return list(csv.DictReader(f, delimiter=CSV_DELIMITER))


def section(title):
    width = 72
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def subsection(title):
    print(f"\n  --- {title} ---")


# ──────────────────────────────────────────────────────────
# 1. LOAD ALL DATA
# ──────────────────────────────────────────────────────────

def run():
    db = load_csv(DATABASE_FILE)
    aot = load_csv(AOT_FILE)
    jit = load_csv(JIT_FILE)

    td_rows = [r for r in aot if r["model"] == "top_down"]
    bu_rows = [r for r in aot if r["model"] == "bottom_up"]

    # ──────────────────────────────────────────────────────
    # 2. REAL DATA FOUNDATIONS
    # ──────────────────────────────────────────────────────
    section("1. REAL DATA FOUNDATIONS (from epoch database)")

    # Get recent epochs with full data
    recent = []
    for row in reversed(db):
        fees = safe_float(row.get("priority_fees_sol"))
        mev = safe_float(row.get("mev_jito_tips_sol"))
        epy = safe_float(row.get("epochs_per_year"))
        price = safe_float(row.get("sol_price_usd"))
        stake = safe_float(row.get("active_stake_sol"))
        validators = safe_float(row.get("validator_count"))
        if all(v is not None for v in [fees, mev, epy, price]):
            recent.append({
                "epoch": int(row["epoch"]),
                "priority_fees_sol": fees,
                "mev_sol": mev,
                "epochs_per_year": epy,
                "sol_price": price,
                "active_stake": stake,
                "validators": validators,
                "annual_fees_usd": fees * epy * price,
                "annual_mev_usd": mev * epy * price,
            })
        if len(recent) >= 10:
            break

    latest = recent[0]
    avg_fees = sum(r["annual_fees_usd"] for r in recent) / len(recent)
    avg_mev = sum(r["annual_mev_usd"] for r in recent) / len(recent)

    print(f"\n  Latest epoch: {latest['epoch']}")
    print(f"  SOL price: ${latest['sol_price']:.2f}")
    print(f"  Active stake: {latest['active_stake']:,.0f} SOL")
    print(f"  Active validators: {latest['validators']:.0f}")
    print(f"  Epochs/year: {latest['epochs_per_year']:.1f}")
    print(f"\n  Priority fees/epoch: {latest['priority_fees_sol']:,.2f} SOL")
    print(f"  MEV tips/epoch:      {latest['mev_sol']:,.2f} SOL")
    print(f"\n  Annualized priority fees (latest): ${latest['annual_fees_usd']:,.0f}")
    print(f"  Annualized priority fees (avg 10):  ${avg_fees:,.0f}")
    print(f"  Annualized MEV tips (latest):       ${latest['annual_mev_usd']:,.0f}")
    print(f"  Annualized MEV tips (avg 10):       ${avg_mev:,.0f}")

    # ──────────────────────────────────────────────────────
    # 3. TOP-DOWN AOT MODEL
    # ──────────────────────────────────────────────────────
    section("2. AOT TOP-DOWN MODEL")
    print("\n  Formula: Total_Priority_Fees × Latency_Sensitive% × RAIKU_Capture% × Protocol_Fee")

    subsection("Base case: Avg 10 epochs, 40% latency-sensitive")
    print(f"  Total priority fees (annualized): ${avg_fees:,.0f}")

    for capture_str in ["5%", "10%", "15%", "20%"]:
        matches = [r for r in td_rows
                   if r["latency_sensitive_pct"] == "40%"
                   and r["raiku_capture_pct"] == capture_str
                   and "Avg" in r["total_market_source"]]
        if matches:
            r = matches[0]
            addressable = int(r["total_market_usd"]) * 0.40
            gross = int(r["gross_revenue_usd"])
            prot = int(r["protocol_revenue_usd"])
            print(f"  Capture {capture_str:>3}: addressable ${addressable:>12,.0f} → gross ${gross:>10,} → protocol ${prot:>8,}/yr (${prot//12:>7,}/mo)")

    # ──────────────────────────────────────────────────────
    # 4. BOTTOM-UP AOT MODEL
    # ──────────────────────────────────────────────────────
    section("3. AOT BOTTOM-UP MODEL (3D Framework)")
    print("\n  Formula: Stake% × Slots/yr × CU_reserved% × Fee/CU × SOL_price (per archetype)")

    SLOTS_PER_YEAR = 78_408_000
    CU_PER_BLOCK = 48_000_000
    LAMPORTS_PER_SOL = 1_000_000_000

    # Show archetype breakdown for 5% stake, 10% CU, mid customers
    subsection("Archetype breakdown (5% stake, 10% CU, mid customers)")
    total_bu_gross = 0
    archetype_details = {}
    for r in bu_rows:
        if r["stake_pct"] == "5%" and r["cu_reserved_pct"] == "10%" and "mid" in r["total_market_source"]:
            arch = r["archetype"]
            gross = int(r["gross_revenue_usd"])
            prot = int(r["protocol_revenue_usd"])
            archetype_details[arch] = {"gross": gross, "protocol": prot, "fee_cu": r["fee_per_cu_lamports"]}
            total_bu_gross += gross
            print(f"    {arch:<25} fee/CU={float(r['fee_per_cu_lamports']):>8.3f} L/CU  gross=${gross:>8,}  protocol=${prot:>6,}/yr")

    total_bu_prot = sum(v["protocol"] for v in archetype_details.values())
    print(f"    {'TOTAL':<25} {'':>18}  gross=${total_bu_gross:>8,}  protocol=${total_bu_prot:>6,}/yr")

    # All stake scenarios
    subsection("Bottom-up by stake% (10% CU, mid customers)")
    bu_by_stake = {}
    for sp in ["1%", "3%", "5%", "10%", "20%"]:
        gross = sum(int(r["gross_revenue_usd"]) for r in bu_rows
                    if r["stake_pct"] == sp and r["cu_reserved_pct"] == "10%" and "mid" in r["total_market_source"])
        prot = sum(int(r["protocol_revenue_usd"]) for r in bu_rows
                   if r["stake_pct"] == sp and r["cu_reserved_pct"] == "10%" and "mid" in r["total_market_source"])
        bu_by_stake[sp] = {"gross": gross, "protocol": prot}
        print(f"    Stake {sp:>3}: gross=${gross:>10,}  protocol=${prot:>8,}/yr (${prot//12:>6,}/mo)")

    # ──────────────────────────────────────────────────────
    # 5. TOP-DOWN vs BOTTOM-UP COMPARISON
    # ──────────────────────────────────────────────────────
    section("4. SANITY CHECK: TOP-DOWN vs BOTTOM-UP")

    # Match comparable scenarios
    # TD: addressable = total_fees × 40% latency-sensitive; then capture = RAIKU share
    # BU: builds revenue from scratch per archetype for given stake%
    #
    # Key insight: TD "10% capture of addressable" != BU "5% stake"
    # TD 10% capture of 40% addressable = 4% of total priority fees
    # BU 5% stake = 5% of slots, but only CU that archetypes consume

    subsection("Direct comparison (protocol revenue, 5% take rate)")
    td_40_10 = [r for r in td_rows if r["latency_sensitive_pct"] == "40%" and r["raiku_capture_pct"] == "10%" and "Avg" in r["total_market_source"]]
    td_gross = int(td_40_10[0]["gross_revenue_usd"]) if td_40_10 else 0
    td_prot = int(td_40_10[0]["protocol_revenue_usd"]) if td_40_10 else 0

    bu_5_10_gross = bu_by_stake.get("5%", {}).get("gross", 0)
    bu_5_10_prot = bu_by_stake.get("5%", {}).get("protocol", 0)

    print(f"  Top-Down  (40% addr, 10% capture):  gross=${td_gross:>10,}  protocol=${td_prot:>8,}/yr")
    print(f"  Bottom-Up (5% stake, 10% CU, mid):  gross=${bu_5_10_gross:>10,}  protocol=${bu_5_10_prot:>8,}/yr")
    if bu_5_10_gross > 0:
        ratio = td_gross / bu_5_10_gross
        print(f"\n  Ratio TD/BU: {ratio:.1f}x")

    # ── EXPLAIN THE DIVERGENCE ──
    subsection("Divergence analysis")

    # What does the top-down assume?
    td_total_fees = avg_fees
    td_addressable = td_total_fees * 0.40
    td_raiku_gross = td_addressable * 0.10  # 10% capture

    # What does the bottom-up actually generate?
    raiku_slots = SLOTS_PER_YEAR * 0.05  # 5% stake
    cu_available = CU_PER_BLOCK * 0.10   # 10% CU reserved = 4.8M CU/slot

    # How much CU do the 6 archetypes actually consume per RAIKU slot?
    # This is the key: archetypes don't fill the entire CU budget
    from aot_revenue import ARCHETYPES

    total_cu_consumed_per_slot = 0
    total_fee_per_slot_lamports = 0
    for arch in ARCHETYPES:
        cu_per_slot = arch["cu_per_tx"] * arch["txs_per_slot"] * arch["pct_slots_active"] * arch["num_customers_mid"]
        fee_per_slot = cu_per_slot * arch["fee_per_cu_lamports"]
        total_cu_consumed_per_slot += cu_per_slot
        total_fee_per_slot_lamports += fee_per_slot

    # Average fee/CU across archetypes (weighted)
    avg_fee_per_cu = total_fee_per_slot_lamports / total_cu_consumed_per_slot if total_cu_consumed_per_slot > 0 else 0

    # CU utilization rate
    cu_utilization = total_cu_consumed_per_slot / cu_available if cu_available > 0 else 0

    print(f"\n  CU budget per RAIKU slot: {cu_available:,.0f} CU (10% of {CU_PER_BLOCK:,})")
    print(f"  CU consumed by archetypes (avg/slot): {total_cu_consumed_per_slot:,.0f} CU")
    print(f"  CU utilization: {cu_utilization*100:.1f}%")
    print(f"  Weighted avg fee/CU: {avg_fee_per_cu:.4f} lamports/CU")

    # What would bottom-up generate if CU was 100% utilized at market rates?
    # Derive market fee/CU from real epoch columns: priority_fees_sol / total_CU
    market_fee_per_cu_samples = []
    for row in reversed(db):
        pf = safe_float(row.get("priority_fees_sol"))
        cu_per_blk = safe_float(row.get("avg_cu_per_block"))
        blocks = safe_float(row.get("total_blocks"))
        if pf is not None and cu_per_blk and blocks:
            total_cu = cu_per_blk * blocks
            fee_per_cu = (pf * LAMPORTS_PER_SOL) / total_cu if total_cu > 0 else 0
            market_fee_per_cu_samples.append(fee_per_cu)
        if len(market_fee_per_cu_samples) >= 10:
            break
    market_fee_per_cu = (sum(market_fee_per_cu_samples) / len(market_fee_per_cu_samples)) if market_fee_per_cu_samples else 0

    print(f"\n  Market avg fee/CU (from epoch data): {market_fee_per_cu:.4f} lamports/CU (avg of {len(market_fee_per_cu_samples)} recent epochs)")
    print(f"  Bottom-up archetype avg fee/CU: {avg_fee_per_cu:.6f} lamports/CU")
    if market_fee_per_cu > 0:
        print(f"  Fee ratio (archetypes/market): {avg_fee_per_cu/market_fee_per_cu:.1f}x")

    # Hypothetical: if all RAIKU CU was sold at market fee/CU rates
    hyp_revenue_per_slot_sol = cu_available * market_fee_per_cu / LAMPORTS_PER_SOL
    hyp_annual_sol = hyp_revenue_per_slot_sol * raiku_slots
    hyp_annual_usd = hyp_annual_sol * latest["sol_price"]
    print(f"\n  Hypothetical BU (100% CU fill, market fee/CU):")
    print(f"    = {cu_available:,.0f} CU × {market_fee_per_cu:.6f} L/CU × {raiku_slots:,.0f} slots × ${latest['sol_price']:.2f}")
    print(f"    = ${hyp_annual_usd:,.0f}/yr gross")
    print(f"    vs Top-Down gross: ${td_raiku_gross:,.0f}/yr")
    if hyp_annual_usd > 0:
        print(f"    Ratio TD/Hypothetical-BU: {td_raiku_gross/hyp_annual_usd:.1f}x")

    # Root causes
    subsection("Root causes of divergence")
    print("""
  1. CU UTILIZATION: Bottom-up only counts CU that specific archetypes
     consume. With 6 archetypes at mid-tier customer counts, CU usage is
     {:.1f}% of the 10% reserved budget. The remaining CU sits idle.

  2. FEE/CU GAP: Archetype avg fee/CU ({:.4f} L/CU) vs market avg
     ({:.6f} L/CU). The archetypes use conservative fee estimates.

  3. APPLES vs ORANGES: Top-down starts from TOTAL Solana priority fees
     and slices off a %. Bottom-up builds from individual customer units.
     They measure different things:
     - Top-down = "if RAIKU captures X% of existing fee flow"
     - Bottom-up = "if RAIKU gets N customers × their actual usage"

  4. CONCLUSION: The two models are not expected to match.
     - Top-down = ceiling estimate (market-size approach)
     - Bottom-up = floor estimate (bottoms-up unit economics)
     - Truth is somewhere in between, likely closer to bottom-up in Y1
       and closer to top-down at maturity.
""".format(cu_utilization * 100, avg_fee_per_cu, market_fee_per_cu))

    # ──────────────────────────────────────────────────────
    # 6. JIT SUMMARY
    # ──────────────────────────────────────────────────────
    section("5. JIT MODEL SUMMARY")

    subsection("Key scenarios (5% protocol fee)")
    for r in jit:
        if r["protocol_fee_pct"] == "5.0%" and r["raiku_market_share_pct"] in ["5%", "10%", "15%"]:
            print(f"  {r['total_market_source']:<45} share={r['raiku_market_share_pct']:>4}  "
                  f"gross=${int(r['gross_revenue_usd']):>12,}  protocol=${int(r['protocol_revenue_usd']):>8,}/yr")

    # ──────────────────────────────────────────────────────
    # 7. UNIFIED REVENUE TABLE
    # ──────────────────────────────────────────────────────
    section("6. UNIFIED REVENUE TABLE (Protocol Revenue, 5% take rate)")
    print("\n  Combining AOT + JIT for key scenarios:\n")

    # Define combined scenarios
    report_rows = []
    header = f"  {'Scenario':<50} {'AOT Protocol':>14} {'JIT Protocol':>14} {'TOTAL':>14} {'Monthly':>10}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    # Scenarios:
    combos = [
        # (label, aot_prot, jit_prot)
        # Conservative: BU 5% stake / JIT on-chain 5% share
        ("Y1 Conservative (BU 5%stk + JIT 5%shr, on-chain)",
         bu_by_stake.get("5%", {}).get("protocol", 0),
         next((int(r["protocol_revenue_usd"]) for r in jit if r["protocol_fee_pct"] == "5.0%" and r["raiku_market_share_pct"] == "5%" and "avg 10" in r["total_market_source"]), 0)),

        ("Y1 Base (BU 5%stk + JIT 10%shr, on-chain)",
         bu_by_stake.get("5%", {}).get("protocol", 0),
         next((int(r["protocol_revenue_usd"]) for r in jit if r["protocol_fee_pct"] == "5.0%" and r["raiku_market_share_pct"] == "10%" and "avg 10" in r["total_market_source"]), 0)),

        ("Y1 Optimistic (BU 10%stk + JIT 15%shr, on-chain)",
         bu_by_stake.get("10%", {}).get("protocol", 0),
         next((int(r["protocol_revenue_usd"]) for r in jit if r["protocol_fee_pct"] == "5.0%" and r["raiku_market_share_pct"] == "15%" and "avg 10" in r["total_market_source"]), 0)),

        ("", 0, 0),  # separator

        ("Mature Conservative (TD 40%×5% + JIT Q4 5%shr)",
         next((int(r["protocol_revenue_usd"]) for r in td_rows if r["latency_sensitive_pct"] == "40%" and r["raiku_capture_pct"] == "5%" and "Avg" in r["total_market_source"]), 0),
         next((int(r["protocol_revenue_usd"]) for r in jit if r["protocol_fee_pct"] == "5.0%" and r["raiku_market_share_pct"] == "5%" and "Q4-2025" in r["total_market_source"]), 0)),

        ("Mature Base (TD 40%×10% + JIT Q4 10%shr)",
         td_prot,
         next((int(r["protocol_revenue_usd"]) for r in jit if r["protocol_fee_pct"] == "5.0%" and r["raiku_market_share_pct"] == "10%" and "Q4-2025" in r["total_market_source"]), 0)),

        ("Mature Optimistic (TD 40%×15% + JIT Q4 15%shr)",
         next((int(r["protocol_revenue_usd"]) for r in td_rows if r["latency_sensitive_pct"] == "40%" and r["raiku_capture_pct"] == "15%" and "Avg" in r["total_market_source"]), 0),
         next((int(r["protocol_revenue_usd"]) for r in jit if r["protocol_fee_pct"] == "5.0%" and r["raiku_market_share_pct"] == "15%" and "Q4-2025" in r["total_market_source"]), 0)),

        ("", 0, 0),  # separator

        ("Bull Conservative (TD 50%×10% + JIT 2025 5%shr)",
         next((int(r["protocol_revenue_usd"]) for r in td_rows if r["latency_sensitive_pct"] == "50%" and r["raiku_capture_pct"] == "10%" and "Avg" in r["total_market_source"]), 0),
         next((int(r["protocol_revenue_usd"]) for r in jit if r["protocol_fee_pct"] == "5.0%" and r["raiku_market_share_pct"] == "5%" and "2025 full" in r["total_market_source"]), 0)),

        ("Bull Base (TD 50%×15% + JIT 2025 10%shr)",
         next((int(r["protocol_revenue_usd"]) for r in td_rows if r["latency_sensitive_pct"] == "50%" and r["raiku_capture_pct"] == "15%" and "Avg" in r["total_market_source"]), 0),
         next((int(r["protocol_revenue_usd"]) for r in jit if r["protocol_fee_pct"] == "5.0%" and r["raiku_market_share_pct"] == "10%" and "2025 full" in r["total_market_source"]), 0)),

        ("Bull Optimistic (TD 60%×20% + JIT 2025 15%shr)",
         next((int(r["protocol_revenue_usd"]) for r in td_rows if r["latency_sensitive_pct"] == "60%" and r["raiku_capture_pct"] == "20%" and "Avg" in r["total_market_source"]), 0),
         next((int(r["protocol_revenue_usd"]) for r in jit if r["protocol_fee_pct"] == "5.0%" and r["raiku_market_share_pct"] == "15%" and "2025 full" in r["total_market_source"]), 0)),
    ]

    for label, aot_p, jit_p in combos:
        if label == "":
            print()
            continue
        total = aot_p + jit_p
        monthly = total // 12
        report_rows.append({
            "scenario": label,
            "aot_protocol_revenue_usd": aot_p,
            "jit_protocol_revenue_usd": jit_p,
            "total_protocol_revenue_usd": total,
            "total_monthly_usd": monthly,
        })
        print(f"  {label:<50} ${aot_p:>12,} ${jit_p:>12,} ${total:>12,} ${monthly:>8,}/mo")

    # Save report
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    cols = ["scenario", "aot_protocol_revenue_usd", "jit_protocol_revenue_usd",
            "total_protocol_revenue_usd", "total_monthly_usd"]
    with open(OUTPUT_FILE, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols, delimiter=CSV_DELIMITER)
        writer.writeheader()
        writer.writerows(report_rows)

    print(f"\n  Saved: {OUTPUT_FILE} ({len(report_rows)} scenarios)")

    # ──────────────────────────────────────────────────────
    # 8. KEY TAKEAWAYS
    # ──────────────────────────────────────────────────────
    section("7. KEY TAKEAWAYS")
    print("""
  1. AOT top-down and bottom-up diverge by ~{:.0f}x. This is EXPECTED:
     - Top-down = market share of existing fee flow (ceiling)
     - Bottom-up = unit economics from specific customers (floor)

  2. Y1 realistic protocol revenue (BU-based AOT + on-chain JIT):
     - Conservative: ~${:,}/yr (${:,}/mo)
     - Base:         ~${:,}/yr (${:,}/mo)

  3. At maturity (TD-based AOT + higher JIT share):
     - Base:         ~${:,}/yr (${:,}/mo)
     - Optimistic:   ~${:,}/yr (${:,}/mo)

  4. JIT dominates revenue in all scenarios (MEV tips market >> AOT fees
     from reserved CU). AOT value is primarily in EXECUTION CERTAINTY,
     not raw fee volume.

  5. Bull case (Jito returning to 2025 peak levels) is 10-20x above
     current on-chain rates — shows high sensitivity to market cycle.
""".format(
        td_gross / bu_5_10_gross if bu_5_10_gross > 0 else 0,
        report_rows[0]["total_protocol_revenue_usd"], report_rows[0]["total_monthly_usd"],
        report_rows[1]["total_protocol_revenue_usd"], report_rows[1]["total_monthly_usd"],
        report_rows[4]["total_protocol_revenue_usd"], report_rows[4]["total_monthly_usd"],
        report_rows[5]["total_protocol_revenue_usd"], report_rows[5]["total_monthly_usd"],
    ))


if __name__ == "__main__":
    print("\n=== RAIKU Revenue Model — Sanity Check & Summary ===")
    run()
    print("Done.")

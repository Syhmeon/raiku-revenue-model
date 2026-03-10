"""
AOT Revenue Model — Raiku Protocol
====================================
Two approaches:
  1) Top-Down: Total_Priority_Fees × Latency_Sensitive_Share × RAIKU_Capture × Protocol_Fee
  2) Bottom-Up (3D Framework): Stake% × Slots/yr × CU_reserved% × Fee/CU × SOL_price

Terminology:
  - total_priority_fees: Annualized priority fees on Solana (all programs combined)
  - latency_sensitive_share: % of those fees from latency-sensitive programs (PropAMMs, arb, HFT)
  - addressable_market: total_priority_fees × latency_sensitive_share
  - raiku_capture: % of addressable market that RAIKU wins
  - gross_revenue: addressable_market × raiku_capture (before protocol/validator split)
  - protocol_revenue: gross × 5% fee (what RAIKU treasury keeps)
  - validator_revenue: gross × 95% (what validators keep)

Bottom-up uses 6 customer archetypes from raiku_usecases.txt:
  1. PropAMMs (oracle/quote updates)
  2. Quant Trading Desks
  3. Market Makers (operational)
  4. DEX-DEX Arbitrage
  5. Protocol Crankers/Keepers
  6. CEX-DEX Arbitrage

Output: data/processed/aot_revenue_scenarios.csv
"""

import csv
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    DATA_RAW, DATA_PROCESSED, CSV_DELIMITER, CSV_ENCODING,
    PROTOCOL_TAKE_RATE, PROTOCOL_TAKE_RATE_HIGH_PERF,
)

OUTPUT_FILE = DATA_PROCESSED / "aot_revenue_scenarios.csv"
DATABASE_FILE = DATA_PROCESSED / "solana_epoch_database.csv"
FEE_BY_PROGRAM_FILE = DATA_RAW / "dune_fee_per_cu_by_program.csv"


def safe_float(val, default=None):
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def load_database():
    rows = []
    with open(DATABASE_FILE, "r", encoding=CSV_ENCODING) as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        rows = list(reader)
    return rows


def load_fee_by_program():
    """Load fee/CU data by program from Dune."""
    if not FEE_BY_PROGRAM_FILE.exists():
        return []
    with open(FEE_BY_PROGRAM_FILE, "r", encoding=CSV_ENCODING) as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        return list(reader)


# ──────────────────────────────────────────────────────────
# TOP-DOWN MODEL
# ──────────────────────────────────────────────────────────

def top_down_model(db_rows):
    """
    AOT_Revenue = Total_Priority_Fees × Latency_Sensitive_Share × RAIKU_Capture × Protocol_Fee

    Total_Priority_Fees: annualized from real epoch data (all Solana priority fees)
    Latency_Sensitive_Share: % of fees from latency-sensitive programs (PropAMMs, arb, HFT)
    RAIKU_Capture: market share assumption (what % of addressable market RAIKU wins)
    """
    print("\n  === TOP-DOWN AOT MODEL ===")

    # Compute annualized priority fees from recent data
    recent = []
    for row in reversed(db_rows):
        fees = safe_float(row.get("priority_fees_sol"))
        epy = safe_float(row.get("epochs_per_year"))
        price = safe_float(row.get("sol_price_usd"))
        if fees is not None and epy is not None and price is not None:
            annual_fees_usd = fees * epy * price
            recent.append({
                "epoch": int(row["epoch"]),
                "annual_fees_usd": annual_fees_usd,
                "fees_sol": fees,
                "sol_price": price,
            })
        if len(recent) >= 10:
            break

    if not recent:
        print("    ERROR: No valid fee data found")
        return []

    latest = recent[0]
    avg_annual = sum(r["annual_fees_usd"] for r in recent) / len(recent)

    print(f"    Latest epoch: {latest['epoch']}")
    print(f"    Priority fees/epoch: {latest['fees_sol']:.2f} SOL")
    print(f"    Latest annualized: ${latest['annual_fees_usd']:,.0f}")
    print(f"    Avg 10 epochs annualized: ${avg_annual:,.0f}")

    # Total priority fees sources (annualized from real data)
    total_market_sources = [
        ("Latest epoch annualized", latest["annual_fees_usd"]),
        ("Avg 10 epochs annualized", avg_annual),
    ]

    # Latency-sensitive share (portion of priority fees from latency-sensitive programs)
    # From fee/CU data: PropAMMs, arb bots, HFT = estimated 30-60% of total priority fees
    latency_sensitive_shares = [0.30, 0.40, 0.50, 0.60]

    # RAIKU capture rate (% of addressable market RAIKU wins)
    capture_rates = [0.05, 0.10, 0.15, 0.20]

    results = []
    for source_label, total_fees_usd in total_market_sources:
        for lat_share in latency_sensitive_shares:
            for capture in capture_rates:
                addressable = total_fees_usd * lat_share
                gross = addressable * capture
                protocol_rev = gross * PROTOCOL_TAKE_RATE

                results.append({
                    "model": "top_down",
                    "total_market_source": source_label,
                    "total_market_usd": round(total_fees_usd),
                    "latency_sensitive_pct": f"{lat_share*100:.0f}%",
                    "raiku_capture_pct": f"{capture*100:.0f}%",
                    "stake_pct": "",
                    "cu_reserved_pct": "",
                    "fee_per_cu_lamports": "",
                    "archetype": "",
                    "gross_revenue_usd": round(gross),
                    "validator_revenue_usd": round(gross * 0.95),
                    "protocol_revenue_usd": round(protocol_rev),
                    "protocol_revenue_monthly": round(protocol_rev / 12),
                })

    # Print key scenarios
    print("\n    Key top-down scenarios (5% protocol fee):")
    for r in results:
        if r["latency_sensitive_pct"] == "40%" and r["raiku_capture_pct"] in ["5%", "10%", "15%"]:
            print(f"      {r['total_market_source']:<30} | Latency-sensitive 40% | Capture {r['raiku_capture_pct']:>4} | "
                  f"Protocol ${r['protocol_revenue_usd']:>10,}/yr")

    return results


# ──────────────────────────────────────────────────────────
# BOTTOM-UP MODEL (3D Framework)
# ──────────────────────────────────────────────────────────

# 6 customer archetypes with estimated parameters
# CU/tx and fee/CU from on-chain data (Dune, Solscan, docs)
ARCHETYPES = [
    {
        "name": "PropAMMs",
        "description": "Oracle/quote updates (BisonFi, HumidiFi, Tessera)",
        "cu_per_tx": 1_400_000,       # ~1.4M CU per oracle update
        "fee_per_cu_lamports": 0.025,  # 0.025 L/CU (empirical: 0.016-0.027)
        "txs_per_slot": 1,            # 1 update per slot when active
        "pct_slots_active": 0.50,     # Active in 50% of slots (quote updates every ~2 slots)
        "num_customers_low": 3,
        "num_customers_mid": 6,
        "num_customers_high": 10,
    },
    {
        "name": "Quant Trading Desks",
        "description": "Parallel channel to Jito, position sizing optimization",
        "cu_per_tx": 300_000,
        "fee_per_cu_lamports": 0.15,   # Higher WTP than PropAMMs
        "txs_per_slot": 2,
        "pct_slots_active": 0.30,
        "num_customers_low": 2,
        "num_customers_mid": 5,
        "num_customers_high": 10,
    },
    {
        "name": "Market Makers (Ops)",
        "description": "Margin top-up, collateral rebalance, position rollover",
        "cu_per_tx": 50_000,
        "fee_per_cu_lamports": 0.10,
        "txs_per_slot": 1,
        "pct_slots_active": 0.10,      # Only critical ops, ~10% of slots
        "num_customers_low": 3,
        "num_customers_mid": 8,
        "num_customers_high": 15,
    },
    {
        "name": "DEX-DEX Arbitrage",
        "description": "Async scheduler, reserved slot pool",
        "cu_per_tx": 300_000,
        "fee_per_cu_lamports": 0.087,  # Empirical from Dune
        "txs_per_slot": 1,
        "pct_slots_active": 0.40,
        "num_customers_low": 5,
        "num_customers_mid": 10,
        "num_customers_high": 20,
    },
    {
        "name": "Protocol Crankers",
        "description": "Drift funding, Jupiter DCA, Kamino rebalance, Marinade",
        "cu_per_tx": 200_000,
        "fee_per_cu_lamports": 0.054,  # Low but reliable cadence
        "txs_per_slot": 1,
        "pct_slots_active": 0.05,      # Cadence-based, every ~20 slots
        "num_customers_low": 5,
        "num_customers_mid": 15,
        "num_customers_high": 30,
    },
    {
        "name": "CEX-DEX Arbitrage",
        "description": "Highest value, deepest slot pools, aggressive bidding",
        "cu_per_tx": 300_000,
        "fee_per_cu_lamports": 0.50,   # Premium WTP for execution certainty
        "txs_per_slot": 2,
        "pct_slots_active": 0.60,
        "num_customers_low": 2,
        "num_customers_mid": 5,
        "num_customers_high": 8,
    },
]


def bottom_up_model(db_rows):
    """
    AOT_Revenue = Stake% × Slots/yr × CU_reserved% × Fee/CU × SOL_price
    Per archetype, per customer tier.

    From raiku_revenue_v2.txt:
    - Total stake: 424.2M SOL, 788 active validators
    - Slots/yr = 78.4M (from Solana ~2 slots/sec × 365.25 days)
    - CU reservation: 10% of block (6M CU/slot) initially
    """
    print("\n  === BOTTOM-UP AOT MODEL (3D Framework) ===")

    # Get latest network parameters from data
    latest = None
    for row in reversed(db_rows):
        stake = safe_float(row.get("active_stake_sol"))
        price = safe_float(row.get("sol_price_usd"))
        if stake and price:
            latest = row
            break

    total_stake = safe_float(latest.get("active_stake_sol")) if latest else 424_200_000
    sol_price = safe_float(latest.get("sol_price_usd")) if latest else 100.0
    total_validators = safe_float(latest.get("validator_count")) if latest else 788

    # Solana constants
    SLOTS_PER_YEAR = 78_408_000  # ~2 slots/sec × 86400 × 365.25
    CU_PER_BLOCK = 48_000_000   # Max CU per block
    LAMPORTS_PER_SOL = 1_000_000_000

    # Scenario parameters
    stake_pcts = [0.01, 0.03, 0.05, 0.10, 0.20]
    cu_reserved_pcts = [0.05, 0.10, 0.15]  # % of block reserved for AOT

    print(f"    Network: {total_stake:,.0f} SOL staked, {total_validators:.0f} validators")
    print(f"    SOL price: ${sol_price:.2f}")
    print(f"    Slots/year: {SLOTS_PER_YEAR:,}")

    results = []

    for stake_pct in stake_pcts:
        raiku_slots_per_year = SLOTS_PER_YEAR * stake_pct

        for cu_pct in cu_reserved_pcts:
            cu_available_per_slot = CU_PER_BLOCK * cu_pct

            # Per-archetype revenue
            for arch in ARCHETYPES:
                # CU consumed per slot by this archetype (when active)
                cu_per_slot = arch["cu_per_tx"] * arch["txs_per_slot"]

                # Make sure we don't exceed available CU
                cu_used = min(cu_per_slot, cu_available_per_slot)

                # Fee per slot in SOL
                fee_per_slot_lamports = cu_used * arch["fee_per_cu_lamports"]
                fee_per_slot_sol = fee_per_slot_lamports / LAMPORTS_PER_SOL

                # Annual revenue per customer
                active_slots = raiku_slots_per_year * arch["pct_slots_active"]
                annual_per_customer_sol = fee_per_slot_sol * active_slots
                annual_per_customer_usd = annual_per_customer_sol * sol_price

                # Customer count scenarios
                for tier, count in [("low", arch["num_customers_low"]),
                                     ("mid", arch["num_customers_mid"]),
                                     ("high", arch["num_customers_high"])]:
                    gross = annual_per_customer_usd * count
                    protocol_rev = gross * PROTOCOL_TAKE_RATE

                    results.append({
                        "model": "bottom_up",
                        "total_market_source": f"3D: {stake_pct*100:.0f}% stake, {cu_pct*100:.0f}% CU, {tier} customers",
                        "total_market_usd": "",
                        "latency_sensitive_pct": "",
                        "raiku_capture_pct": "",
                        "stake_pct": f"{stake_pct*100:.0f}%",
                        "cu_reserved_pct": f"{cu_pct*100:.0f}%",
                        "fee_per_cu_lamports": arch["fee_per_cu_lamports"],
                        "archetype": arch["name"],
                        "gross_revenue_usd": round(gross),
                        "validator_revenue_usd": round(gross * 0.95),
                        "protocol_revenue_usd": round(protocol_rev),
                        "protocol_revenue_monthly": round(protocol_rev / 12),
                    })

    # Aggregate bottom-up by scenario (sum across archetypes)
    print("\n    Bottom-up aggregate by stake % × CU % (mid customers, 5% protocol fee):")
    for stake_pct in stake_pcts:
        for cu_pct in cu_reserved_pcts:
            scenario_total = sum(
                r["protocol_revenue_usd"] for r in results
                if r["stake_pct"] == f"{stake_pct*100:.0f}%"
                and r["cu_reserved_pct"] == f"{cu_pct*100:.0f}%"
                and "mid" in r["total_market_source"]
            )
            if cu_pct == 0.10:  # Only print 10% CU for readability
                print(f"      Stake {stake_pct*100:>5.0f}% | CU 10% | Protocol ${scenario_total:>10,}/yr")

    return results


def model():
    """Run both top-down and bottom-up AOT models."""
    print("  Loading epoch database...")
    db_rows = load_database()
    print(f"    {len(db_rows)} epochs loaded")

    # Run both models
    td_results = top_down_model(db_rows)
    bu_results = bottom_up_model(db_rows)

    # Combine
    all_results = td_results + bu_results

    # Save
    columns = [
        "model", "total_market_source", "total_market_usd", "latency_sensitive_pct", "raiku_capture_pct",
        "stake_pct", "cu_reserved_pct", "fee_per_cu_lamports", "archetype",
        "gross_revenue_usd", "validator_revenue_usd",
        "protocol_revenue_usd", "protocol_revenue_monthly",
    ]

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, delimiter=CSV_DELIMITER)
        writer.writeheader()
        writer.writerows(all_results)

    print(f"\n  Saved: {OUTPUT_FILE} ({len(all_results)} scenarios)")

    # Sanity check: top-down vs bottom-up order of magnitude
    print("\n  === SANITY CHECK: Top-Down vs Bottom-Up ===")
    # Top-down base case: avg total fees, 40% latency-sensitive, 10% capture
    td_base = [r for r in td_results
                if r["latency_sensitive_pct"] == "40%" and r["raiku_capture_pct"] == "10%"
                and "Avg" in r["total_market_source"]]
    # Bottom-up base case: 5% stake, 10% CU, mid customers (sum all archetypes)
    bu_base = sum(
        r["protocol_revenue_usd"] for r in bu_results
        if r["stake_pct"] == "5%" and r["cu_reserved_pct"] == "10%"
        and "mid" in r["total_market_source"]
    )

    if td_base:
        td_val = td_base[0]["protocol_revenue_usd"]
        print(f"    Top-Down (40% addr, 10% capture):  ${td_val:>10,}/yr protocol")
        print(f"    Bottom-Up (5% stake, 10% CU, mid): ${bu_base:>10,}/yr protocol")
        if td_val > 0 and bu_base > 0:
            ratio = td_val / bu_base
            print(f"    Ratio TD/BU: {ratio:.1f}x", end="")
            if 0.1 < ratio < 10:
                print(" ✓ (same order of magnitude)")
            else:
                print(" ⚠ (different orders of magnitude — investigate)")


if __name__ == "__main__":
    print("\n=== AOT Revenue Model ===")
    model()
    print("Done.")

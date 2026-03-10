"""
JIT Revenue Model — Raiku Protocol
====================================
JIT_Revenue = Total_Jito_Tips × RAIKU_Market_Share × Protocol_Take_Rate

Revenue waterfall:
  Step 1: Validator Base = Gross × (1 - Take Rate)
          Protocol Pool  = Gross × Take Rate
  Step 2: JIT Rebate     = funded from Protocol Pool (% of gross, clamped)
          Treasury       = Protocol Pool - Rebate

Terminology:
  - total_market: Total Jito MEV tips on Solana (all players combined)
  - market_share: % of that total that RAIKU captures
  - gross_revenue: total_market × market_share (before protocol/validator split)
  - protocol_pool: gross × take rate (total protocol share before redistributions)
  - jit_rebate: customer rebate funded from protocol pool
  - treasury: protocol pool - rebate (what RAIKU treasury keeps)
  - validator_revenue: gross × (1 - take rate)

Uses real Trillium data (total_mev_earned per epoch, annualized).
Also references Jito 2025 benchmarks from Post-TGE Design doc.

Output: data/processed/jit_revenue_scenarios.csv
"""

import csv
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    DATA_PROCESSED, CSV_DELIMITER, CSV_ENCODING,
    PROTOCOL_TAKE_RATE, PROTOCOL_TAKE_RATE_HIGH_PERF,
    JIT_REBATE_PCT,
    JITO_2025_TOTAL_TIPS_USD, JITO_Q4_2025_ANNUALIZED_USD,
    SCENARIOS,
)

OUTPUT_FILE = DATA_PROCESSED / "jit_revenue_scenarios.csv"
DATABASE_FILE = DATA_PROCESSED / "solana_epoch_database.csv"


def load_database():
    """Load the merged Solana epoch database."""
    rows = []
    with open(DATABASE_FILE, "r", encoding=CSV_ENCODING) as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        rows = list(reader)
    return rows


def safe_float(val, default=None):
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def compute_total_market_from_data(db_rows):
    """
    Compute the total JIT market size from real epoch data.
    = total MEV tips on Solana, annualized from recent epochs.
    """
    # Use most recent 10 Trillium epochs with valid data for annualization
    recent = []
    for row in reversed(db_rows):
        mev = safe_float(row.get("mev_jito_tips_sol"))
        epy = safe_float(row.get("epochs_per_year"))
        price = safe_float(row.get("sol_price_usd"))
        if mev is not None and epy is not None and price is not None:
            annual_mev_usd = mev * epy * price
            recent.append({
                "epoch": int(row["epoch"]),
                "mev_per_epoch": mev,
                "epochs_per_year": epy,
                "sol_price": price,
                "annual_mev_usd": annual_mev_usd,
            })
        if len(recent) >= 10:
            break

    if not recent:
        return None

    latest = recent[0]
    avg_annual = sum(r["annual_mev_usd"] for r in recent) / len(recent)

    return {
        "latest_epoch": latest["epoch"],
        "latest_annual_mev_usd": latest["annual_mev_usd"],
        "avg_10_epoch_annual_mev_usd": avg_annual,
        "latest_mev_per_epoch_sol": latest["mev_per_epoch"],
        "latest_sol_price": latest["sol_price"],
        "latest_epochs_per_year": latest["epochs_per_year"],
        "sample_size": len(recent),
    }


def model():
    """Run JIT revenue model across all scenarios."""
    print("  Loading epoch database...")
    db_rows = load_database()
    print(f"    {len(db_rows)} epochs loaded")

    # Compute total market from real data
    print("  Computing total JIT market from Trillium data...")
    market_data = compute_total_market_from_data(db_rows)

    if market_data:
        print(f"    Latest epoch: {market_data['latest_epoch']}")
        print(f"    MEV/epoch: {market_data['latest_mev_per_epoch_sol']:.2f} SOL")
        print(f"    SOL price: ${market_data['latest_sol_price']:.2f}")
        print(f"    Epochs/year: {market_data['latest_epochs_per_year']:.1f}")
        print(f"    Latest annual MEV (total market): ${market_data['latest_annual_mev_usd']:,.0f}")
        print(f"    Avg 10 epochs (total market): ${market_data['avg_10_epoch_annual_mev_usd']:,.0f}")
    else:
        print("    WARNING: No valid Trillium data, using config benchmarks only")

    # ── Total market estimates (different sources) ──────────
    # "Total market" = all Jito MEV tips on Solana per year
    # This is the full pie — RAIKU's share comes from market_share below
    total_market_sources = {}

    if market_data:
        total_market_sources["trillium_latest"] = {
            "label": "On-chain data latest (annualized)",
            "total_market_usd": market_data["latest_annual_mev_usd"],
            "source": f"Epoch {market_data['latest_epoch']} × epochs/year × SOL price",
        }
        total_market_sources["trillium_avg10"] = {
            "label": "On-chain data avg 10 epochs (annualized)",
            "total_market_usd": market_data["avg_10_epoch_annual_mev_usd"],
            "source": "Last 10 epochs averaged",
        }

    # From Jito benchmarks (config.py)
    total_market_sources["jito_q4_annualized"] = {
        "label": "Jito Q4-2025 annualized (bear case)",
        "total_market_usd": JITO_Q4_2025_ANNUALIZED_USD,
        "source": "Post-TGE Design doc — conservative",
    }
    total_market_sources["jito_2025_full"] = {
        "label": "Jito 2025 full year (bull case)",
        "total_market_usd": JITO_2025_TOTAL_TIPS_USD,
        "source": "Post-TGE Design doc — includes Q1 bull run",
    }

    # ── RAIKU market share scenarios ────────────────────────
    # "Market share" = what % of total Jito tips flow through RAIKU
    market_shares = [0.02, 0.05, 0.10, 0.15, 0.20]

    # Protocol fee scenarios
    protocol_fees = [
        ("5.0%", PROTOCOL_TAKE_RATE),
        ("3.5%", PROTOCOL_TAKE_RATE_HIGH_PERF),
    ]

    # Generate all scenarios
    results = []
    print("\n  Generating JIT revenue scenarios...")

    for src_key, src_info in total_market_sources.items():
        total_mkt = src_info["total_market_usd"]
        for share in market_shares:
            for fee_label, fee_rate in protocol_fees:
                gross_revenue = total_mkt * share
                validator_revenue = gross_revenue * (1 - fee_rate)
                protocol_pool = gross_revenue * fee_rate

                # Waterfall: rebate funded from protocol pool, clamped
                rebate_rate = min(JIT_REBATE_PCT, fee_rate) if fee_rate > 0 else 0
                jit_rebate = gross_revenue * rebate_rate
                treasury = protocol_pool - jit_rebate

                results.append({
                    "total_market_source": src_info["label"],
                    "total_market_usd": round(total_mkt),
                    "raiku_market_share_pct": f"{share*100:.0f}%",
                    "raiku_market_share": share,
                    "protocol_fee_pct": fee_label,
                    "protocol_fee": fee_rate,
                    "gross_revenue_usd": round(gross_revenue),
                    "validator_revenue_usd": round(validator_revenue),
                    "protocol_pool_usd": round(protocol_pool),
                    "jit_rebate_usd": round(jit_rebate),
                    "treasury_usd": round(treasury),
                    "treasury_monthly": round(treasury / 12),
                })

    # Save results
    columns = [
        "total_market_source", "total_market_usd",
        "raiku_market_share_pct", "raiku_market_share",
        "protocol_fee_pct", "protocol_fee",
        "gross_revenue_usd", "validator_revenue_usd",
        "protocol_pool_usd", "jit_rebate_usd",
        "treasury_usd", "treasury_monthly",
    ]

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, delimiter=CSV_DELIMITER)
        writer.writeheader()
        writer.writerows(results)

    print(f"  Saved: {OUTPUT_FILE} ({len(results)} scenarios)")

    # Print key scenarios
    print("\n  === Key JIT Scenarios (5% take rate) ===")
    print("  Total market = all Jito MEV tips/yr | Share = RAIKU's slice | Treasury = pool - rebate")
    for r in results:
        if r["protocol_fee"] == PROTOCOL_TAKE_RATE and r["raiku_market_share"] in [0.05, 0.10, 0.15]:
            print(f"    {r['total_market_source']:<45} | Share {r['raiku_market_share_pct']:>4} | "
                  f"Gross ${r['gross_revenue_usd']:>12,} | Treasury ${r['treasury_usd']:>10,}/yr "
                  f"(${r['treasury_monthly']:>8,}/mo)")


if __name__ == "__main__":
    print("\n=== JIT Revenue Model ===")
    model()
    print("Done.")

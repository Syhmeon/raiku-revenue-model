"""
AOT Revenue Estimation Model
==============================

AOT (Ahead-of-Time) auction is RAIKU's PRIMARY revenue stream.
This model estimates revenue based on customer archetypes and their
willingness to pay for guaranteed blockspace.

Model:
  For each customer archetype:
    Revenue_i = N_customers_i × Avg_reservations_per_day × Avg_bid_per_CU × CU_per_reservation × 365

  Total_AOT_Revenue = Sum(Revenue_i for all archetypes)
  Protocol_Revenue = Total_AOT_Revenue × Take_Rate

Key Economic Insight (from Opportunity Cost doc):
  AOT is justified when the expected cost of missing a target slot (L)
  exceeds the AOT fee premium. Break-even L* ≈ $0.0067 per missed slot.

Customer Archetypes (from Use Cases doc):
  1. PropAMMs — Oracle updates, adverse selection prevention
  2. Quant/HFT Desks — Execution certainty, position sizing
  3. Market Makers — Operational tx insurance
  4. DEX-DEX Arb — Guaranteed slot for arb execution
  5. Protocol Keepers — Cadence-aware crank scheduling
  6. CEX-DEX Arb — Asymmetric risk elimination (highest value)

TODO:
  - [ ] Get real CU pricing data from mainnet testing
  - [ ] Model customer acquisition timeline (5 initial → 50+ at maturity)
  - [ ] Add volume data from BigQuery (PropAMM volumes, Jito arb stats)
  - [ ] Sensitivity analysis on bid prices
  - [ ] Model Raiku validator slot frequency based on stake share
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    PROTOCOL_TAKE_RATE,
    AOT_P_TARGET,
    STANDARD_P_TARGET,
)


# ── Customer Archetype Parameters ──────────────────────
# These are estimates — to be refined with real mainnet data
ARCHETYPES = {
    "propamm": {
        "name": "PropAMM (Oracle Updates)",
        "n_customers_initial": 3,       # BisonFi, HumidiFi, Tessera, SolFi subset
        "n_customers_mature": 8,
        "reservations_per_day": 200,     # High-frequency updates
        "avg_bid_lamports_per_cu": 50,   # Estimated, from opportunity cost doc
        "cu_per_reservation": 200_000,   # Standard CU allocation
        "miss_loss_usd": 0.66,           # L from opportunity cost doc (1 bp markout)
    },
    "quant_hft": {
        "name": "Quant/HFT Desk",
        "n_customers_initial": 2,
        "n_customers_mature": 10,
        "reservations_per_day": 100,     # Signal-driven, lower frequency
        "avg_bid_lamports_per_cu": 80,   # Higher willingness to pay
        "cu_per_reservation": 300_000,
        "miss_loss_usd": 1.58,           # Average arb profit (Helius)
    },
    "market_maker_ops": {
        "name": "Market Maker (Operational)",
        "n_customers_initial": 3,
        "n_customers_mature": 12,
        "reservations_per_day": 50,      # Periodic maintenance slots
        "avg_bid_lamports_per_cu": 40,   # Lower frequency, lower bid
        "cu_per_reservation": 150_000,
        "miss_loss_usd": 1000,           # Liquidation defense
    },
    "dex_dex_arb": {
        "name": "DEX-DEX Arbitrage",
        "n_customers_initial": 3,
        "n_customers_mature": 15,
        "reservations_per_day": 300,     # High frequency, async pool
        "avg_bid_lamports_per_cu": 60,
        "cu_per_reservation": 250_000,
        "miss_loss_usd": 1.58,           # Average arb profit
    },
    "keeper": {
        "name": "Protocol Keeper/Cranker",
        "n_customers_initial": 2,
        "n_customers_mature": 8,
        "reservations_per_day": 30,      # Cadence-aware, lower frequency
        "avg_bid_lamports_per_cu": 30,   # Cost-sensitive
        "cu_per_reservation": 200_000,
        "miss_loss_usd": 10,             # Stale state cost (varies widely)
    },
    "cex_dex_arb": {
        "name": "CEX-DEX Arbitrage",
        "n_customers_initial": 2,
        "n_customers_mature": 10,
        "reservations_per_day": 500,     # Highest frequency, deep slot pools
        "avg_bid_lamports_per_cu": 100,  # Highest willingness to pay
        "cu_per_reservation": 300_000,
        "miss_loss_usd": 50,             # Unhedged position risk
    },
}


def estimate_archetype_revenue(
    archetype: dict,
    n_customers: int = None,
    sol_price_usd: float = 150.0,
    take_rate: float = PROTOCOL_TAKE_RATE,
) -> dict:
    """
    Estimate annual revenue from one customer archetype.

    Args:
        archetype: Dict with archetype parameters
        n_customers: Override customer count (uses initial if None)
        sol_price_usd: SOL price for lamport→USD conversion
        take_rate: Protocol take rate

    Returns:
        dict with revenue breakdown
    """
    n = n_customers or archetype["n_customers_initial"]
    lamports_per_sol = 1_000_000_000

    # Daily revenue per customer (in SOL)
    daily_sol = (
        archetype["reservations_per_day"]
        * archetype["avg_bid_lamports_per_cu"]
        * archetype["cu_per_reservation"]
        / lamports_per_sol
    )

    # Annual totals
    annual_sol = daily_sol * n * 365
    annual_usd = annual_sol * sol_price_usd
    protocol_revenue_usd = annual_usd * take_rate

    return {
        "archetype": archetype["name"],
        "n_customers": n,
        "daily_sol_per_customer": daily_sol,
        "annual_total_sol": annual_sol,
        "annual_total_usd": annual_usd,
        "protocol_revenue_usd": protocol_revenue_usd,
    }


def estimate_total_aot(
    phase: str = "initial",
    sol_price_usd: float = 150.0,
    take_rate: float = PROTOCOL_TAKE_RATE,
) -> dict:
    """
    Estimate total AOT revenue across all archetypes.

    Args:
        phase: "initial" (launch) or "mature" (1-2 years post-launch)
        sol_price_usd: Current SOL price
        take_rate: Protocol take rate
    """
    results = []
    total_volume = 0
    total_protocol = 0

    for key, archetype in ARCHETYPES.items():
        n = archetype[f"n_customers_{phase}"] if phase in ("initial", "mature") else archetype["n_customers_initial"]
        r = estimate_archetype_revenue(archetype, n_customers=n, sol_price_usd=sol_price_usd, take_rate=take_rate)
        results.append(r)
        total_volume += r["annual_total_usd"]
        total_protocol += r["protocol_revenue_usd"]

    return {
        "phase": phase,
        "sol_price_usd": sol_price_usd,
        "take_rate": take_rate,
        "archetypes": results,
        "total_aot_volume_usd": total_volume,
        "total_protocol_revenue_usd": total_protocol,
    }


def run_analysis():
    """Print full AOT revenue analysis."""
    for phase in ("initial", "mature"):
        for sol_price in (100, 150, 250):
            result = estimate_total_aot(phase=phase, sol_price_usd=sol_price)

            print(f"\n{'='*70}")
            print(f"AOT Revenue — Phase: {phase.upper()}, SOL: ${sol_price}")
            print(f"{'='*70}")
            print(f"{'Archetype':<30} {'Customers':>5} {'Annual Vol':>12} {'Protocol':>10}")
            print("-" * 65)

            for r in result["archetypes"]:
                print(
                    f"{r['archetype']:<30} "
                    f"{r['n_customers']:>5} "
                    f"${r['annual_total_usd']/1e3:>9.0f}K "
                    f"${r['protocol_revenue_usd']/1e3:>7.0f}K"
                )

            print("-" * 65)
            print(
                f"{'TOTAL':<30} "
                f"{'':>5} "
                f"${result['total_aot_volume_usd']/1e6:>8.1f}M "
                f"${result['total_protocol_revenue_usd']/1e3:>7.0f}K"
            )


if __name__ == "__main__":
    run_analysis()

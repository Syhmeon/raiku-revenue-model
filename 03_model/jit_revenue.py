"""
JIT Revenue Estimation Model
=============================

JIT (Just-in-Time) auction is RAIKU's secondary revenue stream,
comparable to Jito bundles/tips.

Model:
  JIT_Revenue = MEV_Market_Size × RAIKU_Validator_Share × Protocol_Take_Rate

Key inputs:
  - Total Solana MEV market size (Jito tips as proxy)
  - RAIKU's validator market share (% of total stake)
  - Protocol take rate (1-5%, default 5%)

Reference data:
  - Jito 2025: ~$720M total tips, ~$7-8M DAO revenue
  - Q4-2025 annualized: ~$100M (conservative base)

TODO:
  - [ ] Pull actual Jito tip data from BigQuery/Token Terminal
  - [ ] Model validator share growth over time (launch → maturity)
  - [ ] Add sensitivity analysis on take rate
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    PROTOCOL_TAKE_RATE,
    PROTOCOL_TAKE_RATE_HIGH_PERF,
    JITO_2025_TOTAL_TIPS_USD,
    JITO_Q4_2025_ANNUALIZED_USD,
    SCENARIOS,
)


def estimate_jit_revenue(
    mev_market_usd: float,
    raiku_validator_share: float,
    take_rate: float = PROTOCOL_TAKE_RATE,
) -> dict:
    """
    Estimate annual JIT protocol revenue.

    Args:
        mev_market_usd: Total annual MEV market (Jito tips as proxy)
        raiku_validator_share: RAIKU's share of validator market (0-1)
        take_rate: Protocol fee (0-1, default 0.05)

    Returns:
        dict with revenue breakdown
    """
    total_jit_volume = mev_market_usd * raiku_validator_share
    protocol_revenue = total_jit_volume * take_rate
    validator_revenue = total_jit_volume * (1 - take_rate)

    return {
        "mev_market_usd": mev_market_usd,
        "raiku_validator_share": raiku_validator_share,
        "take_rate": take_rate,
        "total_jit_volume": total_jit_volume,
        "protocol_revenue": protocol_revenue,
        "validator_revenue": validator_revenue,
    }


def run_scenarios():
    """Run all predefined scenarios."""
    print("\n=== JIT Revenue Scenarios ===\n")
    print(f"{'Scenario':<25} {'MEV Market':>12} {'Share':>6} {'JIT Volume':>12} {'Protocol Rev':>12}")
    print("-" * 75)

    for name, params in SCENARIOS.items():
        result = estimate_jit_revenue(
            mev_market_usd=params["mev_market_usd"],
            raiku_validator_share=params["market_share"],
        )
        print(
            f"{name:<25} "
            f"${result['mev_market_usd']/1e6:>9.0f}M "
            f"{result['raiku_validator_share']:>5.0%} "
            f"${result['total_jit_volume']/1e6:>9.1f}M "
            f"${result['protocol_revenue']/1e3:>9.0f}K"
        )

    # Also show with reduced take rate for high performers
    print(f"\n--- With high-perf take rate ({PROTOCOL_TAKE_RATE_HIGH_PERF:.1%}) ---")
    for name, params in SCENARIOS.items():
        result = estimate_jit_revenue(
            mev_market_usd=params["mev_market_usd"],
            raiku_validator_share=params["market_share"],
            take_rate=PROTOCOL_TAKE_RATE_HIGH_PERF,
        )
        print(
            f"{name:<25} "
            f"${result['mev_market_usd']/1e6:>9.0f}M "
            f"{result['raiku_validator_share']:>5.0%} "
            f"${result['total_jit_volume']/1e6:>9.1f}M "
            f"${result['protocol_revenue']/1e3:>9.0f}K"
        )


if __name__ == "__main__":
    run_scenarios()

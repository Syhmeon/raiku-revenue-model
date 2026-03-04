"""
RAIKU Revenue Estimation — Master Pipeline
============================================

Run the full pipeline: Extract → Transform → Model

Usage:
    python run_pipeline.py              # Full pipeline (extract + transform + model)
    python run_pipeline.py --no-extract # Skip extraction (use existing data)
    python run_pipeline.py --model-only # Only run revenue models
"""

import sys
import argparse
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Ensure imports work
sys.path.insert(0, str(Path(__file__).parent))


def run_extract():
    """Phase 1: Extract data from all sources."""
    print("\n" + "=" * 60)
    print("PHASE 1: EXTRACT")
    print("=" * 60)

    # Only import when needed (avoids API calls if --no-extract)
    from _01_extract import dune_epochs, dune_validators, dune_active_stake, coingecko_prices

    print("\n--- Dune: Epoch Economics ---")
    try:
        dune_epochs.extract()
    except Exception as e:
        print(f"  ERROR: {e}")

    print("\n--- Dune: Validators ---")
    try:
        dune_validators.extract()
    except Exception as e:
        print(f"  ERROR: {e}")

    print("\n--- Dune: Active Stake ---")
    try:
        dune_active_stake.extract()
    except Exception as e:
        print(f"  ERROR: {e}")

    print("\n--- CoinGecko: SOL Prices ---")
    try:
        coingecko_prices.extract()
    except Exception as e:
        print(f"  ERROR: {e}")


def run_transform():
    """Phase 2: Merge and validate data."""
    print("\n" + "=" * 60)
    print("PHASE 2: TRANSFORM")
    print("=" * 60)

    from _02_transform.build_database import build
    build()


def run_model():
    """Phase 3: Run revenue estimation models."""
    print("\n" + "=" * 60)
    print("PHASE 3: REVENUE MODEL")
    print("=" * 60)

    from _03_model.jit_revenue import run_scenarios as jit_scenarios
    from _03_model.aot_revenue import run_analysis as aot_analysis

    print("\n--- JIT Revenue Model ---")
    jit_scenarios()

    print("\n--- AOT Revenue Model ---")
    aot_analysis()


def main():
    parser = argparse.ArgumentParser(description="RAIKU Revenue Estimation Pipeline")
    parser.add_argument("--no-extract", action="store_true", help="Skip data extraction")
    parser.add_argument("--model-only", action="store_true", help="Only run revenue models")
    args = parser.parse_args()

    print("=" * 60)
    print("  RAIKU REVENUE ESTIMATION PIPELINE")
    print("=" * 60)

    if args.model_only:
        run_model()
    else:
        if not args.no_extract:
            run_extract()
        run_transform()
        run_model()

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()

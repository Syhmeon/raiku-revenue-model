"""
RAIKU Revenue Estimation — Master Pipeline Orchestrator
========================================================
Runs: Extract → Transform → Model → (optional) Output

Usage:
    python run_pipeline.py                  # Core: Trillium + CoinGecko → DB → Models
    python run_pipeline.py --full           # Full re-extraction (all epochs, not incremental)
    python run_pipeline.py --full-extract   # ALL extractions (requires Dune API key + all APIs)
    python run_pipeline.py --model-only     # Skip extraction/transform, only run models + sanity
    python run_pipeline.py --export         # Include Google Sheets export at end

Pipeline stages:
    Extract  → 10 scripts pulling from Trillium, CoinGecko, Solana Compass, Jito, Dune
    Transform → Build epoch database + program database + program conditions
    Model    → JIT revenue, AOT revenue, sanity check
    Output   → Google Sheets export (optional, requires service_account.json)
"""

import argparse
import sys
import time
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "01_extract"))
sys.path.insert(0, str(Path(__file__).parent / "02_transform"))
sys.path.insert(0, str(Path(__file__).parent / "03_model"))


def run_step(name: str, func, *args):
    """Run a pipeline step with timing."""
    print(f"\n{'='*60}")
    print(f"  STEP: {name}")
    print(f"{'='*60}")
    start = time.time()
    try:
        func(*args)
        elapsed = time.time() - start
        print(f"\n  ✓ {name} completed in {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n  ✗ {name} FAILED after {elapsed:.1f}s: {e}")
        import traceback
        traceback.print_exc()
        return False


def try_step(name: str, import_path: str, func_name: str, *args):
    """Import and run a step, skipping gracefully if import fails."""
    try:
        mod = __import__(import_path)
        func = getattr(mod, func_name)
        return run_step(name, func, *args)
    except ImportError as e:
        print(f"\n  ⊘ Skipping {name} — import failed: {e}")
        return None  # None = skipped, not failed
    except Exception as e:
        print(f"\n  ✗ {name} FAILED: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="RAIKU Revenue Estimation Pipeline")
    parser.add_argument("--full", action="store_true",
                        help="Full re-extraction (all epochs, not incremental)")
    parser.add_argument("--full-extract", action="store_true",
                        help="Run ALL extractions (requires Dune API key + all APIs)")
    parser.add_argument("--model-only", action="store_true",
                        help="Skip extraction/transform, run models + sanity check only")
    parser.add_argument("--export", action="store_true",
                        help="Include Google Sheets export at end")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("  RAIKU Revenue Estimation Pipeline")
    print("=" * 60)

    total_start = time.time()
    steps_ok = 0
    steps_fail = 0
    steps_skip = 0

    def track(result):
        nonlocal steps_ok, steps_fail, steps_skip
        if result is True:
            steps_ok += 1
        elif result is False:
            steps_fail += 1
        else:
            steps_skip += 1

    if not args.model_only:
        # ═══════════════════════════════════════════════════════
        #  PHASE 1: EXTRACT
        # ═══════════════════════════════════════════════════════
        print(f"\n{'─'*60}")
        print("  PHASE 1: EXTRACT")
        print(f"{'─'*60}")

        # Core extractions (always run)
        from extract_trillium import extract as extract_trillium
        track(run_step("Extract Trillium Epoch Data", extract_trillium, args.full))

        track(try_step("Extract CoinGecko Prices", "coingecko_prices", "extract"))

        # Extended extractions (--full-extract or --full)
        if args.full_extract or args.full:
            track(try_step("Extract Solana Compass", "extract_solana_compass", "extract"))
            track(try_step("Extract Jito MEV Rewards", "extract_jito_mev", "extract"))
            track(try_step("Extract Dune Epoch Data", "dune_epochs", "extract"))
            track(try_step("Extract Dune Validators", "dune_validators", "extract"))
            track(try_step("Extract Dune Active Stake", "dune_active_stake", "extract"))
            track(try_step("Extract Dune Program Fees", "extract_dune_programs", "extract"))
            track(try_step("Extract Trillium Intraday", "extract_intraday", "extract"))
        else:
            print("\n  (Use --full-extract to run Dune/SC/Jito extractions)")

        # ═══════════════════════════════════════════════════════
        #  PHASE 2: TRANSFORM
        # ═══════════════════════════════════════════════════════
        print(f"\n{'─'*60}")
        print("  PHASE 2: TRANSFORM")
        print(f"{'─'*60}")

        from build_database import build as build_db
        track(run_step("Build Epoch Database", build_db))

        track(try_step("Build Program Conditions", "build_program_conditions", "build"))
        track(try_step("Build Program Database", "build_program_database", "build"))

    else:
        print("\n  --model-only: Skipping extraction & transform steps")

    # ═══════════════════════════════════════════════════════
    #  PHASE 3: MODEL
    # ═══════════════════════════════════════════════════════
    print(f"\n{'─'*60}")
    print("  PHASE 3: MODEL")
    print(f"{'─'*60}")

    from jit_revenue import model as jit_model
    track(run_step("JIT Revenue Model", jit_model))

    from aot_revenue import model as aot_model
    track(run_step("AOT Revenue Model", aot_model))

    track(try_step("Sanity Check", "sanity_check", "run"))

    # ═══════════════════════════════════════════════════════
    #  PHASE 4: OUTPUT (optional)
    # ═══════════════════════════════════════════════════════
    if args.export:
        print(f"\n{'─'*60}")
        print("  PHASE 4: OUTPUT")
        print(f"{'─'*60}")
        sys.path.insert(0, str(Path(__file__).parent / "04_output"))
        track(try_step("Export to Google Sheets", "sheets_export", "export_all"))
    else:
        print("\n  (Use --export to push results to Google Sheets)")

    # ═══════════════════════════════════════════════════════
    #  SUMMARY
    # ═══════════════════════════════════════════════════════
    total_elapsed = time.time() - total_start
    total_steps = steps_ok + steps_fail + steps_skip
    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE in {total_elapsed:.1f}s")
    print(f"    ✓ {steps_ok} succeeded")
    if steps_fail:
        print(f"    ✗ {steps_fail} failed")
    if steps_skip:
        print(f"    ⊘ {steps_skip} skipped")
    print(f"{'='*60}")

    # List output files
    from config import DATA_PROCESSED
    if DATA_PROCESSED.exists():
        print(f"\n  Output files in {DATA_PROCESSED}:")
        for f in sorted(DATA_PROCESSED.glob("*.csv")):
            size_kb = f.stat().st_size / 1024
            print(f"    {f.name} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()

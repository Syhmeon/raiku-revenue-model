"""
Build clean Solana epoch database — RAW data only
===================================================
Merges 5 sources into a single clean CSV with well-named columns.
Only RAW extracted data — NO pre-computed values (all calculations
will be done as Google Sheet formulas).

Sources:
  1. Trillium API (primary, epochs 552+): MEV breakdown, fees, CU, APY
  2. Dune Analytics (secondary, epochs 150-935): economics, commissions
  3. Dune active stake per epoch
  4. CoinGecko (365 days): SOL price, FDV
  5. Jito Foundation (cross-check, epochs 390+): official MEV
  6. Solana Compass (cross-check, epochs 800+): txns, CU, fees

Output: data/processed/solana_epoch_database.csv (semicolon-delimited)
"""

import csv
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_RAW, DATA_PROCESSED, CSV_DELIMITER, CSV_ENCODING

TRILLIUM_FILE = DATA_RAW / "trillium_epoch_data.csv"
DUNE_EPOCHS_FILE = DATA_RAW / "dune_epoch_data_v2.csv"
DUNE_VALIDATORS_FILE = DATA_RAW / "dune_commission_validators_v2.csv"
DUNE_ACTIVE_STAKE_FILE = DATA_RAW / "dune_active_stake_v1.csv"
COINGECKO_FILE = DATA_RAW / "coingecko_sol_price.csv"
SOLANA_COMPASS_FILE = DATA_RAW / "solana_compass_epochs.csv"
JITO_MEV_FILE = DATA_RAW / "jito_mev_rewards.csv"
INTRADAY_FILE = DATA_RAW / "trillium_intraday_peaks.csv"

OUTPUT_FILE = DATA_PROCESSED / "solana_epoch_database.csv"

# ── Output columns: RAW only, logically ordered ─────────
# Group 1: Epoch metadata
# Group 2: Rewards (SOL)
# Group 3: MEV breakdown (SOL)
# Group 4: Validator economics
# Group 5: Price & market
# Group 6: CU & transactions
# Group 7: Cross-check (Jito Foundation, Solana Compass)
# Group 8: Volatility detection (auto-computed from raw data)
# Group 9: Intra-day peaks (Trillium epoch_timeseries, volatile epochs)

RAW_COLUMNS = [
    # Group 1: Epoch metadata
    "epoch",                    # A - Epoch number (PK)
    "date",                     # B - Start date
    "duration_days",            # C - Duration in days
    # Group 2: Rewards (SOL)
    "inflation_rewards_sol",    # D - Total inflation rewards
    "total_fees_sol",           # E - Total fees (base+priority combined)
    "priority_fees_sol",        # F - Priority fees only (Trillium, 552+)
    "base_fees_sol",            # G - Base/signature fees (Trillium, 552+)
    "mev_jito_tips_sol",        # H - Total Jito MEV tips
    # Group 3: MEV breakdown (Trillium, 552+)
    "mev_to_validators_sol",    # I - MEV → validators
    "mev_to_stakers_sol",       # J - MEV → stakers
    "mev_to_jito_sol",          # K - MEV → Jito (block engine + tip router)
    # Group 4: Validator economics
    "validator_commissions_sol", # L - Commissions on inflation
    "avg_commission_rate",      # M - Stake-weighted avg commission
    "validator_count",          # N - Active validators
    "stake_accounts",           # O - Stake account count
    "active_stake_sol",         # P - Total active stake (SOL)
    # Group 5: Price & market
    "sol_price_usd",            # Q - SOL price (USD)
    "fdv_usd",                  # R - Fully Diluted Valuation
    # Group 6: CU & transactions (Trillium, 552+)
    "epochs_per_year",          # S - Epochs/year (for annualization)
    "avg_cu_per_block",         # T - Avg compute units per block
    "total_user_txns",          # U - Non-vote transactions
    "total_vote_txns",          # V - Vote transactions
    "total_blocks",             # W - Blocks produced in epoch
    # Group 7: Cross-check sources
    "jito_official_mev_sol",    # X - Jito Foundation official MEV
    "sc_priority_fees_sol",     # Y - Solana Compass priority fees
    "sc_jito_tips_sol",         # Z - Solana Compass Jito tips
    # Group 8: Volatility detection (auto-computed from raw data)
    "price_change_pct",         # AA - SOL price % change vs previous epoch
    "mev_zscore",               # AB - MEV z-score (vs rolling 30-epoch window)
    "pf_zscore",                # AC - Priority fees z-score (vs rolling 30-epoch)
    "fee_multiple",             # AD - Priority fees as multiple of rolling median
    "mev_multiple",             # AE - MEV tips as multiple of rolling median
    "volatility_tag",           # AF - Auto-tag: normal / elevated / extreme
    # Group 9: Intra-day peaks (from Trillium epoch_timeseries, volatile epochs only)
    "peak_pf_per_block_sol",    # AG - Peak 15-min priority fee per block (SOL)
    "peak_pf_multiple",         # AH - Peak PF / epoch baseline PF
    "peak_pf_time",             # AI - UTC timestamp of peak PF bucket
    "peak_tx_per_block",        # AJ - Peak 15-min user TX per block
    "peak_tx_multiple",         # AK - Peak TX / epoch baseline TX
    "peak_tx_time",             # AL - UTC timestamp of peak TX bucket
    "peak_hour_pf_sol",         # AM - Peak hourly total priority fees (SOL)
    "peak_hour_pf_time",        # AN - UTC timestamp of peak hour start
    "baseline_pf_per_block_sol",# AO - Epoch-wide avg priority fee per block (SOL)
    "baseline_tx_per_block",    # AP - Epoch-wide avg user TX per block
    "intraday_skip_rate_pct",   # AQ - Skip rate during epoch (from timeseries)
]


# ── Volatility detection parameters ───────────────────
VOLATILITY_WINDOW = 30        # Rolling window (in epochs) for z-score computation
# Thresholds for tagging (any condition triggers the tag)
EXTREME_THRESHOLDS = {
    "price_change_pct": 15.0,  # |price change| > 15%
    "zscore": 3.0,             # MEV or PF z-score > 3
    "multiple": 5.0,           # MEV or PF > 5x rolling median
}
ELEVATED_THRESHOLDS = {
    "price_change_pct": 8.0,   # |price change| > 8%
    "zscore": 2.0,             # MEV or PF z-score > 2
    "multiple": 2.5,           # MEV or PF > 2.5x rolling median
}


def safe_float(val, default=None):
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def load_csv(filepath):
    if not filepath.exists():
        print(f"  WARNING: {filepath.name} not found, skipping")
        return []
    with open(filepath, "r", encoding=CSV_ENCODING) as f:
        content = f.read()
        if content.startswith("\ufeff"):
            content = content[1:]
        reader = csv.DictReader(content.splitlines(), delimiter=CSV_DELIMITER)
        return list(reader)


def compute_volatility(merged):
    """
    Compute volatility metrics for each epoch using rolling statistics.
    Adds: price_change_pct, mev_zscore, pf_zscore, fee_multiple, mev_multiple, volatility_tag

    Uses a rolling 30-epoch window for z-scores and multiples.
    Tags: extreme (crisis events), elevated (above-normal), normal.
    """
    import math

    n = len(merged)
    if n == 0:
        return merged

    # Extract numeric series (None-safe)
    prices = [r.get("sol_price_usd") for r in merged]
    mevs = [r.get("mev_jito_tips_sol") for r in merged]
    pfs = [r.get("priority_fees_sol") for r in merged]

    for i, row in enumerate(merged):
        # ── Price change vs previous epoch ──
        if i > 0 and prices[i] and prices[i - 1] and prices[i - 1] > 0:
            row["price_change_pct"] = round(
                (prices[i] - prices[i - 1]) / prices[i - 1] * 100, 2
            )
        else:
            row["price_change_pct"] = None

        # ── Rolling window for z-scores and multiples ──
        w_start = max(0, i - VOLATILITY_WINDOW)
        w_mevs = [mevs[j] for j in range(w_start, i) if mevs[j] is not None and mevs[j] > 0]
        w_pfs = [pfs[j] for j in range(w_start, i) if pfs[j] is not None and pfs[j] > 0]

        # MEV z-score and multiple
        if len(w_mevs) >= 5 and mevs[i] is not None:
            mean_mev = sum(w_mevs) / len(w_mevs)
            std_mev = math.sqrt(sum((x - mean_mev) ** 2 for x in w_mevs) / len(w_mevs))
            med_mev = sorted(w_mevs)[len(w_mevs) // 2]
            row["mev_zscore"] = round((mevs[i] - mean_mev) / std_mev, 2) if std_mev > 0 else 0.0
            row["mev_multiple"] = round(mevs[i] / med_mev, 2) if med_mev > 0 else None
        else:
            row["mev_zscore"] = None
            row["mev_multiple"] = None

        # Priority fees z-score and multiple
        if len(w_pfs) >= 5 and pfs[i] is not None:
            mean_pf = sum(w_pfs) / len(w_pfs)
            std_pf = math.sqrt(sum((x - mean_pf) ** 2 for x in w_pfs) / len(w_pfs))
            med_pf = sorted(w_pfs)[len(w_pfs) // 2]
            row["pf_zscore"] = round((pfs[i] - mean_pf) / std_pf, 2) if std_pf > 0 else 0.0
            row["fee_multiple"] = round(pfs[i] / med_pf, 2) if med_pf > 0 else None
        else:
            row["pf_zscore"] = None
            row["fee_multiple"] = None

        # ── Volatility tag ──
        abs_price = abs(row["price_change_pct"]) if row["price_change_pct"] is not None else 0
        mev_z = row["mev_zscore"] if row["mev_zscore"] is not None else 0
        pf_z = row["pf_zscore"] if row["pf_zscore"] is not None else 0
        mev_m = row["mev_multiple"] if row["mev_multiple"] is not None else 0
        fee_m = row["fee_multiple"] if row["fee_multiple"] is not None else 0

        if (abs_price >= EXTREME_THRESHOLDS["price_change_pct"]
                or mev_z >= EXTREME_THRESHOLDS["zscore"]
                or pf_z >= EXTREME_THRESHOLDS["zscore"]
                or mev_m >= EXTREME_THRESHOLDS["multiple"]
                or fee_m >= EXTREME_THRESHOLDS["multiple"]):
            row["volatility_tag"] = "extreme"
        elif (abs_price >= ELEVATED_THRESHOLDS["price_change_pct"]
                or mev_z >= ELEVATED_THRESHOLDS["zscore"]
                or pf_z >= ELEVATED_THRESHOLDS["zscore"]
                or mev_m >= ELEVATED_THRESHOLDS["multiple"]
                or fee_m >= ELEVATED_THRESHOLDS["multiple"]):
            row["volatility_tag"] = "elevated"
        else:
            row["volatility_tag"] = "normal"

    # Summary
    tags = [r["volatility_tag"] for r in merged]
    n_extreme = tags.count("extreme")
    n_elevated = tags.count("elevated")
    n_normal = tags.count("normal")
    print(f"  Volatility tags: {n_extreme} extreme, {n_elevated} elevated, {n_normal} normal (of {n} epochs)")

    return merged


def build():
    print("  Loading raw sources...")

    # Load all sources
    trillium = {int(r["epoch"]): r for r in load_csv(TRILLIUM_FILE) if r.get("epoch")}
    dune_ep = {int(r["epoch"]): r for r in load_csv(DUNE_EPOCHS_FILE) if r.get("epoch")}
    dune_val = {int(r["epoch"]): r for r in load_csv(DUNE_VALIDATORS_FILE) if r.get("epoch")}
    dune_stk = {int(r["epoch"]): r for r in load_csv(DUNE_ACTIVE_STAKE_FILE) if r.get("epoch")}
    jito = {int(r["epoch"]): r for r in load_csv(JITO_MEV_FILE) if r.get("epoch")}

    sc_rows = load_csv(SOLANA_COMPASS_FILE)
    sc = {int(r["epoch"]): r for r in sc_rows if r.get("epoch")}

    cg_rows = load_csv(COINGECKO_FILE)
    cg = {}
    for r in cg_rows:
        date_str = r.get("date", r.get("\ufeffdate", ""))
        price = safe_float(r.get("sol_price_usd"))
        fdv = safe_float(r.get("fdv_usd"))
        if date_str and price:
            cg[date_str] = {"price": price, "fdv": fdv}

    for name, d in [("Trillium", trillium), ("Dune epochs", dune_ep),
                    ("Dune validators", dune_val), ("Dune stake", dune_stk),
                    ("Jito", jito), ("Solana Compass", sc), ("CoinGecko", cg)]:
        if d:
            keys = sorted(d.keys())
            print(f"    {name}: {len(d)} entries ({keys[0]}-{keys[-1]})")
        else:
            print(f"    {name}: 0 entries")

    # Merge
    print("\n  Merging...")
    all_epochs = sorted(set(list(trillium) + list(dune_ep)))
    merged = []

    for ep in all_epochs:
        t = trillium.get(ep, {})
        d = dune_ep.get(ep, {})
        v = dune_val.get(ep, {})
        s = dune_stk.get(ep, {})
        j = jito.get(ep, {})
        compass = sc.get(ep, {})

        row = {}

        # ── Group 1: Epoch metadata ──
        row["epoch"] = ep

        # Date: Trillium min_block_time_calendar or Dune block_time
        date_str = (t.get("min_block_time_calendar") or "")[:10]
        if not date_str:
            date_str = (d.get("block_time") or "")[:10]
        row["date"] = date_str

        # Duration: Trillium elapsed_time_minutes → days, or Dune epoch_time (already days)
        elapsed_min = safe_float(t.get("elapsed_time_minutes"))
        if elapsed_min:
            row["duration_days"] = round(elapsed_min / 1440, 4)
        else:
            row["duration_days"] = safe_float(d.get("epoch_time"))

        # ── Group 2: Rewards (SOL) ──
        # Inflation: prefer Trillium, fallback Dune
        row["inflation_rewards_sol"] = safe_float(t.get("total_total_inflation_reward")) or safe_float(d.get("inflationary_reward"))

        # Total fees: for Trillium epochs, sum priority + base. For Dune-only, use fee_reward
        priority = safe_float(t.get("total_validator_priority_fees"))
        base = safe_float(t.get("total_validator_signature_fees"))
        dune_fee = safe_float(d.get("fee_reward"))

        if priority is not None:
            # Trillium epoch: we have the split
            row["total_fees_sol"] = (priority or 0) + (base or 0)
            row["priority_fees_sol"] = priority
            row["base_fees_sol"] = base
        else:
            # Dune-only: combined fees, can't split
            row["total_fees_sol"] = dune_fee
            row["priority_fees_sol"] = None  # Can't split
            row["base_fees_sol"] = None       # Can't split

        # MEV/Jito tips: prefer Trillium, fallback Dune
        row["mev_jito_tips_sol"] = safe_float(t.get("total_mev_earned")) or safe_float(d.get("mev_reward"))

        # ── Group 3: MEV breakdown (Trillium only) ──
        row["mev_to_validators_sol"] = safe_float(t.get("total_mev_to_validator"))
        row["mev_to_stakers_sol"] = safe_float(t.get("total_mev_to_stakers"))
        jito_be = safe_float(t.get("total_mev_to_jito_block_engine")) or 0
        jito_tr = safe_float(t.get("total_mev_to_jito_tip_router")) or 0
        row["mev_to_jito_sol"] = (jito_be + jito_tr) if (jito_be or jito_tr) else None

        # ── Group 4: Validator economics ──
        row["validator_commissions_sol"] = safe_float(v.get("voting_rewards_sol"))
        row["avg_commission_rate"] = safe_float(v.get("avg_commission_rate")) or safe_float(t.get("avg_commission_rate"))
        row["validator_count"] = safe_float(t.get("total_active_validators")) or safe_float(v.get("validator_count"))
        row["stake_accounts"] = safe_float(v.get("stake_account_count"))
        row["active_stake_sol"] = safe_float(t.get("total_active_stake")) or safe_float(s.get("active_stake_sol"))

        # ── Group 5: Price & market ──
        sol_price = safe_float(t.get("sol_price_usd"))
        fdv = None
        if not sol_price and date_str and date_str in cg:
            sol_price = cg[date_str]["price"]
            fdv = cg[date_str]["fdv"]
        if not sol_price:
            # Derive from Dune: total_reward_usd / total_reward
            dune_usd = safe_float(d.get("total_reward_usd"))
            dune_sol = safe_float(d.get("total_reward"))
            if dune_usd and dune_sol and dune_sol > 0:
                sol_price = round(dune_usd / dune_sol, 4)
        if fdv is None and date_str and date_str in cg:
            fdv = cg[date_str]["fdv"]
        row["sol_price_usd"] = sol_price
        row["fdv_usd"] = fdv

        # ── Group 6: CU & transactions (Trillium only) ──
        row["epochs_per_year"] = safe_float(t.get("epochs_per_year"))
        row["avg_cu_per_block"] = safe_float(t.get("avg_cu_per_block"))
        row["total_user_txns"] = safe_float(t.get("total_user_tx"))
        row["total_vote_txns"] = safe_float(t.get("total_vote_tx"))
        row["total_blocks"] = safe_float(t.get("total_blocks_produced"))

        # ── Group 7: Cross-check sources ──
        row["jito_official_mev_sol"] = safe_float(j.get("jito_total_mev_sol"))
        row["sc_priority_fees_sol"] = safe_float(compass.get("total_priority_fees_sol"))
        row["sc_jito_tips_sol"] = safe_float(compass.get("total_jito_tips_sol"))

        merged.append(row)

    # Compute volatility metrics
    print("\n  Computing volatility metrics...")
    merged = compute_volatility(merged)

    # Merge intra-day peaks (optional — only if extract_intraday.py has been run)
    if INTRADAY_FILE.exists():
        intraday = {int(r["epoch"]): r for r in load_csv(INTRADAY_FILE) if r.get("epoch")}
        print(f"\n  Merging intra-day peaks: {len(intraday)} epochs from {INTRADAY_FILE.name}")
        intraday_cols = [
            "peak_pf_per_block_sol", "peak_pf_multiple", "peak_pf_time",
            "peak_tx_per_block", "peak_tx_multiple", "peak_tx_time",
            "peak_hour_pf_sol", "peak_hour_pf_time",
            "baseline_pf_per_block_sol", "baseline_tx_per_block",
        ]
        matched = 0
        for row in merged:
            ep = row["epoch"]
            if ep in intraday:
                idr = intraday[ep]
                for col in intraday_cols:
                    val = safe_float(idr.get(col))
                    if val is not None:
                        row[col] = val
                    elif col in idr and idr[col]:
                        row[col] = idr[col]  # Keep string values (timestamps)
                # Skip rate from intraday (rename to avoid collision with epoch-level)
                row["intraday_skip_rate_pct"] = safe_float(idr.get("skip_rate_pct"))
                matched += 1
        print(f"    Matched {matched} epochs with intra-day data")
    else:
        print(f"\n  No intra-day data yet ({INTRADAY_FILE.name} not found)")
        print("    Run extract_intraday.py first, then re-run build_database.py")

    # Save
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_COLUMNS, delimiter=CSV_DELIMITER, extrasaction="ignore")
        writer.writeheader()
        for row in merged:
            clean = {k: ("" if v is None else v) for k, v in row.items()}
            writer.writerow(clean)

    print(f"\n  Saved: {OUTPUT_FILE}")
    print(f"  {len(merged)} rows x {len(RAW_COLUMNS)} columns (RAW only)")

    # Spot-check
    for check_ep in [150, 552, 800, 934]:
        r = next((r for r in merged if r["epoch"] == check_ep), None)
        if r:
            inf = r.get("inflation_rewards_sol") or 0
            fees = r.get("total_fees_sol") or 0
            mev = r.get("mev_jito_tips_sol") or 0
            price = r.get("sol_price_usd") or 0
            print(f"\n  Epoch {check_ep}: inflation={inf:.0f} fees={fees:.1f} mev={mev:.1f} price=${price:.2f}")


if __name__ == "__main__":
    print("\n=== Building Clean Solana Epoch Database (RAW only) ===")
    build()
    print("\nDone.")

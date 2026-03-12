# RAIKU Revenue Model — Data Lineage & Source-of-Truth Reference

**Last updated:** 2026-03-12
**Purpose:** Definitive map of every data file — who produces it, who consumes it, and which files are the authoritative sources for each data domain.
**Canonical program-fee batch window (current):** `[2026-02-04, 2026-03-05)` (end date exclusive)

---

## 1. Architecture Overview

```
                    PIPELINE A (run_pipeline.py)
  ┌──────────────────────────────────────────────────────────┐
  │                                                          │
  │  5 APIs ──► 01_extract/ ──► data/raw/ ──► 02_transform/ ──► data/processed/ ──► 03_model/ ──► scenario CSVs
  │             (10 scripts)    (14 active     (3 scripts)      (3 databases)       (3 scripts)   (3 outputs)
  │                              raw files)                                                        │
  └──────────────────────────────────────────────────────────┘

                    PIPELINE B (scripts/)
  ┌──────────────────────────────────────────────────────────┐
  │                                                          │
  │  Dune batch ──► download_dune_daily_C.py ──► _30d.csv ──► build_daily_temporal.py ──► inject ──► HTML
  │  (6 queries)    (merge C1-C6)                             (D.daily + D.dailyNet)                │
  └──────────────────────────────────────────────────────────┘

                    SIMULATOR (raiku_revenue_simulator.html)
  ┌──────────────────────────────────────────────────────────┐
  │  6 inline data objects — zero runtime fetches:           │
  │  D.a (scalars) · D.e (epochs) · D.p (programs)          │
  │  D.daily (category×day) · D.dailyNet (network×day)      │
  │  D_JITO (epoch Jito history)                             │
  └──────────────────────────────────────────────────────────┘
```

---

## 2. Complete File Lineage

### 2A. `data/raw/` — Raw Extraction Outputs

Every file here is produced by exactly one extraction script and is re-extractable from its source API. They should never be edited manually.

| # | File | Producer | Consumer(s) | Role |
|---|------|----------|-------------|------|
| 1 | `trillium_epoch_data.csv` | `extract_trillium.py` | `build_database.py`, `jit_revenue.py`, `aot_revenue.py`, `sanity_check.py` | **PRIMARY** epoch source (141 fields/epoch, 383 epochs 552+) |
| 2 | `dune_epoch_data_v2.csv` | `dune_epochs.py` | `build_database.py` | Epoch fallback for epochs 150-551 (where Trillium has no data) |
| 3 | `dune_commission_validators_v2.csv` | `dune_validators.py` | `build_database.py` | Validator commission rates + count per epoch |
| 4 | `dune_active_stake_v1.csv` | `dune_active_stake.py` | `build_database.py` | Active stake per epoch |
| 5 | `coingecko_sol_price.csv` | `coingecko_prices.py` | `build_database.py` | SOL price + FDV timeseries (365 days) |
| 6 | `jito_mev_rewards.csv` | `extract_jito_mev.py` | `build_database.py` | Jito Foundation MEV tips (541 epochs 390+, cross-check) |
| 7 | `solana_compass_epochs.csv` | `extract_solana_compass.py` | `build_database.py` | Per-validator fees/CU (128 epochs 800+, cross-check) |
| 8 | `trillium_intraday_peaks.csv` | `extract_intraday.py` | `build_database.py` | Intra-epoch volatility peak data |
| 9 | `dune_program_fees_v3.csv` | Dune query 6817783 (manual export/import) | `build_program_database.py` | **Canonical base dataset** for program DB merge (shared fee/CU columns), batch window `[2026-02-04, 2026-03-05)` |
| 10 | `dune_jito_tips_qc_20260204_20260305_excl.csv` | Dune query 6818065 (manual export/import) | `build_program_database.py` | **Canonical Jito dataset** for program DB merge (`jito_tx_count`, `jito_tips_sol`), same batch window |
| 11 | `dune_program_fees_aggregate.csv` | `extract_dune_programs.py` | Legacy only | Legacy aggregate source (superseded for current canonical batch) |
| 12 | `dune_fee_per_cu_by_program.csv` | `extract_dune_programs.py` | `aot_revenue.py` | Per-program fee/CU (7-day snapshot, 50 programs) |
| 13 | `dune_program_conditions.csv` | `extract_program_conditions.py` (or local cross-ref) | `build_program_conditions.py` | Program × market condition breakdown (148 rows) |
| 14 | `dune_program_fees_v2.csv` | Dune MCP query (manual extraction) | **No Python reader** — manually embedded as D.p in HTML | Source artifact for simulator D.p data. See note below. |
| 15 | `dune_daily_program_fees_30d.csv` | `download_dune_daily_C.py` (merged from C1-C6) | `build_daily_temporal.py`, conditions pipeline cross-ref | Merged 30-day daily per-program fees (Pipeline B input) |
| 16 | `lead_pipeline_sheet.xlsx` | External (SolWatch export) | **No Python reader** | Reference-only — 1897 programs, used for manual classification |

#### Intermediate files (produced AND consumed by the same pipeline):

| # | File | Producer | Consumer | Role |
|---|------|----------|----------|------|
| 15-20 | `dune_daily_C1_feb04_08.csv` through `dune_daily_C6_mar01_05.csv` (×6) | `download_dune_daily_C.py` | Same script → merged into `dune_daily_program_fees_30d.csv` | **Intermediate** — 6 batch extracts that produce #13 above. Could be deleted after merge, but kept for reproducibility. |

#### Note on `dune_program_fees_v2.csv`:
This file is the source data for the simulator's `D.p` object (228 program records). It was extracted via Dune MCP and manually transformed into inline JSON during simulator builds. No automated pipeline re-generates D.p — it requires manual re-extraction and re-embedding via the scripts/ workflow or direct HTML editing. **Keep this file** as the only record of what data is in D.p.

### 2B. `data/mapping/` — Reference Classifications

| File | Maintained by | Consumers | Role |
|------|--------------|-----------|------|
| `program_categories.csv` | Manual curation (228 programs classified) | `build_program_conditions.py`, `build_program_database.py`, `build_daily_temporal.py` | **SACRED REFERENCE** — maps program_id → name, raiku_category, raiku_product. Never auto-generated. |

### 2C. `data/processed/` — Consolidated Databases & Model Outputs

These are the files produced by the transform and model layers. They are **regenerable** from raw inputs by running the pipeline.

| # | File | Producer | Consumer(s) | Classification |
|---|------|----------|-------------|----------------|
| 1 | `solana_epoch_database.csv` | `build_database.py` | `jit_revenue.py`, `aot_revenue.py`, `sanity_check.py` | **SOURCE-OF-TRUTH: Epoch data** (786 rows × 43 cols) |
| 2 | `program_conditions.csv` | `build_program_conditions.py` | `build_program_database.py` | **SOURCE-OF-TRUTH: Conditions** (55 programs × 27 cols) |
| 3 | `program_database.csv` | `build_program_database.py` | Reference only (not read by models) | **SOURCE-OF-TRUTH: Programs** (466 programs × 37 cols, includes Jito-inclusive derived metrics) |
| 4 | `daily_temporal_payload.js` | `build_daily_temporal.py` | `inject_daily_data.py` → HTML | **Intermediate** — JS snippet injected into simulator |
| 5 | `daily_category_aggregates.csv` | `build_daily_temporal.py` | **No reader** — debug copy of D.daily | **Debug artifact** — CSV mirror of what becomes D.daily. Useful for inspection but not consumed by any script. |
| 6 | `jit_revenue_scenarios.csv` | `jit_revenue.py` | `sanity_check.py`, `sheets_export.py` | **MODEL OUTPUT** — JIT revenue scenarios |
| 7 | `aot_revenue_scenarios.csv` | `aot_revenue.py` | `sanity_check.py`, `sheets_export.py` | **MODEL OUTPUT** — AOT revenue scenarios |
| 8 | `sanity_check_report.csv` | `sanity_check.py` | **Terminal** — no further consumer | **MODEL OUTPUT** — validation report |

---

## 3. Build Chain (how databases are assembled)

### Chain A: Epoch Database

```
trillium_epoch_data.csv ─────────┐
dune_epoch_data_v2.csv ──────────┤
dune_commission_validators_v2.csv┤
dune_active_stake_v1.csv ────────┤──► build_database.py ──► solana_epoch_database.csv
coingecko_sol_price.csv ─────────┤                          (786 rows × 43 cols)
jito_mev_rewards.csv ────────────┤                          AUTHORITATIVE for epoch analysis
solana_compass_epochs.csv ───────┤
trillium_intraday_peaks.csv ─────┘
```

**What `build_database.py` does:** Merges 8 raw CSVs by epoch key. Trillium is primary (epochs 552+). Dune fills epochs 150-551. CoinGecko maps SOL price by date. Jito/SC are cross-checks (not added as new columns, used for validation). Outputs one clean row per epoch with 43 columns.

### Chain B: Program Conditions

```
dune_program_conditions.csv ─────┐
program_categories.csv ──────────┤──► build_program_conditions.py ──► program_conditions.csv
                                                                       (55 programs × 27 cols)
                                                                       AUTHORITATIVE for conditions
```

**What `build_program_conditions.py` does:** Pivots raw condition data (one row per program × condition) into wide format (one row per program, with columns suffixed `_normal`, `_elevated`, `_extreme`). Computes fee multipliers and congestion sensitivity classification.

### Chain C: Program Database

```
dune_program_fees_v3.csv ───────────────────────────────┐
dune_jito_tips_qc_20260204_20260305_excl.csv ──────────┤──► build_program_database.py ──► program_database.csv
program_categories.csv ─────────────────────────────────┤                                   (466 programs × 37 cols)
program_conditions.csv ─────────────────────────────────┘                                   AUTHORITATIVE for programs
```

**What `build_program_database.py` does:** Merges canonical Query 6817783 and Query 6818065 program-level exports by `program_id` for the same fixed batch window (`[2026-02-04, 2026-03-05)`). Query 6817783 is canonical for shared fee/CU columns; Query 6818065 contributes Jito-specific fields (`jito_tx_count`, `jito_tips_sol`), then derived Jito-inclusive metrics are computed (for example: `total_fees_incl_jito`, `non_base_fees_incl_jito`, `total_fees_per_cu_incl_jito`, `avg_jito_fee_per_tx`).

**Per-CU metric semantics (important):**
- `avg_priority_fee_per_cu_lamports` is a transaction-level average metric provided by Query 6817783 output logic.
- `non_base_fees_per_cu_incl_jito` and `total_fees_per_cu_incl_jito` are aggregate period ratios computed from totals (`sum fees / sum total_cu`) and expressed in **lamports/CU**.
- These definitions are analytically different, so they are **not expected to be numerically equal**.

### Chain D: Simulator Temporal Data

```
dune_daily_C1..C6 ──► download_dune_daily_C.py ──► dune_daily_program_fees_30d.csv
                                                              │
program_categories.csv ───────────────────────────────────────┤
                                                              ↓
                                                    build_daily_temporal.py
                                                              │
                                            ┌─────────────────┼─────────────────┐
                                            ↓                 ↓                 ↓
                                   daily_temporal_    daily_category_    (terminal)
                                   payload.js         aggregates.csv
                                        │              (debug only)
                                        ↓
                                inject_daily_data.py
                                        │
                                        ↓
                           raiku_revenue_simulator.html
                           (D.daily + D.dailyNet injected)
```

### Chain E: Revenue Models

```
solana_epoch_database.csv ──┐
dune_fee_per_cu_by_program ─┤──► jit_revenue.py ──► jit_revenue_scenarios.csv
                            ├──► aot_revenue.py ──► aot_revenue_scenarios.csv
                            │
jit_revenue_scenarios.csv ──┤
aot_revenue_scenarios.csv ──┤──► sanity_check.py ──► sanity_check_report.csv
                            │
trillium_epoch_data.csv ────┘
```

---

## 4. Source-of-Truth Reference

### If you need to recalculate or analyze yourself, use these files:

| Data Domain | Source-of-Truth File | Location | Format | What it contains |
|-------------|---------------------|----------|--------|-----------------|
| **Epoch-level Solana metrics** | `solana_epoch_database.csv` | `data/processed/` | 786 rows × 43 cols, `;`-delimited | One row per epoch (150-935). Key columns: `mev_jito_tips_sol`, `priority_fees_sol`, `validator_count`, `active_stake_sol`, `sol_price_usd`, `avg_cu_per_block` |
| **Per-program economics** | `program_database.csv` | `data/processed/` | 466 rows × 37 cols, `;`-delimited | One row per program. Canonical fee/CU columns + Jito merge + Jito-inclusive derived metrics for batch window `[2026-02-04, 2026-03-05)` |
| **Program × market condition** | `program_conditions.csv` | `data/processed/` | 55 rows × 27 cols, `;`-delimited | Fee/CU under normal/elevated/extreme conditions, fee multipliers |
| **Daily per-program (temporal)** | `dune_daily_program_fees_30d.csv` | `data/raw/` | ~1400 rows, `;`-delimited | Day × program granularity. 30-day window (Feb 4 – Mar 5, 2026) |
| **Program classification** | `program_categories.csv` | `data/mapping/` | 228 rows, `;`-delimited | program_id → name, raiku_category, raiku_product |
| **JIT revenue scenarios** | `jit_revenue_scenarios.csv` | `data/processed/` | ~40 rows, `;`-delimited | Market share × take rate × MEV base scenarios |
| **AOT revenue scenarios** | `aot_revenue_scenarios.csv` | `data/processed/` | ~300 rows, `;`-delimited | Top-down + bottom-up × 6 archetypes |
| **Validation report** | `sanity_check_report.csv` | `data/processed/` | Cross-check of model outputs vs on-chain data |
| **Simulator (all data)** | `raiku_revenue_simulator.html` | Root | Self-contained HTML, 6 inline data objects | D.a, D.e, D.p, D.daily, D.dailyNet, D_JITO |

### Rebuild commands:

```bash
# Rebuild epoch database from raw sources
python run_pipeline.py                    # Extract + Transform + Model (core sources)
python run_pipeline.py --full-extract     # Extract ALL sources + Transform + Model

# Rebuild models only (if epoch database already exists)
python run_pipeline.py --model-only

# Rebuild simulator temporal data (after Dune batch downloads)
cd scripts && python build_daily_temporal.py && python inject_daily_data.py
```

---

## 5. File Classification Summary

### `data/raw/` — 20 files (14 active + 6 intermediate)

| Status | Count | Files |
|--------|-------|-------|
| ✅ Active raw inputs | 11 | trillium_epoch_data, dune_epoch_data_v2, dune_commission_validators_v2, dune_active_stake_v1, coingecko_sol_price, jito_mev_rewards, solana_compass_epochs, trillium_intraday_peaks, dune_program_fees_aggregate, dune_fee_per_cu_by_program, dune_program_conditions |
| ✅ Pipeline B merged output | 1 | dune_daily_program_fees_30d |
| ✅ Source artifact (D.p) | 1 | dune_program_fees_v2 |
| ✅ Reference data | 1 | lead_pipeline_sheet.xlsx |
| 🔄 Intermediate (C1-C6) | 6 | dune_daily_C1..C6 (merge inputs, kept for reproducibility) |
| ❌ Orphans | 0 | All archived (4 files moved to archive/) |

### `data/processed/` — 8 files

| Status | Count | Files |
|--------|-------|-------|
| ⭐ Source-of-truth databases | 3 | solana_epoch_database, program_database, program_conditions |
| 📊 Model outputs | 3 | jit_revenue_scenarios, aot_revenue_scenarios, sanity_check_report |
| 🔄 Intermediate | 1 | daily_temporal_payload.js (injected into HTML) |
| 🐛 Debug only | 1 | daily_category_aggregates.csv (CSV mirror of D.daily, no consumer) |

### `data/mapping/` — 1 file

| Status | Files |
|--------|-------|
| ⭐ Sacred reference | program_categories.csv (never auto-generated, manually curated) |

---

## 6. Why Multiple Raw Files Are Necessary

The raw file proliferation is technically justified:

**8 files feed the epoch database** because each API covers different epoch ranges and different metrics. `build_database.py` merges them with Trillium as primary (epochs 552+) and Dune as fallback (epochs 150-551). No single API provides all 43 columns.

**3 files feed the program database** because:
- `dune_program_fees_aggregate.csv` = 30-day aggregate (primary, 500 programs)
- `dune_fee_per_cu_by_program.csv` = 7-day snapshot (fallback for fee distribution percentiles)
- `program_conditions.csv` = congestion sensitivity enrichment (processed, not raw)

**6 C1-C6 batch files** exist because Dune's 1000-row limit required splitting 30 days of daily per-program data into 6 × 5-day batches. They merge into `dune_daily_program_fees_30d.csv`. They could technically be deleted after merge, but are kept so the merge can be re-run without re-downloading from Dune.

**`dune_program_fees_v2.csv`** exists because D.p (simulator program data) was built from a separate Dune extraction that is not part of the automated pipeline. This file is the only record of what data is embedded in the simulator.

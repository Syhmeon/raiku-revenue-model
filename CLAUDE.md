# RAIKU Revenue Estimation — Project Guide

## Project Overview

Build a **data pipeline + revenue model** for RAIKU, a Solana blockspace marketplace protocol.

**RAIKU** sells guaranteed blockspace via two auction types:
- **AOT (Ahead-of-Time)**: Sealed-bid auctions for reserved compute windows. Improves inclusion probability from 0.40 to 0.89.
- **JIT (Just-in-Time)**: Real-time tip-based ordering (similar to Jito bundles).

**Revenue split**: 95% validators / 5% protocol (governance range 1-5%).

**Goal**: Extract real Solana on-chain data, build a revenue model with multiple scenarios, output Excel with live formulas.

---

## Key Principle

**Be objective and scientific.** Do NOT be opinionated about whether numbers are "low" or "high". Extract real data, replace placeholders, present facts. If revenue is low, it's low — don't invent numbers.

---

## Project Structure

```
raiku-revenue-model/
├── CLAUDE.md              ← You are here
├── config.py              ← API keys, business parameters, scenarios
├── run_pipeline.py        ← Main entry point
├── 01_extract/            ← Data extraction scripts
│   ├── dune_client.py     ← Dune API wrapper
│   ├── dune_epochs.py     ← Epoch economics (query 6773409)
│   ├── dune_validators.py ← Commission/validators (query 6773227)
│   ├── dune_active_stake.py
│   ├── coingecko_prices.py
│   └── [TO CREATE] extract_trillium.py ← PRIMARY source
├── 02_transform/          ← Merge & clean
│   └── [TO CREATE] build_database.py
├── 03_model/              ← Revenue models
│   └── [TO CREATE] aot_revenue.py, jit_revenue.py
├── 04_output/             ← Excel/dashboard generation
├── data/
│   ├── raw/               ← Never edit manually. Re-extractable.
│   │   ├── dune_epoch_data_v2.csv        (785 rows, epochs 150-935)
│   │   ├── dune_commission_validators_v2.csv (785 rows)
│   │   ├── coingecko_sol_price.csv       (365 days)
│   │   ├── dune_fee_per_cu_by_program.csv (50 programs, 7-day)
│   │   ├── dune_daily_priority_fees.csv  (91 days)
│   │   └── dune_active_stake_v1.csv
│   └── processed/         ← Generated from raw
└── docs/                  ← 7 RAIKU internal documents (read-only reference)
```

All CSVs use **semicolon delimiter** (`;`) and UTF-8 encoding. See `config.py` for settings.

---

## Data Sources (3 Active)

### 1. Trillium API — PRIMARY (free, no auth)

Base URL: `https://api.trillium.so/`

| Endpoint | Description | Coverage |
|----------|-------------|----------|
| `/epoch_data/{epoch}` | 141 fields: MEV, fees, inflation, APY, validators, CU, price | Epoch 553+ (~Dec 2023) |
| `/validator_rewards/{epoch}` | 200+ fields per validator: MEV, priority fees, commission, APY, skip rate | Epoch 553+ |
| `/epoch_timeseries/{epoch}` | 15-min bucketed intra-epoch data per validator | Epoch 553+ |

**Key fields for RAIKU** (from `/epoch_data/{epoch}`):
- MEV breakdown: `total_mev_earned`, `mev_to_validator`, `mev_to_stakers`, `mev_to_jito_block_engine`, `mev_to_jito_tip_router`
- Fees: `total_validator_priority_fees`, `total_validator_signature_fees`, `total_block_rewards`
- Inflation: `total_total_inflation_reward`, `inflation_rate`, `epochs_per_year`
- APY: all components (inflation, block rewards, MEV) x (delegator, validator, total, compound)
- Network: `total_active_validators`, `total_active_stake`, `sol_price_usd`
- CU: `avg_cu_per_block`, `total_cu`, `avg_cu_per_user_tx`, `avg_priority_fee_per_10m_cu`, `avg_mev_per_10m_cu`

**Extraction pattern** (for `extract_trillium.py`):
```python
import requests
response = requests.get(f"https://api.trillium.so/epoch_data/{epoch}")
data = response.json()  # Returns flat dict with 141 fields
```

No authentication, no rate limiting observed. Extract epochs 553 to current sequentially.

### 2. Dune Analytics — SECONDARY

API Key in `config.py`: `DUNE_API_KEY`

| Query ID | Description | Status |
|----------|-------------|--------|
| 6773409 | Epoch economics (150-935) | Extracted → `dune_epoch_data_v2.csv` |
| 6773227 | Commission/validators | Extracted → `dune_commission_validators_v2.csv` |
| 6776267 | Active stake per epoch | Extracted → `dune_active_stake_v1.csv` |
| 6777333 | Fee/CU by program (7-day) | Extracted → `dune_fee_per_cu_by_program.csv` |
| 6777334 | Daily priority fees (91-day) | Extracted → `dune_daily_priority_fees.csv` |
| 6777420 | Daily Jito tips | Created, not yet executed (DEFERRED) |

**Dune is the ONLY source for**: per-program fee/CU data, daily granularity.

### 3. CoinGecko — COMPLEMENT

Free API (365-day limit). Used for FDV and SOL price backup.
Partially replaced by Trillium `sol_price_usd` for epoch-level data.

---

## Source Priority Matrix

| Data Need | Primary | Secondary |
|-----------|---------|-----------|
| Epoch economics (MEV, fees, rewards) | **Trillium** | Dune 6773409 |
| MEV/Jito tips breakdown | **Trillium** | Dune 6777420 |
| Priority fees (epoch-level) | **Trillium** | Dune 6777334 |
| Priority fees (daily) | **Dune** 6777334 | — |
| Fee/CU by program | **Dune** 6777333 | — |
| SOL price (epoch-level) | **Trillium** | CoinGecko |
| Validator commissions | **Trillium** | Dune 6773227 |
| APY by component | **Trillium** | — |
| FDV / market cap | **CoinGecko** | — |

---

## Existing Solana Database (to enrich)

Located at: `C:\Users\Utilisateur\OneDrive\RAIKU\08 - Revenue - Economics\Raiku - Revenues estimations\`
Build script: `build_final_data.py`

**Current structure**: ONE Excel sheet, epochs 150-935 (~785 rows)
- **RAW columns (A-M)**: epoch, block_time, epoch_time, inflationary_reward, fee_reward, mev_reward, voting_rewards_sol, avg_commission_rate, validator_count, stake_account_count, issue_apy, sol_price_usd, fdv_usd
- **FORMULA columns (N-AD)**: staker_rewards, total_reward, total_reward_usd, fee_percent, commission_rate, fee_apy, mev_apy, total_apy, total_stake_sol, total_supply, staked_ratio, total_burn, net_inflation, inflation_rate, issuance_sol, issuance_usd, fee_burn_pct

**Known gaps Trillium will fill**: MEV breakdown (not just total), priority fee split, APY components, active_stake (real), validator-level data.

---

## Execution Plan

### Phase 1: Enrich Solana Database
1. `extract_trillium.py` — Extract `/epoch_data/{epoch}` for epochs 553-current → `data/raw/trillium_epoch_data.csv`
   - Must support `--full` (re-extract all) and incremental (only new epochs) modes
   - Select ~30 most relevant fields from the 141 available (see Key Fields above)
   - Rate-limit: 0.2s between requests (be polite)
2. Merge Trillium + existing Dune data on `epoch` key
3. Cross-check overlapping fields (MEV, fees, inflation, validator count)
4. Rebuild Excel with enriched columns (Trillium RAW + Dune RAW + FORMULAS)

### Phase 2: Extract RAIKU-Specific Data
5. Jito/MEV breakdown per epoch (already in Trillium epoch_data)
6. Fee/CU by program — extend to 30 days if needed (Dune)
7. Protocol revenues via Token Terminal if needed later
8. Validator economics from Trillium `/validator_rewards/{epoch}` (for profitability model)

### Phase 3: Build Revenue Model
9. **JIT model**: TAM (Jito tips total from Trillium `total_mev_earned`) x market_share x protocol_fee
10. **AOT model top-down**: Priority fees TAM x addressable_pct x RAIKU_capture_rate
11. **AOT model bottom-up (3D)**: Stake% x Slots/yr x CU_reserved% x Fee/CU x SOL_price, by 6 archetypes
12. **Scenario matrix**: TAM x share x fee x stake combinations (see `config.py` SCENARIOS)
13. **Sanity check**: top-down ~ bottom-up (order of magnitude)

### Phase 4: Output
14. Excel with live formulas (raw data + `=` formulas, NOT hardcoded computed values)
15. Revenue split waterfall (total → 95% validators / 5% protocol → rebates → remainder)
16. Sensitivity analysis
17. Validator profitability view

---

## Revenue Model Architecture

### JIT Revenue
```
JIT_Revenue = Total_Jito_Tips × RAIKU_Market_Share × Protocol_Fee
```
- Total_Jito_Tips: from Trillium `total_mev_earned` per epoch, annualized
- Market_Share: 5% / 10% / 15% (scenarios)
- Protocol_Fee: 5% default (3.5% for high performers)

### AOT Revenue — Top-Down
```
AOT_Revenue = Priority_Fees_TAM × Addressable_Pct × RAIKU_Capture
```
- Priority_Fees_TAM: from Trillium `total_validator_priority_fees`, annualized
- Addressable_Pct: % of fees from latency-sensitive programs (from Dune fee/CU data)
- RAIKU_Capture: market share assumption

### AOT Revenue — Bottom-Up (3D Framework)
```
AOT_Revenue = Stake% × Slots_per_year × CU_reserved% × Fee_per_CU × SOL_price
```
Per archetype (6 types):
1. PropAMMs (oracle/quote updates) — e.g. BisonFi: 24.28 L/CU median
2. Quant Trading Desks — parallel channel to Jito
3. Market Makers (operational) — critical margin/collateral txs
4. DEX-DEX Arbitrage — async scheduler
5. Protocol Crankers/Keepers — Drift funding, Jupiter DCA, Kamino rebalance
6. CEX-DEX Arbitrage — highest value, deepest slot pools

### Revenue Split (apply LAST)
```
Total Revenue (100%)
├── Validators: 95%
└── RAIKU Protocol: 5%
    ├── Customer rebates: ~0.5%
    ├── Validator enhancement: ~1.5%
    └── Remainder: ~3% → operations
```

---

## Business Parameters (in config.py)

| Parameter | Value | Source |
|-----------|-------|--------|
| Protocol take rate | 5% (default) | Post-TGE Design doc |
| High-perf take rate | 3.5% | Post-TGE Design doc |
| Validator share | 95% | Post-TGE Design doc |
| P(Include) Standard | 0.40 | Exec Problems doc |
| P(Include) AOT | 0.89 | Mainnet doc |
| P(Include) TPU | 0.394 | Opp Cost doc |
| P(Include) Jito JIT | 0.637 | Opp Cost doc |
| P(Include) Raiku AOT | 0.893 | Opp Cost doc |
| Jito 2025 total tips | $720M | Post-TGE Design doc |
| Jito Q4-25 annualized | $100M | Post-TGE Design doc |
| RAIKU token supply | 1B fixed | Post-TGE Design doc |
| Target FDV at TGE | $200-400M | Post-TGE Design doc |
| TGE target | Q4 2026 | Post-TGE Design doc |

---

## Real Data Already Extracted

### Fee/CU by Program (7-day, from `dune_fee_per_cu_by_program.csv`)
- BisonFi (PropAMM): median 24.28 L/CU — PropAMMs pay 100-400x more than regular DeFi
- Raydium V4: median 0.18 L/CU
- Jupiter V6: median 0.054 L/CU
- Pump.fun: median 0.14 L/CU
- Pyth Oracle: median 0.054 L/CU

### Daily Priority Fees (91-day, from `dune_daily_priority_fees.csv`)
- Feb 2026 avg: ~7,000 SOL/day (~$980K/day)
- Jan 2026 avg: ~6,000 SOL/day
- Dec 2025 avg: ~3,200 SOL/day
- Priority fees = ~88-92% of total fees
- Annualized Feb rate: ~$357M/yr

### Trillium Sample (Epoch 934, confirmed working)
- total_active_stake: 422M SOL
- total_active_validators: 761
- total_mev_earned: 3,418 SOL
- total_validator_priority_fees: 12,776 SOL
- sol_price_usd: $84.94
- avg_compound_overall_apy: 6.86%

---

## Inclusion Probability Model

```
P(Inclusion) = P(Delivery) x P(Scheduling) x P(Execution) x P(Finality)
Standard:  0.70 x 0.60 x 0.95 x 0.99 = 0.40
AOT:       improved Delivery + Scheduling → composite 0.89
```

Opportunity cost equation:
```
E = p_include x F + (1 - p_target) x L
Savings = E_regular - E_AOT
```

---

## RAIKU Internal Documents (in docs/)

7 documents available for reference:
1. `post_tge_design.txt` — Token design, revenue split, governance
2. `raiku_mainnet.txt` — AOT/JIT mechanics, architecture
3. `raiku_aot_opportunity_cost.txt` — Economic model per customer type
4. `raiku_usecases.txt` — 6 customer archetypes
5. `raiku_exec_problems.txt` — P(Inclusion) model, competitive landscape
6. `raiku_revenue_v2.txt` — Revenue brainstorming, 3D AOT framework
7. `raiku_opp_cost.txt` — Per-use-case opportunity cost formulas

---

## Pipeline Design Principles

1. **From-scratch capable**: Pipeline must be able to re-extract ALL data if run with `--full`
2. **Incremental by default**: Only extract new epochs/dates not already in CSVs
3. **Raw data is sacred**: Never manually edit files in `data/raw/`
4. **Excel formulas, not hardcoded**: Computed values = Excel `=` formulas, NOT Python-calculated values
5. **Semicolon CSVs**: European locale compatibility (`;` delimiter)
6. **Reproducible**: Any developer can clone this repo, run the pipeline, get the same output

---

## Quick Start

To begin Phase 1 Step 1:
```bash
cd "C:/Dev/RAIKU/Revenue Estimation/raiku-revenue-model"
python 01_extract/extract_trillium.py --full
```

This should:
1. Fetch `/epoch_data/{epoch}` for epochs 553 to current
2. Select relevant fields (see list above)
3. Save to `data/raw/trillium_epoch_data.csv` (semicolon-delimited)
4. Print progress and summary stats

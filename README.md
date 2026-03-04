# RAIKU Revenue Estimation Model

## Objective

Estimate RAIKU's future protocol revenues from two auction types:
- **AOT (Ahead-of-Time)** — sealed-bid blockspace reservations
- **JIT (Just-in-Time)** — real-time tip-based transaction ordering

Revenue formula: `Protocol Revenue = Total Auction Revenue × Take Rate (1-5%)`

## Architecture

```
Python = computation engine │ Google Sheets = presentation layer

01_EXTRACT                  02_TRANSFORM              03_MODEL                04_OUTPUT
──────────────────          ──────────────            ──────────────          ──────────────

Trillium API ─────┐         Merge on epoch key        JIT Revenue Model      Google Sheets
  (PRIMARY)       │         Cross-check sources        ──────────────        (formatted,
  141 fields/     │         Build processed CSV         Jito TAM × share     shareable,
  epoch           │                                     × protocol fee       scenarios)
                  │
Dune Analytics ───┤         data/processed/            AOT Revenue Model
  (SECONDARY)     │         └── solana_epoch_db.csv    ──────────────
  Fee/CU by       │                                     Top-down + Bottom-up
  program,        │                                     6 customer archetypes
  daily fees      │                                     Scenario matrix
                  │
CoinGecko ────────┘
  (COMPLEMENT)
  FDV, SOL price
```

## Data Sources

| Source | Role | Data | Status |
|--------|------|------|--------|
| **Trillium API** | PRIMARY | Epoch economics, MEV breakdown, APY, validators, CU stats | 🔲 Phase 1 |
| **Dune** (6773409) | Secondary | Epoch rewards, fees, MEV (epochs 150-935) | ✅ Extracted |
| **Dune** (6773227) | Secondary | Validator commissions, count | ✅ Extracted |
| **Dune** (6776267) | Secondary | Active stake per epoch | ✅ Extracted |
| **Dune** (6777333) | Unique | Fee/CU by program (7-day, 50 programs) | ✅ Extracted |
| **Dune** (6777334) | Unique | Daily priority fees (91 days) | ✅ Extracted |
| **CoinGecko** | Complement | SOL price, FDV (365 days) | ✅ Extracted |

### Why Trillium is PRIMARY

Trillium provides **141 fields per epoch** for free, no auth — including MEV breakdown (validator/staker/Jito block engine/tip router split), all APY components, CU stats, priority fees, and validator-level data. It replaces multiple Dune queries and fills gaps (MEV split, active_stake, APY for recent epochs).

**Dune remains essential** for per-program fee/CU data and daily granularity — data Trillium doesn't provide.

### Reserved for later (if needed)

| Source | Use case | Why deferred |
|--------|----------|--------------|
| **Token Terminal for Sheets** | Protocol revenues (Jito DAO fees, Jupiter fees, etc.) | Not needed for core revenue model. Useful later for competitive benchmarking. |
| **BigQuery × Token Terminal** | Transaction-level deep dive (decoded Solana instructions) | Heavy setup (GCP). Only needed for granular customer archetype sizing. |

## Project Structure

```
raiku-revenue-model/
├── CLAUDE.md              ← Complete project guide (for Claude Code)
├── README.md              ← This file (for humans)
├── config.py              ← API keys (.env), business parameters, scenarios
├── run_pipeline.py        ← Master entry point
│
├── 01_extract/            ← Data extraction (API calls)
│   ├── extract_trillium.py    ← PRIMARY (epochs 553+, ~380 epochs) [TO CREATE]
│   ├── dune_client.py         ← Dune API wrapper
│   ├── dune_epochs.py         ← Epoch economics (query 6773409)
│   ├── dune_validators.py     ← Commission/validators (query 6773227)
│   ├── dune_active_stake.py   ← Active stake (query 6776267)
│   └── coingecko_prices.py    ← SOL price + FDV
│
├── 02_transform/          ← Merge & compute
│   └── build_database.py      ← Merge all sources → processed CSV
│
├── 03_model/              ← Revenue estimation
│   ├── aot_revenue.py         ← AOT: top-down + bottom-up (6 archetypes)
│   └── jit_revenue.py         ← JIT: Jito TAM × share × fee
│
├── 04_output/             ← Google Sheets export (presentation layer)
│
├── data/
│   ├── raw/               ← Extracted CSVs (never edit, re-extractable)
│   └── processed/         ← Computed by Python (gitignored)
│
└── docs/                  ← 7 RAIKU internal documents (read-only)
```

## Setup

```bash
# Python 3.10+
pip install requests pandas python-dotenv

# Create .env with your API keys
echo "DUNE_API_KEY=your_key_here" > .env

# Run extraction
python 01_extract/extract_trillium.py --full
```

## Execution Plan

1. **Phase 1** — Enrich Solana database: Extract Trillium → merge with Dune → processed CSV
2. **Phase 2** — RAIKU-specific data: MEV breakdown, fee/CU extension, validator economics
3. **Phase 3** — Revenue model: JIT + AOT (top-down + bottom-up) + scenario matrix
4. **Phase 4** — Output: Google Sheets (presentation, scenarios, sensitivity analysis)

## Key Business Parameters

| Parameter | Value | Source |
|-----------|-------|--------|
| Protocol take rate | 5% (range 1-5%) | Post-TGE Design |
| Validator share | 95% (96.5% high perf) | Post-TGE Design |
| P(Inclusion) AOT vs Standard | 0.89 vs 0.40 | Mainnet doc |
| Jito 2025 total tips | ~$720M | Post-TGE Design |
| Conservative MEV base | $100M/year (Q4-25 ann.) | Post-TGE Design |
| TGE target | Q4 2026 | Post-TGE Design |

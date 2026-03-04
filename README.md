# RAIKU Revenue Estimation Model

## Objective
Estimate RAIKU's future protocol revenues from two sources:
- **AOT (Ahead-of-Time) Auctions** — sealed-bid blockspace reservations
- **JIT (Just-in-Time) Auctions** — real-time tip-based transaction ordering

Revenue formula: `Protocol Revenue = Total Auction Revenue × Take Rate (1-5%)`

## Pipeline Architecture

```
01_EXTRACT                    02_TRANSFORM                03_MODEL                 04_OUTPUT
─────────────────────        ─────────────────────       ─────────────────────    ─────────────

Dune Analytics ──────┐
  - Epoch economics  │       Merge & validate            AOT Revenue Model        Google Sheets
  - Validators       │       ─────────────────           ─────────────────        (live formulas)
  - Active stake     ├──→    Clean nulls                 Per-customer-type
                     │       Align epochs                pricing (6 archetypes)   Looker Studio
BigQuery / TT ───────┤       Compute derived             × market share           (dashboard)
  - Jito tips/MEV    │       columns                     × volume estimates
  - Pump.fun txs     │                                                            Excel model
  - Jupiter volume   │                                   JIT Revenue Model        (revenue
  - Raydium/Orca     │                                   ─────────────────        scenarios)
                     │                                   Jito-comparable tips
CoinGecko ───────────┤                                   × RAIKU validator
  - SOL price        │                                     share
  - FDV, supply      │                                   × take rate
                     │
Jito API ────────────┘
  - Bundle data
  - MEV distribution
```

## Data Sources

| Source | Data | Status | Script |
|--------|------|--------|--------|
| **Dune** (query 6773409) | Epoch economics (rewards, fees, MEV) | ✅ Done | `01_extract/dune_epochs.py` |
| **Dune** (query 6773227) | Validator commissions, count | ✅ Done | `01_extract/dune_validators.py` |
| **Dune** (query 6776267) | Active stake per epoch | ✅ Done | `01_extract/dune_active_stake.py` |
| **CoinGecko** | SOL price, FDV (365 days) | ✅ Done | `01_extract/coingecko_prices.py` |
| **BigQuery / Token Terminal** | Jito decoded (tips, bundles) | 🔲 TODO | `01_extract/bigquery_jito.py` |
| **BigQuery / Token Terminal** | Pump.fun, Jupiter, Raydium, Orca, Marinade | 🔲 TODO | `01_extract/bigquery_protocols.py` |
| **Jito Explorer** | Bundle metrics, tip distribution | 🔲 TODO | `01_extract/jito_api.py` |
| **Dune** (new query) | Fee breakdown (base vs priority) | 🔲 Pending | `01_extract/dune_fee_breakdown.py` |

## Revenue Model Structure

### AOT Revenue (Primary)
```
For each customer archetype:
  AOT_Revenue = N_customers × Avg_slots_reserved/day × Avg_bid/slot × 365

Customer Archetypes (from internal docs):
  1. PropAMMs (BisonFi, HumidiFi, Tessera, SolFi) → oracle updates
  2. Quant/HFT Desks → execution certainty, position sizing
  3. Market Makers → operational tx insurance (CRITICAL tier)
  4. DEX-DEX Arb → signal → reserved slot execution
  5. Protocol Keepers → cadence-aware (Drift, Jupiter DCA, Kamino, Marinade)
  6. CEX-DEX Arb → highest value, asymmetric risk elimination
```

### JIT Revenue (Secondary)
```
JIT_Revenue = Jito_Total_Tips × (RAIKU_Validator_Share / Total_Validators) × RAIKU_Take_Rate

Reference: Jito 2025 = ~$720M total tips, $7-8M DAO revenue
Conservative base: $100M annual MEV market (Q4-25 annualized)
```

### Protocol Revenue Split
```
Total Revenue (100%)
├── Validators: 95% (or 96.5% for high performers)
└── RAIKU Protocol: 5% (governance range 1-5%)
    ├── Customer rebates: ~0.5%
    ├── Validator enhancement: ~1.5%
    └── Operations/growth: ~3%
```

## File Structure
```
raiku-revenue-model/
├── README.md               ← You are here
├── config.py               ← API keys, paths, constants
├── run_pipeline.py         ← Master script: extract → transform → model
│
├── 01_extract/             ← Data extraction (API calls)
│   ├── dune_epochs.py
│   ├── dune_validators.py
│   ├── dune_active_stake.py
│   ├── coingecko_prices.py
│   ├── bigquery_jito.py        (TODO)
│   ├── bigquery_protocols.py   (TODO)
│   └── jito_api.py             (TODO)
│
├── 02_transform/           ← Cleaning, merging, validation
│   └── build_database.py       Merge all sources → single dataset
│
├── 03_model/               ← Revenue estimation
│   ├── aot_revenue.py          AOT model (per-customer-type)
│   └── jit_revenue.py          JIT model (Jito-comparable)
│
├── 04_output/              ← Visualization & export
│   ├── excel_export.py         Excel with formulas
│   └── dashboard.py            HTML/Looker dashboard
│
├── data/
│   ├── raw/                ← Downloaded CSVs (never modified)
│   └── processed/          ← Merged, cleaned datasets
│
└── docs/                   ← Internal documentation
    ├── post_tge_design.txt
    ├── raiku_mainnet.txt
    ├── raiku_aot_opportunity_cost.txt
    └── raiku_usecases.txt
```

## Key Constants (from internal docs)

| Parameter | Value | Source |
|-----------|-------|--------|
| Protocol take rate | 5% (range 1-5%) | Post-TGE Design |
| Validator share | 95% (96.5% high perf) | Post-TGE Design |
| AOT p_target | 0.995 | Opportunity Cost doc |
| Standard p_target | 0.85 | Opportunity Cost doc |
| Composite inclusion (AOT) | 0.89 vs 0.40 standard | Mainnet doc |
| Jito 2025 total tips | ~$720M | Post-TGE Design |
| Conservative MEV base | $100M/year | Post-TGE Design |
| Initial AOT customers | ~5-15 | Mainnet doc |
| Initial JIT customers | ~10-30 | Mainnet doc |
| $RAIKU supply | 1B tokens (fixed) | Post-TGE Design |
| Target FDV at TGE | $200-400M | Post-TGE Design |
| TGE target | Q4 2026 | Post-TGE Design |

## Setup

```bash
# Python 3.10+
pip install requests openpyxl pandas

# Dune API key
export DUNE_API_KEY="your_key_here"

# Google BigQuery (for Phase 2)
# pip install google-cloud-bigquery
# gcloud auth application-default login
```

## Phases

- **Phase 1** (current): Dune + CoinGecko → Solana epoch economics database → Excel + dashboard
- **Phase 2** (next): BigQuery/Token Terminal → Protocol-level data (Jito, Pump, Jupiter, etc.)
- **Phase 3** (later): Full revenue model → AOT + JIT projections → investor-ready output

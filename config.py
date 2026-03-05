"""
RAIKU Revenue Estimation — Configuration & Constants
=====================================================
All API keys, paths, and business parameters in one place.
API keys are loaded from .env file (never hardcode secrets).
"""

import os
from pathlib import Path

# Load .env file if present (pip install python-dotenv)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass  # python-dotenv not installed; rely on system env vars

# ── Paths ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_MAPPING = PROJECT_ROOT / "data" / "mapping"

# ── Google Sheets ─────────────────────────────────────
GOOGLE_SHEET_ID = "14xHMN7HpBuGbRA1SFgehbUgyvQs1Gf3XVwWdoOpuST4"
SERVICE_ACCOUNT_FILE = PROJECT_ROOT / "service_account.json"

# ── API Keys (from .env or environment) ────────────────
DUNE_API_KEY = os.environ.get("DUNE_API_KEY", "")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")

# ── Trillium API (free, no auth) ──────────────────────
TRILLIUM_BASE_URL = "https://api.trillium.so"
TRILLIUM_FIRST_EPOCH = 552  # Earliest available epoch (tested: 551=404, 552=OK)

# ── Solana Compass API (free, no auth) ──────────────────
SOLANA_COMPASS_BASE_URL = "https://solanacompass.com/api"
SOLANA_COMPASS_FIRST_EPOCH = 800  # SC data starts at epoch 800 (tested: 799=422, 800=OK)

# ── Jito Foundation MEV API (free, no auth) ─────────────
JITO_MEV_API_URL = "https://kobe.mainnet.jito.network/api/v1/mev_rewards"
JITO_MEV_FIRST_EPOCH = 396  # Jito data starts ~epoch 400 (tested: 350=500err, 400=OK)

# ── Dune Query IDs ─────────────────────────────────────
DUNE_QUERIES = {
    "epoch_economics": 6773409,      # Epoch rewards, fees, MEV, APY, price
    "commission_validators": 6773227, # Commission rates, validator count, stake accounts
    "active_stake": 6776267,          # Active stake per epoch
    "fee_breakdown": 6776270,         # Base fee vs priority fee per epoch
    "fee_per_cu_by_program": 6777333, # Fee/CU by program (7-day snapshot)
    "daily_priority_fees": 6777334,   # Daily priority fees (91-day)
    "program_fees_30d": 6783409,       # Per-program daily fees/CU 30-day
    "program_fees_aggregate": 6783408, # Top 500 programs aggregate 30-day
}

# ── Solscan Pro API (free tier) ──────────────────────────
SOLSCAN_API_KEY = os.environ.get("SOLSCAN_API_KEY", "")
SOLSCAN_BASE_URL = "https://pro-api.solscan.io/v2.0"

# ── CoinGecko ──────────────────────────────────────────
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
SOL_COIN_ID = "solana"

# ── BigQuery (Phase 2) ────────────────────────────────
# Token Terminal decoded tables on Analytics Hub
BIGQUERY_TABLES = {
    "jito": "tt-contracts.jito_v1_solana",
    "pumpfun": "tt-contracts.pumpfun_v1_solana",
    "jupiter": "tt-contracts.jupiter_v1_solana",
    "raydium": "tt-contracts.raydium_v1_solana",
    "orca": "tt-contracts.orca_v1_solana",
    "marinade": "tt-contracts.marinade_v1_solana",
    "solana_public": "bigquery-public-data.crypto_solana_mainnet_us",
}

# ── RAIKU Business Parameters ─────────────────────────
# From Post-TGE Protocol Design v1.1
PROTOCOL_TAKE_RATE = 0.05          # 5% default (governance range 1-5%)
PROTOCOL_TAKE_RATE_MIN = 0.01     # 1% minimum
PROTOCOL_TAKE_RATE_HIGH_PERF = 0.035  # 3.5% for qualifying validators
VALIDATOR_SHARE = 0.95             # 95% to validators
VALIDATOR_SHARE_HIGH_PERF = 0.965  # 96.5% for high performers

# Revenue waterfall (% of total revenue)
CUSTOMER_REBATE_PCT = 0.005        # ~0.5% (0.25-1% AOT, 0-0.5% JIT)
VALIDATOR_ENHANCEMENT_PCT = 0.015  # ~1.5% (if qualifying)
PROTOCOL_REMAINDER_PCT = 0.03     # ~3% → operations, growth, buyback

# ── AOT Model Parameters ──────────────────────────────
# From Opportunity Cost doc + Mainnet doc
AOT_P_TARGET = 0.995               # Probability of landing in target slot
AOT_P_INCLUDE = 0.995              # Probability of on-chain inclusion
STANDARD_P_TARGET = 0.85           # Standard path target-slot probability
STANDARD_P_INCLUDE = 1.0           # Standard path on-chain probability (lands, but often late)
COMPOSITE_INCLUSION_AOT = 0.89     # Overall composite (from mainnet doc)
COMPOSITE_INCLUSION_STD = 0.40     # Standard composite (from mainnet doc)

# ── Market References ─────────────────────────────────
# Jito 2025 (from Post-TGE Design)
JITO_2025_TOTAL_TIPS_USD = 720_000_000     # ~$720M
JITO_2025_DAO_REVENUE_USD = 7_500_000      # ~$7-8M
JITO_2025_EFFECTIVE_TAKE_RATE = 0.015      # 1-2% effective (despite nominal 6%)
JITO_Q1_2025_TIPS_USD = 445_000_000        # 62% of annual
JITO_Q4_2025_ANNUALIZED_USD = 100_000_000  # ~$25M/quarter annualized

# ── Revenue Scenarios ─────────────────────────────────
SCENARIOS = {
    "conservative": {"mev_market_usd": 100_000_000, "market_share": 0.05},
    "base":         {"mev_market_usd": 100_000_000, "market_share": 0.10},
    "optimistic":   {"mev_market_usd": 100_000_000, "market_share": 0.15},
    "bull_conservative": {"mev_market_usd": 720_000_000, "market_share": 0.05},
    "bull_base":         {"mev_market_usd": 720_000_000, "market_share": 0.10},
    "bull_optimistic":   {"mev_market_usd": 720_000_000, "market_share": 0.15},
}

# ── Token Parameters ──────────────────────────────────
RAIKU_TOTAL_SUPPLY = 1_000_000_000  # 1B tokens, fixed
RAIKU_TARGET_FDV_LOW = 200_000_000  # $200M at TGE
RAIKU_TARGET_FDV_HIGH = 400_000_000 # $400M at TGE

# ── CSV Settings ──────────────────────────────────────
CSV_DELIMITER = ";"  # Semicolon for European locale compatibility
CSV_ENCODING = "utf-8"

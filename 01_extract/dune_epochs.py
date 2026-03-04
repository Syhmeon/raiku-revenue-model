"""
Extract epoch economics data from Dune query 6773409.

Output columns:
  epoch, block_time, epoch_time, inflationary_reward, fee_reward, mev_reward,
  total_reward, total_reward_usd, issue_apy, sol_price_usd

This is the primary Solana validator economics dataset (epochs ~150-935+).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DUNE_QUERIES
from dune_client import DuneClient


COLUMNS = [
    "epoch", "block_time", "epoch_time",
    "inflationary_reward", "fee_reward", "mev_reward",
    "total_reward", "total_reward_usd",
    "issue_apy", "sol_price_usd",
]

OUTPUT_FILE = "dune_epoch_data_v2.csv"


def extract():
    client = DuneClient()
    rows = client.execute_and_fetch(DUNE_QUERIES["epoch_economics"])
    client.save_csv(rows, OUTPUT_FILE, COLUMNS)
    return rows


if __name__ == "__main__":
    print("\n=== Extracting Epoch Economics (Dune 6773409) ===")
    extract()
    print("Done.")

"""
Extract validator commission & count data from Dune query 6773227.

Output columns:
  epoch, voting_rewards_sol, avg_commission_rate, validator_count, stake_account_count
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DUNE_QUERIES
from dune_client import DuneClient


COLUMNS = [
    "epoch", "voting_rewards_sol", "avg_commission_rate",
    "validator_count", "stake_account_count",
]

OUTPUT_FILE = "dune_commission_validators_v2.csv"


def extract():
    client = DuneClient()
    rows = client.execute_and_fetch(DUNE_QUERIES["commission_validators"])
    client.save_csv(rows, OUTPUT_FILE, COLUMNS)
    return rows


if __name__ == "__main__":
    print("\n=== Extracting Validator Data (Dune 6773227) ===")
    extract()
    print("Done.")

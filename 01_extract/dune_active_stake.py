"""
Extract active stake per epoch from Dune query 6776267.

Output columns:
  epoch, active_stake_sol
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DUNE_QUERIES
from dune_client import DuneClient


COLUMNS = ["epoch", "active_stake_sol"]
OUTPUT_FILE = "dune_active_stake_v1.csv"


def extract():
    client = DuneClient()
    rows = client.execute_and_fetch(DUNE_QUERIES["active_stake"])
    client.save_csv(rows, OUTPUT_FILE, COLUMNS)
    return rows


if __name__ == "__main__":
    print("\n=== Extracting Active Stake (Dune 6776267) ===")
    extract()
    print("Done.")

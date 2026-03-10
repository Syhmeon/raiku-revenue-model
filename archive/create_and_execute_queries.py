#!/usr/bin/env python3
"""
Create and execute two Dune queries for RAIKU:
  A. Missing 63 programs aggregate (fixed 30d)
  B. Daily fees 10 missing days (Feb 4-13)
"""

import sys
from pathlib import Path
import time
import importlib.util

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent))

from config import DUNE_API_KEY

# Import dune_client from the numbered module directory
dune_module_path = Path(__file__).parent / "01_extract" / "dune_client.py"
spec = importlib.util.spec_from_file_location("dune_client", dune_module_path)
dune_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dune_module)
DuneClient = dune_module.DuneClient

# Verify API key
if not DUNE_API_KEY:
    raise RuntimeError("DUNE_API_KEY not set in environment or .env file")

# ─────────────────────────────────────────────────────────────────
# QUERY A: Missing 63 programs aggregate (fixed 30d)
# ─────────────────────────────────────────────────────────────────

QUERY_A_SQL = """-- RAIKU: Aggregate fees for 63 missing mapped programs (AOT/JIT/BOTH)
-- Fixed window: 2026-02-04 to 2026-03-05 (same as V2)
WITH tx_with_program AS (
  SELECT
    fee,
    compute_units_consumed,
    instructions[1].executing_account AS program_id,
    success
  FROM solana.transactions
  WHERE block_date >= DATE '2026-02-04'
    AND block_date < DATE '2026-03-06'
    AND compute_units_consumed > 0
    AND fee > 0
    AND instructions[1].executing_account IN (
    '13gDzEXCdocbj8iAiqrScGo47NiSuYENGsRqi3SEAwet',
    '1oopBoJG58DgkUVKkEzKgyG9dvRmpgeEm1AVjoHkF78',
    '2Z2EJavNVHpBgw8gVSdbtZ3E1kseANRrN8z57dE1BnGe',
    '2nAAsYdXF3eTQzaeUQS3fr4o782dDg8L28mX39Wr5j8N',
    '3parcLrT7WnXAcyPfkCz49oofuuf2guUKkjuFkAhZW8Y',
    '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg',
    '5fNfvyp5czQVX77yoACa3JJVEhdRaWjPuazuWgjhTqEH',
    '5ocnV1qiCgaQR8Jb8xWnVbApfaygJ8tNoZfgPwsgx9kx',
    '6UeJYTLU1adaoHWeApWsoj1xNEDbWA2RhM2DLc8CrDDi',
    '7UVimffxr9ow1uXYxsr4LHAcV58mLzhmwaeKvJ1pjLiE',
    '7Zb1bGi32pfsrBkzWdqd4dFhUXwp5Nybr1zuaEwN34hy',
    '7bgvkyKyHvgHdNCTKhheHBYCnvu7BQsBKHAGWNX2jk7D',
    'ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA',
    'AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45',
    'BFYDcwRC2CqnvSpd3ES9WUieSYk4FtGJwhuyMVintbGQ',
    'CgntPoLka5pD5fesJYhGmUCF8KU1QS1ZmZiuAuMZr2az',
    'Crf6uiMVHpuqkQzE2FkWWces3xiYaTbTsFYPDSeNWksM',
    'Dso1bDeDjCQxTrWH7rUUi63oBvV7Mdm6WaobLbQ7gnPQ',
    'EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S',
    'ExponentnaRg3CQbW6dqQNZKXp7gtZ9DGMp1cwC4HAS7',
    'FD1amxhTsDpwzoVX41dxp2ygAESURV2zdUACzxM1Dfw9',
    'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH',
    'FsU1rcaEC361jBr9JE5wm7bpWRSTYeAMN4R2MCs11rNF',
    'GAMMA7meSFWaBXF25oSUgmGRwaW6sCMFLmBNiMSdbHVT',
    'GMGNreQcJFufBiCTLDBgKhYEfEe9B454UjpDr5CaSLA1',
    'Gmso1uvJnLbawvw7yezdfCDcPydwW2s2iqG3w6MDucLo',
    'HEvSKofvBgfaexv23kMabbYqxasxU3mQ4ibBMEmJWHny',
    'HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt',
    'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB',
    'JUP5pEAZeHdHrLxh5UCwAbpjGwYKKoquCpda2hfP4u8',
    'Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb',
    'KLend2g3cP87ber41GtWkPjXCVRKyomUr11Hpe26LhL3',
    'KiNetiCAToLExLv5w7PrfiiKK1iYHNjrUM3RAF2V3A4',
    'MEVbit1CZN1oHXJkcKDNkFmFSMczHVZYnJQpYgGZSnF',
    'MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz',
    'MEViZt7uNtZs47XkjujUuMSAxxxVtQQ25SwToLcEckt',
    'NA247a7YE9S3p9CdKmMyETx8TTwbSdVbVYHHxpnHTUV',
    'PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY',
    'R2y9ip6mxmWUj4pt54jP2hz2dgvMozy9VTSwMWE7evs',
    'SCoRcH8c2dpjvcJD6FiPbCSQyQgu3PcUAWj2Xxx3mqn',
    'SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f',
    'So1endDq2YkqhipRh3WViPa8hFMUpXRQrETEgTcKSaJ',
    'SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe',
    'TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH',
    'TitanLozLMhczcwrioEguG2aAmiATAPXdYpBg3DbeKK',
    'XxXxXwJhqsCqZ5yzrykvjwVnUpqBUJP6g6cYjFt2dfW',
    'ZDx8a8jBqGmJyxi1whFxxCo5vG6Q9t4hTzW2GSixMKK',
    'ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY',
    'ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXRas1EQ',
    'ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD',
    'b1oomGGqPKGD6errbyfbVMBuzSC8WtAAYo8MwNafWW1',
    'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1',
    'cjg3oHmg9uuPsP8D6g29NWvhySJkdYdAo9D25PRbKXJ',
    'goonuddtQRrWqqn5nFyczVKaie28f3KDkHWkHtURSLE',
    'he1iusmfkpAdwvxLNGV8Y1iSbj4rUy6yMhEA3fotn9A',
    'jCebN34bUfdeUYJT13J1yG16XWQpt5PDx6Mse9GUqhR',
    'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So',
    'mv3ekLzLbnVPNxjSKvqBpU3ZeZXPQdEC3bp5MDEBG68',
    'obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y',
    'opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EQMQf8',
    'rexhfZLRRxRkPkw9izswgMFRPDb9U58jeinH7wqUVuw',
    'stork1JUZMKYgjNagHiK2KdMmb42iTnYe9bYUCDUk8n',
    'tuna4uSQZncNeeiAMKbstuxA9CUkHH6HmC64wgmnogD'
    )
)
SELECT
  program_id,
  COUNT(*) AS tx_count,
  SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
  ROUND(SUM(5000.0) / 1e9, 4) AS base_fees_sol,
  ROUND(SUM(GREATEST(CAST(fee AS double) - 5000, 0)) / 1e9, 4) AS priority_fees_sol,
  ROUND(SUM(CAST(fee AS double)) / 1e9, 4) AS total_fees_sol,
  SUM(compute_units_consumed) AS total_cu,
  ROUND(AVG(CAST(compute_units_consumed AS double)), 0) AS avg_cu_per_tx,
  CASE
    WHEN COUNT(DISTINCT block_slot) > 0
    THEN ROUND(CAST(SUM(compute_units_consumed) AS double) / COUNT(DISTINCT block_slot), 0)
    ELSE 0
  END AS avg_cu_per_block,
  COUNT(DISTINCT block_slot) AS blocks_touched,
  SUM(CASE WHEN fee <= 5000 THEN 1 ELSE 0 END) AS base_only_tx_count,
  SUM(CASE WHEN fee > 5000 THEN 1 ELSE 0 END) AS priority_tx_count,
  ROUND(APPROX_PERCENTILE(
    CAST(fee AS double) / CAST(compute_units_consumed AS double), 0.5
  ), 4) AS median_priority_fee_per_cu_lamports,
  ROUND(APPROX_PERCENTILE(
    CAST(fee AS double) / CAST(compute_units_consumed AS double), 0.25
  ), 4) AS p25_priority_fee_per_cu_lamports,
  ROUND(APPROX_PERCENTILE(
    CAST(fee AS double) / CAST(compute_units_consumed AS double), 0.75
  ), 4) AS p75_priority_fee_per_cu_lamports,
  ROUND(AVG(CAST(fee AS double) / CAST(compute_units_consumed AS double)), 4)
    AS avg_priority_fee_per_cu_lamports
FROM tx_with_program
GROUP BY program_id
ORDER BY total_fees_sol DESC"""

# ─────────────────────────────────────────────────────────────────
# QUERY B: Daily fees 10 missing days (Feb 4-13)
# ─────────────────────────────────────────────────────────────────

QUERY_B_SQL = """-- RAIKU: Daily per-program fees for the 10 missing days
-- Fills gap: existing daily data covers Feb 14 - Mar 5
-- This covers Feb 4 - Feb 13 (same programs as existing daily)
-- Fixed dates to match V2 window
WITH tx_with_program AS (
  SELECT
    block_date AS day,
    fee,
    compute_units_consumed,
    instructions[1].executing_account AS program_id,
    success
  FROM solana.transactions
  WHERE block_date >= DATE '2026-02-04'
    AND block_date < DATE '2026-02-14'
    AND compute_units_consumed > 0
    AND fee > 0
)
SELECT
  day,
  program_id,
  COUNT(*) AS tx_count,
  SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
  ROUND(SUM(CAST(fee AS double)) / 1e9, 4) AS total_fees_sol,
  ROUND(SUM(GREATEST(CAST(fee AS double) - 5000, 0)) / 1e9, 4) AS priority_fees_sol,
  SUM(compute_units_consumed) AS total_cu,
  ROUND(AVG(CAST(compute_units_consumed AS double)), 0) AS avg_cu_consumed,
  ROUND(AVG(CAST(fee AS double) / CAST(compute_units_consumed AS double)), 4)
    AS fee_per_cu_lamports
FROM tx_with_program
WHERE program_id IN (
    SELECT program_id
    FROM tx_with_program
    GROUP BY program_id
    HAVING SUM(fee) / 1e9 > 0.1
)
GROUP BY 1, 2
ORDER BY day DESC, total_fees_sol DESC"""

def main():
    client = DuneClient()

    print("=" * 70)
    print("DUNE QUERY CREATION & EXECUTION")
    print("=" * 70)

    # ───────────────────────────────────────────────────────────────
    # QUERY A
    # ───────────────────────────────────────────────────────────────
    print("\n[QUERY A] Creating and executing...")
    query_a_name = "RAIKU: Missing 63 programs aggregate (fixed 30d)"

    try:
        query_a_id = client.create_query(query_a_name, QUERY_A_SQL, is_private=True)
        print(f"✓ Query A created: {query_a_id}")

        print(f"\nExecuting Query A ({query_a_id})...")
        exec_a_id = client.execute_query(query_a_id)
        print(f"✓ Execution started: {exec_a_id}")

        print(f"\nWaiting for Query A to complete (up to 30 minutes)...")
        rows_a = client.execute_and_fetch(query_a_id, max_wait_sec=1800)
        print(f"✓ Query A completed: {len(rows_a)} rows")

        print(f"\n--- QUERY A RESULTS (first 10 rows) ---")
        print(f"Total rows: {len(rows_a)}")
        for i, row in enumerate(rows_a[:10]):
            print(f"  Row {i+1}: {row}")

    except Exception as e:
        print(f"✗ Query A failed: {e}")
        return 1

    # ───────────────────────────────────────────────────────────────
    # QUERY B
    # ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("[QUERY B] Creating and executing...")
    query_b_name = "RAIKU: Daily fees 10 missing days (Feb 4-13)"

    try:
        query_b_id = client.create_query(query_b_name, QUERY_B_SQL, is_private=True)
        print(f"✓ Query B created: {query_b_id}")

        print(f"\nExecuting Query B ({query_b_id})...")
        exec_b_id = client.execute_query(query_b_id)
        print(f"✓ Execution started: {exec_b_id}")

        print(f"\nWaiting for Query B to complete (up to 30 minutes)...")
        rows_b = client.execute_and_fetch(query_b_id, max_wait_sec=1800)
        print(f"✓ Query B completed: {len(rows_b)} rows")

        print(f"\n--- QUERY B RESULTS (first 10 rows) ---")
        print(f"Total rows: {len(rows_b)}")
        for i, row in enumerate(rows_b[:10]):
            print(f"  Row {i+1}: {row}")

    except Exception as e:
        print(f"✗ Query B failed: {e}")
        return 1

    # ───────────────────────────────────────────────────────────────
    # SUMMARY
    # ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Query A ID: {query_a_id}")
    print(f"Query A Execution ID: {exec_a_id}")
    print(f"Query A Rows: {len(rows_a)}")
    print()
    print(f"Query B ID: {query_b_id}")
    print(f"Query B Execution ID: {exec_b_id}")
    print(f"Query B Rows: {len(rows_b)}")

    return 0

if __name__ == "__main__":
    sys.exit(main())

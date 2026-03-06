-- REFERENCE: Solana Priority Fee distribution (min/max/median)
-- Source: Dune query 3299816 (@ilemi)
-- Purpose: Cross-check priority fee levels and distribution
-- NOTE: Uses ComputeBudget instruction opcodes 0x02/0x03 to identify
--   txs that explicitly set priority fees (stricter than fee - base > 0)
-- Also uses required_signatures (not cardinality(signatures)) for base calc
-- NOT a RAIKU query — used for validation only
-- ============================================================

WITH base AS (
  SELECT DISTINCT tx_id
  FROM solana.instruction_calls
  WHERE executing_account = 'ComputeBudget111111111111111111111111111111'
    AND substr(cast(data AS varchar), 1, 4) IN ('0x02', '0x03')
    AND block_time >= date(now() - interval '1' month)
    AND tx_success = true
)

SELECT
  date_trunc('day', block_time) AS time,
  min(fee / 1e9 - 0.000005 * required_signatures) AS min_priority,
  max(fee / 1e9 - 0.000005 * required_signatures) AS max_priority,
  approx_percentile(fee / 1e9 - 0.000005 * required_signatures, 0.5) AS med_priority
FROM solana.transactions
WHERE block_time >= date(now() - interval '1' month)
  AND id IN (SELECT tx_id FROM base)
GROUP BY 1

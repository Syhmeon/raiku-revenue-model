-- RAIKU Query 2: Jito tips per program (30 days)
-- Uses system_program_solana.system_program_call_Transfer (decoded table)
-- instead of raw solana.account_activity (inspired by @ilemi 4314353)
--
-- Returns: per program, the total Jito tips in SOL with distribution stats
-- ============================================================

WITH jito_tips AS (
  -- All SOL transfers to Jito tip accounts (decoded table = cleaner)
  SELECT
    call_tx_id AS tx_id,
    call_block_time AS block_time,
    call_block_date AS block_date,
    CAST(amount AS double) / 1e9 AS tip_sol
  FROM system_program_solana.system_program_call_Transfer
  WHERE account_to IN (
    '96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5',
    'HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe',
    'Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY',
    'ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49',
    'DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh',
    'ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt',
    'DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL',
    '3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT'
  )
  AND call_block_time >= NOW() - INTERVAL '30' DAY
),

-- For each tipping tx, find which program it called
tip_programs AS (
  SELECT
    t.tx_id,
    t.tip_sol,
    t.block_time,
    ic.outer_executing_account AS program_id
  FROM jito_tips t
  JOIN solana.instruction_calls ic
    ON ic.tx_id = t.tx_id
    AND ic.block_time = t.block_time
    AND ic.block_time >= NOW() - INTERVAL '30' DAY
  WHERE ic.outer_executing_account NOT IN (
    '11111111111111111111111111111111',
    'ComputeBudget111111111111111111111111111111',
    'T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt',
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
    'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',
    'Vote111111111111111111111111111111111111111',
    'Stake11111111111111111111111111111111111111',
    'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr',
    'Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo'
  )
)

SELECT
  program_id,
  p.namespace AS program_label,
  COUNT(DISTINCT tx_id) AS tip_tx_count,
  ROUND(SUM(tip_sol), 4) AS total_jito_tips_sol,
  ROUND(AVG(tip_sol), 6) AS avg_tip_sol,
  ROUND(APPROX_PERCENTILE(tip_sol, 0.5), 6) AS median_tip_sol,
  ROUND(APPROX_PERCENTILE(tip_sol, 0.75), 6) AS p75_tip_sol,
  ROUND(APPROX_PERCENTILE(tip_sol, 0.95), 6) AS p95_tip_sol,
  COUNT(DISTINCT DATE(block_time)) AS days_active
FROM tip_programs tp
LEFT JOIN solana.programs p ON p.program_id = tp.program_id
GROUP BY tp.program_id, p.namespace
HAVING COUNT(DISTINCT tx_id) >= 100
ORDER BY total_jito_tips_sol DESC
LIMIT 300

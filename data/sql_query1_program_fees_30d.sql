-- RAIKU Query 1: Fee breakdown per program (30 days)
-- Inspired by @ilemi query 4314353 (Solana Transaction by Fee Type)
-- Adapted: returns SOL amounts (not just tx counts) per program
--
-- Output columns: program_id, program_label,
--   base_fees_sol, priority_fees_sol, jito_tips_sol, total_fees_sol,
--   tx_count, success_count, total_cu, avg_cu_per_tx,
--   base_tx_count, priority_tx_count, jito_tx_count, priority_and_jito_tx_count
-- ============================================================

WITH tx_fees AS (
  SELECT
    tx.id AS tx_id,
    tx.block_time,
    tx.block_date,
    tx.success,
    tx.fee,
    tx.compute_units_consumed,
    cardinality(tx.signatures) AS num_signatures,
    -- Base fee = 5000 lamports per signature (Solana protocol constant)
    cardinality(tx.signatures) * 5000 AS base_fee_lamports,
    -- Priority fee = total fee minus base fee
    GREATEST(tx.fee - cardinality(tx.signatures) * 5000, 0) AS priority_fee_lamports,
    -- Jito tip detection: is this tx in the Jito tip transfer table?
    CASE WHEN tx.id IN (
      SELECT DISTINCT call_tx_id
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
    ) THEN TRUE ELSE FALSE END AS has_jito_tip
  FROM solana.transactions tx
  WHERE tx.block_date >= CURRENT_DATE - INTERVAL '30' DAY
    AND tx.compute_units_consumed > 0
    AND tx.fee > 0
),

-- Get Jito tip AMOUNTS per transaction (not just boolean)
jito_tip_amounts AS (
  SELECT
    call_tx_id AS tx_id,
    SUM(CAST(amount AS double)) AS jito_tip_lamports
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
  GROUP BY call_tx_id
),

-- Join tx with program via instruction_calls
tx_with_program AS (
  SELECT
    tf.tx_id,
    tf.success,
    tf.base_fee_lamports,
    tf.priority_fee_lamports,
    tf.compute_units_consumed,
    tf.has_jito_tip,
    COALESCE(jta.jito_tip_lamports, 0) AS jito_tip_lamports,
    ic.outer_executing_account AS program_id
  FROM tx_fees tf
  JOIN solana.instruction_calls ic
    ON ic.tx_id = tf.tx_id
    AND ic.block_time = tf.block_time
    AND ic.block_time >= NOW() - INTERVAL '30' DAY
  LEFT JOIN jito_tip_amounts jta
    ON jta.tx_id = tf.tx_id
  WHERE ic.outer_executing_account NOT IN (
    -- Exclude infrastructure programs
    '11111111111111111111111111111111',
    'ComputeBudget111111111111111111111111111111',
    'Vote111111111111111111111111111111111111111',
    'Stake11111111111111111111111111111111111111'
  )
)

SELECT
  program_id,
  p.namespace AS program_label,
  -- Fee breakdown in SOL
  ROUND(SUM(CAST(base_fee_lamports AS double)) / 1e9, 4) AS base_fees_sol,
  ROUND(SUM(CAST(priority_fee_lamports AS double)) / 1e9, 4) AS priority_fees_sol,
  ROUND(SUM(jito_tip_lamports) / 1e9, 4) AS jito_tips_sol,
  ROUND((SUM(CAST(base_fee_lamports AS double)) + SUM(CAST(priority_fee_lamports AS double)) + SUM(jito_tip_lamports)) / 1e9, 4) AS total_fees_sol,
  -- Volume metrics
  COUNT(*) AS tx_count,
  SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
  SUM(compute_units_consumed) AS total_cu,
  ROUND(AVG(CAST(compute_units_consumed AS double)), 0) AS avg_cu_per_tx,
  -- Fee type distribution (tx counts)
  SUM(CASE WHEN priority_fee_lamports = 0 AND NOT has_jito_tip THEN 1 ELSE 0 END) AS base_only_tx_count,
  SUM(CASE WHEN priority_fee_lamports > 0 AND NOT has_jito_tip THEN 1 ELSE 0 END) AS priority_tx_count,
  SUM(CASE WHEN has_jito_tip AND priority_fee_lamports = 0 THEN 1 ELSE 0 END) AS jito_only_tx_count,
  SUM(CASE WHEN has_jito_tip AND priority_fee_lamports > 0 THEN 1 ELSE 0 END) AS priority_and_jito_tx_count,
  -- Fee/CU metrics (priority only, most relevant for RAIKU AOT)
  ROUND(APPROX_PERCENTILE(
    CAST(priority_fee_lamports AS double) / NULLIF(CAST(compute_units_consumed AS double), 0), 0.5
  ), 4) AS median_priority_fee_per_cu_lamports,
  ROUND(AVG(
    CAST(priority_fee_lamports AS double) / NULLIF(CAST(compute_units_consumed AS double), 0)
  ), 4) AS avg_priority_fee_per_cu_lamports
FROM tx_with_program twp
LEFT JOIN solana.programs p ON p.program_id = twp.program_id
GROUP BY twp.program_id, p.namespace
HAVING COUNT(*) >= 1000  -- At least 1000 txs in 30 days
ORDER BY total_fees_sol DESC
LIMIT 500

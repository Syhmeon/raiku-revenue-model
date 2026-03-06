-- RAIKU Query 3: Fee breakdown per program × market condition (2024+)
-- Point 11: Which programs pay more during congestion?
-- Now with REAL base/priority split (inspired by @ilemi query 4314353)
--
-- Market conditions from epoch volatility_tag classification
-- Scans 2024+ with partition pruning on block_date
-- ============================================================

WITH tx_with_program AS (
  SELECT
    block_date,
    fee,
    compute_units_consumed,
    cardinality(signatures) AS num_signatures,
    -- Real base/priority split
    cardinality(signatures) * 5000 AS base_fee_lamports,
    GREATEST(fee - cardinality(signatures) * 5000, 0) AS priority_fee_lamports,
    instructions[1].executing_account AS program_id,
    success,
    CASE
      WHEN block_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-05' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2024-01-06' AND DATE '2024-01-09' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-01-10' AND DATE '2024-01-12' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2024-01-25' AND DATE '2024-01-26' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-01-29' AND DATE '2024-01-31' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2024-02-01' AND DATE '2024-02-02' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-02-07' AND DATE '2024-02-08' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-02-28' AND DATE '2024-02-29' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2024-03-01' AND DATE '2024-03-02' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-03-03' AND DATE '2024-03-06' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2024-03-07' AND DATE '2024-03-08' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-03-09' AND DATE '2024-03-19' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2024-03-20' AND DATE '2024-04-08' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-04-12' AND DATE '2024-04-13' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2024-04-18' AND DATE '2024-04-20' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-05-14' AND DATE '2024-05-15' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-05-27' AND DATE '2024-05-28' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-06-07' AND DATE '2024-06-08' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2024-06-27' AND DATE '2024-06-28' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-07-02' AND DATE '2024-07-03' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-07-13' AND DATE '2024-07-14' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-07-27' AND DATE '2024-07-30' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-07-31' AND DATE '2024-08-06' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2024-08-26' AND DATE '2024-08-27' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-09-26' AND DATE '2024-10-04' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-10-07' AND DATE '2024-10-13' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-10-14' AND DATE '2024-10-29' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2024-11-04' AND DATE '2024-11-07' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-11-11' AND DATE '2024-11-12' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2024-11-13' AND DATE '2024-11-21' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2024-11-24' AND DATE '2024-11-25' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-01-06' AND DATE '2025-01-08' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-01-15' AND DATE '2025-01-16' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-01-17' AND DATE '2025-01-20' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2025-01-23' AND DATE '2025-01-26' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-01-31' AND DATE '2025-02-01' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-02-16' AND DATE '2025-02-17' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-02-22' AND DATE '2025-02-25' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-02-28' AND DATE '2025-03-03' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-03-08' AND DATE '2025-03-09' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-03-23' AND DATE '2025-03-24' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-03-27' AND DATE '2025-03-28' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-04-10' AND DATE '2025-04-11' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-04-30' AND DATE '2025-05-01' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2025-05-08' AND DATE '2025-05-09' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-05-12' AND DATE '2025-05-13' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2025-05-14' AND DATE '2025-05-15' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-06-11' AND DATE '2025-06-12' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-07-15' AND DATE '2025-07-18' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-08-11' AND DATE '2025-08-14' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-08-21' AND DATE '2025-08-22' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-09-30' AND DATE '2025-10-01' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-10-08' AND DATE '2025-10-09' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-10-12' AND DATE '2025-10-13' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2025-11-03' AND DATE '2025-11-04' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2025-11-11' AND DATE '2025-11-14' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-11-23' AND DATE '2025-11-24' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2025-12-02' AND DATE '2025-12-05' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2026-01-03' AND DATE '2026-01-04' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2026-01-05' AND DATE '2026-01-06' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2026-01-07' AND DATE '2026-01-08' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2026-01-09' AND DATE '2026-01-15' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2026-01-18' AND DATE '2026-01-19' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2026-01-24' AND DATE '2026-01-27' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2026-01-28' AND DATE '2026-01-31' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2026-02-03' AND DATE '2026-02-06' THEN 'extreme'
      WHEN block_date BETWEEN DATE '2026-02-09' AND DATE '2026-02-10' THEN 'elevated'
      WHEN block_date BETWEEN DATE '2026-03-03' AND DATE '2026-03-04' THEN 'elevated'
      ELSE 'normal'
    END AS market_condition
  FROM solana.transactions
  WHERE block_date >= DATE '2024-01-01'
    AND compute_units_consumed > 0
    AND fee > 0
)

SELECT
  market_condition,
  program_id,
  COUNT(*) AS tx_count,
  SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
  -- Real fee split
  ROUND(SUM(CAST(base_fee_lamports AS double)) / 1e9, 4) AS base_fees_sol,
  ROUND(SUM(CAST(priority_fee_lamports AS double)) / 1e9, 4) AS priority_fees_sol,
  ROUND((SUM(CAST(base_fee_lamports AS double)) + SUM(CAST(priority_fee_lamports AS double))) / 1e9, 4) AS total_onchain_fees_sol,
  SUM(compute_units_consumed) AS total_cu,
  -- Fee/CU stats (priority only)
  ROUND(AVG(CAST(priority_fee_lamports AS double) / NULLIF(CAST(compute_units_consumed AS double), 0)), 4)
    AS avg_priority_fee_per_cu_lamports,
  ROUND(APPROX_PERCENTILE(
    CAST(priority_fee_lamports AS double) / NULLIF(CAST(compute_units_consumed AS double), 0), 0.5
  ), 4) AS median_priority_fee_per_cu_lamports,
  COUNT(DISTINCT block_date) AS days_observed
FROM tx_with_program
GROUP BY market_condition, program_id
HAVING SUM(compute_units_consumed) > 1000000
ORDER BY market_condition, total_onchain_fees_sol DESC

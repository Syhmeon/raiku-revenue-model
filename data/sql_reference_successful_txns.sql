-- REFERENCE: Solana Number of Successful Transactions
-- Source: @ilemi dashboard (Solana Compute, Blocks, Transactions)
-- Purpose: Cross-check vote vs non-vote tx ratio, total throughput
-- Uses solana.blocks table (aggregated, lighter than solana.transactions)
-- NOT a RAIKU query — used for validation only
-- ============================================================

WITH base AS (
  SELECT
    date_trunc('day', time) AS time,
    SUM(successful_non_vote_transactions + successful_vote_transactions) AS total_txns,
    SUM(successful_non_vote_transactions) AS non_vote_txns,
    SUM(successful_vote_transactions) AS vote_txns
  FROM solana.blocks
  WHERE time >= date(now() - interval '1' year)
  GROUP BY 1
)

SELECT
  *,
  CAST(non_vote_txns AS double) / CAST(total_txns AS double) AS non_vote_percent
FROM base

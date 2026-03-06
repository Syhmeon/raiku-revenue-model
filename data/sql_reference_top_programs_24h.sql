-- REFERENCE: Top Solana Programs in Past 24 hours
-- Source: @ilemi dashboard (Solana Compute, Blocks, Transactions)
-- Purpose: Cross-check our program mapping and relevance
-- NOT a RAIKU query — used for validation only
-- ============================================================

WITH latest AS (
  SELECT
    namespace,
    executing_account,
    count(*) AS calls,
    count(DISTINCT tx_signer) AS users
  FROM solana.instruction_calls a
  LEFT JOIN solana.programs b ON a.executing_account = b.program_id
  WHERE block_time >= now() - interval '24' hour
    AND tx_success = true
  GROUP BY 1, 2
  HAVING count(*) > 10000
  ORDER BY 3 DESC
  LIMIT 100
),

delta_1d AS (
  SELECT
    executing_account,
    count(*) AS calls_delta_1d,
    count(DISTINCT tx_signer) AS users_delta_1d
  FROM solana.instruction_calls
  WHERE block_date = date(now() - interval '1' day)
    AND tx_success = true
    AND executing_account IN (SELECT executing_account FROM latest)
  GROUP BY 1
),

delta_7d AS (
  SELECT
    executing_account,
    count(*) AS calls_delta_7d,
    count(DISTINCT tx_signer) AS users_delta_7d
  FROM solana.instruction_calls
  WHERE block_date = date(now() - interval '7' day)
    AND tx_success = true
    AND executing_account IN (SELECT executing_account FROM latest)
  GROUP BY 1
)

SELECT
  rank() OVER (ORDER BY calls DESC) AS ranking,
  CASE
    WHEN namespace IS NULL AND a.executing_account = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL' THEN 'Associated Token Account Program'
    WHEN namespace IS NULL AND a.executing_account = 'TLPv2tuSVvn3fSk8RgW3yPddkp5oFivzZV3rA9hQxtX' THEN 'Tulip Protocol V2 Vaults'
    WHEN namespace IS NULL AND a.executing_account = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA' THEN 'Marginfi V2'
    WHEN namespace IS NULL AND a.executing_account = 'cjg3oHmg9uuPsP8D6g29NWvhySJkdYdAo9D25PRbKXJ' THEN 'Chainlink Data Store Program'
    ELSE namespace
  END AS program_name,
  a.executing_account,
  CAST(calls AS double) AS calls,
  CAST(users AS double) AS users,
  CAST(calls AS double) / CAST(calls_delta_1d AS double) - 1 AS calls_delta_1d,
  CAST(users AS double) / CAST(users_delta_1d AS double) - 1 AS users_delta_1d,
  CAST(calls AS double) / CAST(calls_delta_7d AS double) - 1 AS calls_delta_7d,
  CAST(users AS double) / CAST(users_delta_7d AS double) - 1 AS users_delta_7d
FROM latest a
LEFT JOIN delta_1d b ON a.executing_account = b.executing_account
LEFT JOIN delta_7d c ON a.executing_account = c.executing_account

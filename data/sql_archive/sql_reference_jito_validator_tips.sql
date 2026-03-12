-- REFERENCE: Jito Validator Tips (daily, 1 year)
-- Source: Dune query 3272756 (@ilemi)
-- Purpose: Cross-check total Jito tips per day vs our Trillium/Jito Foundation data
-- Uses solana.account_activity (balance_change) + prices.usd for USD conversion
-- Useful to validate our Query 2 totals (sum across all programs should match)
-- NOT a RAIKU query — used for validation only
-- ============================================================

WITH base AS (
  SELECT
    date_trunc('day', block_date) AS day,
    SUM(balance_change / 1e9) AS sol
  FROM solana.account_activity
  WHERE address IN (
    '96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5',
    'HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe',
    'Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY',
    'ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49',
    'DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh',
    'ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt',
    'DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL',
    '3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT'
  )
  AND token_mint_address IS NULL
  AND balance_change > 0
  AND block_date >= date(now() - interval '1' year)
  GROUP BY 1
),

price AS (
  SELECT
    date_trunc('day', minute) AS day,
    avg(price) AS price
  FROM prices.usd
  WHERE contract_address IS NULL
    AND symbol = 'SOL'
    AND minute >= date(now() - interval '1' year)
  GROUP BY 1
)

SELECT
  a.day,
  sol,
  sol * price AS tip_usd
FROM base a
LEFT JOIN price b ON a.day = b.day

-- REFERENCE: Solana Annualized Volatility (30d, 90d, 200d)
-- Source: @ilemi dashboard
-- Purpose: Cross-check our epoch volatility_tag classification
--   from Trillium against actual SOL price volatility
-- NOT a RAIKU query — used for validation only
-- ============================================================

WITH volatility_30d AS (
  SELECT day,
    stddev_samp(daily_return) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) * sqrt(365) AS volatility_30d
  FROM (
    SELECT day,
      ((price - (lag(price, 1) OVER (ORDER BY day ASC))) / (lag(price, 1) OVER (ORDER BY day ASC))) AS daily_return
    FROM (
      SELECT date_trunc('day', minute) AS day, avg(price) AS price
      FROM prices.usd
      WHERE contract_address IS NULL AND symbol = 'SOL'
      GROUP BY 1
    ) a
  ) b
),

volatility_90d AS (
  SELECT day,
    stddev_samp(daily_return) OVER (ORDER BY day ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) * sqrt(365) AS volatility_90d
  FROM (
    SELECT day,
      ((price - (lag(price, 1) OVER (ORDER BY day ASC))) / (lag(price, 1) OVER (ORDER BY day ASC))) AS daily_return
    FROM (
      SELECT date_trunc('day', minute) AS day, avg(price) AS price
      FROM prices.usd
      WHERE contract_address IS NULL AND symbol = 'SOL'
      GROUP BY 1
    ) a
  ) b
),

volatility_200d AS (
  SELECT day,
    stddev_samp(daily_return) OVER (ORDER BY day ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) * sqrt(365) AS volatility_200d
  FROM (
    SELECT day,
      ((price - (lag(price, 1) OVER (ORDER BY day ASC))) / (lag(price, 1) OVER (ORDER BY day ASC))) AS daily_return
    FROM (
      SELECT date_trunc('day', minute) AS day, avg(price) AS price
      FROM prices.usd
      WHERE contract_address IS NULL AND symbol = 'SOL'
      GROUP BY 1
    ) a
  ) b
)

SELECT
  a.day,
  a.volatility_30d,
  b.volatility_90d,
  c.volatility_200d
FROM volatility_30d a
LEFT JOIN volatility_90d b ON a.day = b.day
LEFT JOIN volatility_200d c ON a.day = c.day
WHERE a.day > date('2020-12-31')

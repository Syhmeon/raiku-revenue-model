# Query B: Daily program fees for 10 missing days (Feb 4-13)

## Context
The daily per-program dataset (`dune_daily_program_fees.csv`) only covers 20 days (Feb 14 → Mar 5) because Dune's 1000-row LIMIT truncated the results. This query fills the 10 missing days (Feb 4 → Feb 13) to complete the 30-day window and allow reconciliation with the V2 aggregate.

**Important**: This query covers ALL programs (top by fees), not just the 63 missing ones. The daily granularity is needed for all programs.

## SQL Query

```sql
-- RAIKU: Daily program fees for missing 10 days (2026-02-04 to 2026-02-13)
-- Same structure as query 6783409 (daily)
-- Covers Feb 4–13 only (the existing daily file has Feb 14–Mar 5)
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
  ROUND(AVG(CAST(fee AS double) / CAST(compute_units_consumed AS double)), 4) AS fee_per_cu_lamports
FROM tx_with_program
GROUP BY day, program_id
HAVING COUNT(*) >= 100
ORDER BY day DESC, total_fees_sol DESC
LIMIT 1000
```

## Instructions for Claude Code

1. **Create the query on Dune:**
   ```
   createDuneQuery("RAIKU: Daily program fees Feb 4-13 (10 missing days)", <SQL above>, true)
   ```

2. **Execute it:**
   ```
   executeQueryById(<query_id>)
   ```

3. **Poll for results** — retry `getExecutionResults(<execution_id>)` every 30 seconds until complete. Expect ~5-15 minutes (10 days × many programs).

4. **Save results** to:
   ```
   data/raw/dune_daily_program_fees_feb4_13.csv
   ```
   Use **semicolon delimiter** (`;`), **UTF-8 encoding**.

5. **Validate:**
   - All dates should be between 2026-02-04 and 2026-02-13 inclusive
   - Columns must match existing daily file: `day;program_id;tx_count;success_count;total_fees_sol;priority_fees_sol;total_cu;avg_cu_consumed;fee_per_cu_lamports`
   - Check: no dates from Feb 14+ (those are already in the main daily file)
   - If result has exactly 1000 rows, it hit the LIMIT — note this but it's acceptable (we already know the LIMIT issue)

## Expected Output
- **Columns**: 9 (same as existing daily file)
- **Rows**: ~500-1000 (10 days × ~50-100 programs per day)
- **Period**: 2026-02-04 → 2026-02-13 (10 days)

## After Both Queries Are Done

Once you have both CSVs, bring them back to Cowork. I will:
1. **Merge Query A results** into `dune_program_fees_v2.csv` (append the 63 missing programs)
2. **Merge Query B results** into `dune_daily_program_fees.csv` (prepend the 10 missing days)
3. **Rebuild** `program_database.csv` with the updated V2 + full mapping
4. **Reconcile**: verify sum of 30 daily days ≈ 30-day aggregate per program

# Query C — Daily Per-Program Fee Data (Temporal Extraction)

## Context

The RAIKU Revenue Simulator currently has only **30-day aggregates** per program (`D.p` in the simulator). To build per-category temporal charts (fee/CU over time with mean/median/p25/p75 bands), we need **daily granularity** data for the 53 AOT/Both programs that have CU data.

**Data gap being filled:** This extraction enables the "Temporal Market Context" section in the Category Explorer to show real per-category fee/CU distributions over time, replacing the current "PLANNED — NOT YET EXTRACTED" notice.

## Batch Strategy

53 programs × 30 days = ~1590 rows → exceeds Dune free tier 1000-row limit.

**Solution: 6 batches of 5-day windows**, each returning ≤265 rows.

| Batch | Date Range | Expected Rows |
|-------|-----------|---------------|
| C1 | 2026-02-04 → 2026-02-08 | ≤265 |
| C2 | 2026-02-09 → 2026-02-13 | ≤265 |
| C3 | 2026-02-14 → 2026-02-18 | ≤265 |
| C4 | 2026-02-19 → 2026-02-23 | ≤265 |
| C5 | 2026-02-24 → 2026-02-28 | ≤265 |
| C6 | 2026-03-01 → 2026-03-05 | ≤265 |

## SQL Template (parameterized by date range)

Replace `__START__` and `__END__` with batch dates.

```sql
WITH tx_data AS (
  SELECT
    instructions[1].executing_account AS program_id,
    block_date,
    fee,
    compute_units_consumed,
    success
  FROM solana.transactions
  WHERE block_date >= DATE '__START__'
    AND block_date < DATE '__END__'
    AND compute_units_consumed > 0
    AND fee > 0
    AND instructions[1].executing_account IN (
      '9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp',
      'BiSoNHVpsVZW2F7rx2eQ59yQwKxzU5NvBcmKshCSUypi',
      'KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD',
      'routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS',
      'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
      'DT3X9y2w3S13M4GiQNbMtsrYeA9eEZpLg6frM4V28at5',
      'phDEVv4w6BcfkLrLNeXr8HhhgQxnxziVGXpGPcaadMf',
      '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
      'HFn8GnPADiny6XqUoWE8uRPPxb29ikn4yTuPa9MF2fWJ',
      'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA',
      'D4aBeFn9DBhU41kFNvr9kVKRuBy858ATxwTevmJCCJYL',
      'EtrnLzgbS7nMMy5fbD42kXiUzGg8XQzJ972Xtk1cjWih',
      'FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9',
      'pythWSnswVUd12oZpeFP8e9CVaEqJg25g1Vtc2biRsT',
      'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',
      '7SyiQ664tWYCyWyKDftkfLspE85pJrUR4kJQwb6brYb9',
      'So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo',
      'cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG',
      'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN',
      'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',
      'ENicYBBNZQ91toN7ggmTxnDGZW14uv9UkumN7XBGeYJ4',
      'RangohQxaWip6i1twAAnRVLmob9j88fid7sq2DMAATW',
      'FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn',
      '6WPHMhYD7JLAF1HEtC6Ddkn93ADbGtyC93HUh7wy75m2',
      'EmTX93xYtDE3h7G5EF2m9VRPE1UT1XBoohpVmEzE4oEU',
      'FHKBuiohcYB5n636h7UmHahRGU5UCHX8CSb6DNSUqX65',
      'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH',
      'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
      '6sbWW8qnrXoNXVMcTGLBQJefuaNcJsuqxztXUegHi4Wq',
      'FarmsPZpWu9i7Kky8tPN37rs2TpmMrAZrC7S7vJa91Hr',
      'G77G9wS9JzkzWNGwQjUhRswbA5N8hYKpCK4TZRj9G1Pw',
      '25uXrrL4p6xd9Uk6jQPiFQoipg4nH48Jk7RNUGEcqbg8',
      'APR1MEny25pKupwn72oVqMH4qpDouArsX8zX4VwwfoXD',
      'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
      '2qGyiNeWyZxNdkvWHc2jT5qkCnYa1j1gDLSSUmyoWMh8',
      '99vQwtBwYtrqqD9YSXbdum3KBdxPAVxYTaQ3cfnJSrN2',
      'wMNFSTkir3HgyZTsB7uqu3i7FA73grFCptPXgrZjksL',
      'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB',
      'MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms',
      'KvauGMspG5k6rtzrqqn7WNn3oZdyKqLKwK2XWQ8FLjd',
      'zapvX9M3uf5pvy4wRPAbQgdQsM1xmuiFnkfHKPvwMiz',
      'strmRqUCoQUgGUan5YhzUZa6KqdzwX5L6FpUxfmKg5m',
      'jup3YeL8QhtSx1e253b2FDvsMNC87fDrgQZivbrndc9',
      'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu',
      'HuTkmnrv4zPnArMqpbMbFhfwzTR7xfWQZHH1aQKzDKFZ',
      'FW6zUqn4iKRaeopwwhwsquTY6ABWLLgjxtrC3VPnaWBf',
      'DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH',
      'SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy',
      'SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF',
      'T1TANpTeScyeqVzzgNViGDNrkQ6qHz9KrSBS4aNXvGT',
      'HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq',
      'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD',
      'fUSioN9YKKSa3CUC2YUc4tPkHJ5Y6XW1yz8y6F7qWz9'
    )
)
SELECT
  program_id,
  CAST(block_date AS VARCHAR) AS day,
  COUNT(*) AS tx_count,
  SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
  ROUND(SUM(5000.0) / 1e9, 6) AS base_fees_sol,
  ROUND(SUM(GREATEST(CAST(fee AS double) - 5000, 0)) / 1e9, 6) AS priority_fees_sol,
  ROUND(SUM(CAST(fee AS double)) / 1e9, 6) AS total_fees_sol,
  SUM(compute_units_consumed) AS total_cu
FROM tx_data
GROUP BY program_id, block_date
ORDER BY block_date, total_fees_sol DESC
```

## Instructions for Claude Code

Execute 6 batches sequentially. For each batch:

### Batch C1 (Feb 4–8)
1. Create query: `createDuneQuery("RAIKU daily fees C1 Feb04-08", <SQL with __START__='2026-02-04' __END__='2026-02-09'>, true)`
2. Execute: `executeQueryById(<query_id>)`
3. Poll: `getExecutionResults(<execution_id>)` — retry every 30s until COMPLETED
4. Save to: `data/raw/dune_daily_C1_feb04_08.csv` (semicolon delimiter, UTF-8)

### Batch C2 (Feb 9–13)
Same as above with `__START__='2026-02-09'` `__END__='2026-02-14'`
Save to: `data/raw/dune_daily_C2_feb09_13.csv`

### Batch C3 (Feb 14–18)
`__START__='2026-02-14'` `__END__='2026-02-19'`
Save to: `data/raw/dune_daily_C3_feb14_18.csv`

### Batch C4 (Feb 19–23)
`__START__='2026-02-19'` `__END__='2026-02-24'`
Save to: `data/raw/dune_daily_C4_feb19_23.csv`

### Batch C5 (Feb 24–28)
`__START__='2026-02-24'` `__END__='2026-03-01'`
Save to: `data/raw/dune_daily_C5_feb24_28.csv`

### Batch C6 (Mar 1–5)
`__START__='2026-03-01'` `__END__='2026-03-06'`
Save to: `data/raw/dune_daily_C6_mar01_05.csv`

### After all 6 batches complete:
5. Concatenate all 6 CSVs into a single file: `data/raw/dune_daily_program_fees_30d.csv`
   - Keep only one header row
   - Sort by `day ASC, total_fees_sol DESC`
6. Validate:
   - Row count should be ~1200–1590 (53 programs × 30 days, some programs may have 0 activity on some days)
   - All 53 program IDs should appear
   - Days should span 2026-02-04 to 2026-03-05
   - `base_fees_sol + priority_fees_sol ≈ total_fees_sol` for each row (within rounding)
   - No negative values

## Expected Output

### Per-batch CSV
- **Columns:** `program_id;day;tx_count;success_count;base_fees_sol;priority_fees_sol;total_fees_sol;total_cu`
- **Rows per batch:** ~200–265
- **Delimiter:** semicolon (`;`)

### Final merged CSV (`dune_daily_program_fees_30d.csv`)
- **Rows:** ~1200–1590
- **Period:** 2026-02-04 to 2026-03-05
- **Programs:** 53 AOT/Both programs with CU data

## What This Data Unlocks

Once embedded in the simulator's `D` object (as a new `D.daily` array), it enables:
1. Per-category fee/CU over time (line chart with p25/p75 bands)
2. Category vs network baseline comparison
3. Daily CU volume per category
4. Fee volatility / stability analysis per category
5. Removal of the "PLANNED — NOT YET EXTRACTED" notice in the temporal section

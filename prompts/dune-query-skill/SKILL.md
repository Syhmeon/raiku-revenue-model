---
name: dune-query
description: >
  Workflow for running Dune Analytics SQL queries against Solana blockchain data.
  Use this skill whenever the user asks to query Dune, extract on-chain data from Solana,
  run SQL against solana.transactions, or needs per-program fee/CU data.
  Also trigger when the user mentions "Dune", "on-chain query", "program fees",
  "priority fees per program", "compute units by program", or any Solana data extraction task.
  CRITICAL: This skill encodes a hard constraint — Cowork's VM CANNOT call the Dune API
  (HTTPS proxy blocks it). All Dune operations must go through Claude Code in VS Code via MCP.
---

# Dune Query Workflow for RAIKU

## The Golden Rule

**You CANNOT execute Dune queries from Cowork's Linux VM.**

The VM's HTTPS proxy blocks outbound calls to `api.dune.com`. This has been tested and confirmed multiple times. Do NOT attempt `urllib`, `requests`, `curl`, or any HTTP call to Dune — it will fail with `403 Forbidden`.

Instead, your job is to **prepare everything** and hand it to the user as a ready-to-paste prompt for Claude Code in VS Code, which has Dune MCP access.

## Workflow

### Step 1: Write the SQL

Write the SQL query following the patterns that work on Dune's free tier (see below). Save it to a `.sql` file in the project.

### Step 2: Write a Claude Code prompt

Create a markdown file containing:
1. The SQL query (in a code block)
2. Step-by-step instructions for Claude Code to execute it via Dune MCP
3. Where to save the results (CSV path, delimiter, encoding)
4. Validation checks to run after getting results

### Step 3: Hand off to the user

Tell Sylvain: "Copy-paste this prompt into Claude Code in VS Code. It will create the query on Dune, execute it, and save the results."

### Step 4: After results arrive

Once the user has the CSV, then you can:
- Read and validate the results
- Merge into existing datasets
- Update the pipeline (build_program_database.py, etc.)

## SQL Patterns That Work on Dune Free Tier

### The efficient pattern (single-table, < 5 min)

```sql
WITH tx_with_program AS (
  SELECT
    fee,
    compute_units_consumed,
    instructions[1].executing_account AS program_id,
    success
  FROM solana.transactions
  WHERE block_date >= DATE '2026-02-04'
    AND block_date < DATE '2026-03-06'
    AND compute_units_consumed > 0
    AND fee > 0
    -- Optional: filter to specific programs
    AND instructions[1].executing_account IN ('addr1', 'addr2', ...)
)
SELECT
  program_id,
  COUNT(*) AS tx_count,
  SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
  ROUND(SUM(5000.0) / 1e9, 4) AS base_fees_sol,
  ROUND(SUM(GREATEST(CAST(fee AS double) - 5000, 0)) / 1e9, 4) AS priority_fees_sol,
  ROUND(SUM(CAST(fee AS double)) / 1e9, 4) AS total_fees_sol,
  SUM(compute_units_consumed) AS total_cu,
  ROUND(AVG(CAST(compute_units_consumed AS double)), 0) AS avg_cu_per_tx
FROM tx_with_program
GROUP BY program_id
ORDER BY total_fees_sol DESC
```

Key points:
- **Always use `block_date` partition pruning** — without it, full table scan = timeout
- **Use fixed dates** (`DATE '2026-02-04'`) not `CURRENT_DATE - INTERVAL` for reproducibility
- **`instructions[1].executing_account`** = first instruction's program (program attribution)
- **Base fee = 5000 lamports per tx** (simplified; real = 5000 × num_signatures, but 99%+ of txs have 1 signature)
- **Priority fee = fee - 5000** (the remainder after base)

### Patterns that TIMEOUT on free tier (avoid)

- JOINing `solana.transactions` with `solana.instruction_calls` (too many rows)
- JOINing with `system_program_solana.system_program_call_Transfer` (Jito tip detection)
- Any query touching > 30 days of `solana.transactions` without program filtering
- Queries returning > 1000 rows (Dune free tier limit)

### Consequence: No per-program Jito tips

Jito tips are off-chain transfers to 8 known wallets. Detecting them requires joining with the transfer table, which always timeouts. Per-program Jito tip data is NOT available on Dune free tier. Global Jito tips come from Trillium API instead.

## Dune MCP Tools (for Claude Code prompts)

Claude Code in VS Code has these Dune MCP tools:
- `createDuneQuery(name, query_sql, is_private)` — Creates a saved query
- `executeQueryById(query_id, parameters)` — Runs a query (async)
- `getExecutionResults(execution_id)` — Polls for results
- `searchTables(search_term)` — Explore table schemas

## CSV Format Convention

All output CSVs must use:
- **Semicolon delimiter** (`;`)
- **UTF-8 encoding**
- Saved to `data/raw/` in the raiku-revenue-model project

## Project Context

This skill is part of the RAIKU revenue model project. Key files:
- `config.py` — API keys, paths, CSV settings
- `data/raw/dune_program_fees_v2.csv` — Canonical 30-day per-program data (500 programs)
- `data/raw/dune_daily_program_fees.csv` — Daily per-program data (20 days, 82 programs)
- `data/mapping/program_categories.csv` — 314 programs classified (aot/jit/both/potential/neither)
- `02_transform/build_program_database.py` — Merges Dune + mapping → program_database.csv

## Prompt Template for Claude Code

When writing a prompt for Claude Code, use this structure:

```markdown
# [Query Name]

## SQL Query
[SQL in code block]

## Instructions
1. Create this query on Dune: `createDuneQuery("query-name", sql, true)`
2. Execute it: `executeQueryById(query_id)`
3. Poll for results: `getExecutionResults(execution_id)` — retry every 30s until complete
4. Save results to `data/raw/[filename].csv` with semicolon delimiter (`;`), UTF-8
5. Validate: [specific checks]

## Expected Output
- Columns: [list]
- Rows: ~[estimate]
- Period: [dates]
```

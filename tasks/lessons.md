# RAIKU Revenue Model — Lessons Learned

> **Instruction for Claude**: Read this file at the start of every session.
> After any correction from the user, add an entry here.
> Format: `## [Category]` → `**Mistake**:` → `**Rule**:`

---

## CSV / Data

**Mistake**: Using comma as CSV delimiter.
**Rule**: Always use **semicolon** (`;`) as delimiter. UTF-8 encoding. See `config.py`.

**Mistake**: Manually editing files in `data/raw/`.
**Rule**: `data/raw/` is read-only. These files are produced by extraction scripts and re-extractable from source APIs.

**Mistake**: Modifying `data/mapping/program_categories.csv`.
**Rule**: This file is **SACRED**. Manual classification of 314 programs. Never touch without explicit instruction.

---

## APIs — Known Behaviors

**Jito Foundation API**:
**Rule**: Always use **POST**, never GET. `POST https://kobe.mainnet.jito.network/api/v1/mev_rewards` with body `{"epoch": N}`. GET only returns the latest epoch.

**Trillium API**:
**Rule**: Epoch 551 = 404 (unavailable). Epoch 552 = first valid epoch. Epoch 935+ = 404 (not yet indexed). No authentication required.

**Solana Compass API**:
**Rule**: Epochs 801, 803-809 = **permanently unavailable** (HTTP errors). Use 3 retries, 60s timeout. Returns a per-validator array — must aggregate (SUM). All values in lamports (÷1e9 for SOL).

**Dune Analytics**:
**Rule**: Use existing `dune_client.py` wrapper (stdlib urllib, no `requests`). Never rewrite the wrapper. API key in `config.py` via `.env`.

---

## HTML Simulator

**Mistake**: Modifying the simulator without reading the relevant section first.
**Rule**: The file is **2683 lines**. Always `grep` for the specific function/variable name first, then read only the relevant lines.

**Mistake**: Extracting the 6 data objects to separate files.
**Rule**: The 6 data objects (`D.a`, `D.e`, `D.p`, `D.daily`, `D.dailyNet`, `D_JITO`) are **inline in the HTML**. This is intentional — zero runtime fetches. Do not change this architecture.

**Mistake**: Changing the Chart.js version.
**Rule**: Chart.js **4.5.1** via CDN. Never change the version without explicit instruction.

**Mistake**: Injecting data through any mechanism other than `inject_daily_data.py`.
**Rule**: Temporal data injection always goes through Pipeline B: `download_dune_daily_C.py` → `build_daily_temporal.py` → `inject_daily_data.py`.

---

## Python Pipeline

**Mistake**: Importing `requests` in a new script.
**Rule**: **stdlib only**. Use `urllib.request` / `urllib.parse`. See pattern in `dune_client.py` and `extract_trillium.py`.

**Mistake**: Hardcoding derived values in Python scripts.
**Rule**: Python = RAW extraction only. No derived/computed values. Calculations happen in the HTML simulator or Google Sheet.

**Mistake**: Running the full pipeline to test a single module.
**Rule**: Use flags: `--model-only` (models only), `--full-extract` (force full re-extraction), `--export` (with Sheets).

**Mistake**: Modifying columns A-W of the epoch DB without updating both `build_database.py` AND `PLAN_COMPLET.md`.
**Rule**: The 23 RAW columns (A-W) are documented in `PLAN_COMPLET.md`. Any change must be consistent across `build_database.py`, `PLAN_COMPLET.md`, and `DATA_LINEAGE.md`.

---

## Revenue Models

**Mistake**: Confusing Trillium `priority_fees_sol` (non-vote only) with Solana Compass (vote + non-vote).
**Rule**: For the RAIKU model, always use **Trillium `priority_fees_sol`** (non-vote only). The SC/Trillium ratio ≈ 2.15x is expected and documented.

**Mistake**: Modifying hardcoded protocol constants without verification.
**Rule**: The **only** allowed hardcoded constants are: take rate 1-5%, validator share 95-99%, RAIKU token supply 1B. Everything else is a scenario parameter.

---

## Program Classification

**Mistake**: Prefix-inferred names like "Forge Protocol" or "Fragment Protocol" assumed to be real without verification.
**Rule**: Program names derived from address prefixes are **NOT real names**. Always verify on Solscan before keeping. If unverifiable, use pattern `"Trading Bot (prefix)"` or `"Unknown (prefix)"`.

**Mistake**: Leaving `raiku_product` empty for auto-classified programs.
**Rule**: Every program in `program_categories.csv` MUST have a `raiku_product` value. For `unknown` category with high CU: use `potential`. For known categories: derive from `TAXONOMY[category]["raiku_product"]`.

**Mistake**: Trying to write to a file open in VS Code on Windows.
**Rule**: VS Code locks files with exclusive OS-level locks on Windows. Cannot `cp`, `mv`, or `open(..., 'w')`. Must close the VS Code tab first.

---

## External APIs — Identification Sources

**SolanaFM API**: Persistently returns 502 Bad Gateway (as of 2026-03-11). Not reliable.
**Solscan API (api.solscan.io)**: DNS resolution fails from this Windows VM. Use Firecrawl scraping of `solscan.io/account/{address}` instead.
**Dune solana.labels**: Requires query execution credits. When out of credits, cannot look up program names.
**OtterSec verify.osec.io**: Only covers ~200 verified programs. Most are already known (Jupiter, Raydium, etc.).
**SolWatch (lead_pipeline_sheet.xlsx)**: 20630 rows but only matches ~1 unknown program (Pyth Receiver). Low hit rate for unknowns.

---

## Git / GitHub

**Rule**: Always verify `data/processed/` is in `.gitignore` before pushing (large generated files).
**Rule**: Check that no API keys are visible in any file before pushing (use `security-auditor`).

---

## Revenue Model — Core Waterfall Correction

> Recorded: 2026-03-10. ✅ IMPLEMENTED 2026-03-10.
> Status: Implemented in simulator (separate AOT/JIT panels, guard logic, ratio display) + Python models (jit_revenue.py, aot_revenue.py, sanity_check.py) + config.py.

**Mistake**: Rebates and validator bonus were applied directly on gross revenue, not constrained by the protocol take.

**Rule**: The correct revenue waterfall is strictly:

```
Gross Revenue = 100

Step 1 — Split gross revenue:
  Validator Base    = Gross Revenue × (1 - Protocol Take Rate)
  Protocol Pool     = Gross Revenue × Protocol Take Rate

Step 2 — Protocol redistributes its own pool:
  Customer Rebate   = funded from Protocol Pool
  Validator Bonus   = funded from Protocol Pool (AOT only)
  Raiku Treasury    = Protocol Pool − Rebate − Validator Bonus

Step 3 — Total check:
  Validator Base + Rebate + Validator Bonus + Raiku Treasury = Gross Revenue ✓
```

**Example (take rate = 5%, rebate = 1%, validator bonus = 0%)**:
```
Protocol Pool     = 5.00
Validator Base    = 95.00
Customer Rebate   = 1.00   (out of the 5, not out of 100)
Raiku Treasury    = 4.00
Total             = 100.00 ✓
```

**Guard rule**: If Protocol Take Rate = 0%, then Rebate = 0 and Validator Bonus = 0 must be enforced. The model must not allow positive rebate or bonus when there is no protocol pool to fund them.

**AOT vs JIT separation**:
- AOT: has Protocol Take Rate + Customer Rebate + Validator Bonus (separate parameter panel)
- JIT: has Protocol Take Rate + Customer Rebate only (no Validator Bonus)
- Both follow the same top-level waterfall rule

**Naming correction**:
- Rename "commission" → "Protocol Take Rate" everywhere (simulator labels, sliders, tooltips, Python variable names, documentation)

**Economic interpretation**:
- 1% rebate on gross = 20% of the protocol take (when take rate = 5%)
- The simulator should make this ratio explicit for clarity

**Files that must be updated when implemented**:
- `raiku_revenue_simulator.html` — slider labels, waterfall formula, guard logic, AOT/JIT separation
- `03_model/jit_revenue.py` — waterfall logic
- `03_model/aot_revenue.py` — waterfall logic + validator bonus handling
- `CLAUDE.md` → Protocol Constants section
- `DATA_LINEAGE.md` — if output column names change

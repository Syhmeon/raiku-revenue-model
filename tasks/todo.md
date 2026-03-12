# RAIKU Revenue Model — Task Tracker

> **Instruction for Claude**: Read this file at the start of every session.
> Update "In Progress" and "Recently Completed" at the end of each session.

---

## Project State (updated: 2026-03-10)

**Simulator**: v6, 2683 lines, functional — deployed on GitHub Pages
**Pipeline A**: Complete (786×43 epoch DB, 500-program DB, JIT+AOT models)
**Pipeline B**: Complete (6 Dune queries → D.daily + D.dailyNet injected)
**Google Sheets export**: Deferred (replaced by HTML simulator)

---

## In Progress
<!-- Fill at the start of each session -->
- [ ] Update downstream `BIZ_CATEGORIES` in `build_daily_temporal.py` and simulator HTML (deferred task)

---

## To Do — Features

### Tab 1: Revenue Model
- [ ] Improve AOT scenario calculations
- [ ] Add Trillium real-time data source
- [ ] Refresh epoch data (Pipeline A)

### Tab 2: AOT Block Simulator
- [ ] [Features to define]

### Tab 3: Solana General Data
- [ ] [Features to define]

### Infrastructure / Pipeline
- [ ] Set up automatic Dune data refresh
- [ ] [Other pipeline tasks]

---

## Recently Completed

- [x] Dune Q1 (program fees v3) — 461 programs, proper base/priority split, saved to dune_program_fees_v3.csv ✅ (2026-03-12)
- [x] Dune Q2 (Jito tips QC 48h) — 343 programs, 42 with Jito txs, saved to dune_jito_tips_qc_48h.csv ✅ (2026-03-12)
- [x] Transform pipeline rerun — program_database.csv (461×23) + program_conditions.csv (55×27) rebuilt ✅ (2026-03-12)
- [x] Program categories reclassified + copied to program_categories.csv ✅ (2026-03-12)
- [x] Program taxonomy reclassification — 616 programs, 15 RAIKU categories, zero empty raiku_product ✅ (2026-03-11)
- [x] Program investigation — 6/80 unknown programs identified via Solscan/GitHub/web research ✅ (2026-03-11)
- [x] classify_programs.py — full rewrite with TAXONOMY dict, MANUAL_OVERRIDES (22 entries), Task 1+2 logic ✅ (2026-03-11)
- [x] Revenue waterfall correction — all files updated, pipeline verified ✅ (2026-03-10)
- [x] Agent workflow infrastructure complete — 6 custom agents + task tracking ✅ (2026-03-10)
- [x] Pipeline A complete — epoch DB 786×43, program DB 500×23 ✅
- [x] Pipeline B complete — 6 Dune batch queries, temporal injection ✅
- [x] Simulator v6 — D.daily + D.dailyNet injected ✅
- [x] Revenue models — JIT + AOT (top-down + bottom-up) ✅
- [x] Conditions pipeline — market condition × program analysis ✅
- [x] Revenue model column bugs fixed — March 2026 ✅

---

## Blocked / Waiting

- [ ] Google Sheets export — `04_output/sheets_export.py` (deferred, low priority)
- [ ] BigQuery × Token Terminal — GCP setup too heavy, deferred
- [ ] Token Terminal for Sheets — not needed for core revenue model

---

## Session Notes
<!-- Add key decisions made during the session -->

### 2026-03-12 — Dune Extraction + Transform Pipeline Complete
- **Session 1**: Built SQL queries, explored Dune schema, found base/priority bug in old aggregate
- **Session 2**: Switched to Compte B (@syh) API key (2260 credits available)
- Q1 executed (query 6817783): 461 programs × 14 columns, proper base/priority split → `dune_program_fees_v3.csv`
- Q2 created via MCP (query 6818146, made public), executed via Compte B REST API
- Q2 results: 343 programs, 42 with Jito txs, 1854 SOL tips in 48h → `dune_jito_tips_qc_48h.csv`
- Updated `build_program_database.py` to read v3 file (path + `avg_cu_per_tx` column name)
- Rebuilt: `program_database.csv` (461×23, 226 classified) + `program_conditions.csv` (55×27)
- Cleaned up temp `data/q2_clean.sql`
- **NEXT**: Update BIZ_CATEGORIES downstream, integrate Jito QC data into simulator/model

### 2026-03-11 — Program Taxonomy Reclassification + Investigation
- Reclassified 314 existing programs from 20 ad-hoc categories → 15 RAIKU taxonomy categories
- Added ~302 new programs from `dune_program_fees_v2.csv` (auto-classified by name patterns)
- Investigated 80 unknown programs (41 Group A high-CU + 39 Group B empty-product)
- Identified 6 programs: idem arb bot, Bull-or-Bear, CFL, SAbEr arb bot, Pyth Receiver, Huma Finance
- Sources exhausted: Solscan, GitHub, OtterSec, SolanaFM (502), Dune (0 credits), SolWatch
- Blocking issue resolved: all 39 Group B programs now have `raiku_product` assigned
- Output at `program_categories_new.csv` (original locked by VS Code — pending copy)
- **NEXT**: Close VS Code tab, copy new CSV over, update BIZ_CATEGORIES downstream

### 2026-03-10 — Agent Infrastructure Setup
- Created 6 custom agents in `.claude/agents/`: python-pipeline-dev, html-simulator-dev, dune-data-fetcher, revenue-model-analyst, solana-data-analyst, competitive-analyst
- Added task tracking system: `tasks/todo.md` + `tasks/lessons.md`
- Updated CLAUDE.md with Session Initialization workflow and Subagent Routing table
- Committed: cd688f1 "feat: add agent workflow infrastructure and project organization"
- **NEXT**: Implement revenue waterfall correction (staged in lessons.md)

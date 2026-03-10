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
- No tasks in progress currently

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

### 2026-03-10 — Agent Infrastructure Setup
- Created 6 custom agents in `.claude/agents/`: python-pipeline-dev, html-simulator-dev, dune-data-fetcher, revenue-model-analyst, solana-data-analyst, competitive-analyst
- Added task tracking system: `tasks/todo.md` + `tasks/lessons.md`
- Updated CLAUDE.md with Session Initialization workflow and Subagent Routing table
- Committed: cd688f1 "feat: add agent workflow infrastructure and project organization"
- **NEXT**: Implement revenue waterfall correction (staged in lessons.md)

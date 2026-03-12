# AGENTS.md

## Project
RAIKU Revenue Model

## Goal
Maintain and extend the Solana data pipeline and revenue model with minimal, safe, auditable changes.

## Tech stack
Python, SQL, CSV/XLSX data inputs, VS Code, local scripts.

## Source of truth
Read these first when relevant:
- README.md
- PLAN_COMPLET.md
- docs/PROJECT_CONTEXT.md
- docs/DATA_LINEAGE.md
- todo.md

## Code rules
- Prefer minimal diffs
- Do not rewrite architecture unless necessary
- Preserve existing file naming and pipeline conventions
- Ask before destructive refactors
- When editing Python, keep functions small and explicit
- When editing SQL, preserve current output schema unless task explicitly changes it

## Validation
- Explain assumptions
- Run or propose the smallest relevant validation
- Summarize modified files at the end

## Important files
- scripts/build_daily_temporal.py
- scripts/classify_programs.py
- sql_archive/
- data/raw/
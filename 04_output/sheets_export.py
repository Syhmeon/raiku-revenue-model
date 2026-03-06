"""
Google Sheets Export — RAW data + Sheet formulas (v4)
=====================================================
Exports processed CSVs to Google Sheets and writes formulas for ALL calculations.

Architecture:
  Python pushes RAW data only → Google Sheet formulas do ALL calculations.
  This makes every number traceable and auditable.

Tabs created:
  1. "Parameters"       : Single source of truth for all adjustable parameters
  2. "Epoch Database"   : RAW cols + 18 FORMULA cols (positions computed dynamically)
  3. "Program Database" : Per-program fee/CU data (top 500)
  4. "Program Mapping"  : Classification → RAIKU archetypes
  5. "JIT Model"        : JIT revenue scenarios (all Sheet formulas)
  6. "AOT Top-Down"     : AOT macro model (all Sheet formulas)
  7. "AOT Bottom-Up"    : AOT 3D framework (all Sheet formulas)
  8. "Revenue Waterfall" : Protocol fee decomposition
  9. "Revenue Summary"  : Consolidated view (all Sheet formulas)
  10. "Data Sources"    : Documentation of every column

Requires:
  pip install gspread
  service_account.json in project root

Usage:
  python 04_output/sheets_export.py              # Export all tabs
  python 04_output/sheets_export.py --tab epoch  # Single tab
  python 04_output/sheets_export.py --dry-run    # Preview
"""

import argparse
import csv
import sys
import time
from pathlib import Path

try:
    import gspread
    from gspread.utils import rowcol_to_a1
except ImportError:
    print("ERROR: gspread not installed. Run: pip install gspread")
    sys.exit(1)

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    DATA_PROCESSED, DATA_MAPPING, CSV_DELIMITER, CSV_ENCODING,
    GOOGLE_SHEET_ID, SERVICE_ACCOUNT_FILE,
)

# ── Constants ─────────────────────────────────────────────
BATCH_SIZE = 500
RATE_LIMIT_SLEEP = 1.5  # seconds between API calls


# ── Utility functions ─────────────────────────────────────

def load_csv(filepath):
    """Load a semicolon-delimited CSV → (headers, data_rows)."""
    if not filepath.exists():
        print(f"  WARNING: {filepath} not found")
        return [], []
    with open(filepath, "r", encoding=CSV_ENCODING) as f:
        content = f.read()
        if content.startswith("\ufeff"):
            content = content[1:]
        reader = csv.reader(content.splitlines(), delimiter=CSV_DELIMITER)
        rows = list(reader)
    if not rows:
        return [], []
    headers = rows[0]
    data = rows[1:]
    while data and all(cell.strip() == "" for cell in data[-1]):
        data.pop()
    return headers, data


def convert_numerics(rows):
    """Convert numeric strings to proper Python numbers for gspread."""
    converted = []
    for row in rows:
        new_row = []
        for cell in row:
            cell = cell.strip()
            if cell == "":
                new_row.append("")
                continue
            try:
                val = int(cell)
                new_row.append(val)
                continue
            except ValueError:
                pass
            try:
                val = float(cell)
                new_row.append(val)
                continue
            except ValueError:
                pass
            new_row.append(cell)
        converted.append(new_row)
    return converted


def get_or_create_ws(spreadsheet, name, rows, cols):
    """Get or create a worksheet, resizing if needed."""
    try:
        ws = spreadsheet.worksheet(name)
        if ws.row_count < rows:
            ws.resize(rows=rows)
        if ws.col_count < cols:
            ws.resize(cols=cols)
        return ws, False
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=name, rows=max(rows, 100), cols=max(cols, 30))
        return ws, True


def write_batch(ws, all_data, total_cols):
    """Write data in batches to avoid API limits."""
    for i in range(0, len(all_data), BATCH_SIZE):
        batch = all_data[i:i + BATCH_SIZE]
        start_row = i + 1
        end_row = start_row + len(batch) - 1
        cell_range = f"{rowcol_to_a1(start_row, 1)}:{rowcol_to_a1(end_row, total_cols)}"
        ws.update(cell_range, batch, value_input_option="USER_ENTERED")
        if len(all_data) > BATCH_SIZE:
            print(f"    Written rows {start_row}-{end_row}/{len(all_data)}")
            time.sleep(RATE_LIMIT_SLEEP)


def format_header(ws, bg_color=None):
    """Bold + freeze header row."""
    if bg_color is None:
        bg_color = {"red": 0.15, "green": 0.15, "blue": 0.25}
    ws.format("1:1", {
        "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
        "backgroundColor": bg_color,
    })
    ws.freeze(rows=1)
    time.sleep(RATE_LIMIT_SLEEP)


def col_letter(n):
    """Convert 1-indexed column number to letter(s). 1=A, 27=AA."""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


# ── Epoch formula column definitions (must be before TAB 1) ─────────

EPOCH_FORMULA_HEADERS = [
    "total_rewards_sol",            # n+1:  = D+E+H
    "total_rewards_usd",            # n+2:  = (n+1)*Q
    "staker_rewards_sol",           # n+3:  = D-L (L = validator inflation share)
    "fee_pct_of_total",             # n+4:  = E/(n+1)
    "mev_pct_of_total",             # n+5:  = H/(n+1)
    "effective_commission",          # n+6:  = M (avg commission rate from Dune)
    "inflation_apr",                # n+7:  = D*(365.25/C)/P
    "fee_apr",                      # n+8:  = E*(365.25/C)/P
    "mev_apr",                      # n+9:  = H*(365.25/C)/P
    "total_apr",                    # n+10: = (n+7)+(n+8)+(n+9)
    "total_apy",                    # n+11: = (1+(n+10)/365.25)^365.25-1
    "total_supply_sol",             # n+12: = R/Q
    "staked_ratio",                 # n+13: = P/(n+12)
    "burn_sol",                     # n+14: = IF(A<620, E*0.5, 0)
    "net_inflation_sol",            # n+15: = D-(n+14)
    "annual_total_fees_usd",        # n+16: = E*(365.25/C)*Q
    "annual_mev_usd",               # n+17: = H*(365.25/C)*Q
    "annual_priority_fees_usd",     # n+18: = F*(365.25/C)*Q
]


def _build_formula_col_map(n_raw):
    """Build a mapping of formula name → actual column letter based on n_raw columns."""
    mapping = {}
    for i, name in enumerate(EPOCH_FORMULA_HEADERS):
        mapping[name] = col_letter(n_raw + 1 + i)
    return mapping


def _get_epoch_col_map():
    """Compute actual formula column letters from the epoch database CSV.

    Returns dict mapping formula column names to their actual Sheet letters.
    This is critical because the number of RAW columns determines where
    formula columns start. E.g. with 43 RAW cols, annual_mev_usd is at BH,
    not AN as the old hardcoded comments assumed.
    """
    filepath = DATA_PROCESSED / "solana_epoch_database.csv"
    if filepath.exists():
        headers, _ = load_csv(filepath)
        n_raw = len(headers) if headers else 23
    else:
        n_raw = 23  # fallback
    return _build_formula_col_map(n_raw)


# Compute once at import time
EPOCH_FCOLS = _get_epoch_col_map()


# ══════════════════════════════════════════════════════════
# TAB 1: PARAMETERS (NEW)
# ══════════════════════════════════════════════════════════

def export_parameters(spreadsheet, dry_run=False):
    """Create Parameters tab — single source of truth for all adjustable parameters."""
    print("  Parameters: building single source of truth tab")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "Parameters", 40, 6)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)

    # Dynamic column letters for Epoch Database formula columns
    MEV_COL = EPOCH_FCOLS["annual_mev_usd"]
    PF_COL = EPOCH_FCOLS["annual_priority_fees_usd"]

    data = [
        # Row 1: Title
        ["RAIKU Revenue Model — Parameters (Single Source of Truth)", "", "", "", "", ""],
        # Row 2: Blank
        ["", "", "", "", "", ""],
        # Row 3: Section header
        ["MARKET DATA (auto-populated from Epoch Database)", "", "", "", "", ""],
        # Row 4: Column headers
        ["Parameter", "Value", "Unit", "Source", "", ""],
        # Row 5: SOL Price (latest)
        ["SOL Price (latest)",
         "=INDEX('Epoch Database'!Q:Q, MATCH(9.99E+307, 'Epoch Database'!Q:Q, 1))",
         "USD", "Epoch Database", "", ""],
        # Row 6: Annual MEV (latest)
        ["Annual MEV (latest epoch)",
         f"=INDEX('Epoch Database'!{MEV_COL}:{MEV_COL}, MATCH(9.99E+307, 'Epoch Database'!{MEV_COL}:{MEV_COL}, 1))",
         "USD/yr", "Epoch Database", "", ""],
        # Row 7: Annual MEV (avg 10)
        ["Annual MEV (avg 10 epochs)",
         f"=AVERAGE(INDEX('Epoch Database'!{MEV_COL}:{MEV_COL}, COUNTA('Epoch Database'!{MEV_COL}:{MEV_COL})-9):INDEX('Epoch Database'!{MEV_COL}:{MEV_COL}, COUNTA('Epoch Database'!{MEV_COL}:{MEV_COL})))",
         "USD/yr", "Epoch Database", "", ""],
        # Row 8: Annual Priority Fees (latest)
        ["Annual Priority Fees (latest)",
         f"=INDEX('Epoch Database'!{PF_COL}:{PF_COL}, MATCH(9.99E+307, 'Epoch Database'!{PF_COL}:{PF_COL}, 1))",
         "USD/yr", "Epoch Database", "", ""],
        # Row 9: Annual Priority Fees (avg 10)
        ["Annual Priority Fees (avg 10)",
         f"=AVERAGE(INDEX('Epoch Database'!{PF_COL}:{PF_COL}, COUNTA('Epoch Database'!{PF_COL}:{PF_COL})-9):INDEX('Epoch Database'!{PF_COL}:{PF_COL}, COUNTA('Epoch Database'!{PF_COL}:{PF_COL})))",
         "USD/yr", "Epoch Database", "", ""],
        # Row 10: Blank
        ["", "", "", "", "", ""],
        # Row 11: Section header
        ["NETWORK CONSTANTS", "", "", "", "", ""],
        # Row 12: Column headers
        ["Parameter", "Value", "Unit", "Description", "", ""],
        # Row 13: Slots per year
        ["Slots per year", 78894000, "slots", "Solana target: 2.5 slots/sec × 86400 × 365.25", "", ""],
        # Row 14: Max CU per block
        ["Max CU per block", 48000000, "CU", "Solana protocol constant", "", ""],
        # Row 15: Blank
        ["", "", "", "", "", ""],
        # Row 16: Section header
        ["RAIKU ASSUMPTIONS (editable)", "", "", "", "", ""],
        # Row 17: Column headers
        ["Parameter", "Value", "Unit", "Description", "", ""],
        # Row 18: Protocol fee (AOT)
        ["Protocol fee (AOT)", 0.05, "%", "Governance range 1-5%", "", ""],
        # Row 19: Protocol fee (JIT)
        ["Protocol fee (JIT)", 0.05, "%", "Governance range 1-5%", "", ""],
        # Row 20: RAIKU stake %
        ["RAIKU stake %", 0.05, "%", "Network share assumption", "", ""],
        # Row 21: Latency-sensitive share
        ["Latency-sensitive share", 0.40, "%", "% of priority fees from latency-sensitive ops", "", ""],
        # Row 22: Customer rebate rate (AOT)
        ["Customer rebate rate (AOT)", 0.005, "%", "Post-TGE Design: 0.25-1% on AOT", "", ""],
        # Row 23: Customer rebate rate (JIT)
        ["Customer rebate rate (JIT)", 0.0025, "%", "Post-TGE Design: 0-0.5% on JIT", "", ""],
        # Row 24: Validator enhancement rate
        ["Validator enhancement rate", 0.015, "%", "Post-TGE Design: 5%→3.5% for qualifying validators", "", ""],
        # Row 25: % validators qualifying
        ["% validators qualifying", 0.80, "%", "Assumption", "", ""],
        # Row 26: Treasury allocation — Operations
        ["Treasury alloc: Operations", 0.40, "%", "Indicative split of net treasury", "", ""],
        # Row 27: Treasury allocation — Growth
        ["Treasury alloc: Growth & Incentives", 0.35, "%", "Indicative split of net treasury", "", ""],
        # Row 28: Treasury allocation — Value Accrual
        ["Treasury alloc: Value Accrual", 0.25, "%", "Indicative split of net treasury (must sum to 100%)", "", ""],
        # Row 29: Blank
        ["", "", "", "", "", ""],
        # Row 30: Section header
        ["EXTERNAL REFERENCES", "", "", "", "", ""],
        # Row 31: Column headers
        ["Parameter", "Value", "Unit", "Description", "", ""],
        # Row 32: Jito Q4-2025 annualized (computed from Epoch Database)
        ["Jito Q4-2025 annualized",
         '=IFERROR(SUMPRODUCT((\'Epoch Database\'!B$2:B$1000>="2025-10-01")*(\'Epoch Database\'!B$2:B$1000<="2025-12-31")*\'Epoch Database\'!H$2:H$1000*\'Epoch Database\'!Q$2:Q$1000)*365.25/SUMPRODUCT((\'Epoch Database\'!B$2:B$1000>="2025-10-01")*(\'Epoch Database\'!B$2:B$1000<="2025-12-31")*\'Epoch Database\'!C$2:C$1000), 0)',
         "USD/yr", "Computed: annualized MEV from Q4-2025 epochs", "", ""],
        # Row 33: Jito 2025 full year (computed from Epoch Database)
        ["Jito 2025 full year",
         '=IFERROR(SUMPRODUCT((\'Epoch Database\'!B$2:B$1000>="2025-01-01")*(\'Epoch Database\'!B$2:B$1000<="2025-12-31")*\'Epoch Database\'!H$2:H$1000*\'Epoch Database\'!Q$2:Q$1000)*365.25/SUMPRODUCT((\'Epoch Database\'!B$2:B$1000>="2025-01-01")*(\'Epoch Database\'!B$2:B$1000<="2025-12-31")*\'Epoch Database\'!C$2:C$1000), 0)',
         "USD/yr", "Computed: annualized MEV from all 2025 epochs", "", ""],
    ]

    write_batch(ws, data, 6)

    # Format title
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})

    # Format section headers (rows 3, 11, 16, 30)
    for sr in [3, 11, 16, 30]:
        ws.format(f"A{sr}", {"textFormat": {"bold": True, "fontSize": 12}})

    # Format column header rows (rows 4, 12, 17, 31)
    for hr in [4, 12, 17, 31]:
        ws.format(f"A{hr}:D{hr}", {
            "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.3},
            "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
        })

    # Format percentage columns (B18:B28 — includes treasury allocations)
    ws.format("B18:B28", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})

    # Format currency columns (B5:B9, B32:B33)
    ws.format("B5:B9", {"numberFormat": {"type": "NUMBER", "pattern": "$#,##0"}})
    ws.format("B32:B33", {"numberFormat": {"type": "NUMBER", "pattern": "$#,##0"}})

    # Format network constants (B13:B14)
    ws.format("B13:B14", {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})

    ws.freeze(rows=1)
    time.sleep(RATE_LIMIT_SLEEP)

    print(f"  ✓ Parameters: single source of truth created")
    return True


# ══════════════════════════════════════════════════════════
# TAB 2: EPOCH DATABASE
# ══════════════════════════════════════════════════════════

# Note: EPOCH_FORMULA_HEADERS and _build_formula_col_map are defined above
# (before TAB 1) because they are needed at module-level for EPOCH_FCOLS.


def epoch_formula(col_name, row, fcols):
    """Return the Google Sheet formula for a given column at a given row.

    Args:
        col_name: Name of the formula column
        row: Sheet row number
        fcols: Dict mapping formula column names → actual column letters
               (built by _build_formula_col_map)
    """
    r = str(row)
    # Formula column letters (computed dynamically from n_raw)
    F_total_rewards = fcols["total_rewards_sol"]
    F_inflation_apr = fcols["inflation_apr"]
    F_fee_apr = fcols["fee_apr"]
    F_mev_apr = fcols["mev_apr"]
    F_total_apr = fcols["total_apr"]
    F_total_supply = fcols["total_supply_sol"]
    F_burn = fcols["burn_sol"]

    formulas = {
        "total_rewards_sol":        f'=IF(D{r}<>"", D{r}+E{r}+H{r}, "")',
        "total_rewards_usd":        f'=IF({F_total_rewards}{r}<>"", {F_total_rewards}{r}*Q{r}, "")',
        "staker_rewards_sol":       f'=IF(AND(D{r}<>"",L{r}<>""), D{r}-L{r}, "")',
        "fee_pct_of_total":         f'=IF(AND({F_total_rewards}{r}<>"",{F_total_rewards}{r}>0), E{r}/{F_total_rewards}{r}, "")',
        "mev_pct_of_total":         f'=IF(AND({F_total_rewards}{r}<>"",{F_total_rewards}{r}>0), H{r}/{F_total_rewards}{r}, "")',
        "effective_commission":      f'=IF(M{r}<>"", M{r}, "")',
        "inflation_apr":            f'=IF(AND(P{r}<>"",P{r}>0,C{r}<>"",C{r}>0), D{r}*(365.25/C{r})/P{r}, "")',
        "fee_apr":                  f'=IF(AND(P{r}<>"",P{r}>0,C{r}<>"",C{r}>0), E{r}*(365.25/C{r})/P{r}, "")',
        "mev_apr":                  f'=IF(AND(P{r}<>"",P{r}>0,C{r}<>"",C{r}>0,H{r}<>""), H{r}*(365.25/C{r})/P{r}, "")',
        "total_apr":                f'=IF(AND({F_inflation_apr}{r}<>"",{F_fee_apr}{r}<>""), {F_inflation_apr}{r}+{F_fee_apr}{r}+IF({F_mev_apr}{r}<>"",{F_mev_apr}{r},0), "")',
        "total_apy":               f'=IF(AND({F_total_apr}{r}<>"",{F_total_apr}{r}>0), (1+{F_total_apr}{r}/365.25)^365.25-1, "")',
        "total_supply_sol":         f'=IF(AND(R{r}<>"",Q{r}<>"",Q{r}>0), R{r}/Q{r}, "")',
        "staked_ratio":             f'=IF(AND(P{r}<>"",{F_total_supply}{r}<>"",{F_total_supply}{r}>0), P{r}/{F_total_supply}{r}, "")',
        "burn_sol":                 f'=IF(A{r}<620, E{r}*0.5, 0)',
        "net_inflation_sol":        f'=IF(D{r}<>"", D{r}-{F_burn}{r}, "")',
        "annual_total_fees_usd":    f'=IF(AND(C{r}<>"",C{r}>0,Q{r}<>""), E{r}*(365.25/C{r})*Q{r}, "")',
        "annual_mev_usd":           f'=IF(AND(C{r}<>"",C{r}>0,Q{r}<>"",H{r}<>""), H{r}*(365.25/C{r})*Q{r}, "")',
        "annual_priority_fees_usd": f'=IF(AND(C{r}<>"",C{r}>0,Q{r}<>"",F{r}<>""), F{r}*(365.25/C{r})*Q{r}, "")',
    }
    return formulas.get(col_name, "")


def export_epoch_database(spreadsheet, dry_run=False):
    """Export epoch database with RAW data + formula columns."""
    filepath = DATA_PROCESSED / "solana_epoch_database.csv"
    headers, data = load_csv(filepath)
    if not headers:
        print("  SKIP: No epoch database found")
        return False

    data = convert_numerics(data)
    n_raw = len(headers)
    n_formula = len(EPOCH_FORMULA_HEADERS)
    total_cols = n_raw + n_formula
    total_rows = len(data) + 1

    # Use precomputed formula column mapping (adapts to however many RAW columns exist)
    fcols = _build_formula_col_map(n_raw)

    print(f"  Epoch Database: {len(data)} rows × {n_raw} RAW + {n_formula} FORMULA = {total_cols} cols")
    print(f"    Formula columns: {fcols['total_rewards_sol']}-{fcols['annual_priority_fees_usd']}")

    if dry_run:
        return True

    ws, created = get_or_create_ws(spreadsheet, "Epoch Database", total_rows + 5, total_cols + 2)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)

    # Write RAW headers + formula headers
    all_headers = headers + EPOCH_FORMULA_HEADERS
    ws.update(f"A1:{col_letter(total_cols)}1", [all_headers], value_input_option="USER_ENTERED")
    time.sleep(RATE_LIMIT_SLEEP)

    # Write RAW data in batches
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        start_row = i + 2
        end_row = start_row + len(batch) - 1
        cell_range = f"A{start_row}:{col_letter(n_raw)}{end_row}"
        ws.update(cell_range, batch, value_input_option="USER_ENTERED")
        print(f"    RAW data: rows {start_row}-{end_row}")
        time.sleep(RATE_LIMIT_SLEEP)

    # Write FORMULA columns in batches
    formula_start_col = n_raw + 1
    for i in range(0, len(data), BATCH_SIZE):
        batch_formulas = []
        for j in range(BATCH_SIZE):
            row_idx = i + j
            if row_idx >= len(data):
                break
            row_num = row_idx + 2
            row_formulas = [epoch_formula(col, row_num, fcols) for col in EPOCH_FORMULA_HEADERS]
            batch_formulas.append(row_formulas)

        if batch_formulas:
            start_row = i + 2
            end_row = start_row + len(batch_formulas) - 1
            cell_range = f"{col_letter(formula_start_col)}{start_row}:{col_letter(total_cols)}{end_row}"
            ws.update(cell_range, batch_formulas, value_input_option="USER_ENTERED")
            print(f"    Formulas: rows {start_row}-{end_row}")
            time.sleep(RATE_LIMIT_SLEEP)

    format_header(ws)
    # Color formula columns differently
    formula_range = f"{col_letter(formula_start_col)}1:{col_letter(total_cols)}1"
    ws.format(formula_range, {
        "backgroundColor": {"red": 0.1, "green": 0.3, "blue": 0.2},
        "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
    })
    time.sleep(RATE_LIMIT_SLEEP)

    print(f"  ✓ Epoch Database: {len(data)} rows × {total_cols} cols ({n_formula} formula cols)")
    return True


# ══════════════════════════════════════════════════════════
# TAB 3: PROGRAM DATABASE
# ══════════════════════════════════════════════════════════

def export_program_database(spreadsheet, dry_run=False):
    """Export program database (RAW only)."""
    filepath = DATA_PROCESSED / "program_database.csv"
    headers, data = load_csv(filepath)
    if not headers:
        print("  SKIP: No program database found")
        return False

    data = convert_numerics(data)
    print(f"  Program Database: {len(data)} rows × {len(headers)} cols")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "Program Database", len(data) + 5, len(headers) + 2)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)
    write_batch(ws, [headers] + data, len(headers))
    format_header(ws, bg_color={"red": 0.2, "green": 0.15, "blue": 0.25})
    print(f"  ✓ Program Database: {len(data)} rows × {len(headers)} cols")
    return True


# ══════════════════════════════════════════════════════════
# TAB 4: PROGRAM MAPPING
# ══════════════════════════════════════════════════════════

def export_program_mapping(spreadsheet, dry_run=False):
    """Export program category mapping."""
    filepath = DATA_MAPPING / "program_categories.csv"
    headers, data = load_csv(filepath)
    if not headers:
        print("  SKIP: No mapping file found")
        return False

    print(f"  Program Mapping: {len(data)} programs mapped")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "Program Mapping", len(data) + 5, len(headers) + 2)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)
    write_batch(ws, [headers] + data, len(headers))
    format_header(ws, bg_color={"red": 0.25, "green": 0.15, "blue": 0.15})
    print(f"  ✓ Program Mapping: {len(data)} rows")
    return True


# ══════════════════════════════════════════════════════════
# TAB 5: JIT MODEL (REFACTORED)
# ══════════════════════════════════════════════════════════

# Layout constants — used by Revenue Summary to compute cross-tab references
JIT_PARAM_ROW_PROTOCOL_FEE = 8           # C8 = protocol fee
JIT_SCENARIO_START_ROW = 12              # First scenario row
JIT_MARKET_SOURCES = [                   # (label, cell_ref_for_total_market)
    ("Latest epoch annualized", "C4"),
    ("Avg 10 epochs", "C5"),
    ("Q4-2025 bear", "C6"),
    ("2025 full year bull", "C7"),
]
JIT_SHARES = [0.02, 0.05, 0.10, 0.15, 0.20]
# Columns: A=source label, B=share, C=gross, D=validator, E=protocol, F=protocol/month


def jit_scenario_row(source_idx, share_idx):
    """Compute the sheet row number for a JIT scenario."""
    return JIT_SCENARIO_START_ROW + source_idx * len(JIT_SHARES) + share_idx


def export_jit_model(spreadsheet, dry_run=False):
    """Create JIT revenue model tab with all formulas (v4: refs Parameters tab)."""
    print("  JIT Model: building formula-based tab (v4)")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "JIT Model", 50, 12)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)

    # Header section (rows 1-11)
    header_data = [
        ["JIT Revenue Model", "", "", "", "", ""],
        ["Formula: Total_Jito_MEV × RAIKU_Market_Share (gross) | Protocol = Gross × Fee", "", "", "", "", ""],
        ["", "", "", "", "", ""],
        # Row 4: Latest epoch annualized MEV
        ["Total Jito MEV market (latest epoch, annualized)", "",
         "=Parameters!B6",
         "USD/yr", "← From Parameters tab", ""],
        # Row 5: Avg 10 epochs
        ["Total Jito MEV market (avg last 10 epochs)", "",
         "=Parameters!B7",
         "USD/yr", "← From Parameters tab", ""],
        # Row 6: Q4-2025 bear reference
        ["Jito Q4-2025 annualized (bear)", "",
         "=Parameters!B32",
         "USD/yr", "← Computed from Epoch Database", ""],
        # Row 7: 2025 full year bull reference
        ["Jito 2025 full year (bull)", "",
         "=Parameters!B33",
         "USD/yr", "← Computed from Epoch Database", ""],
        # Row 8: Protocol fee
        ["Protocol fee (JIT)", "",
         "=Parameters!B19",
         "", "← From Parameters tab", ""],
        # Row 9: blank
        ["", "", "", "", "", ""],
        # Row 10: section label
        ["SCENARIO MATRIX", "", "", "", "", ""],
        # Row 11: column headers
        ["Total Market Source", "Market Share", "Gross Revenue", "Validator (95%)", "Protocol (5%)", "Protocol/month"],
    ]

    # Scenario rows starting at row 12
    scenario_data = []
    for src_idx, (label, cell_ref) in enumerate(JIT_MARKET_SOURCES):
        for sh_idx, share in enumerate(JIT_SHARES):
            r = jit_scenario_row(src_idx, sh_idx)
            scenario_data.append([
                label,
                share,
                f'={cell_ref}*B{r}',              # C: Gross = market × share
                f'=C{r}*(1-$C$8)',                 # D: Validator = gross × (1-fee)
                f'=C{r}*$C$8',                     # E: Protocol = gross × fee
                f'=E{r}/12',                       # F: Protocol/month
            ])

    all_data = header_data + scenario_data
    write_batch(ws, all_data, 6)

    last_row = JIT_SCENARIO_START_ROW + len(scenario_data) - 1

    # Format
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})
    ws.format("A3", {"textFormat": {"bold": True}})
    ws.format("A10", {"textFormat": {"bold": True}})
    ws.format("A11:F11", {
        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.3},
        "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
    })
    ws.freeze(rows=11)
    time.sleep(RATE_LIMIT_SLEEP)

    # Number formats
    ws.format("C4:C7", {"numberFormat": {"type": "NUMBER", "pattern": "$#,##0"}})
    ws.format(f"B12:B{last_row}", {"numberFormat": {"type": "PERCENT", "pattern": "0%"}})
    ws.format(f"C12:F{last_row}", {"numberFormat": {"type": "NUMBER", "pattern": "$#,##0"}})
    ws.format("C8", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})
    time.sleep(RATE_LIMIT_SLEEP)

    print(f"  ✓ JIT Model: {len(scenario_data)} scenarios (rows {JIT_SCENARIO_START_ROW}-{last_row})")
    return True


# ══════════════════════════════════════════════════════════
# TAB 6: AOT TOP-DOWN (REFACTORED)
# ══════════════════════════════════════════════════════════

# Layout constants — used by Revenue Summary
AOT_TD_PARAM_ROW_PROTOCOL_FEE = 7       # C7 = protocol fee
AOT_TD_SCENARIO_START_ROW = 11          # First scenario row
AOT_TD_FEE_SOURCES = [                  # (label, cell_ref)
    ("Latest", "C5"),
    ("Avg 10ep", "C6"),
]
AOT_TD_LAT_SHARES = [0.30, 0.40, 0.50, 0.60]
AOT_TD_CAPTURES = [0.05, 0.10, 0.15, 0.20]
# Columns: A=source, B=lat%, C=capture%, D=addressable, E=gross, F=protocol, G=protocol/mo


def aot_td_scenario_row(source_idx, lat_idx, cap_idx):
    """Compute the sheet row number for an AOT Top-Down scenario."""
    scenarios_per_source = len(AOT_TD_LAT_SHARES) * len(AOT_TD_CAPTURES)
    scenarios_per_lat = len(AOT_TD_CAPTURES)
    return (AOT_TD_SCENARIO_START_ROW
            + source_idx * scenarios_per_source
            + lat_idx * scenarios_per_lat
            + cap_idx)


def export_aot_topdown(spreadsheet, dry_run=False):
    """Create AOT top-down model tab with formulas (v4: refs Parameters tab)."""
    print("  AOT Top-Down: building formula-based tab (v4)")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "AOT Top-Down", 60, 10)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)

    header_data = [
        # Row 1
        ["AOT Revenue Model — Top-Down", "", "", "", "", "", ""],
        # Row 2
        ["Formula: Total_Priority_Fees × Latency_Sensitive_% × RAIKU_Capture_%", "", "", "", "", "", ""],
        # Row 3
        ["", "", "", "", "", "", ""],
        # Row 4
        ["PARAMETERS", "", "", "", "", "", ""],
        # Row 5
        ["Annual priority fees (latest epoch)", "",
         "=Parameters!B8",
         "USD/yr", "← From Parameters tab", "", ""],
        # Row 6
        ["Annual priority fees (avg 10 epochs)", "",
         "=Parameters!B9",
         "USD/yr", "← From Parameters tab", "", ""],
        # Row 7
        ["Protocol fee (AOT)", "",
         "=Parameters!B18",
         "", "← From Parameters tab", "", ""],
        # Row 8
        ["", "", "", "", "", "", ""],
        # Row 9
        ["SCENARIO MATRIX", "", "", "", "", "", ""],
        # Row 10: column headers
        ["Priority Fee Source", "Latency-Sensitive %", "RAIKU Capture %", "Addressable Market",
         "Gross Revenue", "Protocol (5%)", "Protocol/month"],
    ]

    scenario_data = []
    for src_idx, (label, cell_ref) in enumerate(AOT_TD_FEE_SOURCES):
        for lat_idx, lat in enumerate(AOT_TD_LAT_SHARES):
            for cap_idx, cap in enumerate(AOT_TD_CAPTURES):
                r = aot_td_scenario_row(src_idx, lat_idx, cap_idx)
                scenario_data.append([
                    label,
                    lat,
                    cap,
                    f'={cell_ref}*B{r}',           # D: Addressable = total × latency%
                    f'=D{r}*C{r}',                   # E: Gross = addressable × capture%
                    f'=E{r}*$C$7',                   # F: Protocol = gross × fee
                    f'=F{r}/12',                     # G: Protocol/month
                ])

    all_data = header_data + scenario_data
    write_batch(ws, all_data, 7)

    last_row = AOT_TD_SCENARIO_START_ROW + len(scenario_data) - 1

    # Formatting
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})
    ws.format("A4", {"textFormat": {"bold": True}})
    ws.format("A9", {"textFormat": {"bold": True}})
    ws.format("A10:G10", {
        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.3},
        "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
    })
    ws.freeze(rows=10)
    time.sleep(RATE_LIMIT_SLEEP)

    ws.format("C5:C6", {"numberFormat": {"type": "NUMBER", "pattern": "$#,##0"}})
    ws.format(f"B11:C{last_row}", {"numberFormat": {"type": "PERCENT", "pattern": "0%"}})
    ws.format(f"D11:G{last_row}", {"numberFormat": {"type": "NUMBER", "pattern": "$#,##0"}})
    ws.format("C7", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})
    time.sleep(RATE_LIMIT_SLEEP)

    print(f"  ✓ AOT Top-Down: {len(scenario_data)} scenarios (rows {AOT_TD_SCENARIO_START_ROW}-{last_row})")
    return True


# ══════════════════════════════════════════════════════════
# TAB 7: AOT BOTTOM-UP (REFACTORED + VALIDATION)
# ══════════════════════════════════════════════════════════

# 6 customer archetypes — editable in the Sheet once exported
AOT_BU_ARCHETYPES = [
    ["PropAMMs", 1400000, 0.025, 1, 0.50, 3, 6, 10],
    ["Quant Trading", 300000, 0.15, 2, 0.30, 2, 5, 10],
    ["Market Maker (Ops)", 50000, 0.10, 1, 0.10, 3, 8, 15],
    ["DEX-DEX Arb", 300000, 0.087, 1, 0.40, 5, 10, 20],
    ["Protocol Crankers", 200000, 0.054, 1, 0.05, 5, 15, 30],
    ["CEX-DEX Arb", 300000, 0.50, 2, 0.60, 2, 5, 8],
]

# Layout constants
AOT_BU_PARAM_ROW_SOL_PRICE = 5          # C5
AOT_BU_PARAM_ROW_SLOTS_PER_YEAR = 6     # C6
AOT_BU_PARAM_ROW_PROTOCOL_FEE = 8       # C8
AOT_BU_PARAM_ROW_STAKE_PCT = 9          # C9
AOT_BU_ARCH_HEADER_ROW = 13
AOT_BU_ARCH_START_ROW = 14
N_ARCHETYPES = len(AOT_BU_ARCHETYPES)


def _aot_bu_rev_start():
    """Row where revenue calculation section starts."""
    return AOT_BU_ARCH_START_ROW + N_ARCHETYPES + 1


def _aot_bu_rev_header():
    """Row for revenue calculation column headers."""
    return _aot_bu_rev_start() + 1


def _aot_bu_rev_data_start():
    """First revenue data row."""
    return _aot_bu_rev_header() + 1


def _aot_bu_total_row():
    """Row for TOTAL row in mid-customer revenue."""
    return _aot_bu_rev_data_start() + N_ARCHETYPES


def export_aot_bottomup(spreadsheet, dry_run=False):
    """Create AOT bottom-up 3D model with refs to Parameters (v4: with validation column)."""
    print("  AOT Bottom-Up: building formula-based tab (v4)")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "AOT Bottom-Up", 80, 16)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)

    data = [
        # Row 1
        ["AOT Revenue Model — Bottom-Up (3D Framework)", "", "", "", "", "", "", "", "", "", ""],
        # Row 2
        ["Formula: N_customers × Slots_active/yr × CU/slot × Fee/CU × SOL_price", "", "", "", "", "", "", "", "", "", ""],
        # Row 3
        ["", "", "", "", "", "", "", "", "", "", ""],
        # Row 4
        ["NETWORK PARAMETERS", "", "", "", "", "", "", "", "", "", ""],
        # Row 5: SOL Price (from Parameters)
        ["SOL Price (USD)", "",
         "=Parameters!B5",
         "", "← From Parameters tab", "", "", "", "", "", ""],
        # Row 6: Slots per year (from Parameters)
        ["Slots per year", "",
         "=Parameters!B13",
         "", "← From Parameters tab", "", "", "", "", "", ""],
        # Row 7: Max CU per block
        ["Max CU per block", "",
         "=Parameters!B14",
         "", "← From Parameters tab", "", "", "", "", "", ""],
        # Row 8: Protocol fee (from Parameters)
        ["Protocol fee (AOT)", "",
         "=Parameters!B18",
         "", "← From Parameters tab", "", "", "", "", "", ""],
        # Row 9: RAIKU stake % (from Parameters)
        ["RAIKU stake %", "",
         "=Parameters!B20",
         "", "← From Parameters tab", "", "", "", "", "", ""],
        # Row 10: RAIKU slots/year (formula)
        ["RAIKU slots/year", "",
         f'=C6*C9',
         "", "← Slots × Stake%", "", "", "", "", "", ""],
        # Row 11: blank
        ["", "", "", "", "", "", "", "", "", "", ""],
        # Row 12: section label
        ["ARCHETYPE PARAMETERS (editable)", "", "", "", "", "", "", "", "", "", ""],
        # Row 13: column headers
        ["Archetype", "CU/tx", "Fee/CU (lamports)", "Txs/slot", "% Slots Active",
         "Customers (Low)", "Customers (Mid)", "Customers (High)", "Source", "Data Check", ""],
    ]

    # Rows 14-19: archetype data
    for arch in AOT_BU_ARCHETYPES:
        data.append(arch + ["Empirical from on-chain data + estimates", "See Program DB"])

    # Blank row
    data.append(["", "", "", "", "", "", "", "", "", "", ""])

    # Revenue calculation section
    rev_start = _aot_bu_rev_start()
    rev_header = _aot_bu_rev_header()
    rev_data = _aot_bu_rev_data_start()
    total_row = _aot_bu_total_row()

    data.append(["REVENUE BY ARCHETYPE — Mid customers", "", "", "", "", "", "", "", "", "", ""])
    data.append(["Archetype", "CU/slot", "Fee/slot (SOL)", "Active slots/yr",
                 "Rev/customer/yr (SOL)", "Rev/customer/yr (USD)", "N customers",
                 "Gross revenue (USD)", "Protocol (5%)", "Protocol/month", ""])

    for i in range(N_ARCHETYPES):
        ar = AOT_BU_ARCH_START_ROW + i
        r = rev_data + i
        data.append([
            f'=A{ar}',
            f'=B{ar}*D{ar}',
            f'=B{r}*C{ar}/1000000000',
            f'=$C$10*E{ar}',
            f'=C{r}*D{r}',
            f'=E{r}*$C$5',
            f'=G{ar}',
            f'=F{r}*G{r}',
            f'=H{r}*$C$8',
            f'=I{r}/12',
            "",
        ])

    # TOTAL row
    data.append([
        "TOTAL", "", "", "", "", "",
        f'=SUM(G{rev_data}:G{total_row-1})',
        f'=SUM(H{rev_data}:H{total_row-1})',
        f'=SUM(I{rev_data}:I{total_row-1})',
        f'=SUM(J{rev_data}:J{total_row-1})',
        "",
    ])

    # LOW scenario
    low_start = total_row + 2
    data.append(["", "", "", "", "", "", "", "", "", "", ""])
    data.append(["REVENUE BY ARCHETYPE — Low customers (conservative)", "", "", "", "", "", "", "", "", "", ""])
    data.append(["Archetype", "CU/slot", "Fee/slot (SOL)", "Active slots/yr",
                 "Rev/customer/yr (SOL)", "Rev/customer/yr (USD)", "N customers",
                 "Gross revenue (USD)", "Protocol (5%)", "Protocol/month", ""])

    low_data_start = low_start + 2
    for i in range(N_ARCHETYPES):
        ar = AOT_BU_ARCH_START_ROW + i
        mid_r = rev_data + i
        r = low_data_start + i
        data.append([
            f'=A{ar}',
            f'=B{mid_r}',
            f'=C{mid_r}',
            f'=D{mid_r}',
            f'=E{mid_r}',
            f'=F{mid_r}',
            f'=F{ar}',
            f'=F{r}*G{r}',
            f'=H{r}*$C$8',
            f'=I{r}/12',
            "",
        ])

    low_total = low_data_start + N_ARCHETYPES
    data.append([
        "TOTAL (Low)", "", "", "", "", "",
        f'=SUM(G{low_data_start}:G{low_total-1})',
        f'=SUM(H{low_data_start}:H{low_total-1})',
        f'=SUM(I{low_data_start}:I{low_total-1})',
        f'=SUM(J{low_data_start}:J{low_total-1})',
        "",
    ])

    # HIGH scenario
    data.append(["", "", "", "", "", "", "", "", "", "", ""])
    high_start = low_total + 2
    data.append(["REVENUE BY ARCHETYPE — High customers (optimistic)", "", "", "", "", "", "", "", "", "", ""])
    data.append(["Archetype", "CU/slot", "Fee/slot (SOL)", "Active slots/yr",
                 "Rev/customer/yr (SOL)", "Rev/customer/yr (USD)", "N customers",
                 "Gross revenue (USD)", "Protocol (5%)", "Protocol/month", ""])

    high_data_start = high_start + 2
    for i in range(N_ARCHETYPES):
        ar = AOT_BU_ARCH_START_ROW + i
        mid_r = rev_data + i
        r = high_data_start + i
        data.append([
            f'=A{ar}',
            f'=B{mid_r}',
            f'=C{mid_r}',
            f'=D{mid_r}',
            f'=E{mid_r}',
            f'=F{mid_r}',
            f'=H{ar}',
            f'=F{r}*G{r}',
            f'=H{r}*$C$8',
            f'=I{r}/12',
            "",
        ])

    high_total = high_data_start + N_ARCHETYPES
    data.append([
        "TOTAL (High)", "", "", "", "", "",
        f'=SUM(G{high_data_start}:G{high_total-1})',
        f'=SUM(H{high_data_start}:H{high_total-1})',
        f'=SUM(I{high_data_start}:I{high_total-1})',
        f'=SUM(J{high_data_start}:J{high_total-1})',
        "",
    ])

    write_batch(ws, data, 11)

    # Formatting
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})
    ws.format("A4", {"textFormat": {"bold": True}})
    ws.format("A12", {"textFormat": {"bold": True}})

    # Style archetype parameter headers
    ws.format(f"A{AOT_BU_ARCH_HEADER_ROW}:K{AOT_BU_ARCH_HEADER_ROW}", {
        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.3},
        "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
    })

    # Style revenue section headers
    for hdr_row in [rev_header, low_start + 1, high_start + 1]:
        ws.format(f"A{hdr_row}:K{hdr_row}", {
            "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.3},
            "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
        })

    # Bold total rows
    for tr in [total_row, low_total, high_total]:
        ws.format(f"A{tr}:K{tr}", {"textFormat": {"bold": True}})

    # Bold section labels
    for sr in [rev_start, low_start, high_start]:
        ws.format(f"A{sr}", {"textFormat": {"bold": True}})

    ws.freeze(rows=1)
    time.sleep(RATE_LIMIT_SLEEP)

    print(f"  ✓ AOT Bottom-Up: {N_ARCHETYPES} archetypes × 3 scenarios (Low/Mid/High)")
    return True


# ══════════════════════════════════════════════════════════
# TAB 8: REVENUE WATERFALL (NEW)
# ══════════════════════════════════════════════════════════

def export_revenue_waterfall(spreadsheet, dry_run=False):
    """Create Revenue Waterfall tab — protocol fee decomposition."""
    print("  Revenue Waterfall: building fee decomposition tab (NEW)")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "Revenue Waterfall", 30, 7)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)

    data = [
        # Row 1: Title
        ["RAIKU Revenue Waterfall — Protocol Fee Decomposition", "", "", "", "", "", ""],
        # Row 2: Subtitle
        ["All values in USD/year. Implements Post-TGE Design fee waterfall.", "", "", "", "", "", ""],
        # Row 3: Blank
        ["", "", "", "", "", "", ""],
        # Row 4: Column headers
        ["Step", "Conservative", "Base", "Optimistic", "Formula", "", ""],
        # Row 5: Gross Auction Revenue (blend from Revenue Summary row 12)
        ["1. Gross Auction Revenue",
         "='Revenue Summary'!B12",
         "='Revenue Summary'!C12",
         "='Revenue Summary'!D12",
         "Blended average from Revenue Summary", "", ""],
        # Row 6: Validator Share
        ["2. Validator Share (95%)",
         "=B5*(1-Parameters!B18)",
         "=C5*(1-Parameters!B18)",
         "=D5*(1-Parameters!B18)",
         "Gross × (1 - protocol_fee)", "", ""],
        # Row 7: Protocol Fee Capture
        ["3. Protocol Fee Capture",
         "=B5*Parameters!B18",
         "=C5*Parameters!B18",
         "=D5*Parameters!B18",
         "Gross × protocol_fee", "", ""],
        # Row 8: Customer Loyalty Rebates
        ["4. Customer Loyalty Rebates",
         "=B5*Parameters!B22",
         "=C5*Parameters!B22",
         "=D5*Parameters!B22",
         "Gross × rebate_rate", "", ""],
        # Row 9: Validator Enhancement
        ["5. Validator Enhancement Program",
         "=B7*(Parameters!B24*Parameters!B25)",
         "=C7*(Parameters!B24*Parameters!B25)",
         "=D7*(Parameters!B24*Parameters!B25)",
         "Protocol × enhancement% × qualifying%", "", ""],
        # Row 10: Net Protocol Treasury
        ["6. Net Protocol Treasury",
         "=B7-B8-B9",
         "=C7-C8-C9",
         "=D7-D8-D9",
         "Protocol - Rebates - Enhancement", "", ""],
        # Row 11: Blank
        ["", "", "", "", "", "", ""],
        # Row 12: Section header
        ["NET TREASURY ALLOCATION (indicative)", "", "", "", "", "", ""],
        # Row 13: Column headers
        ["Allocation", "Conservative", "Base", "Optimistic", "Formula", "", ""],
        # Row 14: Operations (from Parameters)
        ["Operations",
         "=B10*Parameters!B26",
         "=C10*Parameters!B26",
         "=D10*Parameters!B26",
         "← Parameters!B26 (treasury alloc %)", "", ""],
        # Row 15: Growth & Incentives (from Parameters)
        ["Growth & Incentives",
         "=B10*Parameters!B27",
         "=C10*Parameters!B27",
         "=D10*Parameters!B27",
         "← Parameters!B27 (treasury alloc %)", "", ""],
        # Row 16: Value Accrual (from Parameters)
        ["Value Accrual / Buyback",
         "=B10*Parameters!B28",
         "=C10*Parameters!B28",
         "=D10*Parameters!B28",
         "← Parameters!B28 (treasury alloc %)", "", ""],
        # Row 17: Blank
        ["", "", "", "", "", "", ""],
        # Row 18: Section header
        ["MONTHLY VIEW", "", "", "", "", "", ""],
        # Row 19: Column headers
        ["Metric", "Conservative", "Base", "Optimistic", "Unit", "", ""],
        # Row 20: Protocol Fee / month
        ["Protocol Fee / month",
         "=B7/12",
         "=C7/12",
         "=D7/12",
         "USD/month", "", ""],
        # Row 21: Net Treasury / month
        ["Net Treasury / month",
         "=B10/12",
         "=C10/12",
         "=D10/12",
         "USD/month", "", ""],
        # Row 22: Net Treasury / month (SOL)
        ["Net Treasury / month (SOL)",
         "=B21/Parameters!B5",
         "=C21/Parameters!B5",
         "=D21/Parameters!B5",
         "SOL/month @ current price", "", ""],
    ]

    write_batch(ws, data, 7)

    # Format title
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})

    # Format section headers (rows 12, 18)
    for sr in [12, 18]:
        ws.format(f"A{sr}", {"textFormat": {"bold": True, "fontSize": 12}})

    # Format column header rows (rows 4, 13, 19)
    for hr in [4, 13, 19]:
        ws.format(f"A{hr}:E{hr}", {
            "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.3},
            "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
        })

    # Format currency columns (B5:D22)
    ws.format("B5:D22", {"numberFormat": {"type": "NUMBER", "pattern": "$#,##0"}})

    ws.freeze(rows=1)
    time.sleep(RATE_LIMIT_SLEEP)

    print(f"  ✓ Revenue Waterfall: fee decomposition tab created")
    return True


# ══════════════════════════════════════════════════════════
# TAB 9: REVENUE SUMMARY (REFACTORED)
# ══════════════════════════════════════════════════════════

def export_revenue_summary(spreadsheet, dry_run=False):
    """Create revenue summary tab linking JIT + AOT models (v4: refs Parameters + Waterfall)."""
    print("  Revenue Summary: building consolidated view (v4)")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "Revenue Summary", 45, 10)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)

    # Compute exact cell references for each scenario
    jit_cons = jit_scenario_row(2, 1)   # Q4-2025 bear, 5% share
    jit_base = jit_scenario_row(3, 2)   # 2025 bull, 10% share
    jit_opt = jit_scenario_row(3, 3)    # 2025 bull, 15% share

    aot_td_cons = aot_td_scenario_row(0, 2, 1)  # Latest, 50% latency, 10% capture
    aot_td_base = aot_td_scenario_row(0, 2, 2)  # Latest, 50% latency, 15% capture
    aot_td_opt = aot_td_scenario_row(1, 3, 3)   # Avg10, 60% latency, 20% capture

    aot_bu_mid_total = _aot_bu_total_row()                      # 29
    # Low section: mid_total +1 blank +1 label +1 header +6 data = low total
    _low_start = aot_bu_mid_total + 2                            # 31
    _low_data_start = _low_start + 2                             # 33
    aot_bu_low_total = _low_data_start + N_ARCHETYPES            # 39
    # High section: same offset from low total
    _high_start = aot_bu_low_total + 2                           # 41
    _high_data_start = _high_start + 2                           # 43
    aot_bu_high_total = _high_data_start + N_ARCHETYPES          # 49

    data = [
        # Row 1: Title
        ["RAIKU Revenue Model Summary", "", "", "", "", "", ""],
        # Row 2: Blank
        ["", "", "", "", "", "", ""],
        # Row 3: Protocol fee reference
        ["Protocol fee rate:", "=Parameters!B18", "", "", "← From Parameters tab", "", ""],
        # Row 4: Blank
        ["", "", "", "", "", "", ""],
        # Row 5: Column headers
        ["Revenue Source", "Conservative", "Base", "Optimistic", "Notes", "", ""],
        # Row 6: JIT Model
        [f"JIT Model (gross)",
         f"='JIT Model'!C{jit_cons}",
         f"='JIT Model'!C{jit_base}",
         f"='JIT Model'!C{jit_opt}",
         "From JIT Model tab", "", ""],
        # Row 7: AOT Top-Down
        [f"AOT Top-Down (gross)",
         f"='AOT Top-Down'!E{aot_td_cons}",
         f"='AOT Top-Down'!E{aot_td_base}",
         f"='AOT Top-Down'!E{aot_td_opt}",
         "From AOT Top-Down tab", "", ""],
        # Row 8: AOT Bottom-Up (Low=Conservative, Mid=Base, High=Optimistic)
        [f"AOT Bottom-Up (gross)",
         f"='AOT Bottom-Up'!H{aot_bu_low_total}",
         f"='AOT Bottom-Up'!H{aot_bu_mid_total}",
         f"='AOT Bottom-Up'!H{aot_bu_high_total}",
         "Low/Mid/High customer scenarios", "", ""],
        # Row 9: Blank
        ["", "", "", "", "", "", ""],
        # Row 10: Section header
        ["GROSS REVENUE BLEND (equal weight)", "", "", "", "", "", ""],
        # Row 11: Column headers
        ["Scenario", "Conservative", "Base", "Optimistic", "Formula", "", ""],
        # Row 12: Simple average of three models
        ["Simple Average (1/3 each)",
         "=(B6+B7+B8)/3",
         "=(C6+C7+C8)/3",
         "=(D6+D7+D8)/3",
         "Average of JIT + AOT TD + AOT BU (Mid)", "", ""],
        # Row 13: Blank
        ["", "", "", "", "", "", ""],
        # Row 14: Section header
        ["PROTOCOL ECONOMICS (from Revenue Waterfall)", "", "", "", "", "", ""],
        # Row 15: Column headers
        ["Metric", "Conservative", "Base", "Optimistic", "Unit", "", ""],
        # Row 16: Protocol Capture
        ["Protocol Capture",
         "='Revenue Waterfall'!B7",
         "='Revenue Waterfall'!C7",
         "='Revenue Waterfall'!D7",
         "USD/yr", "", ""],
        # Row 17: Customer Rebates
        ["Customer Rebates",
         "='Revenue Waterfall'!B8",
         "='Revenue Waterfall'!C8",
         "='Revenue Waterfall'!D8",
         "USD/yr", "", ""],
        # Row 18: Validator Enhancement
        ["Validator Enhancement Program",
         "='Revenue Waterfall'!B9",
         "='Revenue Waterfall'!C9",
         "='Revenue Waterfall'!D9",
         "USD/yr", "", ""],
        # Row 19: Net Treasury
        ["Net Protocol Treasury",
         "='Revenue Waterfall'!B10",
         "='Revenue Waterfall'!C10",
         "='Revenue Waterfall'!D10",
         "USD/yr", "", ""],
        # Row 20: Blank
        ["", "", "", "", "", "", ""],
        # Row 21: Section header
        ["VALIDATOR PERSPECTIVE", "", "", "", "", "", ""],
        # Row 22: Column headers
        ["Metric", "Conservative", "Base", "Optimistic", "Unit", "", ""],
        # Row 23: Validator Gross (use $B$3 — protocol fee is in B3 only)
        ["Validator Gross Revenue",
         "=B12*(1-$B$3)",
         "=C12*(1-$B$3)",
         "=D12*(1-$B$3)",
         "USD/yr", "", ""],
        # Row 24: Blank
        ["", "", "", "", "", "", ""],
        # Row 25: Section header
        ["REVENUE MIX (%)", "", "", "", "", "", ""],
        # Row 26: Column headers
        ["Source", "Conservative", "Base", "Optimistic", "Unit", "", ""],
        # Row 27: JIT % of gross
        ["JIT % of gross",
         "=B6/B12",
         "=C6/C12",
         "=D6/D12",
         "%", "", ""],
        # Row 28: AOT TD % of gross
        ["AOT TD % of gross",
         "=B7/B12",
         "=C7/C12",
         "=D7/D12",
         "%", "", ""],
        # Row 29: AOT BU % of gross
        ["AOT BU % of gross",
         "=B8/B12",
         "=C8/C12",
         "=D8/D12",
         "%", "", ""],
    ]

    write_batch(ws, data, 7)

    # Format title
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})

    # Format section headers (rows 10, 14, 21, 25)
    for sr in [10, 14, 21, 25]:
        ws.format(f"A{sr}", {"textFormat": {"bold": True, "fontSize": 12}})

    # Format column header rows (rows 5, 11, 15, 22, 26)
    for hr in [5, 11, 15, 22, 26]:
        ws.format(f"A{hr}:E{hr}", {
            "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.3},
            "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
        })

    # Format currency columns (B6:D19, B23)
    ws.format("B6:D19", {"numberFormat": {"type": "NUMBER", "pattern": "$#,##0"}})
    ws.format("B23:D23", {"numberFormat": {"type": "NUMBER", "pattern": "$#,##0"}})

    # Format percentage columns (B3, C3, D3, B27:D29)
    ws.format("B3:D3", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})
    ws.format("B27:D29", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})

    ws.freeze(rows=1)
    time.sleep(RATE_LIMIT_SLEEP)

    print(f"  ✓ Revenue Summary: consolidated view with Waterfall refs")
    return True


# ══════════════════════════════════════════════════════════
# TAB 10: DATA SOURCES (DOCUMENTATION)
# ══════════════════════════════════════════════════════════

def export_data_sources(spreadsheet, dry_run=False):
    """Export data sources documentation."""
    print("  Data Sources: building documentation tab")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "Data Sources", 120, 6)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)

    data = [
        ["RAIKU Revenue Model — Data Sources & Definitions", "", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["OVERVIEW", "", "", "", "", ""],
        ["This spreadsheet implements the RAIKU revenue estimation model using real Solana on-chain data.", "", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["TAB 1: PARAMETERS — Single Source of Truth", "", "", "", "", ""],
        ["Purpose", "Central repository for all adjustable model parameters", "", "", "", ""],
        ["Sections", "Market Data (auto-populated from Epoch Database), Network Constants, RAIKU Assumptions (editable), External References", "", "", "", ""],
        ["Key Cells", "B5=SOL Price, B6=MEV Latest, B7=MEV Avg10, B8=Fees Latest, B9=Fees Avg10, B18=AOT Fee%, B19=JIT Fee%, B20=RAIKU Stake%, B21=Latency Share, B26-B28=Treasury Alloc %s, B32-B33=Jito MEV refs", "", "", "", ""],
        ["Notes", "All model tabs reference this tab instead of hardcoding parameters. Edit here to propagate changes everywhere.", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["TAB 2: EPOCH DATABASE — Raw Solana Data", "", "", "", "", ""],
        ["Source", "Extracted from Trillium API + merged with Jito/Solana Compass/Dune data", "", "", "", ""],
        ["Rows", "786 epochs (covers epochs 150-935, Trillium from 552+)", "", "", "", ""],
        ["RAW Columns (A-W)", "epoch, block_time, inflationary_reward, fee_reward, mev_reward, voting_rewards_sol, avg_commission_rate, validator_count, stake_account_count, issue_apy, sol_price_usd, fdv_usd, + Trillium-specific fields", "", "", "", ""],
        ["FORMULA Columns (X-AO)", "Derived values (total_rewards_sol, staker_rewards_sol, fee_pct_of_total, mev_pct_of_total, effective_commission, inflation_apr, fee_apr, mev_apr, total_apr, total_apy, total_supply_sol, staked_ratio, burn_sol, net_inflation_sol, annual_total_fees_usd, annual_mev_usd, annual_priority_fees_usd)", "", "", "", ""],
        [f"Key Fields", f"Q=SOL Price (USD), {EPOCH_FCOLS['annual_mev_usd']}=Annual MEV (USD/yr), {EPOCH_FCOLS['annual_priority_fees_usd']}=Annual Priority Fees (USD/yr)", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["TAB 3: PROGRAM DATABASE — Fee/CU by Program", "", "", "", "", ""],
        ["Source", "Dune Analytics (top 500 programs by priority fees, 30-day aggregate)", "", "", "", ""],
        ["Rows", "500 programs (30d) or 50 (7d fallback) — sorted by priority fees descending", "", "", "", ""],
        ["Key Columns", "program_id (PK), raiku_category/product, priority_fees_sol, fee/CU stats (all in lamports), CU consumed, fail_rate, avg_cu_per_block, pct_of_total_priority (computed)", "", "", "", ""],
        ["Removed (v2)", "solwatch_pain_score (non-reproducible), solwatch_fail_rate (0/500 populated), data_source (constant). Renamed: total_fees_sol→base_plus_priority_fees_sol, fee/CU cols now explicitly _lamports", "", "", "", ""],
        ["SQL note", "Updated SQL includes failed txns for accurate fail_rate. Fee/CU = priority fee only (base fee negligible at ~5000 lamports/sig). avg_cu_per_block = blockspace perspective.", "", "", "", ""],
        ["Usage", "Supports archetype fee assumptions in AOT Bottom-Up model + Program Mapping validation", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["TAB 4: PROGRAM MAPPING — Classification", "", "", "", "", ""],
        ["Source", "Manual + data-driven classification", "", "", "", ""],
        ["Categories", "JIT-only, AOT-only, Both, Potential, Neither, Unknown", "", "", "", ""],
        ["Purpose", "Map programs to RAIKU use-cases (PropAMM, Quant Trading, Market Maker, DEX-DEX Arb, Protocol Cranker, CEX-DEX Arb)", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["TAB 5: JIT MODEL — Jito MEV Market Revenue", "", "", "", "", ""],
        ["Formula", "Gross = Total_Jito_MEV × RAIKU_Market_Share | Protocol = Gross × Fee%", "", "", "", ""],
        ["Sources (C4:C7)", "C4=Latest epoch (from Parameters!B6), C5=Avg 10ep (from Parameters!B7), C6=Q4 bear (from Parameters!B32), C7=2025 bull (from Parameters!B33)", "", "", "", ""],
        ["Shares (B12:B41)", "Market share assumptions: 2%, 5%, 10%, 15%, 20% (editable)", "", "", "", ""],
        ["Scenarios", "4 sources × 5 shares = 20 scenarios (rows 12-41)", "", "", "", ""],
        ["Protocol Fee", "C8 = from Parameters!B19 (JIT fee, typically 5%)", "", "", "", ""],
        ["Output", "Gross Revenue (C), Validator share (D), Protocol (E), Protocol/month (F)", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["TAB 6: AOT TOP-DOWN — Priority Fee Market", "", "", "", "", ""],
        ["Formula", "Addressable = Total_Priority_Fees × Latency_Sensitive% | Gross = Addressable × RAIKU_Capture% | Protocol = Gross × Fee%", "", "", "", ""],
        ["Sources (C5:C6)", "C5=Latest (from Parameters!B8), C6=Avg 10ep (from Parameters!B9)", "", "", "", ""],
        ["Latency-Sensitive % (B11:B43)", "30%, 40%, 50%, 60% — % of priority fees attributable to latency-sensitive ops (editable)", "", "", "", ""],
        ["RAIKU Capture % (C11:C43)", "5%, 10%, 15%, 20% — market share assumption (editable)", "", "", "", ""],
        ["Scenarios", "2 sources × 4 latency% × 4 capture% = 32 scenarios (rows 11-42)", "", "", "", ""],
        ["Protocol Fee", "C7 = from Parameters!B18 (AOT fee, typically 5%)", "", "", "", ""],
        ["Output", "Addressable (D), Gross (E), Protocol (F), Protocol/month (G)", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["TAB 7: AOT BOTTOM-UP — Customer Archetype Framework", "", "", "", "", ""],
        ["Formula", "Revenue = N_customers × Slots_active/yr × CU/slot × Fee/CU × SOL_price", "", "", "", ""],
        ["Archetypes", "PropAMMs, Quant Trading, Market Maker (Ops), DEX-DEX Arb, Protocol Crankers, CEX-DEX Arb (6 total)", "", "", "", ""],
        ["Parameters (rows 5-10)", "SOL Price (C5, from Parameters!B5), Slots/year (C6, from Parameters!B13), Max CU (C7, from Parameters!B14), Protocol Fee (C8, from Parameters!B18), RAIKU Stake% (C9, from Parameters!B20), RAIKU Slots/yr (C10)", "", "", "", ""],
        ["Archetype Data (rows 14-19)", "CU/tx, Fee/CU (lamports), Txs/slot, % Slots Active, N customers (Low/Mid/High)", "", "", "", ""],
        ["Scenarios", "3 customer count scenarios: Low (conservative), Mid (base), High (optimistic)", "", "", "", ""],
        ["Totals", "Mid: row 20, Low: row ~33, High: row ~46 (varies based on N_archetypes=6)", "", "", "", ""],
        ["Output per scenario", "Per-archetype revenue (USD), Per-archetype protocol (5%), Total protocol/month", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["TAB 8: REVENUE WATERFALL — Fee Decomposition (NEW in v4)", "", "", "", "", ""],
        ["Purpose", "Implements Post-TGE Design fee waterfall from gross revenue to net treasury", "", "", "", ""],
        ["Inputs", "Gross revenue from Revenue Summary (JIT + AOT TD + AOT BU blend)", "", "", "", ""],
        ["Steps", "1. Gross Revenue | 2. Validator Share (95%) | 3. Protocol Fee Capture | 4. Customer Rebates | 5. Validator Enhancement | 6. Net Treasury", "", "", "", ""],
        ["Allocation", "Net Treasury split from Parameters tab (B26=Operations, B27=Growth, B28=Value Accrual) — editable", "", "", "", ""],
        ["Monthly View", "Shows protocol/month and treasury/month in USD and SOL", "", "", "", ""],
        ["Scenarios", "Conservative, Base, Optimistic (columns B, C, D)", "", "", "", ""],
        ["Key Refs", "B7=Protocol Capture, B10=Net Treasury, B21=Net Treasury/month", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["TAB 9: REVENUE SUMMARY — Consolidated Model", "", "", "", "", ""],
        ["Purpose", "Central dashboard showing gross revenue blend + protocol economics", "", "", "", ""],
        ["Gross Revenue Sources", "JIT Model (row 6), AOT Top-Down (row 7), AOT Bottom-Up (row 8)", "", "", "", ""],
        ["Revenue Blend", "Simple average of three models (row 12)", "", "", "", ""],
        ["Protocol Economics", "References Revenue Waterfall for Protocol Capture, Rebates, Enhancement, Net Treasury (rows 16-19)", "", "", "", ""],
        ["Validator Perspective", "Validator gross = Total × (1 - Protocol Fee %) (row 23)", "", "", "", ""],
        ["Revenue Mix", "% contribution of JIT, AOT TD, AOT BU to gross (rows 27-29)", "", "", "", ""],
        ["Scenarios", "Conservative (Col B), Base (Col C), Optimistic (Col D)", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["TAB 10: DATA SOURCES — This Tab", "", "", "", "", ""],
        ["Purpose", "Documentation of all tabs, columns, formulas, and data sources", "", "", "", ""],
        ["Maintainer", "Auto-generated by sheets_export.py v4", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["CROSS-TAB REFERENCE MAP", "", "", "", "", ""],
        ["JIT Model", "Sources from Parameters (C4-C7, C8) | Outputs used by Revenue Summary (B6:D6)", "", "", "", ""],
        ["AOT Top-Down", "Sources from Parameters (C5-C6, C7) | Outputs used by Revenue Summary (B7:D7)", "", "", "", ""],
        ["AOT Bottom-Up", "Sources from Parameters (C5, C6, C8, C9) | Outputs used by Revenue Summary (B8:D8)", "", "", "", ""],
        ["Revenue Summary", "Aggregates all three models | Uses Parameters for protocol fee (B3) | References Revenue Waterfall (B16:D19)", "", "", "", ""],
        ["Revenue Waterfall", "Inputs from Revenue Summary (B5:D5=Gross) | References Parameters for fees, rebates, enhancement | Outputs used by Waterfall calculations", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["KEY ASSUMPTIONS & RANGES", "", "", "", "", ""],
        ["Protocol Fee", "1-5% (governance range). Default 5% in Parameters. Editable in B18 (AOT) and B19 (JIT).", "", "", "", ""],
        ["RAIKU Market Share", "JIT: 2-20% | AOT TD: 5-20% capture of addressable latency-sensitive fees | AOT BU: customer count scenarios", "", "", "", ""],
        ["Latency-Sensitive %", "30-60% of priority fees. Default 40% in Parameters (B21).", "", "", "", ""],
        ["RAIKU Stake %", "Default 5% (B20 in Parameters). Used to calculate RAIKU slots/year.", "", "", "", ""],
        ["SOL Price", "Auto-populated from latest Epoch Database row (Parameters B5)", "", "", "", ""],
        ["Jito MEV References", "Q4-2025 bear annualized + 2025 full year bull (computed from Epoch Database via SUMPRODUCT, not hardcoded)", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["DATA QUALITY NOTES", "", "", "", "", ""],
        ["active_stake_sol discontinuity", "Source switches at epoch 552: Dune (epochs 150-551, ~293M SOL) → Trillium (552+, ~387M SOL). Trillium includes activating stake, Dune does not. 32% jump. Affects historical APR series but NOT revenue model (which uses only latest epochs, all Trillium).", "", "", "", ""],
        ["avg_commission_rate gaps", "Missing for epochs 150-204 (54 epochs). Available from epoch 205 onwards (Dune source).", "", "", "", ""],
        ["MEV columns sparse before epoch 552", "mev_jito_tips_sol, mev_to_validators_sol, mev_to_stakers_sol only populated from Trillium (epoch 552+). Earlier epochs have no MEV breakdown.", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["AUDIT TRAIL", "", "", "", "", ""],
        ["Last Updated", "v4.1 — Dynamic formula column mapping (adapts to n_raw cols), treasury alloc in Parameters, all cross-refs verified", "", "", "", ""],
        ["Formula Validation", "All 18 formula columns verified: cross-references dynamically computed from n_raw, no hardcoded column letters", "", "", "", ""],
        ["Data Freshness", "Raw data imported from Trillium/Dune/Jito APIs — refresh by re-running 01_extract/ scripts", "", "", "", ""],
    ]

    write_batch(ws, data, 6)

    # Format title
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})

    # Format section headers (every cell in column A that looks like "TAB X:" or uppercase header)
    for row in range(1, len(data) + 1):
        cell_val = data[row - 1][0] if len(data[row - 1]) > 0 else ""
        if cell_val.startswith(("TAB ", "OVERVIEW", "KEY ", "CROSS", "AUDIT")):
            ws.format(f"A{row}", {"textFormat": {"bold": True, "fontSize": 11}})

    ws.freeze(rows=1)
    time.sleep(RATE_LIMIT_SLEEP)

    print(f"  ✓ Data Sources: {len(data)} rows of documentation")
    return True


# ══════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════

EXPORT_FUNCTIONS = {
    "parameters": export_parameters,
    "epoch": export_epoch_database,
    "program_db": export_program_database,
    "mapping": export_program_mapping,
    "jit": export_jit_model,
    "aot_td": export_aot_topdown,
    "aot_bu": export_aot_bottomup,
    "waterfall": export_revenue_waterfall,
    "summary": export_revenue_summary,
    "sources": export_data_sources,
}

TAB_ORDER = [
    "parameters", "epoch", "program_db", "mapping",
    "jit", "aot_td", "aot_bu", "waterfall", "summary", "sources"
]


def main():
    """Main orchestrator."""
    parser = argparse.ArgumentParser(description="Export RAIKU revenue model to Google Sheets")
    parser.add_argument("--tab", choices=list(EXPORT_FUNCTIONS.keys()),
                        help="Export single tab only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Dry-run (no Google Sheets updates)")
    parser.add_argument("--no-auth", action="store_true",
                        help="Authenticate locally (for testing)")
    args = parser.parse_args()

    # Authenticate
    if args.no_auth:
        print("DRY-RUN MODE (no auth)")
        dry_run = True
        spreadsheet = None
    else:
        if not SERVICE_ACCOUNT_FILE.exists():
            print(f"ERROR: {SERVICE_ACCOUNT_FILE} not found")
            print("  Setup: cp /path/to/service_account.json .")
            sys.exit(1)
        try:
            gc = gspread.service_account(filename=str(SERVICE_ACCOUNT_FILE))
            spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
            print(f"Connected to sheet: {spreadsheet.title}")
        except Exception as e:
            print(f"ERROR: Authentication failed: {e}")
            sys.exit(1)
        dry_run = args.dry_run

    # Export
    if args.tab:
        # Single tab
        tabs_to_export = [args.tab]
    else:
        # All tabs in order
        tabs_to_export = TAB_ORDER

    print("\n=== RAIKU Revenue Model Export (v4) ===\n")
    results = {}
    for tab_name in tabs_to_export:
        try:
            print(f"[{tab_name.upper()}]")
            func = EXPORT_FUNCTIONS[tab_name]
            success = func(spreadsheet, dry_run=dry_run)
            results[tab_name] = "✓" if success else "SKIP"
            print()
        except Exception as e:
            print(f"ERROR in {tab_name}: {e}")
            import traceback
            traceback.print_exc()
            results[tab_name] = "FAIL"
            print()

    # Summary
    print("=== SUMMARY ===")
    for tab_name in tabs_to_export:
        status = results[tab_name]
        print(f"  {tab_name:20} {status}")

    if dry_run:
        print("\n(DRY-RUN — no data written to Google Sheets)")
    else:
        print(f"\n✓ Export complete. View: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit")

    return 0 if all(v in ["✓", "SKIP"] for v in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())

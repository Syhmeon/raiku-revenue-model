"""
Google Sheets Export — RAW data + Sheet formulas
=================================================
Exports processed CSVs to Google Sheets and writes formulas for ALL calculations.

Architecture:
  Python pushes RAW data only → Google Sheet formulas do ALL calculations.
  This makes every number traceable and auditable.

Tabs created:
  1. "Epoch Database"   : 23 RAW cols (A-W) + 18 FORMULA cols (X-AO)
  2. "Program Database" : Per-program fee/CU data (top 500)
  3. "Program Mapping"  : Classification → RAIKU archetypes
  4. "JIT Model"        : JIT revenue scenarios (all Sheet formulas)
  5. "AOT Top-Down"     : AOT macro model (all Sheet formulas)
  6. "AOT Bottom-Up"    : AOT 3D framework (all Sheet formulas)
  7. "Revenue Summary"  : Consolidated view (all Sheet formulas)
  8. "Data Sources"     : Documentation of every column

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
    PROTOCOL_TAKE_RATE, JITO_2025_TOTAL_TIPS_USD, JITO_Q4_2025_ANNUALIZED_USD,
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


# ══════════════════════════════════════════════════════════
# TAB 1: EPOCH DATABASE
# ══════════════════════════════════════════════════════════

EPOCH_FORMULA_HEADERS = [
    "total_rewards_sol",            # X  = D+E+H
    "total_rewards_usd",            # Y  = X*Q
    "staker_rewards_sol",           # Z  = D-L
    "fee_pct_of_total",             # AA = E/X
    "mev_pct_of_total",             # AB = H/X
    "effective_commission",          # AC = L/D or M
    "inflation_apr",                # AD = D*(365.25/C)/P
    "fee_apr",                      # AE = E*(365.25/C)/P
    "mev_apr",                      # AF = H*(365.25/C)/P
    "total_apr",                    # AG = AD+AE+AF
    "total_apy",                    # AH = (1+AG/365.25)^365.25-1
    "total_supply_sol",             # AI = R/Q
    "staked_ratio",                 # AJ = P/AI
    "burn_sol",                     # AK = IF(A<620, E*0.5, 0)
    "net_inflation_sol",            # AL = D-AK
    "annual_total_fees_usd",        # AM = E*(365.25/C)*Q
    "annual_mev_usd",               # AN = H*(365.25/C)*Q
    "annual_priority_fees_usd",     # AO = F*(365.25/C)*Q
]


def epoch_formula(col_name, row):
    """Return the Google Sheet formula for a given column at a given row."""
    r = str(row)
    formulas = {
        "total_rewards_sol":        f'=IF(D{r}<>"", D{r}+E{r}+H{r}, "")',
        "total_rewards_usd":        f'=IF(X{r}<>"", X{r}*Q{r}, "")',
        "staker_rewards_sol":       f'=IF(AND(D{r}<>"",L{r}<>""), D{r}-L{r}, "")',
        "fee_pct_of_total":         f'=IF(AND(X{r}<>"",X{r}>0), E{r}/X{r}, "")',
        "mev_pct_of_total":         f'=IF(AND(X{r}<>"",X{r}>0), H{r}/X{r}, "")',
        "effective_commission":      f'=IF(AND(D{r}<>"",D{r}>0,L{r}<>""), L{r}/D{r}, M{r})',
        "inflation_apr":            f'=IF(AND(P{r}<>"",P{r}>0,C{r}<>"",C{r}>0), D{r}*(365.25/C{r})/P{r}, "")',
        "fee_apr":                  f'=IF(AND(P{r}<>"",P{r}>0,C{r}<>"",C{r}>0), E{r}*(365.25/C{r})/P{r}, "")',
        "mev_apr":                  f'=IF(AND(P{r}<>"",P{r}>0,C{r}<>"",C{r}>0,H{r}<>""), H{r}*(365.25/C{r})/P{r}, "")',
        "total_apr":                f'=IF(AND(AD{r}<>"",AE{r}<>""), AD{r}+AE{r}+IF(AF{r}<>"",AF{r},0), "")',
        "total_apy":               f'=IF(AND(AG{r}<>"",AG{r}>0), (1+AG{r}/365.25)^365.25-1, "")',
        "total_supply_sol":         f'=IF(AND(R{r}<>"",Q{r}<>"",Q{r}>0), R{r}/Q{r}, "")',
        "staked_ratio":             f'=IF(AND(P{r}<>"",AI{r}<>"",AI{r}>0), P{r}/AI{r}, "")',
        "burn_sol":                 f'=IF(A{r}<620, E{r}*0.5, 0)',
        "net_inflation_sol":        f'=IF(D{r}<>"", D{r}-AK{r}, "")',
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

    print(f"  Epoch Database: {len(data)} rows × {n_raw} RAW + {n_formula} FORMULA = {total_cols} cols")

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
        start_row = i + 2  # +2 because row 1 = header
        end_row = start_row + len(batch) - 1
        cell_range = f"A{start_row}:{col_letter(n_raw)}{end_row}"
        ws.update(cell_range, batch, value_input_option="USER_ENTERED")
        print(f"    RAW data: rows {start_row}-{end_row}")
        time.sleep(RATE_LIMIT_SLEEP)

    # Write FORMULA columns in batches
    formula_start_col = n_raw + 1  # Column X (24th)
    for i in range(0, len(data), BATCH_SIZE):
        batch_formulas = []
        for j in range(BATCH_SIZE):
            row_idx = i + j
            if row_idx >= len(data):
                break
            row_num = row_idx + 2  # Sheet row number
            row_formulas = [epoch_formula(col, row_num) for col in EPOCH_FORMULA_HEADERS]
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
# TAB 2: PROGRAM DATABASE
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
# TAB 3: PROGRAM MAPPING
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
# TAB 4: JIT MODEL
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
    """Create JIT revenue model tab with all formulas."""
    print("  JIT Model: building formula-based tab")

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
         f'=INDEX(\'Epoch Database\'!AN:AN, MATCH(9.99E+307, \'Epoch Database\'!AN:AN, 1))',
         "USD/yr", "← From Epoch Database col AN (last non-empty)", ""],
        # Row 5: Avg 10 epochs
        ["Total Jito MEV market (avg last 10 epochs)", "",
         f'=AVERAGE(INDEX(\'Epoch Database\'!AN:AN, COUNTA(\'Epoch Database\'!AN:AN)-9):INDEX(\'Epoch Database\'!AN:AN, COUNTA(\'Epoch Database\'!AN:AN)))',
         "USD/yr", "← Avg last 10 epochs", ""],
        # Row 6: Q4-2025 bear reference
        ["Jito Q4-2025 annualized (bear)", "", JITO_Q4_2025_ANNUALIZED_USD, "USD/yr", "← Post-TGE Design doc", ""],
        # Row 7: 2025 full year bull reference
        ["Jito 2025 full year (bull)", "", JITO_2025_TOTAL_TIPS_USD, "USD/yr", "← Post-TGE Design doc", ""],
        # Row 8: Protocol fee
        ["Protocol fee", "", PROTOCOL_TAKE_RATE, "", "← Governance range 1-5%", ""],
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
# TAB 5: AOT TOP-DOWN
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
    """Create AOT top-down model tab with formulas."""
    print("  AOT Top-Down: building formula-based tab")

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
         f'=INDEX(\'Epoch Database\'!AO:AO, MATCH(9.99E+307, \'Epoch Database\'!AO:AO, 1))',
         "USD/yr", "← Epoch Database col AO", "", ""],
        # Row 6
        ["Annual priority fees (avg 10 epochs)", "",
         f'=AVERAGE(INDEX(\'Epoch Database\'!AO:AO, COUNTA(\'Epoch Database\'!AO:AO)-9):INDEX(\'Epoch Database\'!AO:AO, COUNTA(\'Epoch Database\'!AO:AO)))',
         "USD/yr", "← Avg last 10 epochs", "", ""],
        # Row 7
        ["Protocol fee", "", PROTOCOL_TAKE_RATE, "", "", "", ""],
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
# TAB 6: AOT BOTTOM-UP
# ══════════════════════════════════════════════════════════

# 6 customer archetypes — editable in the Sheet once exported
# name, cu_per_tx, fee_per_cu_lamports, txs_per_slot, pct_slots_active, customers_low/mid/high
AOT_BU_ARCHETYPES = [
    ["PropAMMs", 1400000, 0.025, 1, 0.50, 3, 6, 10],
    ["Quant Trading", 300000, 0.15, 2, 0.30, 2, 5, 10],
    ["Market Maker (Ops)", 50000, 0.10, 1, 0.10, 3, 8, 15],
    ["DEX-DEX Arb", 300000, 0.087, 1, 0.40, 5, 10, 20],
    ["Protocol Crankers", 200000, 0.054, 1, 0.05, 5, 15, 30],
    ["CEX-DEX Arb", 300000, 0.50, 2, 0.60, 2, 5, 8],
]

# Layout constants — computed from archetype count
AOT_BU_PARAM_ROW_SOL_PRICE = 5          # C5
AOT_BU_PARAM_ROW_SLOTS_PER_YEAR = 6     # C6
AOT_BU_PARAM_ROW_PROTOCOL_FEE = 8       # C8
AOT_BU_PARAM_ROW_STAKE_PCT = 9          # C9
AOT_BU_PARAM_ROW_RAIKU_SLOTS = 10       # C10 = C6*C9
AOT_BU_ARCH_HEADER_ROW = 13             # Archetype parameter header
AOT_BU_ARCH_START_ROW = 14              # First archetype data row
N_ARCHETYPES = len(AOT_BU_ARCHETYPES)


def _aot_bu_rev_start():
    """Row where the revenue calculation section starts (section label).
    After 6 archetypes (rows 14-19) + 1 blank row (row 20), section label is at row 21.
    """
    return AOT_BU_ARCH_START_ROW + N_ARCHETYPES + 1  # arch end + blank row → section label


def _aot_bu_rev_header():
    """Row for the revenue calculation column headers."""
    return _aot_bu_rev_start() + 1


def _aot_bu_rev_data_start():
    """First revenue data row."""
    return _aot_bu_rev_header() + 1


def _aot_bu_total_row():
    """Row for the TOTAL row in mid-customer revenue."""
    return _aot_bu_rev_data_start() + N_ARCHETYPES


def export_aot_bottomup(spreadsheet, dry_run=False):
    """Create AOT bottom-up 3D model with formulas referencing archetype params."""
    print("  AOT Bottom-Up: building formula-based tab")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "AOT Bottom-Up", 80, 15)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)

    data = [
        # Row 1
        ["AOT Revenue Model — Bottom-Up (3D Framework)", "", "", "", "", "", "", "", "", ""],
        # Row 2
        ["Formula: N_customers × Slots_active/yr × CU/slot × Fee/CU × SOL_price", "", "", "", "", "", "", "", "", ""],
        # Row 3
        ["", "", "", "", "", "", "", "", "", ""],
        # Row 4
        ["NETWORK PARAMETERS", "", "", "", "", "", "", "", "", ""],
        # Row 5: SOL Price
        ["SOL Price (USD)", "",
         f'=INDEX(\'Epoch Database\'!Q:Q, MATCH(9.99E+307, \'Epoch Database\'!Q:Q, 1))',
         "", "← Latest from Epoch Database", "", "", "", "", ""],
        # Row 6: Slots per year
        ["Slots per year", "", 78408000, "", "← ~2 slots/sec × 86400 × 365.25", "", "", "", "", ""],
        # Row 7: Max CU per block
        ["Max CU per block", "", 48000000, "", "", "", "", "", "", ""],
        # Row 8: Protocol fee
        ["Protocol fee", "", PROTOCOL_TAKE_RATE, "", "", "", "", "", "", ""],
        # Row 9: RAIKU stake %
        ["RAIKU stake %", "", 0.05, "", "← Adjustable parameter", "", "", "", "", ""],
        # Row 10: RAIKU slots/year (formula)
        ["RAIKU slots/year", "", f'=C6*C9', "", "← Slots × Stake%", "", "", "", "", ""],
        # Row 11: blank
        ["", "", "", "", "", "", "", "", "", ""],
        # Row 12: section label
        ["ARCHETYPE PARAMETERS (editable)", "", "", "", "", "", "", "", "", ""],
        # Row 13: column headers
        ["Archetype", "CU/tx", "Fee/CU (lamports)", "Txs/slot", "% Slots Active",
         "Customers (Low)", "Customers (Mid)", "Customers (High)", "Source", ""],
    ]

    # Rows 14-19: archetype data
    for arch in AOT_BU_ARCHETYPES:
        data.append(arch + ["Empirical from on-chain data + estimates"])

    # Blank row
    data.append(["", "", "", "", "", "", "", "", "", ""])

    # Revenue calculation — MID customers scenario
    rev_start = _aot_bu_rev_start()
    rev_header = _aot_bu_rev_header()
    rev_data = _aot_bu_rev_data_start()
    total_row = _aot_bu_total_row()

    data.append(["REVENUE BY ARCHETYPE — Mid customers", "", "", "", "", "", "", "", "", ""])
    data.append(["Archetype", "CU/slot", "Fee/slot (SOL)", "Active slots/yr",
                 "Rev/customer/yr (SOL)", "Rev/customer/yr (USD)", "N customers",
                 "Gross revenue (USD)", "Protocol (5%)", "Protocol/month"])

    for i in range(N_ARCHETYPES):
        ar = AOT_BU_ARCH_START_ROW + i  # Archetype parameter row
        r = rev_data + i                 # Revenue row
        data.append([
            f'=A{ar}',                                    # A: Name
            f'=B{ar}*D{ar}',                               # B: CU per slot = CU/tx × txs/slot
            f'=B{r}*C{ar}/1000000000',                     # C: Fee/slot (SOL) = CU/slot × fee/CU / 1e9
            f'=$C$10*E{ar}',                                # D: Active slots/yr = RAIKU slots × pct_active
            f'=C{r}*D{r}',                                  # E: Rev/customer/yr SOL
            f'=E{r}*$C$5',                                  # F: Rev/customer/yr USD
            f'=G{ar}',                                       # G: N customers (Mid column)
            f'=F{r}*G{r}',                                  # H: Gross USD = rev/cust × N
            f'=H{r}*$C$8',                                  # I: Protocol = gross × fee
            f'=I{r}/12',                                    # J: Protocol/month
        ])

    # TOTAL row
    data.append([
        "TOTAL", "", "", "", "", "",
        f'=SUM(G{rev_data}:G{total_row-1})',
        f'=SUM(H{rev_data}:H{total_row-1})',
        f'=SUM(I{rev_data}:I{total_row-1})',
        f'=SUM(J{rev_data}:J{total_row-1})',
    ])

    # Add LOW and HIGH customer scenarios below
    low_start = total_row + 2
    data.append(["", "", "", "", "", "", "", "", "", ""])
    data.append(["REVENUE BY ARCHETYPE — Low customers (conservative)", "", "", "", "", "", "", "", "", ""])
    data.append(["Archetype", "CU/slot", "Fee/slot (SOL)", "Active slots/yr",
                 "Rev/customer/yr (SOL)", "Rev/customer/yr (USD)", "N customers",
                 "Gross revenue (USD)", "Protocol (5%)", "Protocol/month"])

    low_data_start = low_start + 2
    for i in range(N_ARCHETYPES):
        ar = AOT_BU_ARCH_START_ROW + i
        mid_r = rev_data + i  # reuse mid-scenario per-customer values
        r = low_data_start + i
        data.append([
            f'=A{ar}',
            f'=B{mid_r}',                      # Same CU/slot
            f'=C{mid_r}',                      # Same Fee/slot
            f'=D{mid_r}',                      # Same active slots
            f'=E{mid_r}',                      # Same rev/cust SOL
            f'=F{mid_r}',                      # Same rev/cust USD
            f'=F{ar}',                          # N customers (Low column = F in arch params)
            f'=F{r}*G{r}',
            f'=H{r}*$C$8',
            f'=I{r}/12',
        ])

    low_total = low_data_start + N_ARCHETYPES
    data.append([
        "TOTAL (Low)", "", "", "", "", "",
        f'=SUM(G{low_data_start}:G{low_total-1})',
        f'=SUM(H{low_data_start}:H{low_total-1})',
        f'=SUM(I{low_data_start}:I{low_total-1})',
        f'=SUM(J{low_data_start}:J{low_total-1})',
    ])

    # HIGH scenario
    data.append(["", "", "", "", "", "", "", "", "", ""])
    high_start = low_total + 2
    data.append(["REVENUE BY ARCHETYPE — High customers (optimistic)", "", "", "", "", "", "", "", "", ""])
    data.append(["Archetype", "CU/slot", "Fee/slot (SOL)", "Active slots/yr",
                 "Rev/customer/yr (SOL)", "Rev/customer/yr (USD)", "N customers",
                 "Gross revenue (USD)", "Protocol (5%)", "Protocol/month"])

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
            f'=H{ar}',                          # N customers (High column = H in arch params)
            f'=F{r}*G{r}',
            f'=H{r}*$C$8',
            f'=I{r}/12',
        ])

    high_total = high_data_start + N_ARCHETYPES
    data.append([
        "TOTAL (High)", "", "", "", "", "",
        f'=SUM(G{high_data_start}:G{high_total-1})',
        f'=SUM(H{high_data_start}:H{high_total-1})',
        f'=SUM(I{high_data_start}:I{high_total-1})',
        f'=SUM(J{high_data_start}:J{high_total-1})',
    ])

    write_batch(ws, data, 10)

    # Formatting
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})
    ws.format("A4", {"textFormat": {"bold": True}})
    ws.format("A12", {"textFormat": {"bold": True}})

    # Style archetype parameter headers
    ws.format(f"A{AOT_BU_ARCH_HEADER_ROW}:J{AOT_BU_ARCH_HEADER_ROW}", {
        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.3},
        "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
    })

    # Style revenue section headers (Mid, Low, High)
    for hdr_row in [rev_header, low_start + 1, high_start + 1]:
        ws.format(f"A{hdr_row}:J{hdr_row}", {
            "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.3},
            "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
        })

    # Bold total rows
    for tr in [total_row, low_total, high_total]:
        ws.format(f"A{tr}:J{tr}", {"textFormat": {"bold": True}})

    # Bold section labels
    for sr in [rev_start, low_start, high_start]:
        ws.format(f"A{sr}", {"textFormat": {"bold": True}})

    ws.freeze(rows=1)
    time.sleep(RATE_LIMIT_SLEEP)

    print(f"  ✓ AOT Bottom-Up: {N_ARCHETYPES} archetypes × 3 scenarios (Low/Mid/High)")
    print(f"    Mid total: row {total_row} | Low total: row {low_total} | High total: row {high_total}")
    return True


# ══════════════════════════════════════════════════════════
# TAB 7: REVENUE SUMMARY
# ══════════════════════════════════════════════════════════

def export_revenue_summary(spreadsheet, dry_run=False):
    """Create revenue summary tab linking JIT + AOT models.

    Cross-tab references are computed programmatically to guarantee correctness.
    """
    print("  Revenue Summary: building consolidated view")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "Revenue Summary", 35, 10)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)

    # ── Compute exact cell references for each scenario ──────

    # JIT references (col C = Gross Revenue, col E = Protocol)
    #   Conservative: Q4-2025 bear (source idx=2) × 5% share (share idx=1)
    #   Base: Avg 10 epochs (idx=1) × 10% share (idx=2)
    #   Optimistic: 2025 full year bull (idx=3) × 15% share (idx=3)
    jit_conservative_row = jit_scenario_row(2, 1)   # Q4 bear × 5%
    jit_base_row = jit_scenario_row(1, 2)           # Avg 10ep × 10%
    jit_optimistic_row = jit_scenario_row(3, 3)     # Bull × 15%

    # AOT Top-Down references (col E = Gross Revenue, col F = Protocol)
    #   Conservative: Avg 10ep (idx=1) × 30% latency (idx=0) × 5% capture (idx=0)
    #   Base: Avg 10ep (idx=1) × 40% latency (idx=1) × 10% capture (idx=1)
    #   Optimistic: Latest (idx=0) × 50% latency (idx=2) × 15% capture (idx=2)
    aot_td_conservative_row = aot_td_scenario_row(1, 0, 0)  # Avg × 30% × 5%
    aot_td_base_row = aot_td_scenario_row(1, 1, 1)          # Avg × 40% × 10%
    aot_td_optimistic_row = aot_td_scenario_row(0, 2, 2)    # Latest × 50% × 15%

    # AOT Bottom-Up references (col H = Gross Revenue)
    #   Conservative: Low customer total
    #   Base: Mid customer total
    #   Optimistic: High customer total
    bu_mid_total = _aot_bu_total_row()
    # Low total = mid_total + 2 (blank) + 1 (section label) + 1 (header) + N_ARCHETYPES
    bu_low_total = bu_mid_total + 2 + 1 + 1 + N_ARCHETYPES
    # High total = low_total + 2 + 1 + 1 + N_ARCHETYPES
    bu_high_total = bu_low_total + 2 + 1 + 1 + N_ARCHETYPES

    # Verify computed positions match the actual function results
    # (These MUST match the layout in export_aot_bottomup)
    assert bu_mid_total == _aot_bu_total_row(), "BU mid total row mismatch"

    print(f"    JIT refs: Conservative=C{jit_conservative_row}, Base=C{jit_base_row}, Optimistic=C{jit_optimistic_row}")
    print(f"    AOT TD refs: Conservative=E{aot_td_conservative_row}, Base=E{aot_td_base_row}, Optimistic=E{aot_td_optimistic_row}")
    print(f"    AOT BU refs: Low=H{bu_low_total}, Mid=H{bu_mid_total}, High=H{bu_high_total}")

    data = [
        # Row 1
        ["RAIKU Revenue Summary", "", "", "", "", "", "", ""],
        # Row 2
        ["All values in USD/year — 100% driven by Google Sheet formulas", "", "", "", "", "", "", ""],
        # Row 3: protocol fee parameter
        ["Protocol fee", PROTOCOL_TAKE_RATE, "", "", "", "", "", ""],
        # Row 4: column headers
        ["Scenario", "JIT Gross", "AOT TD Gross", "AOT BU Gross", "Total Gross",
         "Protocol Revenue", "Protocol/month", "JIT + AOT TD + BU description"],
        # Row 5: Conservative
        ["Conservative (Y1)",
         f"='JIT Model'!C{jit_conservative_row}",                  # JIT gross
         f"='AOT Top-Down'!E{aot_td_conservative_row}",            # AOT TD gross
         f"='AOT Bottom-Up'!H{bu_low_total}",                      # AOT BU gross (low)
         f'=B5+C5+D5',                                             # Total gross
         f'=E5*$B$3',                                               # Protocol
         f'=F5/12',                                                 # Protocol/month
         f"JIT: Q4 bear×5% | AOT TD: avg×30%×5% | AOT BU: low customers"],
        # Row 6: Base
        ["Base",
         f"='JIT Model'!C{jit_base_row}",
         f"='AOT Top-Down'!E{aot_td_base_row}",
         f"='AOT Bottom-Up'!H{bu_mid_total}",
         f'=B6+C6+D6',
         f'=E6*$B$3',
         f'=F6/12',
         f"JIT: avg 10ep×10% | AOT TD: avg×40%×10% | AOT BU: mid customers"],
        # Row 7: Optimistic
        ["Optimistic (Bull)",
         f"='JIT Model'!C{jit_optimistic_row}",
         f"='AOT Top-Down'!E{aot_td_optimistic_row}",
         f"='AOT Bottom-Up'!H{bu_high_total}",
         f'=B7+C7+D7',
         f'=E7*$B$3',
         f'=F7/12',
         f"JIT: bull×15% | AOT TD: latest×50%×15% | AOT BU: high customers"],
        # Row 8: blank
        ["", "", "", "", "", "", "", ""],
        # Row 9: Validator perspective
        ["VALIDATOR PERSPECTIVE", "", "", "", "", "", "", ""],
        # Row 10
        ["Scenario", "Total Gross", "To Validators (95%)", "To Protocol (5%)",
         "", "", "", ""],
        # Row 11
        ["Conservative",
         f'=E5', f'=E5*(1-$B$3)', f'=E5*$B$3', "", "", "", ""],
        # Row 12
        ["Base",
         f'=E6', f'=E6*(1-$B$3)', f'=E6*$B$3', "", "", "", ""],
        # Row 13
        ["Optimistic",
         f'=E7', f'=E7*(1-$B$3)', f'=E7*$B$3', "", "", "", ""],
        # Row 14: blank
        ["", "", "", "", "", "", "", ""],
        # Row 15: Revenue breakdown
        ["REVENUE MIX (% of gross)", "", "", "", "", "", "", ""],
        # Row 16
        ["Scenario", "JIT %", "AOT TD %", "AOT BU %", "", "", "", ""],
        # Row 17
        ["Conservative",
         f'=IF(E5>0, B5/E5, "")', f'=IF(E5>0, C5/E5, "")', f'=IF(E5>0, D5/E5, "")',
         "", "", "", ""],
        # Row 18
        ["Base",
         f'=IF(E6>0, B6/E6, "")', f'=IF(E6>0, C6/E6, "")', f'=IF(E6>0, D6/E6, "")',
         "", "", "", ""],
        # Row 19
        ["Optimistic",
         f'=IF(E7>0, B7/E7, "")', f'=IF(E7>0, C7/E7, "")', f'=IF(E7>0, D7/E7, "")',
         "", "", "", ""],
        # Row 20: blank
        ["", "", "", "", "", "", "", ""],
        # Row 21: Notes
        ["CELL REFERENCE MAP (for audit)", "", "", "", "", "", "", ""],
        # Row 22
        [f"Conservative JIT: 'JIT Model'!C{jit_conservative_row}", "",
         f"= Q4-2025 bear market × 5% share", "", "", "", "", ""],
        # Row 23
        [f"Conservative AOT TD: 'AOT Top-Down'!E{aot_td_conservative_row}", "",
         f"= Avg 10ep × 30% latency × 5% capture", "", "", "", "", ""],
        # Row 24
        [f"Conservative AOT BU: 'AOT Bottom-Up'!H{bu_low_total}", "",
         f"= 6 archetypes, low customer count", "", "", "", "", ""],
        # Row 25
        [f"Base JIT: 'JIT Model'!C{jit_base_row}", "",
         f"= Avg 10 epochs × 10% share", "", "", "", "", ""],
        # Row 26
        [f"Base AOT TD: 'AOT Top-Down'!E{aot_td_base_row}", "",
         f"= Avg 10ep × 40% latency × 10% capture", "", "", "", "", ""],
        # Row 27
        [f"Base AOT BU: 'AOT Bottom-Up'!H{bu_mid_total}", "",
         f"= 6 archetypes, mid customer count", "", "", "", "", ""],
        # Row 28
        [f"Optimistic JIT: 'JIT Model'!C{jit_optimistic_row}", "",
         f"= 2025 bull year × 15% share", "", "", "", "", ""],
        # Row 29
        [f"Optimistic AOT TD: 'AOT Top-Down'!E{aot_td_optimistic_row}", "",
         f"= Latest epoch × 50% latency × 15% capture", "", "", "", "", ""],
        # Row 30
        [f"Optimistic AOT BU: 'AOT Bottom-Up'!H{bu_high_total}", "",
         f"= 6 archetypes, high customer count", "", "", "", "", ""],
    ]

    write_batch(ws, data, 8)

    # Formatting
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 16}})
    ws.format("B3", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})
    ws.format("A3", {"textFormat": {"bold": True}})
    ws.format("A4:H4", {
        "backgroundColor": {"red": 0.15, "green": 0.15, "blue": 0.25},
        "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
    })
    ws.format("A5:A7", {"textFormat": {"bold": True}})
    ws.format("B5:G7", {"numberFormat": {"type": "NUMBER", "pattern": "$#,##0"}})

    # Validator perspective section
    ws.format("A9", {"textFormat": {"bold": True}})
    ws.format("A10:D10", {
        "backgroundColor": {"red": 0.15, "green": 0.15, "blue": 0.25},
        "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
    })
    ws.format("B11:D13", {"numberFormat": {"type": "NUMBER", "pattern": "$#,##0"}})

    # Revenue mix section
    ws.format("A15", {"textFormat": {"bold": True}})
    ws.format("A16:D16", {
        "backgroundColor": {"red": 0.15, "green": 0.15, "blue": 0.25},
        "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}}},
    })
    ws.format("B17:D19", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})

    # Audit section (grey, italic)
    ws.format("A21", {"textFormat": {"bold": True}})
    ws.format("A22:C30", {"textFormat": {"italic": True, "foregroundColorStyle": {"rgbColor": {"red": 0.5, "green": 0.5, "blue": 0.5}}}})

    ws.freeze(rows=4)
    time.sleep(RATE_LIMIT_SLEEP)

    print(f"  ✓ Revenue Summary: 3 scenarios × 3 revenue streams (all formulas)")
    return True


# ══════════════════════════════════════════════════════════
# TAB 8: DATA SOURCES
# ══════════════════════════════════════════════════════════

def export_data_sources(spreadsheet, dry_run=False):
    """Create documentation tab."""
    print("  Data Sources: writing documentation")

    if dry_run:
        return True

    ws, _ = get_or_create_ws(spreadsheet, "Data Sources", 80, 6)
    ws.clear()
    time.sleep(RATE_LIMIT_SLEEP)

    data = [
        ["RAIKU Revenue Model — Data Sources & Column Documentation", "", "", "", "", ""],
        ["", "", "", "", "", ""],
        ["EPOCH DATABASE — RAW Columns (A-W)", "", "", "", "", ""],
        ["Column", "Name", "Type", "Source", "Coverage", "Description"],
        ["A", "epoch", "RAW", "All", "150-935+", "Epoch number (primary key)"],
        ["B", "date", "RAW", "Trillium/Dune", "150-935+", "Start date of epoch"],
        ["C", "duration_days", "RAW", "Trillium/Dune", "150-935+", "Duration in days"],
        ["D", "inflation_rewards_sol", "RAW", "Trillium/Dune", "150-935+", "Total inflation rewards (SOL)"],
        ["E", "total_fees_sol", "RAW", "Trillium/Dune", "150-935+", "Total fees base+priority (SOL)"],
        ["F", "priority_fees_sol", "RAW", "Trillium", "552+", "Priority fees only (SOL)"],
        ["G", "base_fees_sol", "RAW", "Trillium", "552+", "Base/signature fees (SOL)"],
        ["H", "mev_jito_tips_sol", "RAW", "Trillium/Dune", "390+", "Total Jito MEV tips (SOL)"],
        ["I", "mev_to_validators_sol", "RAW", "Trillium", "552+", "MEV share → validators"],
        ["J", "mev_to_stakers_sol", "RAW", "Trillium", "552+", "MEV share → stakers"],
        ["K", "mev_to_jito_sol", "RAW", "Trillium", "552+", "MEV share → Jito (block engine + tip router)"],
        ["L", "validator_commissions_sol", "RAW", "Dune", "150-935+", "Validator commissions on inflation"],
        ["M", "avg_commission_rate", "RAW", "Dune/Trillium", "150-935+", "Stake-weighted avg commission rate"],
        ["N", "validator_count", "RAW", "Trillium/Dune", "150-935+", "Number of active validators"],
        ["O", "stake_accounts", "RAW", "Dune", "150-935+", "Number of stake accounts"],
        ["P", "active_stake_sol", "RAW", "Trillium/Dune", "150-935+", "Total active stake (SOL)"],
        ["Q", "sol_price_usd", "RAW", "Trillium/CoinGecko", "150-935+", "SOL price (USD)"],
        ["R", "fdv_usd", "RAW", "CoinGecko", "365 days", "Fully Diluted Valuation (USD)"],
        ["S", "epochs_per_year", "RAW", "Trillium", "552+", "Epochs per year (=365.25/duration)"],
        ["T", "avg_cu_per_block", "RAW", "Trillium", "552+", "Average compute units per block"],
        ["U", "total_user_txns", "RAW", "Trillium", "552+", "Non-vote transactions in epoch"],
        ["V", "total_vote_txns", "RAW", "Trillium", "552+", "Vote transactions in epoch"],
        ["W", "total_blocks", "RAW", "Trillium", "552+", "Blocks produced in epoch"],
        ["", "", "", "", "", ""],
        ["EPOCH DATABASE — FORMULA Columns (X-AO)", "", "", "", "", ""],
        ["Column", "Name", "Type", "Formula", "", "Description"],
        ["X", "total_rewards_sol", "FORMULA", "=D+E+H", "", "Inflation + Fees + MEV"],
        ["Y", "total_rewards_usd", "FORMULA", "=X*Q", "", "Total rewards in USD"],
        ["Z", "staker_rewards_sol", "FORMULA", "=D-L", "", "Inflation minus commissions"],
        ["AA", "fee_pct_of_total", "FORMULA", "=E/X", "", "Fee share of total rewards"],
        ["AB", "mev_pct_of_total", "FORMULA", "=H/X", "", "MEV share of total rewards"],
        ["AC", "effective_commission", "FORMULA", "=L/D", "", "Effective commission rate"],
        ["AD", "inflation_apr", "FORMULA", "=D*(365.25/C)/P", "", "Inflation APR annualized"],
        ["AE", "fee_apr", "FORMULA", "=E*(365.25/C)/P", "", "Fee APR annualized"],
        ["AF", "mev_apr", "FORMULA", "=H*(365.25/C)/P", "", "MEV APR annualized"],
        ["AG", "total_apr", "FORMULA", "=AD+AE+AF", "", "Total APR"],
        ["AH", "total_apy", "FORMULA", "=(1+AG/365.25)^365.25-1", "", "Total APY (compound)"],
        ["AI", "total_supply_sol", "FORMULA", "=R/Q", "", "Estimated total supply"],
        ["AJ", "staked_ratio", "FORMULA", "=P/AI", "", "Staked ratio"],
        ["AK", "burn_sol", "FORMULA", "=IF(epoch<620, E*0.5, 0)", "", "Fee burn (pre-SIMD-96)"],
        ["AL", "net_inflation_sol", "FORMULA", "=D-AK", "", "Net inflation after burn"],
        ["AM", "annual_total_fees_usd", "FORMULA", "=E*(365.25/C)*Q", "", "Annualized total fees (USD)"],
        ["AN", "annual_mev_usd", "FORMULA", "=H*(365.25/C)*Q", "", "Annualized MEV (USD)"],
        ["AO", "annual_priority_fees_usd", "FORMULA", "=F*(365.25/C)*Q", "", "Annualized priority fees (USD)"],
        ["", "", "", "", "", ""],
        ["MODEL TABS — Cross-Reference Guide", "", "", "", "", ""],
        ["Tab", "Key Cells", "", "Description", "", ""],
        ["JIT Model", f"C4:C7 = market sources", "", "4 market × 5 shares = 20 scenarios", "", ""],
        ["JIT Model", f"C8 = protocol fee", "", f"Scenarios start row {JIT_SCENARIO_START_ROW}", "", ""],
        ["AOT Top-Down", f"C5:C6 = fee sources", "", "2 sources × 4 lat × 4 cap = 32 scenarios", "", ""],
        ["AOT Top-Down", f"C7 = protocol fee", "", f"Scenarios start row {AOT_TD_SCENARIO_START_ROW}", "", ""],
        ["AOT Bottom-Up", f"C5=SOL price, C8=fee, C9=stake%", "", f"6 archetypes × Low/Mid/High", "", ""],
        ["AOT Bottom-Up", f"Row {_aot_bu_total_row()}=Mid total", "", f"Row {_aot_bu_total_row()+2+1+1+N_ARCHETYPES}=Low, Row {_aot_bu_total_row()+2*(2+1+1+N_ARCHETYPES)}=High", "", ""],
        ["Revenue Summary", "B3 = protocol fee", "", "3 scenarios referencing all model tabs", "", ""],
        ["", "", "", "", "", ""],
        ["DATA SOURCES", "", "", "", "", ""],
        ["Source", "Coverage", "Status", "Auth", "", "Notes"],
        ["Trillium API", "Epochs 552+", "Active", "None (free)", "", "Primary source: 141 fields per epoch"],
        ["Dune Analytics", "Epochs 150+", "Active", "API key", "", "Secondary: economics, commissions, stake"],
        ["Jito Foundation", "Epochs 390+", "Cross-check", "None (free)", "", "Official MEV totals (1.000x match)"],
        ["Solana Compass", "Epochs 800+", "Cross-check", "None (free)", "", "Per-validator aggregated (~2.15x fees ratio)"],
        ["CoinGecko", "365 days", "Active", "None (free)", "", "SOL price and FDV"],
        ["SolWatch (internal)", "13 days", "Reference", "N/A", "", "1897 programs, pain scores, fail rates"],
    ]

    write_batch(ws, data, 6)
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})
    ws.format("A3", {"textFormat": {"bold": True, "fontSize": 12}})
    ws.format("A4:F4", {"textFormat": {"bold": True}, "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.95}})

    # Find and format section headers
    section_rows = []
    for idx, row in enumerate(data):
        if row and row[0] in ("EPOCH DATABASE — FORMULA Columns (X-AO)",
                              "MODEL TABS — Cross-Reference Guide",
                              "DATA SOURCES"):
            section_rows.append(idx + 1)  # 1-indexed

    for sr in section_rows:
        ws.format(f"A{sr}", {"textFormat": {"bold": True, "fontSize": 12}})
        if sr + 1 <= len(data):
            ws.format(f"A{sr+1}:F{sr+1}", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.95},
            })

    ws.freeze(rows=1)
    time.sleep(RATE_LIMIT_SLEEP)

    print(f"  ✓ Data Sources: documentation written")
    return True


# ══════════════════════════════════════════════════════════
# CLEANUP: Delete old tabs
# ══════════════════════════════════════════════════════════

OLD_TABS_TO_DELETE = [
    "Epoch Economics",
    "JIT Scenarios",
    "AOT Scenarios",
]


def cleanup_old_tabs(spreadsheet, dry_run=False):
    """Delete deprecated tabs."""
    for tab_name in OLD_TABS_TO_DELETE:
        try:
            ws = spreadsheet.worksheet(tab_name)
            if dry_run:
                print(f"  DRY RUN: Would delete tab '{tab_name}'")
            else:
                spreadsheet.del_worksheet(ws)
                print(f"  Deleted old tab: '{tab_name}'")
                time.sleep(RATE_LIMIT_SLEEP)
        except gspread.WorksheetNotFound:
            pass  # Already gone


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

EXPORT_FUNCTIONS = {
    "epoch": ("Epoch Database", export_epoch_database),
    "programs": ("Program Database", export_program_database),
    "mapping": ("Program Mapping", export_program_mapping),
    "jit": ("JIT Model", export_jit_model),
    "aot_td": ("AOT Top-Down", export_aot_topdown),
    "aot_bu": ("AOT Bottom-Up", export_aot_bottomup),
    "summary": ("Revenue Summary", export_revenue_summary),
    "sources": ("Data Sources", export_data_sources),
}


def main():
    parser = argparse.ArgumentParser(description="Export to Google Sheets (RAW data + formulas)")
    parser.add_argument("--tab", choices=list(EXPORT_FUNCTIONS.keys()) + ["all"], default="all",
                        help="Which tab to export (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--no-cleanup", action="store_true", help="Don't delete old tabs")
    args = parser.parse_args()

    print("=" * 60)
    print("  RAIKU Revenue Model — Google Sheets Export v3")
    print("  RAW data + Google Sheet formulas (no pre-computation)")
    print("=" * 60)

    # Connect
    if not args.dry_run:
        if not SERVICE_ACCOUNT_FILE.exists():
            print(f"\nERROR: {SERVICE_ACCOUNT_FILE} not found")
            sys.exit(1)
        print(f"\nConnecting to Google Sheets...")
        gc = gspread.service_account(filename=str(SERVICE_ACCOUNT_FILE))
        spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
        print(f"  Connected: '{spreadsheet.title}'")
    else:
        print(f"\n  DRY RUN MODE")
        spreadsheet = None

    # Cleanup old tabs
    if not args.no_cleanup and not args.dry_run:
        print(f"\n--- Cleaning up old tabs ---")
        cleanup_old_tabs(spreadsheet, dry_run=args.dry_run)

    # Export
    tabs = list(EXPORT_FUNCTIONS.keys()) if args.tab == "all" else [args.tab]
    success = 0

    for key in tabs:
        label, func = EXPORT_FUNCTIONS[key]
        print(f"\n--- {label} ---")
        try:
            ok = func(spreadsheet, dry_run=args.dry_run)
            if ok:
                success += 1
            if not args.dry_run:
                time.sleep(2)
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()

    # Summary
    print(f"\n{'='*60}")
    print(f"  Export complete: {success}/{len(tabs)} tabs")
    if not args.dry_run:
        print(f"  Sheet: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

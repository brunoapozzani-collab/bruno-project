# Fixed Expense Extractor from General Ledger

## Objective
Pull a multi-company general ledger (.xlsx) from Dropbox, filter by company, date range, and pre-configured expense categories, then produce a categorized Excel export and a formatted PDF executive summary ("Resumo Executivo").

## Inputs
- **Source file**: downloaded from Dropbox path `LEDGER_DROPBOX_PATH` (env var). Cached in `.tmp/`. Override with `--local <path>`.
- **Categories config**: `config/categorias.xlsx` — columns `palavra_chave | categoria | empresa` (empresa optional). User edits this file directly.
- **At runtime** (interactive prompts):
  - Sheet name (if multiple)
  - Confirm/select empresa, data, descrição, valor, conta columns
  - Multi-select companies
  - Use default period (previous full month) or enter MM/AAAA range
  - Multi-select categorias

## Tools
- `tools/dropbox_client.py` — `download_ledger()` pulls from Dropbox, caches by mtime
- `tools/extract_fixed_expenses.py` — main interactive CLI

## How matching works
- Each rule in `config/categorias.xlsx` is `(palavra_chave, categoria, empresa)`
- Match is accent/case-insensitive substring across **descrição + conta** columns
- Rules with an `empresa` only fire on rows from that empresa; blank empresa = matches any
- A row gets the **first** matching categoria (no double-counting)

## Outputs
- `output/transacoes_<timestamp>.xlsx` — one tab per selected empresa
- `output/resumo_executivo_<timestamp>.pdf` — totals by empresa / categoria / mês

## Setup (Windows end-user)
1. Run `setup.bat` once (installs Python deps into venv)
2. Edit `.env` with `DROPBOX_ACCESS_TOKEN` and `LEDGER_DROPBOX_PATH`
3. Edit `config/categorias.xlsx` to add expense rules
4. Run `build.bat` once → produces `dist\Thais.exe`
5. Double-click `Thais.exe` thereafter

## Edge Cases & Notes
- Header row auto-detected by scanning first 10 rows for cells matching column hints
- Date column coerced with `dayfirst=True`
- BR currency parsed (`1.234,56` → `1234.56`)
- If multiple sheets, prompt asks which one
- Cache: file is re-downloaded only when Dropbox `server_modified` is newer than local mtime

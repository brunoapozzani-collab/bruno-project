"""
Extrator de Despesas Fixas — Razão Geral

CLI interativo em português. Baixa o razão geral do Dropbox (ou usa
um arquivo local), filtra por empresa, período e categorias definidas
em config/categorias.xlsx, e gera:
  - output/transacoes_<timestamp>.xlsx (uma aba por empresa)
  - output/resumo_executivo_<timestamp>.pdf

Uso:
    python tools/extract_fixed_expenses.py              # baixa do Dropbox
    python tools/extract_fixed_expenses.py --local <arquivo.xlsx>
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from calendar import monthrange
from datetime import datetime
from pathlib import Path

import pandas as pd
import questionary
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table as RLTable, TableStyle, PageBreak,
    Image as RLImage,
)
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import HorizontalBarChart

# ---------- TAAG brand ----------
TAAG_BLACK    = colors.HexColor("#1a1a1a")
TAAG_BROWN    = colors.HexColor("#887653")
TAAG_LIGHT    = colors.HexColor("#f5f1ea")
TAAG_GREY     = colors.HexColor("#6b6b6b")
TAAG_TAGLINE  = "Liderança em Automação, Áudio e Vídeo desde 1997"
LOGO_PATH     = Path(__file__).resolve().parent.parent / "assets" / "logo.png"

# Allow `python tools/extract_fixed_expenses.py` from project root
sys.path.insert(0, str(Path(__file__).resolve().parent))
from dropbox_client import download_ledger, DropboxError  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "categorias.xlsx"

console = Console()

# ---------- text helpers ----------

def strip_accents(s) -> str:
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    ).lower().strip()


COLUMN_HINTS = {
    "empresa":   ["empresa", "company", "filial", "razao social", "cnpj"],
    "data":      ["data", "date", "competencia", "emissao", "vencimento"],
    "descricao": ["descricao", "historico", "description", "memo", "nome"],
    "valor":     ["valor", "value", "amount", "montante", "total"],
    "conta":     ["conta", "account", "codigo", "plano de contas", "categoria"],
    "endereco":  ["endereco", "address", "logradouro", "rua", "local"],
    "favorecido":["favorecido", "cliente", "fornecedor", "beneficiario", "beneficiário", "pagador", "razao social"],
}


def detect_header_row(xlsx_path: Path, sheet: str) -> int:
    """Scan first 10 rows; pick the row whose cells most resemble headers."""
    raw = pd.read_excel(xlsx_path, sheet_name=sheet, header=None, nrows=10)
    best_row, best_score = 0, -1
    all_hints = [h for hs in COLUMN_HINTS.values() for h in hs]
    for i, row in raw.iterrows():
        score = 0
        for cell in row:
            n = strip_accents(cell)
            if not n:
                continue
            if any(h in n for h in all_hints):
                score += 2
            elif isinstance(cell, str):
                score += 1
        if score > best_score:
            best_score, best_row = score, i
    return int(best_row)


def auto_detect_columns(df: pd.DataFrame) -> dict[str, str | None]:
    norm = {col: strip_accents(str(col)) for col in df.columns}
    mapping: dict[str, str | None] = {}
    for key, hints in COLUMN_HINTS.items():
        found = None
        for col, n in norm.items():
            if any(h in n for h in hints):
                found = col
                break
        mapping[key] = found
    return mapping


def confirm_or_pick_column(df: pd.DataFrame, role: str, detected: str | None, required: bool) -> str | None:
    cols = list(df.columns)
    if detected:
        ok = questionary.confirm(
            f"Coluna detectada para '{role}': '{detected}'. Está correta?",
            default=True,
        ).ask()
        if ok:
            return detected
    if not required:
        skip = questionary.confirm(f"Pular coluna opcional '{role}'?", default=True).ask()
        if skip:
            return None
    return questionary.select(f"Selecione a coluna para '{role}':", choices=cols).ask()


def parse_value(v) -> float:
    if pd.isna(v):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = re.sub(r"[^\d,.\-]", "", str(v).strip())
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


# ---------- categories config ----------

def load_categories() -> list[dict]:
    """Load palavra_chave / categoria / empresa rows from config/categorias.xlsx."""
    if not CONFIG_PATH.exists():
        console.print(f"[red]Arquivo de categorias não encontrado: {CONFIG_PATH}[/red]")
        sys.exit(1)
    df = pd.read_excel(CONFIG_PATH)
    needed = {"palavra_chave", "categoria"}
    if not needed.issubset({c.lower() for c in df.columns}):
        console.print(f"[red]config/categorias.xlsx precisa das colunas: palavra_chave, categoria, empresa[/red]")
        sys.exit(1)
    df.columns = [c.lower() for c in df.columns]
    if "empresa" not in df.columns:
        df["empresa"] = ""
    out = []
    for _, r in df.iterrows():
        kw = str(r["palavra_chave"]).strip()
        cat = str(r["categoria"]).strip()
        emp = "" if pd.isna(r["empresa"]) else str(r["empresa"]).strip()
        if kw and cat:
            out.append({"kw": kw, "categoria": cat, "empresa": emp})
    return out


def categorize_row(row, search_cols: list[str], rules: list[dict], empresa_value: str) -> str | None:
    """Return the first matching categoria for this row, or None.

    A rule matches if its keyword (accent/case-insensitive) appears in any
    of the searchable columns. Rules scoped to an empresa only match rows
    from that empresa; unscoped rules match any.
    """
    haystack = " ".join(strip_accents(row[c]) for c in search_cols if c in row.index)
    emp_n = strip_accents(empresa_value)
    for rule in rules:
        if rule["empresa"] and strip_accents(rule["empresa"]) != emp_n:
            continue
        if strip_accents(rule["kw"]) in haystack:
            return rule["categoria"]
    return None


# ---------- date helpers ----------

def previous_month_range(today: datetime | None = None) -> tuple[datetime, datetime, str, str]:
    today = today or datetime.now()
    first_this = datetime(today.year, today.month, 1)
    last_prev_year = first_this.year if first_this.month > 1 else first_this.year - 1
    last_prev_month = first_this.month - 1 if first_this.month > 1 else 12
    start = datetime(last_prev_year, last_prev_month, 1)
    end_excl = first_this  # exclusive
    return start, end_excl, start.strftime("%m/%Y"), start.strftime("%m/%Y")


# ---------- main ----------

def main():
    load_dotenv(PROJECT_ROOT / ".env")

    ap = argparse.ArgumentParser()
    ap.add_argument("--local", help="Caminho local para o .xlsx (pula o Dropbox)")
    args = ap.parse_args()

    # Source
    if args.local:
        excel_path = Path(args.local).expanduser().resolve()
        if not excel_path.exists():
            console.print(f"[red]Arquivo não encontrado: {excel_path}[/red]")
            sys.exit(1)
    else:
        console.print("[cyan]Baixando razão do Dropbox...[/cyan]")
        try:
            excel_path = download_ledger()
        except DropboxError as e:
            console.print(f"[red]{e}[/red]")
            sys.exit(1)
        console.print(f"[green]✓[/green] {excel_path.name}")

    # Categories config
    rules = load_categories()
    if not rules:
        console.print("[red]Nenhuma regra em config/categorias.xlsx.[/red]")
        sys.exit(1)
    categorias_disponiveis = sorted({r["categoria"] for r in rules})

    # Sheet
    xls = pd.ExcelFile(excel_path)
    sheet = xls.sheet_names[0]
    if len(xls.sheet_names) > 1:
        sheet = questionary.select("Qual aba contém o razão geral?", choices=xls.sheet_names).ask()

    # Header detection
    header_row = detect_header_row(excel_path, sheet)
    console.print(f"[dim]Cabeçalho detectado na linha {header_row + 1}.[/dim]")
    df = pd.read_excel(excel_path, sheet_name=sheet, header=header_row)
    df = df.dropna(how="all")
    console.print(f"[green]✓[/green] {len(df)} linhas carregadas.\n")

    # Columns
    detected = auto_detect_columns(df)
    col_empresa = confirm_or_pick_column(df, "empresa",   detected["empresa"],   True)
    col_data    = confirm_or_pick_column(df, "data",      detected["data"],      True)
    col_desc    = confirm_or_pick_column(df, "descricao", detected["descricao"], True)
    col_valor   = confirm_or_pick_column(df, "valor",     detected["valor"],     True)
    col_conta   = confirm_or_pick_column(df, "conta",     detected["conta"],     False)

    # Normalize
    df[col_data] = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)
    df["_valor_num"] = df[col_valor].apply(parse_value)
    df = df.dropna(subset=[col_data, col_empresa])

    # Empresas
    empresas = sorted(df[col_empresa].dropna().astype(str).unique().tolist())
    selected_companies = questionary.checkbox("Selecione a(s) empresa(s):", choices=empresas).ask()
    if not selected_companies:
        console.print("[red]Nenhuma empresa selecionada.[/red]")
        sys.exit(0)

    # Date range — default = previous full month
    start_def, end_def_excl, start_str_def, end_str_def = previous_month_range()
    use_default = questionary.confirm(
        f"Usar período padrão (mês anterior: {start_str_def})?", default=True
    ).ask()
    if use_default:
        start, end_excl, start_str, end_str = start_def, end_def_excl, start_str_def, end_str_def
    else:
        start_str = questionary.text("Mês/Ano inicial (MM/AAAA):").ask()
        end_str   = questionary.text("Mês/Ano final (MM/AAAA):").ask()
        try:
            start = datetime.strptime(start_str.strip(), "%m/%Y")
            end_m = datetime.strptime(end_str.strip(), "%m/%Y")
            last_day = monthrange(end_m.year, end_m.month)[1]
            end_excl = datetime(end_m.year, end_m.month, last_day) + pd.Timedelta(days=1)
        except ValueError:
            console.print("[red]Formato inválido. Use MM/AAAA.[/red]")
            sys.exit(1)

    # Categorias
    selected_cats = questionary.checkbox(
        "Selecione as categorias a incluir:", choices=categorias_disponiveis
    ).ask()
    if not selected_cats:
        console.print("[red]Nenhuma categoria selecionada.[/red]")
        sys.exit(0)
    active_rules = [r for r in rules if r["categoria"] in selected_cats]

    # Filter by empresa + período
    mask = (
        df[col_empresa].astype(str).isin(selected_companies)
        & (df[col_data] >= start)
        & (df[col_data] < end_excl)
    )
    filtered = df.loc[mask].copy()

    # Apply rules across description + conta
    search_cols = [col_desc] + ([col_conta] if col_conta else [])
    filtered["_categoria"] = filtered.apply(
        lambda r: categorize_row(r, search_cols, active_rules, str(r[col_empresa])), axis=1
    )
    filtered = filtered[filtered["_categoria"].notna()]
    filtered["_mes"] = filtered[col_data].dt.to_period("M").astype(str)

    if filtered.empty:
        console.print("[yellow]Nenhuma transação encontrada.[/yellow]")
        sys.exit(0)

    console.print(f"\n[green]✓[/green] {len(filtered)} transações encontradas.\n")

    table = Table(title="Prévia (top 10)")
    for c in [col_empresa, col_data, col_desc, "_categoria", "_valor_num"]:
        table.add_column(str(c))
    for _, row in filtered.head(10).iterrows():
        table.add_row(
            str(row[col_empresa]),
            row[col_data].strftime("%d/%m/%Y"),
            str(row[col_desc])[:50],
            str(row["_categoria"]),
            f"{row['_valor_num']:,.2f}",
        )
    console.print(table)

    # Outputs
    output_dir = PROJECT_ROOT / "output"
    output_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    xlsx_out = output_dir / f"transacoes_{ts}.xlsx"
    pdf_out  = output_dir / f"resumo_executivo_{ts}.pdf"

    cols_keep = [col_empresa, col_data, col_desc, "_categoria", "_valor_num"]
    rename = {col_empresa: "Empresa", col_data: "Data", col_desc: "Descrição",
              "_categoria": "Categoria", "_valor_num": "Valor"}
    if col_conta:
        cols_keep.insert(3, col_conta)
        rename[col_conta] = "Conta"

    with pd.ExcelWriter(xlsx_out, engine="openpyxl") as writer:
        for emp in selected_companies:
            sub = filtered[filtered[col_empresa].astype(str) == emp][cols_keep].rename(columns=rename)
            sub = sub.sort_values("Data")
            sheet_name = re.sub(r"[\[\]\*\?:/\\]", "_", str(emp))[:31]
            sub.to_excel(writer, sheet_name=sheet_name, index=False)
    console.print(f"[green]✓[/green] Excel: {xlsx_out}")

    build_pdf(pdf_out, filtered, selected_companies, selected_cats, start_str, end_str,
              col_empresa, col_data)
    console.print(f"[green]✓[/green] PDF:   {pdf_out}\n[bold green]Concluído.[/bold green]")


# ---------- PDF ----------

def fmt_brl(v: float) -> str:
    s = f"{v:,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")


def _bar_chart(series, title=""):
    """Build a horizontal bar chart Drawing from a pandas Series (label -> value)."""
    series = series.sort_values(ascending=True)
    labels = [str(x)[:25] for x in series.index]
    values = [float(v) for v in series.values]
    n = max(len(values), 1)
    height = 18 * n + 40
    d = Drawing(440, height)
    bc = HorizontalBarChart()
    bc.x = 110
    bc.y = 15
    bc.width = 300
    bc.height = 18 * n
    bc.data = [values]
    bc.categoryAxis.categoryNames = labels
    bc.categoryAxis.labels.fontSize = 8
    bc.valueAxis.valueMin = 0
    bc.valueAxis.valueMax = max(values) * 1.1 if values else 1
    bc.valueAxis.labels.fontSize = 7
    bc.bars[0].fillColor = colors.HexColor("#1f3864")
    bc.barLabelFormat = lambda v: fmt_brl(v)
    bc.barLabels.fontSize = 7
    bc.barLabels.nudge = 4
    bc.barLabels.boxAnchor = "w"
    d.add(bc)
    return d


def _styled_table(data, total_row=False):
    t = RLTable(data, hAlign="LEFT")
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), TAAG_BLACK),
        ("TEXTCOLOR",  (0, 0), (-1, 0), colors.white),
        ("FONTNAME",   (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN",      (1, 0), (-1, -1), "RIGHT"),
        ("LINEBELOW",  (0, 0), (-1, 0), 1.2, TAAG_BROWN),
        ("LINEBELOW",  (0, -1), (-1, -1), 0.6, TAAG_BLACK),
        ("ROWBACKGROUNDS", (0, 1), (-1, -2 if total_row else -1),
            [TAAG_LIGHT, colors.white]),
        ("FONTSIZE",   (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, 0), 7),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 7),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
    ]
    if total_row:
        style += [
            ("BACKGROUND", (0, -1), (-1, -1), TAAG_BROWN),
            ("TEXTCOLOR",  (0, -1), (-1, -1), colors.white),
            ("FONTNAME",   (0, -1), (-1, -1), "Helvetica-Bold"),
        ]
    t.setStyle(TableStyle(style))
    return t


def _narrative_distribution(series, kind: str, scope: str) -> str:
    """Build a short Portuguese paragraph commenting on a distribution.

    kind  : "empresa" | "categoria"
    scope : human label of what's being divided ("o consolidado", "a empresa X")
    """
    s = series.sort_values(ascending=False)
    total = float(s.sum())
    if total <= 0 or s.empty:
        return ("Não há valores positivos significativos para comentar neste recorte.")
    n = len(s)
    top_name = str(s.index[0])
    top_val = float(s.iloc[0])
    top_pct = top_val / total * 100
    second = ""
    if n >= 2:
        second_name = str(s.index[1])
        second_val = float(s.iloc[1])
        second_pct = second_val / total * 100
        second = (
            f" Em seguida aparece <b>{second_name}</b> com {fmt_brl(second_val)} "
            f"({second_pct:.1f}%)."
        )
    # Concentration: how many items make up 80% of the total
    cum = 0.0
    n80 = 0
    for v in s.values:
        cum += float(v)
        n80 += 1
        if cum / total >= 0.8:
            break
    if kind == "empresa":
        head = (
            f"As despesas estão distribuídas entre <b>{n}</b> empresa(s). "
            f"<b>{top_name}</b> concentra a maior parte, com {fmt_brl(top_val)} "
            f"({top_pct:.1f}% de {scope})."
        )
    else:
        head = (
            f"Foram identificadas <b>{n}</b> categoria(s) de despesa em {scope}. "
            f"<b>{top_name}</b> é a mais relevante, somando {fmt_brl(top_val)} "
            f"({top_pct:.1f}% do total)."
        )
    if n >= 2:
        if n80 == 1:
            tail = (
                f" A concentração é alta: um único item representa mais de 80% "
                f"do total, indicando forte dependência desse componente."
            )
        elif n80 <= max(2, n // 3):
            tail = (
                f" Cerca de 80% do total se concentra em apenas <b>{n80}</b> "
                f"item(ns), o que sugere poucos componentes determinantes."
            )
        else:
            tail = (
                f" A distribuição é relativamente equilibrada: são necessários "
                f"<b>{n80}</b> itens para acumular 80% do total."
            )
    else:
        tail = ""
    return head + second + tail


def _narrative_monthly(series, scope: str) -> str:
    """Portuguese paragraph commenting on a monthly time series."""
    s = series.sort_index()
    if s.empty or float(s.sum()) <= 0:
        return "Não há movimentação mensal significativa no período."
    if len(s) == 1:
        only_m = str(s.index[0])
        return (
            f"O período analisado contempla apenas o mês <b>{only_m}</b>, "
            f"com total de {fmt_brl(float(s.iloc[0]))} em {scope}."
        )
    avg = float(s.mean())
    max_m, max_v = str(s.idxmax()), float(s.max())
    min_m, min_v = str(s.idxmin()), float(s.min())
    first_v = float(s.iloc[0])
    last_v = float(s.iloc[-1])
    if first_v > 0:
        delta_pct = (last_v - first_v) / first_v * 100
        if delta_pct > 5:
            trend = (
                f" Comparando o primeiro e o último mês, observa-se "
                f"<b>aumento de {delta_pct:.1f}%</b>, sinalizando tendência de alta."
            )
        elif delta_pct < -5:
            trend = (
                f" Comparando o primeiro e o último mês, observa-se "
                f"<b>redução de {abs(delta_pct):.1f}%</b>, sinalizando tendência de queda."
            )
        else:
            trend = (
                f" O comportamento entre o primeiro e o último mês é estável "
                f"(variação de {delta_pct:+.1f}%)."
            )
    else:
        trend = ""
    return (
        f"Ao longo de <b>{len(s)}</b> mês(es) em {scope}, a média mensal foi de "
        f"<b>{fmt_brl(avg)}</b>. O maior gasto ocorreu em <b>{max_m}</b> "
        f"({fmt_brl(max_v)}) e o menor em <b>{min_m}</b> ({fmt_brl(min_v)})."
        + trend
    )


def _draw_header_footer(canvas, doc):
    """Painted on every page: small TAAG logo top-right + footer line."""
    canvas.saveState()
    page_w, page_h = A4
    # Header rule
    canvas.setStrokeColor(TAAG_BROWN)
    canvas.setLineWidth(0.8)
    canvas.line(2*cm, page_h - 1.4*cm, page_w - 2*cm, page_h - 1.4*cm)
    # Tiny logo in top-right
    if LOGO_PATH.exists():
        try:
            canvas.drawImage(str(LOGO_PATH),
                             page_w - 2*cm - 1.0*cm, page_h - 1.3*cm,
                             width=1.0*cm, height=1.0*cm,
                             mask="auto", preserveAspectRatio=True)
        except Exception:
            pass
    # Footer
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(TAAG_GREY)
    canvas.drawString(2*cm, 1.2*cm, TAAG_TAGLINE)
    canvas.drawRightString(page_w - 2*cm, 1.2*cm,
                           f"Página {doc.page}  ·  taagbrasil.com.br")
    canvas.setStrokeColor(TAAG_BROWN)
    canvas.setLineWidth(0.4)
    canvas.line(2*cm, 1.5*cm, page_w - 2*cm, 1.5*cm)
    canvas.restoreState()


def build_pdf(path, df, companies, categorias, start_str, end_str, col_empresa, col_data):
    # Accept either a filesystem path or a file-like object (e.g. BytesIO).
    target = path if hasattr(path, "write") else str(path)
    doc = SimpleDocTemplate(
        target, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm, topMargin=2.2*cm, bottomMargin=2*cm,
        title="Resumo Executivo — TAAG Brasil",
        author="TAAG Brasil",
    )
    styles = getSampleStyleSheet()
    cover_title = ParagraphStyle(
        "cover_title", parent=styles["Heading1"], fontSize=28, leading=34,
        textColor=TAAG_BLACK, spaceAfter=4, alignment=0,
    )
    cover_sub = ParagraphStyle(
        "cover_sub", parent=styles["Heading2"], fontSize=14, leading=18,
        textColor=TAAG_BROWN, spaceAfter=18, alignment=0,
    )
    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=16,
                        textColor=TAAG_BLACK, spaceAfter=10, spaceBefore=4)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=12,
                        textColor=TAAG_BROWN, spaceBefore=10, spaceAfter=6)
    body = ParagraphStyle("body", parent=styles["BodyText"], fontSize=10,
                          textColor=TAAG_BLACK, leading=14)
    meta = ParagraphStyle("meta", parent=body, fontSize=9, textColor=TAAG_GREY)
    narr = ParagraphStyle(
        "narr", parent=body, fontSize=9, textColor=TAAG_BLACK,
        leading=12, leftIndent=8, rightIndent=8,
        spaceBefore=6, spaceAfter=4, borderPadding=6,
        backColor=TAAG_LIGHT, borderColor=TAAG_BROWN, borderWidth=0,
    )
    kpi_lbl = ParagraphStyle("kpi_lbl", parent=body, fontSize=9,
                             textColor=TAAG_GREY, alignment=1)
    kpi_val = ParagraphStyle("kpi_val", parent=body, fontSize=16,
                             textColor=TAAG_BLACK, alignment=1, leading=20)

    story = []

    # ---------------- COVER ----------------
    if LOGO_PATH.exists():
        try:
            story.append(RLImage(str(LOGO_PATH), width=3.5*cm, height=3.5*cm))
            story.append(Spacer(1, 0.4*cm))
        except Exception:
            pass
    story.append(Paragraph("Resumo Executivo", cover_title))
    story.append(Paragraph("Despesas Fixas — Análise Financeira", cover_sub))
    story.append(Spacer(1, 0.6*cm))

    total_geral = float(df["_valor_num"].sum())
    n_tx = int(len(df))
    n_emp = int(df[col_empresa].nunique())
    n_cat = int(df["_categoria"].nunique())

    # KPI strip
    kpi_data = [[
        Paragraph("TOTAL DO PERÍODO", kpi_lbl),
        Paragraph("EMPRESAS", kpi_lbl),
        Paragraph("CATEGORIAS", kpi_lbl),
        Paragraph("TRANSAÇÕES", kpi_lbl),
    ], [
        Paragraph(fmt_brl(total_geral), kpi_val),
        Paragraph(str(n_emp), kpi_val),
        Paragraph(str(n_cat), kpi_val),
        Paragraph(str(n_tx), kpi_val),
    ]]
    kpi_tbl = RLTable(kpi_data, colWidths=[4.25*cm]*4, hAlign="LEFT")
    kpi_tbl.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (-1, -1), TAAG_LIGHT),
        ("LINEABOVE",   (0, 0), (-1, 0), 1.5, TAAG_BROWN),
        ("LINEBELOW",   (0, -1), (-1, -1), 1.5, TAAG_BROWN),
        ("TOPPADDING",  (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    story.append(kpi_tbl)
    story.append(Spacer(1, 0.8*cm))

    story.append(Paragraph(
        f"<b>Período analisado:</b> {start_str} a {end_str}<br/>"
        f"<b>Empresas incluídas:</b> {', '.join(companies)}<br/>"
        f"<b>Categorias incluídas:</b> {', '.join(categorias)}<br/>"
        f"<b>Gerado em:</b> {datetime.now().strftime('%d/%m/%Y às %H:%M')}",
        body,
    ))
    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph(
        "Este relatório consolida as despesas fixas extraídas do razão geral, "
        "agrupadas por empresa, categoria e período. Cada empresa abaixo recebe "
        "uma página dedicada com seu detalhamento completo.",
        meta,
    ))

    # ---------------- CONSOLIDATED ANALYSIS ----------------
    story.append(PageBreak())
    story.append(Paragraph("Visão Consolidada", h1))

    total_emp = df.groupby(col_empresa)["_valor_num"].sum().sort_values(ascending=False)
    story.append(Paragraph("Distribuição por Empresa", h2))
    data = [["Empresa", "Total (R$)", "% do Total"]]
    for e, v in total_emp.items():
        pct = (v / total_geral * 100) if total_geral else 0
        data.append([str(e), fmt_brl(v), f"{pct:.1f}%"])
    data.append(["TOTAL", fmt_brl(total_emp.sum()), "100,0%"])
    story.append(_styled_table(data, total_row=True))
    story.append(Spacer(1, 0.3*cm))
    story.append(_bar_chart(total_emp, "Total por Empresa"))
    story.append(Paragraph(
        "<b>Análise:</b> " + _narrative_distribution(total_emp, "empresa", "o consolidado"),
        narr,
    ))

    total_cat = df.groupby("_categoria")["_valor_num"].sum().sort_values(ascending=False)
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph("Distribuição por Categoria", h2))
    data = [["Categoria", "Total (R$)", "% do Total"]]
    for k, v in total_cat.items():
        pct = (v / total_geral * 100) if total_geral else 0
        data.append([str(k), fmt_brl(v), f"{pct:.1f}%"])
    data.append(["TOTAL", fmt_brl(total_cat.sum()), "100,0%"])
    story.append(_styled_table(data, total_row=True))
    story.append(Spacer(1, 0.3*cm))
    story.append(_bar_chart(total_cat, "Total por Categoria"))
    story.append(Paragraph(
        "<b>Análise:</b> " + _narrative_distribution(total_cat, "categoria", "o consolidado"),
        narr,
    ))

    total_mes = df.groupby("_mes")["_valor_num"].sum().sort_index()
    if len(total_mes) > 1:
        story.append(Spacer(1, 0.5*cm))
        story.append(Paragraph("Evolução Mensal", h2))
        data = [["Mês", "Total (R$)"]] + [[str(k), fmt_brl(v)] for k, v in total_mes.items()]
        data.append(["TOTAL", fmt_brl(total_mes.sum())])
        story.append(_styled_table(data, total_row=True))
        story.append(Spacer(1, 0.3*cm))
        story.append(_bar_chart(total_mes, "Total por Mês"))
        story.append(Paragraph(
            "<b>Análise:</b> " + _narrative_monthly(total_mes, "o consolidado"),
            narr,
        ))

    # ---------------- PER COMPANY ----------------
    for emp in companies:
        sub = df[df[col_empresa].astype(str) == emp]
        if sub.empty:
            continue
        story.append(PageBreak())
        story.append(Paragraph(f"Empresa: {emp}", h1))

        emp_total = float(sub["_valor_num"].sum())
        emp_n_tx  = int(len(sub))
        emp_n_cat = int(sub["_categoria"].nunique())
        share = (emp_total / total_geral * 100) if total_geral else 0
        story.append(Paragraph(
            f"<b>Total da empresa:</b> {fmt_brl(emp_total)} "
            f"({share:.1f}% do consolidado)  ·  "
            f"<b>{emp_n_tx}</b> transações  ·  "
            f"<b>{emp_n_cat}</b> categorias",
            body,
        ))
        story.append(Spacer(1, 0.4*cm))

        story.append(Paragraph("Por Categoria", h2))
        cat = sub.groupby("_categoria")["_valor_num"].sum().sort_values(ascending=False)
        data = [["Categoria", "Total (R$)", "% da Empresa"]]
        for k, v in cat.items():
            pct = (v / emp_total * 100) if emp_total else 0
            data.append([str(k), fmt_brl(v), f"{pct:.1f}%"])
        data.append(["TOTAL", fmt_brl(cat.sum()), "100,0%"])
        story.append(_styled_table(data, total_row=True))
        story.append(Spacer(1, 0.3*cm))
        story.append(_bar_chart(cat, "Por Categoria"))
        story.append(Paragraph(
            "<b>Análise:</b> " + _narrative_distribution(cat, "categoria", f"a empresa <b>{emp}</b>"),
            narr,
        ))
        story.append(Spacer(1, 0.4*cm))

        story.append(Paragraph("Por Mês", h2))
        mes = sub.groupby("_mes")["_valor_num"].sum().sort_index()
        data = [["Mês", "Total (R$)"]] + [[str(k), fmt_brl(v)] for k, v in mes.items()]
        data.append(["TOTAL", fmt_brl(mes.sum())])
        story.append(_styled_table(data, total_row=True))
        if len(mes) > 1:
            story.append(Spacer(1, 0.3*cm))
            story.append(_bar_chart(mes, "Por Mês"))
        story.append(Paragraph(
            "<b>Análise:</b> " + _narrative_monthly(mes, f"a empresa <b>{emp}</b>"),
            narr,
        ))

    doc.build(story, onFirstPage=_draw_header_footer, onLaterPages=_draw_header_footer)


if __name__ == "__main__":
    main()

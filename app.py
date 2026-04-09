"""
Extrator de Despesas Fixas — Interface Web (Streamlit)

Interface visual em português para o sistema de extração de despesas fixas.
Suporta três fontes de dados:
  1. Upload por arrastar-e-soltar
  2. Arquivo do disco local
  3. Arquivo do Dropbox (navegação por pastas)

Como executar:
    streamlit run app.py
"""
from __future__ import annotations

import io
import os
import re
import sys
import tempfile
from calendar import monthrange
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "tools"))

# Reuse pure helpers from the existing CLI tool
from extract_fixed_expenses import (  # noqa: E402
    strip_accents,
    auto_detect_columns,
    detect_header_row,
    parse_value,
    categorize_row,
    previous_month_range,
    fmt_brl,
    build_pdf,
)


def smart_parse_dates(series: pd.Series) -> tuple[pd.Series, str]:
    """Try BR (dayfirst) and US (monthfirst) parsing, pick whichever yields more
    valid dates. Returns (parsed_series, label)."""
    br = pd.to_datetime(series, errors="coerce", dayfirst=True)
    us = pd.to_datetime(series, errors="coerce", dayfirst=False)
    br_valid = br.notna().sum()
    us_valid = us.notna().sum()
    if br_valid >= us_valid:
        return br, f"DD/MM/AAAA (Brasil) — {br_valid}/{len(series)} datas válidas"
    return us, f"MM/DD/AAAA (EUA) — {us_valid}/{len(series)} datas válidas"


def detect_value_mode(columns) -> tuple[str, str | None, str | None]:
    """Return (mode, debit_col, credit_col) by scanning column names."""
    debit_col = credit_col = None
    for c in columns:
        n = strip_accents(str(c))
        if debit_col is None and ("debit" in n or "debito" in n):
            debit_col = str(c)
        if credit_col is None and ("credit" in n or "credito" in n):
            credit_col = str(c)
    if debit_col and credit_col:
        return "Débito + Crédito", debit_col, credit_col
    return "Coluna única", None, None

CONFIG_PATH = PROJECT_ROOT / "config" / "categorias.xlsx"
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
TMP_DIR = PROJECT_ROOT / ".tmp"
TMP_DIR.mkdir(exist_ok=True)

load_dotenv(PROJECT_ROOT / ".env")

st.set_page_config(
    page_title="Extrator de Despesas Fixas",
    page_icon="📊",
    layout="wide",
)

# ---------- session state ----------
if "excel_path" not in st.session_state:
    st.session_state.excel_path = None
if "source_label" not in st.session_state:
    st.session_state.source_label = None


# ---------- helpers ----------
def save_uploaded(file) -> Path:
    p = TMP_DIR / file.name
    with open(p, "wb") as f:
        f.write(file.getbuffer())
    return p


def load_categories_df() -> pd.DataFrame:
    empty = pd.DataFrame(columns=["palavra_chave", "categoria", "empresa"])
    if not CONFIG_PATH.exists():
        return empty
    try:
        df = pd.read_excel(CONFIG_PATH)
    except Exception as e:
        st.warning(f"Não foi possível ler config/categorias.xlsx ({e}). Começando vazio.")
        return empty
    df.columns = [c.lower() for c in df.columns]
    for col in ("palavra_chave", "categoria", "empresa"):
        if col not in df.columns:
            df[col] = ""
    df = df[["palavra_chave", "categoria", "empresa"]].fillna("")
    return df


def categories_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    """Serialize the rules editor state to an in-memory .xlsx for download."""
    df = df.copy()
    df = df[df["palavra_chave"].astype(str).str.strip() != ""]
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buf.seek(0)
    return buf.getvalue()


def df_to_rules(df: pd.DataFrame) -> list[dict]:
    out = []
    for _, r in df.iterrows():
        kw = str(r["palavra_chave"]).strip()
        cat = str(r["categoria"]).strip()
        emp = "" if pd.isna(r["empresa"]) else str(r["empresa"]).strip()
        if kw and cat:
            out.append({"kw": kw, "categoria": cat, "empresa": emp})
    return out


# ============================================================
# HEADER
# ============================================================
st.title("📊 Extrator de Despesas Fixas")
st.caption(
    "Carregue um razão geral, defina na barra lateral o que cada palavra-chave significa "
    "(ex: \"maria\" = \"Aluguel\") e o sistema classificará automaticamente todas as linhas "
    "correspondentes no período escolhido."
)

# ============================================================
# SIDEBAR — Persistent categories editor (always visible)
# ============================================================
with st.sidebar:
    st.header("📚 Regras de categorias")
    st.caption(
        "Defina aqui o que cada palavra-chave significa. Ex: **maria → Aluguel**. "
        "O sistema procurará a palavra-chave (sem acento, sem caixa) na descrição e na "
        "conta de cada linha do razão. Se você selecionou uma coluna de Empresa na "
        "Seção 2, a empresa será detectada automaticamente da planilha — deixe o "
        "campo *Empresa* das regras em branco."
    )

    # Initialize rules in session state on first load using the bundled sample.
    if "rules_df" not in st.session_state:
        st.session_state.rules_df = load_categories_df()

    # Importer: lets the user replace the editor content with their own file
    rules_upload = st.file_uploader(
        "📤 Importar regras (.xlsx)",
        type=["xlsx"],
        key="rules_uploader",
        help="Carregue um arquivo .xlsx com as colunas palavra_chave, categoria, empresa.",
    )
    if rules_upload is not None:
        try:
            new_df = pd.read_excel(rules_upload)
            new_df.columns = [c.lower() for c in new_df.columns]
            for col in ("palavra_chave", "categoria", "empresa"):
                if col not in new_df.columns:
                    new_df[col] = ""
            st.session_state.rules_df = (
                new_df[["palavra_chave", "categoria", "empresa"]].fillna("")
            )
            st.success(f"{len(st.session_state.rules_df)} regra(s) importada(s).")
        except Exception as e:
            st.error(f"Não foi possível ler o arquivo: {e}")

    edited = st.data_editor(
        st.session_state.rules_df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "palavra_chave": st.column_config.TextColumn("Palavra-chave", required=True),
            "categoria":     st.column_config.TextColumn("Categoria",     required=True),
            "empresa":       st.column_config.TextColumn(
                "Empresa (opcional)",
                help="Ignorado quando a planilha tem coluna de Empresa selecionada na Seção 2.",
            ),
        },
        key="cat_editor",
    )
    # Keep session state in sync with edits so a download grabs latest content
    st.session_state.rules_df = edited

    st.download_button(
        "📥 Baixar regras (.xlsx)",
        data=categories_to_xlsx_bytes(edited),
        file_name="categorias.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        help="Salve este arquivo no seu computador. Na próxima visita, importe-o novamente.",
    )

    rules = df_to_rules(edited)
    categorias_disponiveis = sorted({r["categoria"] for r in rules})
    st.caption(f"📌 {len(rules)} regra(s) ativa(s) · {len(categorias_disponiveis)} categoria(s).")
    st.caption(
        "💡 **Dica:** se sua planilha tem coluna de empresa, selecione-a na Seção 2 "
        "e deixe *Empresa (opcional)* em branco — a empresa de cada lançamento será "
        "lida diretamente da planilha. Use *Empresa (opcional)* apenas quando a "
        "planilha **não** tiver coluna de empresa."
    )
    st.caption(
        "⚠️ **As regras NÃO ficam salvas no servidor.** Use **Baixar regras** ao "
        "terminar e **Importar regras** na próxima visita."
    )

# ============================================================
# 1. SOURCE PICKER
# ============================================================
st.header("1️⃣  Fonte do arquivo")

# Only offer Dropbox if the client library + token are actually available.
try:
    import dropbox  # noqa: F401
    _has_dropbox = bool(os.environ.get("DROPBOX_ACCESS_TOKEN"))
except Exception:
    _has_dropbox = False

_source_options = ["📤 Upload (arrastar e soltar)", "💻 Arquivo local"]
if _has_dropbox:
    _source_options.append("☁️ Dropbox")
source = st.radio(
    "De onde vem o razão geral?",
    _source_options,
    horizontal=True,
)

if source.startswith("📤"):
    up = st.file_uploader("Solte o arquivo .xlsx aqui", type=["xlsx"])
    if up is not None:
        st.session_state.excel_path = save_uploaded(up)
        st.session_state.source_label = f"Upload: {up.name}"

elif source.startswith("💻"):
    local_path = st.text_input(
        "Caminho completo do arquivo .xlsx",
        placeholder="/Users/seu_usuario/Desktop/razao.xlsx",
    )
    if st.button("Carregar arquivo local"):
        p = Path(local_path).expanduser()
        if not p.exists():
            st.error(f"Arquivo não encontrado: {p}")
        elif p.suffix.lower() != ".xlsx":
            st.error("O arquivo precisa ser .xlsx")
        else:
            st.session_state.excel_path = p
            st.session_state.source_label = f"Local: {p.name}"

else:  # Dropbox
    folder = st.text_input("Pasta no Dropbox", value="/", help="Ex: / ou /Razões/2026")
    col_l, col_r = st.columns([1, 3])
    with col_l:
        listar = st.button("Listar arquivos")
    if listar:
        try:
            from dropbox_client import list_xlsx_in_folder
            files = list_xlsx_in_folder(folder.strip())
            if not files:
                st.warning("Nenhum .xlsx encontrado nessa pasta.")
            else:
                st.session_state.dbx_files = files
        except Exception as e:
            st.error(f"Erro Dropbox: {e}")
    if "dbx_files" in st.session_state and st.session_state.dbx_files:
        choice = st.selectbox(
            "Selecione o arquivo:",
            st.session_state.dbx_files,
            format_func=lambda f: f"{f['name']}  —  {f['modified'].strftime('%d/%m/%Y %H:%M')}",
        )
        if st.button("Baixar do Dropbox"):
            try:
                from dropbox_client import download_path
                p = download_path(choice["path"])
                st.session_state.excel_path = p
                st.session_state.source_label = f"Dropbox: {choice['name']}"
                st.success(f"Baixado: {p.name}")
            except Exception as e:
                st.error(f"Erro Dropbox: {e}")

if st.session_state.excel_path:
    st.success(f"✅ Arquivo carregado — {st.session_state.source_label}")

# ============================================================
# Stop here if no file
# ============================================================
if not st.session_state.excel_path:
    st.info("👆 Carregue um arquivo para continuar.")
    st.stop()

excel_path = Path(st.session_state.excel_path)

# ============================================================
# 2. SHEET + COLUMN MAPPING
# ============================================================
st.header("2️⃣  Mapeamento das colunas")

xls = pd.ExcelFile(excel_path)
sheet = xls.sheet_names[0]
if len(xls.sheet_names) > 1:
    sheet = st.selectbox("Aba a usar:", xls.sheet_names)

header_row = detect_header_row(excel_path, sheet)
st.caption(f"Cabeçalho detectado na linha {header_row + 1}.")
df = pd.read_excel(excel_path, sheet_name=sheet, header=header_row).dropna(how="all")
st.caption(
    f"📥 **{len(df):,} linhas** carregadas do arquivo (todas serão analisadas, "
    "filtradas pelo período definido na seção Filtros). Pré-visualização das 5 primeiras:"
)
st.dataframe(df.head(), use_container_width=True)

detected = auto_detect_columns(df)
cols = ["(nenhuma)"] + list(df.columns.astype(str))


def col_select(label: str, key: str, hint_key: str) -> str | None:
    default = detected.get(hint_key) or "(nenhuma)"
    idx = cols.index(str(default)) if str(default) in cols else 0
    val = st.selectbox(label, cols, index=idx, key=key)
    return None if val == "(nenhuma)" else val


# Auto-detect value mode from column names
auto_mode, auto_deb, auto_cre = detect_value_mode(df.columns)

c1, c2, c3 = st.columns(3)
with c1:
    col_data = col_select("📅 Data", "col_data", "data")
    col_empresa = col_select("🏢 Empresa (opcional)", "col_empresa", "empresa")
with c2:
    col_desc = col_select("📝 Descrição", "col_desc", "descricao")
    col_conta = col_select("📂 Conta (opcional)", "col_conta", "conta")
with c3:
    mode_options = ["Coluna única", "Débito + Crédito"]
    valor_mode = st.radio(
        "💰 Modo do valor",
        mode_options,
        index=mode_options.index(auto_mode),
        help="Use 'Débito + Crédito' se o razão tiver duas colunas separadas.",
    )
    if valor_mode == "Coluna única":
        col_valor = col_select("Coluna de valor", "col_valor", "valor")
        col_debito = col_credito = None
    else:
        deb_idx = cols.index(auto_deb) if auto_deb and auto_deb in cols else 0
        cre_idx = cols.index(auto_cre) if auto_cre and auto_cre in cols else 0
        col_debito = st.selectbox("Coluna Débito", cols, index=deb_idx, key="col_debito")
        col_credito = st.selectbox("Coluna Crédito", cols, index=cre_idx, key="col_credito")
        col_debito = None if col_debito == "(nenhuma)" else col_debito
        col_credito = None if col_credito == "(nenhuma)" else col_credito
        col_valor = None

# Non-blocking validation: track mapping_ok but never st.stop()
missing = []
if not col_data:
    missing.append("Data")
if not col_desc:
    missing.append("Descrição")
if valor_mode == "Coluna única" and not col_valor:
    missing.append("Valor")
if valor_mode == "Débito + Crédito" and not (col_debito or col_credito):
    missing.append("Débito ou Crédito")
mapping_ok = not missing
if missing:
    st.warning(f"⚠️ Faltam colunas obrigatórias: **{', '.join(missing)}**")
    if "Valor" in missing and auto_deb and auto_cre:
        st.info(
            f"💡 Sua planilha parece ter colunas separadas de débito e crédito "
            f"(`{auto_deb}` e `{auto_cre}`). Mude **Modo do valor** para "
            f"**Débito + Crédito** acima."
        )
    elif "Débito ou Crédito" in missing:
        st.info("💡 Selecione pelo menos uma das colunas (Débito ou Crédito) acima.")

# Build a normalized working dataframe (only when mapping is valid)
work = None
col_empresa_eff = None
if mapping_ok:
    work = df.copy()
    work[col_data], date_fmt_label = smart_parse_dates(work[col_data])
    st.caption(f"📅 Formato de data detectado: {date_fmt_label}")
    if valor_mode == "Coluna única":
        work["_valor_num"] = work[col_valor].apply(parse_value)
    else:
        deb = work[col_debito].apply(parse_value) if col_debito else 0
        cre = work[col_credito].apply(parse_value) if col_credito else 0
        work["_valor_num"] = (
            (deb if not isinstance(deb, int) else 0)
            - (cre if not isinstance(cre, int) else 0)
        )

    if not col_empresa:
        work["_empresa_virtual"] = "Geral"
        col_empresa_eff = "_empresa_virtual"
    else:
        col_empresa_eff = col_empresa
        work[col_empresa_eff] = work[col_empresa_eff].astype(str)

    work = work.dropna(subset=[col_data])

# ============================================================
# 3. FILTERS
# ============================================================
st.header("3️⃣  Filtros")

if mapping_ok:
    empresas_disp = sorted(work[col_empresa_eff].dropna().astype(str).unique().tolist())
else:
    empresas_disp = []

# Hide the empresas filter when there's no real empresa column — it would only
# show the virtual "Geral" and confuse the user.
has_real_empresa = mapping_ok and col_empresa_eff != "_empresa_virtual"
if has_real_empresa:
    sel_emp = st.multiselect("Empresas", empresas_disp, default=empresas_disp)
else:
    # Build the list of companies from the rules' empresa field, since the
    # ledger itself has no empresa column. Each matched row will be assigned
    # to whatever empresa its rule says.
    rule_empresas = sorted({r["empresa"] for r in rules if r["empresa"]})
    sel_emp = rule_empresas or ["Geral"]
    if mapping_ok:
        if rule_empresas:
            st.caption(
                "ℹ️ Sua planilha não tem coluna de empresa, mas as regras na "
                f"barra lateral atribuem cada despesa a uma empresa. "
                f"Empresas detectadas nas regras: **{', '.join(rule_empresas)}**."
            )
        else:
            st.caption(
                "ℹ️ Sua planilha não tem coluna de empresa e nenhuma regra "
                "atribui empresa. Todas as linhas serão agrupadas como **Geral**. "
                "Para separar por empresa, preencha a coluna *Empresa* de cada "
                "regra na barra lateral."
            )

# Default period = actual min/max date in the ledger (not hardcoded previous month)
if mapping_ok and work[col_data].notna().any():
    default_start = work[col_data].min().date()
    default_end = work[col_data].max().date()
else:
    today = datetime.now()
    ds, de_excl, _, _ = previous_month_range(today)
    default_start = ds.date()
    default_end = (de_excl - pd.Timedelta(days=1)).date()

date_range = st.date_input(
    "Período",
    value=(default_start, default_end),
    format="DD/MM/YYYY",
)
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_d, end_d = date_range
else:
    start_d = end_d = date_range

sel_cats = st.multiselect("Categorias", categorias_disponiveis, default=categorias_disponiveis)

# ============================================================
# 4. RUN
# ============================================================
st.header("4️⃣  Gerar relatório")

if not mapping_ok:
    st.info("⚙️ Conclua o mapeamento das colunas acima para liberar o botão de processar.")
if not rules:
    st.warning(
        "📚 Você ainda não definiu nenhuma regra de categoria. "
        "Adicione regras na **barra lateral à esquerda** "
        "(ex: palavra-chave **maria** → categoria **Aluguel**)."
    )

run_disabled = (not mapping_ok) or (not rules)
if st.button("🚀 Processar e gerar relatório", type="primary", disabled=run_disabled):
    if not sel_emp:
        st.error("Selecione ao menos uma empresa.")
        st.stop()
    if not sel_cats:
        st.error("Selecione ao menos uma categoria.")
        st.stop()

    active_rules = [r for r in rules if r["categoria"] in sel_cats]
    ledger_has_empresa = (col_empresa_eff != "_empresa_virtual")
    # When the ledger has no empresa column, the rule's empresa is used to
    # ASSIGN the matched row to a company (not as a filter). So we drop the
    # filter behavior here — every rule applies to every row, and the row's
    # empresa is taken from whichever rule matches.
    # Keep empresa on each rule — match_rule treats it as the assigned
    # empresa when the ledger has no empresa column, and as a filter when
    # it does (handled inside match_rule).
    active_rules_for_match = active_rules
    start_dt = datetime.combine(start_d, datetime.min.time())
    end_excl = datetime.combine(end_d, datetime.min.time()) + pd.Timedelta(days=1)

    date_mask = (work[col_data] >= start_dt) & (work[col_data] < end_excl)
    if ledger_has_empresa:
        mask = work[col_empresa_eff].astype(str).isin(sel_emp) & date_mask
    else:
        # No real empresa column → don't pre-filter by empresa; the rules
        # will assign each matched row to a company afterward.
        mask = date_mask
    filtered = work.loc[mask].copy()
    rows_total = len(work)
    rows_in_period = len(filtered)

    search_cols = [col_desc] + ([col_conta] if col_conta else [])

    def match_rule(row):
        """Return (categoria, empresa_assigned) for the first matching rule, or (None, None)."""
        haystack = " ".join(strip_accents(row[c]) for c in search_cols if c in row.index)
        row_emp = str(row[col_empresa_eff]) if ledger_has_empresa else ""
        row_emp_n = strip_accents(row_emp)
        for rule in active_rules_for_match:
            # When the ledger has its own empresa column, the row's empresa is
            # the source of truth — rules match purely by keyword and the
            # rule's empresa field is ignored. Without an empresa column, fall
            # back to the rule's empresa (or "Geral").
            if ledger_has_empresa:
                if strip_accents(rule["kw"]) in haystack:
                    return rule["categoria"], (row_emp or "Geral")
            else:
                if strip_accents(rule["kw"]) in haystack:
                    return rule["categoria"], (rule["empresa"] or "Geral")
        return None, None

    matched = filtered.apply(match_rule, axis=1)
    filtered["_categoria"] = [m[0] for m in matched]
    filtered["_empresa_assigned"] = [m[1] for m in matched]
    filtered = filtered[filtered["_categoria"].notna()].copy()

    # When the ledger has no empresa column, the assigned empresa BECOMES
    # the empresa column for all downstream grouping/output.
    if not ledger_has_empresa:
        filtered[col_empresa_eff] = filtered["_empresa_assigned"]
        sel_emp = sorted(filtered[col_empresa_eff].astype(str).unique().tolist())

    filtered["_mes"] = filtered[col_data].dt.to_period("M").astype(str)

    if filtered.empty:
        st.warning("Nenhuma transação correspondeu aos filtros e regras.")
        st.stop()

    st.success(
        f"✅ **{len(filtered):,}** transações classificadas  ·  "
        f"({rows_total:,} no arquivo → {rows_in_period:,} no período → "
        f"{len(filtered):,} casaram com regras)"
    )

    # Summary metrics
    total = filtered["_valor_num"].sum()
    m1, m2, m3 = st.columns(3)
    m1.metric("Total geral", fmt_brl(total))
    m2.metric("Transações", len(filtered))
    m3.metric("Empresas", filtered[col_empresa_eff].nunique())

    # Charts
    st.subheader("📊 Visão geral")
    cc1, cc2 = st.columns(2)
    with cc1:
        by_emp = filtered.groupby(col_empresa_eff)["_valor_num"].sum().sort_values(ascending=False)
        st.bar_chart(by_emp, height=280)
        st.caption("Total por empresa")
    with cc2:
        by_cat = filtered.groupby("_categoria")["_valor_num"].sum().sort_values(ascending=False)
        st.bar_chart(by_cat, height=280)
        st.caption("Total por categoria")

    # Preview table
    st.subheader(f"🔍 Prévia (50 primeiras de {len(filtered):,} — o Excel e o PDF abaixo contêm tudo)")
    preview_cols = [col_empresa_eff, col_data, col_desc, "_categoria", "_valor_num"]
    preview = filtered[preview_cols].rename(columns={
        col_empresa_eff: "Empresa", col_data: "Data", col_desc: "Descrição",
        "_categoria": "Categoria", "_valor_num": "Valor",
    })
    preview["Data"] = preview["Data"].dt.strftime("%d/%m/%Y")
    preview["Valor"] = preview["Valor"].map(fmt_brl)
    st.dataframe(preview.head(50), use_container_width=True)

    # Build outputs in memory
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    cols_keep = [col_empresa_eff, col_data, col_desc, "_categoria", "_valor_num"]
    rename = {col_empresa_eff: "Empresa", col_data: "Data", col_desc: "Descrição",
              "_categoria": "Categoria", "_valor_num": "Valor"}
    if col_conta:
        cols_keep.insert(3, col_conta)
        rename[col_conta] = "Conta"

    xlsx_buf = io.BytesIO()
    with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as writer:
        for emp in sel_emp:
            sub = filtered[filtered[col_empresa_eff].astype(str) == emp][cols_keep].rename(columns=rename)
            sub = sub.sort_values("Data")
            sheet_name = re.sub(r"[\[\]\*\?:/\\]", "_", str(emp))[:31] or "Geral"
            sub.to_excel(writer, sheet_name=sheet_name, index=False)
    xlsx_buf.seek(0)
    xlsx_name = f"transacoes_{ts}.xlsx"

    pdf_buf = io.BytesIO()
    start_str = start_d.strftime("%m/%Y")
    end_str = end_d.strftime("%m/%Y")
    build_pdf(pdf_buf, filtered, sel_emp, sel_cats, start_str, end_str,
              col_empresa_eff, col_data)
    pdf_buf.seek(0)
    pdf_bytes = pdf_buf.getvalue()
    pdf_name = f"resumo_executivo_{ts}.pdf"

    st.subheader("⬇️  Downloads")
    d1, d2 = st.columns(2)
    with d1:
        st.download_button(
            "📥 Baixar Excel detalhado",
            data=xlsx_buf,
            file_name=xlsx_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with d2:
        st.download_button(
            "📄 Baixar PDF executivo",
            data=pdf_bytes,
            file_name=pdf_name,
            mime="application/pdf",
        )

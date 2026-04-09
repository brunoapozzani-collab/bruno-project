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
    empty = pd.DataFrame(columns=["palavra_chave", "categoria", "descricao"])
    if not CONFIG_PATH.exists():
        return empty
    try:
        df = pd.read_excel(CONFIG_PATH)
    except Exception as e:
        st.warning(f"Não foi possível ler config/categorias.xlsx ({e}). Começando vazio.")
        return empty
    df.columns = [c.lower() for c in df.columns]
    for col in ("palavra_chave", "categoria", "descricao"):
        if col not in df.columns:
            df[col] = ""
    df = df[["palavra_chave", "categoria", "descricao"]].fillna("")
    return df


def categories_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    """Serialize the rules editor state to an in-memory .xlsx for download."""
    df = df.copy()
    # Keep rows with at least a keyword OR a description
    has_matcher = (
        df["palavra_chave"].astype(str).str.strip() != ""
    ) | (df["descricao"].astype(str).str.strip() != "")
    df = df[has_matcher]
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buf.seek(0)
    return buf.getvalue()


def _file_signature(p: Path) -> str:
    try:
        st_ = p.stat()
        return f"{p.name}-{st_.st_size}-{int(st_.st_mtime)}"
    except Exception:
        return p.name


def bootstrap_rules_from_work(work_df: pd.DataFrame, col_desc: str, col_favorecido: str | None) -> tuple[int, list[str]]:
    """Run Claude on the last 100 rows and merge proposed rules into session state.

    Returns (n_added, ambiguous_favorecidos).
    """
    from categorize_with_claude import propose_rules_from_rows  # lazy import

    sample = work_df.tail(100).copy()
    rows = []
    for _, r in sample.iterrows():
        rows.append({
            "descricao": str(r.get(col_desc, "") or "").strip(),
            "favorecido": str(r.get(col_favorecido, "") or "").strip() if col_favorecido else "",
        })
    rows = [r for r in rows if r["descricao"] or r["favorecido"]]
    if not rows:
        return 0, []
    proposed = propose_rules_from_rows(rows)

    # Group by favorecido → set of categorias (detect ambiguity)
    fav_to_cats: dict[str, set[str]] = {}
    for r in proposed:
        fav = r.get("favorecido", "").strip()
        if fav:
            fav_to_cats.setdefault(fav.lower(), set()).add(r["categoria"])
    ambiguous = sorted([k for k, v in fav_to_cats.items() if len(v) > 1])

    # Build deduped rule sets
    new_rules: list[dict] = []
    seen: set[tuple[str, str]] = set()

    # Description rules: shortest descricao that maps to one categoria consistently
    desc_to_cat: dict[str, str] = {}
    for r in proposed:
        d = r.get("descricao", "").strip()
        if not d:
            continue
        # Use first 60 chars normalized as the dedupe key for the descricao field
        key = d[:60]
        # Only keep if consistent
        prev = desc_to_cat.get(key.lower())
        if prev is None:
            desc_to_cat[key.lower()] = r["categoria"]
        elif prev != r["categoria"]:
            # inconsistent — skip this descricao rule
            desc_to_cat[key.lower()] = ""
    for key, cat in desc_to_cat.items():
        if not cat:
            continue
        new_rules.append({"palavra_chave": "", "categoria": cat, "descricao": key})

    # Favorecido (palavra_chave) rules: only when unambiguous
    for fav, cats in fav_to_cats.items():
        if len(cats) == 1:
            new_rules.append({"palavra_chave": fav, "categoria": next(iter(cats)), "descricao": ""})

    # Merge into session state, skip duplicates by (palavra_chave, descricao)
    cur = st.session_state.rules_df.copy()
    existing = set(
        (str(r["palavra_chave"]).strip().lower(), str(r["descricao"]).strip().lower())
        for _, r in cur.iterrows()
    )
    added_rows = []
    for nr in new_rules:
        k = (nr["palavra_chave"].strip().lower(), nr["descricao"].strip().lower())
        if k in existing or (not k[0] and not k[1]):
            continue
        existing.add(k)
        added_rows.append(nr)
    if added_rows:
        cur = pd.concat([cur, pd.DataFrame(added_rows)], ignore_index=True)
        st.session_state.rules_df = cur[["palavra_chave", "categoria", "descricao"]].fillna("")
    return len(added_rows), ambiguous


_ADDRESS_RE_GLOBAL = re.compile(
    r"\b(rua|r\.|av|av\.|avenida|alameda|al\.|travessa|trav\.|estrada|"
    r"rodovia|rod\.|praca|praça|largo|viela|beco|quadra|lote|bloco|"
    r"cep|n[º°]|num\.|numero|número)\b|\d{5}-?\d{3}",
    re.IGNORECASE,
)


def categorize_descriptions(
    descriptions: list[str], rules: list[dict]
) -> list[str | None]:
    """Apply the rule pipeline (descrição-priority then keyword) to a list of
    descriptions. Returns one categoria per description, or None when no rule
    matches. Used by the Section 2 preview."""
    out: list[str | None] = []
    for raw in descriptions:
        text = strip_accents(raw or "")
        is_addr = bool(_ADDRESS_RE_GLOBAL.search(raw or "")) if (raw or "").strip() else False
        match = None
        if not is_addr:
            for r in rules:
                if r["descricao"] and strip_accents(r["descricao"]) in text:
                    match = r["categoria"]
                    break
        if match is None:
            for r in rules:
                if r["kw"] and strip_accents(r["kw"]) in text:
                    match = r["categoria"]
                    break
        out.append(match)
    return out


def df_to_rules(df: pd.DataFrame) -> list[dict]:
    out = []
    for _, r in df.iterrows():
        kw = "" if pd.isna(r.get("palavra_chave")) else str(r["palavra_chave"]).strip()
        cat = "" if pd.isna(r.get("categoria")) else str(r["categoria"]).strip()
        desc = "" if pd.isna(r.get("descricao")) else str(r["descricao"]).strip()
        # A rule needs a categoria and at least one matcher (kw or desc).
        if cat and (kw or desc):
            out.append({"kw": kw, "categoria": cat, "descricao": desc})
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
        "Defina o que cada lançamento significa. Cada regra tem **palavra-chave**, "
        "**categoria** e **descrição** (prioritária). Se a *descrição* da regra "
        "aparecer no texto da descrição da linha, ela vence — mesmo que uma "
        "palavra-chave de outra regra também combine. A *palavra-chave* é o "
        "fallback. A empresa é detectada automaticamente da coluna escolhida na "
        "Seção 2."
    )

    # Initialize rules in session state on first load using the bundled sample.
    if "rules_df" not in st.session_state:
        st.session_state.rules_df = load_categories_df()

    edited = st.data_editor(
        st.session_state.rules_df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "palavra_chave": st.column_config.TextColumn(
                "Palavra-chave",
                help="Fallback: usado quando nenhuma regra de descrição combina.",
            ),
            "categoria":     st.column_config.TextColumn("Categoria", required=True),
            "descricao":     st.column_config.TextColumn(
                "Descrição (prioritária)",
                help="Se este texto aparecer na descrição da linha, esta regra vence sobre qualquer palavra-chave.",
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
        "💡 **Dica:** comece preenchendo apenas a *Descrição* — ela é a forma mais "
        "precisa de classificar. Use *Palavra-chave* só quando a descrição variar "
        "muito entre lançamentos. Para linhas sem regra, ative o sugeridor "
        "automático (Claude) na Seção 4."
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
    col_endereco = col_select("📍 Endereço (opcional)", "col_endereco", "endereco")
    col_favorecido = col_select("👤 Cliente / Fornecedor / Favorecido (opcional)", "col_favorecido", "favorecido")
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

    # ----- Auto-bootstrap rules from the file (Claude) -----
    _has_key = bool(os.environ.get("ANTHROPIC_API_KEY"))
    if not _has_key:
        try:
            _has_key = bool(st.secrets.get("ANTHROPIC_API_KEY"))  # type: ignore
        except Exception:
            _has_key = False
    _file_sig = _file_signature(excel_path)
    _bootstrap_key = f"bootstrap_done_{_file_sig}"

    with st.sidebar:
        st.divider()
        st.subheader("🤖 Bootstrap de regras")
        st.caption(
            "Analisa as últimas 100 linhas com Claude e adiciona regras "
            "(descrição → categoria e favorecido → categoria) à lista acima. "
            "Suas regras já existentes são preservadas."
        )
        manual_run = st.button(
            "Sugerir regras automaticamente",
            disabled=not _has_key,
            help=("Requer ANTHROPIC_API_KEY no .env ou nos Secrets do Streamlit Cloud."
                  if not _has_key else None),
        )

    should_run = manual_run or (_has_key and not st.session_state.get(_bootstrap_key))
    if should_run and _has_key:
        try:
            with st.spinner("🤖 Sugerindo regras a partir das últimas 100 linhas..."):
                n_added, ambiguous = bootstrap_rules_from_work(work, col_desc, col_favorecido)
            st.session_state[_bootstrap_key] = True
            if n_added:
                st.success(f"✅ {n_added} regra(s) sugerida(s) adicionada(s) à barra lateral. Revise antes de processar.")
            else:
                st.info("Nenhuma regra nova foi sugerida (ou todas já existiam).")
            if ambiguous:
                st.info(
                    "ℹ️ Os seguintes favorecidos apareceram com categorias diferentes "
                    "e foram pulados como regra de palavra-chave (a descrição decidirá): "
                    f"**{', '.join(ambiguous[:10])}**"
                    + (f" (+{len(ambiguous)-10})" if len(ambiguous) > 10 else "")
                )
        except Exception as e:
            st.warning(f"⚠️ Bootstrap automático falhou: {e}")

# ============================================================
# 2.5  PREVIEW — descrição → categoria
# ============================================================
if mapping_ok and rules:
    st.header("🔍 Pré-visualização da categorização")
    st.caption(
        "Cada descrição da planilha foi comparada com a lista de regras na barra "
        "lateral. Veja abaixo quantas linhas casaram com cada categoria e exemplos "
        "de descrições. Edite uma regra na barra lateral e a tabela atualiza sozinha."
    )

    descs_all = work[col_desc].astype(str).tolist()
    cats_all = categorize_descriptions(descs_all, rules)
    n_total = len(cats_all)
    n_matched = sum(1 for c in cats_all if c)
    n_unmatched = n_total - n_matched

    pcol1, pcol2, pcol3 = st.columns(3)
    pcol1.metric("Linhas totais", f"{n_total:,}")
    pcol2.metric("Casaram com regra", f"{n_matched:,}")
    pcol3.metric("Sem categoria", f"{n_unmatched:,}")

    # Group: categoria → list of sample descriptions
    by_cat: dict[str, list[str]] = {}
    for d, c in zip(descs_all, cats_all):
        if c:
            by_cat.setdefault(c, []).append(d)
    if by_cat:
        rows_preview = []
        for cat in sorted(by_cat.keys()):
            samples = by_cat[cat]
            rows_preview.append({
                "Categoria": cat,
                "Linhas": len(samples),
                "Exemplos de descrição": " · ".join(s[:60] for s in samples[:3]),
            })
        st.dataframe(
            pd.DataFrame(rows_preview),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.warning(
            "Nenhuma descrição da planilha casou com as regras atuais. "
            "Edite a barra lateral para adicionar regras mais específicas."
        )

    if n_unmatched:
        with st.expander(f"Ver até 20 descrições sem categoria ({n_unmatched:,} no total)"):
            unmatched_samples = [d for d, c in zip(descs_all, cats_all) if not c][:20]
            st.dataframe(
                pd.DataFrame({"Descrição sem regra": unmatched_samples}),
                use_container_width=True,
                hide_index=True,
            )

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
    sel_emp = ["Geral"]
    if mapping_ok:
        st.caption(
            "ℹ️ Sua planilha não tem coluna de empresa selecionada. Todas as "
            "linhas serão agrupadas como **Geral**. Para separar por empresa, "
            "selecione a coluna de empresa na Seção 2."
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

_has_anthropic_key = bool(os.environ.get("ANTHROPIC_API_KEY"))
if not _has_anthropic_key:
    try:
        _has_anthropic_key = bool(st.secrets.get("ANTHROPIC_API_KEY"))  # type: ignore
    except Exception:
        _has_anthropic_key = False
claude_suggest = st.checkbox(
    "🤖 Sugerir categorias automaticamente para linhas sem regra (Claude)",
    value=_has_anthropic_key,
    disabled=not _has_anthropic_key,
    help=(
        "Para linhas que nenhuma regra cobriu, envia a descrição ao Claude e usa a "
        "categoria sugerida. Requer ANTHROPIC_API_KEY no .env (local) ou nos Secrets "
        "do Streamlit Cloud."
    ),
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

    # Heuristic regex: street/road words and CEP. If the "description" cell
    # actually holds an address, the real description is missing or
    # duplicated elsewhere — we should skip descrição rules and go straight
    # to palavra-chave matching.
    _ADDRESS_RE = re.compile(
        r"\b(rua|r\.|av|av\.|avenida|alameda|al\.|travessa|trav\.|estrada|"
        r"rodovia|rod\.|praca|praça|largo|viela|beco|quadra|lote|bloco|"
        r"cep|n[º°]|num\.|numero|número)\b|\d{5}-?\d{3}",
        re.IGNORECASE,
    )

    def description_is_address(row) -> bool:
        if col_desc not in row.index:
            return False
        raw = str(row[col_desc] or "")
        if not raw.strip():
            return False
        # If an address column is mapped and the description equals it, it's clearly the address.
        if col_endereco and col_endereco in row.index:
            addr = str(row[col_endereco] or "").strip()
            if addr and strip_accents(addr) == strip_accents(raw):
                return True
        # Otherwise fall back to the regex heuristic.
        return bool(_ADDRESS_RE.search(raw))

    def match_rule(row):
        """Return (categoria, empresa_assigned, source) where source is 'desc'|'kw'|None."""
        desc_text = strip_accents(row[col_desc]) if col_desc in row.index else ""
        haystack = " ".join(strip_accents(row[c]) for c in search_cols if c in row.index)
        row_emp = str(row[col_empresa_eff]) if ledger_has_empresa else ""
        desc_is_addr = description_is_address(row)
        # Pass 1: descrição rules win — UNLESS the description is actually an
        # address, in which case the description carries no useful category
        # signal and we go straight to palavra-chave.
        if not desc_is_addr:
            for rule in active_rules_for_match:
                if rule["descricao"] and strip_accents(rule["descricao"]) in desc_text:
                    return rule["categoria"], (row_emp or "Geral"), "desc"
        # Pass 2: keyword fallback (also the primary path when description is an address)
        for rule in active_rules_for_match:
            if rule["kw"] and strip_accents(rule["kw"]) in haystack:
                return rule["categoria"], (row_emp or "Geral"), "kw"
        return None, None, None

    matched = filtered.apply(match_rule, axis=1)
    filtered["_categoria"] = [m[0] for m in matched]
    filtered["_empresa_assigned"] = [m[1] for m in matched]
    filtered["_match_source"] = [m[2] for m in matched]

    # Claude auto-categorization for unmatched rows (opt-in)
    if claude_suggest and categorias_disponiveis:
        unmatched_mask = filtered["_categoria"].isna()
        n_unmatched = int(unmatched_mask.sum())
        if n_unmatched > 0:
            try:
                from categorize_with_claude import suggest_categories
                with st.spinner(f"🤖 Sugerindo categorias para {n_unmatched} linha(s) com Claude..."):
                    descs = filtered.loc[unmatched_mask, col_desc].astype(str).tolist()
                    suggestions = suggest_categories(descs, categorias_disponiveis)
                idxs = filtered.index[unmatched_mask].tolist()
                for i, idx in enumerate(idxs):
                    if suggestions[i] is not None:
                        filtered.at[idx, "_categoria"] = suggestions[i]
                        if ledger_has_empresa:
                            filtered.at[idx, "_empresa_assigned"] = (
                                str(filtered.at[idx, col_empresa_eff]) or "Geral"
                            )
                        else:
                            filtered.at[idx, "_empresa_assigned"] = "Geral"
                        filtered.at[idx, "_match_source"] = "claude"
                n_filled = sum(1 for s in suggestions if s is not None)
                st.info(f"🤖 Claude sugeriu categoria para **{n_filled}** de {n_unmatched} linha(s) sem regra.")
            except Exception as e:
                st.warning(f"⚠️ Sugestão automática falhou: {e}")

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
    preview_cols = [col_empresa_eff, col_data, col_desc]
    if col_endereco:
        preview_cols.append(col_endereco)
    preview_cols += ["_categoria", "_valor_num"]
    preview_rename = {
        col_empresa_eff: "Empresa", col_data: "Data", col_desc: "Descrição",
        "_categoria": "Categoria", "_valor_num": "Valor",
    }
    if col_endereco:
        preview_rename[col_endereco] = "Endereço"
    preview = filtered[preview_cols].rename(columns=preview_rename)
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
    if col_endereco:
        cols_keep.insert(3, col_endereco)
        rename[col_endereco] = "Endereço"

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

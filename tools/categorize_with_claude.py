"""
Suggest categories for ledger rows using Claude.

Used by app.py for rows that no user-defined rule matched. Sends a single
batched request to Claude with all unmatched descriptions and the list of
allowed categories, asking for one category per row (or null).
"""
from __future__ import annotations

import json
import os
from typing import Iterable

try:
    from anthropic import Anthropic
except ImportError:  # pragma: no cover
    Anthropic = None  # type: ignore


MODEL = "claude-haiku-4-5-20251001"
MAX_BATCH = 200


def _client() -> "Anthropic":
    if Anthropic is None:
        raise RuntimeError(
            "O pacote 'anthropic' não está instalado. Adicione `anthropic` ao requirements.txt."
        )
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        # Streamlit Cloud secrets fallback
        try:
            import streamlit as st  # type: ignore
            api_key = st.secrets.get("ANTHROPIC_API_KEY")  # type: ignore
        except Exception:
            api_key = None
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY não encontrada. Defina no .env (local) ou nos Secrets do Streamlit Cloud."
        )
    return Anthropic(api_key=api_key)


def _suggest_batch(client, descriptions: list[str], allowed: list[str]) -> list[str | None]:
    system = (
        "Você é um classificador de despesas contábeis. Você receberá uma lista numerada "
        "de descrições de lançamentos e uma lista de categorias permitidas. Para cada "
        "descrição, escolha a categoria mais provável da lista permitida, ou null se "
        "nenhuma se encaixar. Responda APENAS com JSON válido no formato: "
        '{"results": ["Categoria1", null, "Categoria2", ...]} '
        "preservando a mesma ordem e o mesmo número de itens da entrada."
    )
    numbered = "\n".join(f"{i+1}. {d}" for i, d in enumerate(descriptions))
    user = (
        f"Categorias permitidas: {json.dumps(allowed, ensure_ascii=False)}\n\n"
        f"Descrições ({len(descriptions)}):\n{numbered}\n\n"
        "Responda apenas com o JSON."
    )
    msg = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    text = "".join(block.text for block in msg.content if getattr(block, "type", "") == "text").strip()
    # Strip code fences if present
    if text.startswith("```"):
        text = text.strip("`")
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    data = json.loads(text)
    results = data.get("results", [])
    if not isinstance(results, list):
        raise ValueError("Resposta do Claude não contém uma lista 'results'.")
    # Normalize length to input
    out: list[str | None] = []
    for i in range(len(descriptions)):
        if i < len(results):
            v = results[i]
            if v in (None, "", "null"):
                out.append(None)
            elif isinstance(v, str) and v in allowed:
                out.append(v)
            else:
                out.append(None)
        else:
            out.append(None)
    return out


def suggest_categories(
    descriptions: Iterable[str],
    allowed_categories: list[str],
) -> list[str | None]:
    """Return a list of suggested categories (one per description), same order.

    Each element is either a category from allowed_categories, or None when
    Claude could not confidently classify the row.
    """
    descs = [str(d) if d is not None else "" for d in descriptions]
    if not descs or not allowed_categories:
        return [None] * len(descs)
    client = _client()
    out: list[str | None] = []
    for start in range(0, len(descs), MAX_BATCH):
        chunk = descs[start : start + MAX_BATCH]
        out.extend(_suggest_batch(client, chunk, allowed_categories))
    return out

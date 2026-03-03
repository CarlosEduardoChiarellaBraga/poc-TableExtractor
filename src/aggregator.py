"""
aggregator.py

Unifies items and metadata coming from:
  - the main ConLicitação JSON (data.itens, + other metadata)
  - parsed attachments via parsers.pdf_parser (tables extracted from PDF/DOCX)

It delegates item fusion to merger.merge_sources.
"""
from __future__ import annotations

from typing import Any

from models import ItemExtraido
from merger import merge_sources


def merge_metadata(
    *,
    primary: dict[str, Any],
    fallbacks: list[dict[str, str]],
) -> dict[str, str]:
    """
    Fill missing metadata fields from fallbacks.
    """
    numero_pregao = (primary.get("numero_pregao") or "").strip()
    orgao = (primary.get("orgao") or "").strip()
    cidade = (primary.get("cidade") or "").strip()
    estado = (primary.get("estado") or "").strip()

    for fb in fallbacks:
        if not numero_pregao and fb.get("numero_pregao"):
            numero_pregao = fb["numero_pregao"]
        if not orgao and fb.get("orgao"):
            orgao = fb["orgao"]
        if not cidade and fb.get("cidade"):
            cidade = fb["cidade"]
        if not estado and fb.get("estado"):
            estado = fb["estado"]

    return {
        "numero_pregao": numero_pregao,
        "orgao": orgao,
        "cidade": cidade,
        "estado": estado,
    }


def aggregate_items(
    *,
    json_items: list[ItemExtraido],
    other_sources: dict[str, list[ItemExtraido]],
    debug: bool,
    json_source_label: str,
) -> list[ItemExtraido]:
    """
    Merge item lists from JSON and parsed attachments.

    - Ensures JSON items get fonte=json_source_label when debug=True (so every item has a fonte).
    """
    if debug:
        for it in json_items:
            if it.fonte is None:
                it.fonte = json_source_label

    sources: dict[str, list[ItemExtraido]] = {}
    if json_items:
        sources["json"] = json_items

    for k, v in other_sources.items():
        if v:
            sources[k] = v

    merged = merge_sources(sources)

    if not merged and json_items:
        merged = json_items

    return merged

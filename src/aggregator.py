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
    """Combine metadata from the primary JSON source and attachment fallbacks.

    The primary source (the ConLicitação JSON ``data`` object) is preferred.
    Each fallback (typically extracted from a PDF by the pdf_parser) is
    consulted only when the primary left a field blank, in the order they
    are provided.

    Args:
        primary:   Metadata dict from the main JSON (may have empty strings).
        fallbacks: Ordered list of metadata dicts from parsed attachments.
                   Earlier entries take precedence over later ones.

    Returns:
        Dict with keys ``numero_pregao``, ``orgao``, ``cidade``, ``estado``,
        each filled with the best available value (empty string if none found).
    """
    # Start from primary values, stripping surrounding whitespace.
    numero_pregao = (primary.get("numero_pregao") or "").strip()
    orgao = (primary.get("orgao") or "").strip()
    cidade = (primary.get("cidade") or "").strip()
    estado = (primary.get("estado") or "").strip()

    # Fill any remaining blanks from fallbacks in order.
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
    """Merge item lists from the JSON field and all parsed attachments.

    Builds the ``sources`` dict expected by ``merger.merge_sources``:
      - ``"json"`` key → items parsed from ``data.itens``
      - one key per ``doc_type`` → items extracted from each attachment type

    When ``debug=True``, ensures every JSON item carries a ``fonte`` label
    so the full provenance chain is preserved in the output.

    Args:
        json_items:        Items produced by ``parse_itens_field``.
        other_sources:     Dict mapping doc_type strings (e.g. ``"edital"``,
                           ``"relacaoitens"``) to their extracted item lists.
        debug:             When True, annotates JSON items with ``fonte``.
        json_source_label: Relative path of the source JSON file, used as the
                           ``fonte`` value for JSON items in debug mode.

    Returns:
        Merged list from ``merge_sources``, falling back to ``json_items``
        directly when the merge returns nothing (e.g. all sources empty).
    """
    if debug:
        # Tag every JSON item with its source path so the final output is fully
        # traceable. Items from PDFs already receive their fonte in pdf_parser.
        for it in json_items:
            if it.fonte is None:
                it.fonte = json_source_label

    # Assemble the sources dict, omitting empty lists to avoid polluting the
    # merge with phantom sources that carry no actual items.
    sources: dict[str, list[ItemExtraido]] = {}
    if json_items:
        sources["json"] = json_items

    for k, v in other_sources.items():
        if v:
            sources[k] = v

    merged = merge_sources(sources)

    # Safety fallback: if merge_sources returns nothing but we do have JSON items
    # (e.g. no attachments were found or all attachments failed to parse),
    # return the JSON items directly rather than an empty list.
    if not merged and json_items:
        merged = json_items

    return merged
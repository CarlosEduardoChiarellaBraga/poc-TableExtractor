"""
Parser for the `data.itens` field of the ConLicitação JSON.

Handles the following known format variants:

VARIANT A — ComprasNet full format (most common):
    ----------------------------------------
    Itens de Material/Serviços
    ----------------------------------------
    1 - ITEM_NAME
    DESCRIPTION
    Tratamento Diferenciado: ...
    Quantidade: X
    Unidade de fornecimento: Y
    ----------

VARIANT B — ComprasNet with Grupos at end:
    [VARIANT A items]
    Grupos
    G1
    1 - Item Name
    G2
    3 - Item Name

VARIANT C — Lote-prefixed (licitar.digital):
    Lote: 1 -
    Descrição: DESCRIPTION - more text

VARIANT D — Sparse / code-prefixed:
    MC0800538-TRAVA SEGURANCA PARA NOTEBOOK

VARIANT E — Empty string or whitespace only
"""
from __future__ import annotations

import re
import logging
from typing import Optional

from models import ItemExtraido

logger = logging.getLogger(__name__)

# ── Regex constants ────────────────────────────────────────────────────────────

# "1 - ITEM NAME" at start of a line
RE_ITEM_HEADER = re.compile(r"^\s*(\d+)\s*-\s+(.+)$")
# "Quantidade: 3"
RE_QUANTIDADE = re.compile(r"Quantidade\s*:\s*(\d+)", re.IGNORECASE)
# "Unidade de fornecimento: Unidade"
RE_UNIDADE = re.compile(r"Unidade de fornecimento\s*:\s*(.+)", re.IGNORECASE)
# Group section header
RE_GRUPOS_HEADER = re.compile(r"^\s*Grupos\s*$", re.IGNORECASE)
# Group label line: "G1", "G2", or integer "1", "2"
RE_GROUP_LABEL = re.compile(r"^\s*(G\d+|\d+)\s*$")
# "Lote: 1 -" or "Lote: 1 - "
RE_LOTE_LINE = re.compile(r"^\s*Lote\s*:\s*(\S+)\s*-\s*(.*)?$", re.IGNORECASE)
# Description line after Lote:
RE_DESCRICAO = re.compile(r"^\s*Descri[çc][aã]o\s*:\s*(.+)$", re.IGNORECASE)
# Skip metadata lines (these don't belong to objeto description)
RE_METADATA = re.compile(
    r"^(Tratamento Diferenciado|Aplicabilidade|Quantidade|Unidade de fornecimento"
    r"|Itens de (Material|Servi[çc]os)|---+|-{3,})",
    re.IGNORECASE,
)
# Product code prefix like "MC0800538-"
RE_CODE_PREFIX = re.compile(r"^[A-Z]{1,4}\d{4,}-(.+)$")


def parse_itens_field(raw_itens: list[str]) -> list[ItemExtraido]:
    """
    Entry point: receives `data.itens` (always a list, usually 1 element)
    and returns a list of ItemExtraido.

    Returns empty list if the field carries no parseable item information.
    """
    if not raw_itens:
        return []

    text = "\n".join(raw_itens).strip()
    if not text:
        return []

    # Try structured variants in order of specificity
    for parser in (_parse_comprasnet, _parse_lote_format, _parse_sparse):
        items = parser(text)
        if items:
            logger.debug("Parsed %d items using %s", len(items), parser.__name__)
            return items

    logger.warning("Could not parse itens field — returning empty list")
    return []


# ── Variant A/B: ComprasNet structured ────────────────────────────────────────

def _parse_comprasnet(text: str) -> list[ItemExtraido]:
    """Parse ComprasNet full format (variants A and B)."""
    lines = text.splitlines()

    # Quick check: must have at least one "N - NAME" header line
    if not any(RE_ITEM_HEADER.match(l) for l in lines):
        return []

    items: list[ItemExtraido] = []
    current: dict | None = None
    desc_lines: list[str] = []

    def _flush():
        """Finalise current item and append to items list."""
        nonlocal current, desc_lines
        if current is None:
            return
        objeto = " ".join(desc_lines).strip()
        # Prepend item name if description is empty or very short
        name = current.get("name", "")
        if objeto and not objeto.startswith(name[:10]):
            objeto = f"{name}, {objeto}" if objeto else name
        elif not objeto:
            objeto = name

        items.append(ItemExtraido(
            item=str(current["num"]),
            objeto=objeto,
            quantidade=current.get("quantidade", 0),
            unidade_fornecimento=current.get("unidade", ""),
            lote=None,  # groups resolved later
            confianca=_confidence(current),
        ))
        current = None
        desc_lines = []

    # ── First pass: parse items ────────────────────────────────────────────
    in_grupos = False
    for line in lines:
        stripped = line.strip()

        # Detect Grupos section — stop collecting items
        if RE_GRUPOS_HEADER.match(stripped):
            _flush()
            in_grupos = True
            continue

        if in_grupos:
            continue  # handled in second pass

        m_header = RE_ITEM_HEADER.match(stripped)
        if m_header:
            _flush()
            current = {"num": str(int(m_header.group(1))), "name": m_header.group(2).strip()}
            desc_lines = []
            continue

        if current is None:
            continue

        m_qty = RE_QUANTIDADE.match(stripped)
        if m_qty:
            current["quantidade"] = int(m_qty.group(1))
            continue

        m_unit = RE_UNIDADE.match(stripped)
        if m_unit:
            current["unidade"] = m_unit.group(1).strip()
            continue

        # Skip separator / metadata lines from description
        if RE_METADATA.match(stripped) or stripped in ("-" * 10, "-" * 40):
            continue

        if stripped:
            desc_lines.append(stripped)

    _flush()

    if not items:
        return []

    # ── Second pass: resolve groups ────────────────────────────────────────
    _apply_groups(text, items)

    return items


def _apply_groups(text: str, items: list[ItemExtraido]) -> None:
    """
    Parse the optional 'Grupos' section and assign lote to each item.

    Format:
        Grupos
        G1
        1 - Pacote de Serviços...
        2 - Pacote de Serviços...
        G2
        3 - Pacote de Serviços...

    Also handles integer group labels (1, 2) instead of G1, G2.
    """
    lines = text.splitlines()
    in_grupos = False
    current_group: Optional[str] = None
    item_index = {i.item: i for i in items}

    for line in lines:
        stripped = line.strip()
        if RE_GRUPOS_HEADER.match(stripped):
            in_grupos = True
            continue
        if not in_grupos:
            continue

        m_group = RE_GROUP_LABEL.match(stripped)
        if m_group:
            lbl = m_group.group(1)
            # Normalize: pure integer "1" stays "1", "G1" stays "G1"
            current_group = lbl
            continue

        m_item = RE_ITEM_HEADER.match(stripped)
        if m_item and current_group is not None:
            num = str(int(m_item.group(1)))
            if num in item_index:
                item_index[num].lote = current_group


def _confidence(current: dict) -> float:
    """Estimate extraction confidence for a parsed item dict."""
    score = 0.5
    if current.get("quantidade", 0) > 0:
        score += 0.3
    if current.get("unidade", ""):
        score += 0.2
    return round(min(score, 1.0), 2)


# ── Variant C: Lote-prefixed (licitar.digital) ─────────────────────────────────

def _parse_lote_format(text: str) -> list[ItemExtraido]:
    """
    Parse format:
        Lote: 1 -
        Descrição: FULL DESCRIPTION TEXT -
    """
    lines = text.splitlines()
    items: list[ItemExtraido] = []
    current_lote: Optional[str] = None
    current_desc: Optional[str] = None
    item_num = 0

    for line in lines:
        stripped = line.strip()
        m_lote = RE_LOTE_LINE.match(stripped)
        if m_lote:
            # Flush previous
            if current_desc:
                item_num += 1
                items.append(ItemExtraido(
                    item=str(item_num),
                    objeto=current_desc.strip(" -"),
                    quantidade=0,
                    unidade_fornecimento="",
                    lote=current_lote,
                    confianca=0.4,
                ))
            current_lote = m_lote.group(1)
            # Sometimes description is inline: "Lote: 1 - DESCRIPTION"
            inline_desc = m_lote.group(2).strip(" -")
            current_desc = inline_desc if inline_desc else None
            continue

        m_desc = RE_DESCRICAO.match(stripped)
        if m_desc and current_lote is not None:
            current_desc = m_desc.group(1).strip(" -")
            continue

    # Flush last
    if current_lote and current_desc:
        item_num += 1
        items.append(ItemExtraido(
            item=str(item_num),
            objeto=current_desc,
            quantidade=0,
            unidade_fornecimento="",
            lote=current_lote,
            confianca=0.4,
        ))

    return items


# ── Variant D: Sparse / code-prefixed ─────────────────────────────────────────

def _parse_sparse(text: str) -> list[ItemExtraido]:
    """
    Last-resort parser for sparse content like:
        MC0800538-TRAVA SEGURANCA PARA NOTEBOOK
    or just:
        PAPEL SULFITE A4
    Extracts only the objeto; quantidade/unidade will come from PDFs.
    """
    clean = text.strip()
    if not clean or len(clean) < 3:
        return []

    # Remove code prefix like "MC0800538-"
    m_code = RE_CODE_PREFIX.match(clean)
    objeto = m_code.group(1).strip() if m_code else clean

    # Only treat as valid if it looks like a product name (not a URL, not JSON)
    if objeto.startswith("http") or "{" in objeto:
        return []

    return [ItemExtraido(
        item="1",
        objeto=objeto,
        quantidade=0,
        unidade_fornecimento="",
        lote=None,
        confianca=0.2,
    )]
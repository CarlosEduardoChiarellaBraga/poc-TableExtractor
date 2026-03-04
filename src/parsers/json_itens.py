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

# Matches the numbered item header at the start of a line: "1 - ITEM NAME"
RE_ITEM_HEADER = re.compile(r"^\s*(\d+)\s*-\s+(.+)$")

# Matches "Quantidade: 3" — the quantity field inside a ComprasNet item block
RE_QUANTIDADE = re.compile(r"Quantidade\s*:\s*(\d+)", re.IGNORECASE)

# Matches "Unidade de fornecimento: Unidade" — the unit field
RE_UNIDADE = re.compile(r"Unidade de fornecimento\s*:\s*(.+)", re.IGNORECASE)

# Matches the standalone "Grupos" section header that signals Variant B
RE_GRUPOS_HEADER = re.compile(r"^\s*Grupos\s*$", re.IGNORECASE)

# Matches group label lines inside the Grupos section: "G1", "G2", or plain "1", "2"
RE_GROUP_LABEL = re.compile(r"^\s*(G\d+|\d+)\s*$")

# Matches Variant C lot header: "Lote: 1 -" or "Lote: 1 - DESCRIPTION (inline)"
RE_LOTE_LINE = re.compile(r"^\s*Lote\s*:\s*(\S+)\s*-\s*(.*)?$", re.IGNORECASE)

# Matches the description line that follows a Lote header in Variant C
RE_DESCRICAO = re.compile(r"^\s*Descri[çc][aã]o\s*:\s*(.+)$", re.IGNORECASE)

# Lines that must be skipped when collecting free-text description content.
# These are metadata fields, section headers, and separator dashes that are
# not part of the item's objeto description.
RE_METADATA = re.compile(
    r"^(Tratamento Diferenciado|Aplicabilidade|Quantidade|Unidade de fornecimento"
    r"|Itens de (Material|Servi[çc]os)|---+|-{3,})",
    re.IGNORECASE,
)

# Matches a CATMAT/internal-code prefix like "MC0800538-" in Variant D
RE_CODE_PREFIX = re.compile(r"^[A-Z]{1,4}\d{4,}-(.+)$")


def parse_itens_field(raw_itens: list[str]) -> list[ItemExtraido]:
    """Parse the ``data.itens`` field of a ConLicitação JSON document.

    The field is always delivered as a list of strings (usually one element)
    containing a free-text block with varying formats depending on the source
    platform (ComprasNet, licitar.digital, etc.).

    The three sub-parsers are tried in decreasing order of specificity:
      1. ``_parse_comprasnet``  — structured ComprasNet format (Variants A/B)
      2. ``_parse_lote_format`` — lote-prefixed format (Variant C)
      3. ``_parse_sparse``      — last-resort single-line extraction (Variant D)

    Args:
        raw_itens: The raw list from ``data.itens``, typically one long string.

    Returns:
        List of ``ItemExtraido`` objects, empty when nothing parseable is found.
    """
    if not raw_itens:
        return []

    # Join all list elements into one block — handles the rare case where items
    # are split across multiple list entries.
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
    """Parse ComprasNet full format (Variants A and B).

    Performs two passes over the text:
      1. First pass:  collects items, descriptions, quantities and units.
      2. Second pass: resolves group/lot assignments from the optional
                      ``Grupos`` section (Variant B only).

    Guard: returns [] immediately if no "N - NAME" header line is found,
    letting the caller fall through to the next parser variant.
    """
    lines = text.splitlines()

    # Quick check: must have at least one "N - NAME" header line
    if not any(RE_ITEM_HEADER.match(l) for l in lines):
        return []

    items: list[ItemExtraido] = []
    current: dict | None = None   # fields accumulated for the item being parsed
    desc_lines: list[str] = []    # free-text lines that form the item description

    def _flush():
        """Finalise the current item buffer and append it to `items`.

        Description assembly rules:
        - If description lines were collected AND they don't already start with
          the item name, the name is prepended: "NAME, description…"
        - If no description was collected, the item name alone becomes `objeto`.
        """
        nonlocal current, desc_lines
        if current is None:
            return
        objeto = " ".join(desc_lines).strip()
        name = current.get("name", "")
        if objeto and not objeto.startswith(name[:10]):
            # Description exists but starts differently from the header name —
            # prepend the name so the objeto is self-contained.
            objeto = f"{name}, {objeto}" if objeto else name
        elif not objeto:
            # No description lines collected; fall back to the header name only.
            objeto = name

        items.append(ItemExtraido(
            item=str(current["num"]),
            objeto=objeto,
            quantidade=current.get("quantidade", 0),
            unidade_fornecimento=current.get("unidade", ""),
            lote=None,  # groups are resolved in the second pass by _apply_groups
            confianca=_confidence(current),
        ))
        current = None
        desc_lines = []

    # ── First pass: parse items ────────────────────────────────────────────
    in_grupos = False
    for line in lines:
        stripped = line.strip()

        # Detect the start of the Grupos section — flush the last item and
        # stop collecting; the second pass will handle group assignments.
        if RE_GRUPOS_HEADER.match(stripped):
            _flush()
            in_grupos = True
            continue

        if in_grupos:
            continue  # handled in second pass by _apply_groups

        m_header = RE_ITEM_HEADER.match(stripped)
        if m_header:
            # New item header — flush any previously accumulated item first.
            _flush()
            current = {"num": str(int(m_header.group(1))), "name": m_header.group(2).strip()}
            desc_lines = []
            continue

        if current is None:
            # No active item yet (e.g. section headers before first item)
            continue

        m_qty = RE_QUANTIDADE.match(stripped)
        if m_qty:
            current["quantidade"] = int(m_qty.group(1))
            continue

        m_unit = RE_UNIDADE.match(stripped)
        if m_unit:
            current["unidade"] = m_unit.group(1).strip()
            continue

        # Skip separator and metadata lines — these carry no description value.
        if RE_METADATA.match(stripped) or stripped in ("-" * 10, "-" * 40):
            continue

        if stripped:
            # Any remaining non-empty line is treated as part of the description.
            desc_lines.append(stripped)

    _flush()  # flush the final item after the loop ends

    if not items:
        return []

    # ── Second pass: resolve groups ────────────────────────────────────────
    _apply_groups(text, items)

    return items


def _apply_groups(text: str, items: list[ItemExtraido]) -> None:
    """Parse the optional ``Grupos`` section and assign ``lote`` to each item.

    The section appears after all item blocks and maps group labels to item
    numbers. Both ``G1``/``G2`` style labels and plain integer labels (``1``,
    ``2``) are supported.

    Example format (Variant B):
        Grupos
        G1
        1 - Pacote de Serviços...
        2 - Pacote de Serviços...
        G2
        3 - Pacote de Serviços...

    The function mutates the ``lote`` field of matching items in-place.
    Items not listed under any group label retain ``lote=None``.
    """
    lines = text.splitlines()
    in_grupos = False
    current_group: Optional[str] = None
    # Build a lookup by item number string for O(1) assignment
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
            # Start a new group; both "G1" and plain "1" are kept as-is.
            lbl = m_group.group(1)
            current_group = lbl
            continue

        m_item = RE_ITEM_HEADER.match(stripped)
        if m_item and current_group is not None:
            # Assign the current group label to the matching item.
            num = str(int(m_item.group(1)))
            if num in item_index:
                item_index[num].lote = current_group


def _confidence(current: dict) -> float:
    """Estimate extraction confidence for a ComprasNet item dict.

    Scoring breakdown:
      0.5  — base score (item header was found)
      +0.3 — quantity field was present and non-zero
      +0.2 — unit field was present and non-empty
      ─────
      1.0  — maximum (all fields populated)
    """
    score = 0.5
    if current.get("quantidade", 0) > 0:
        score += 0.3
    if current.get("unidade", ""):
        score += 0.2
    return round(min(score, 1.0), 2)


# ── Variant C: Lote-prefixed (licitar.digital) ─────────────────────────────────

def _parse_lote_format(text: str) -> list[ItemExtraido]:
    """Parse the licitar.digital lote-prefixed format (Variant C).

    Expected structure (one or more lots):
        Lote: 1 -
        Descrição: FULL DESCRIPTION TEXT -

    Inline descriptions on the ``Lote:`` line are also handled:
        Lote: 1 - DESCRIPTION HERE

    Items produced by this parser have ``quantidade=0`` and
    ``unidade_fornecimento=""`` because those fields are absent in this
    format; they are expected to be filled later by the PDF parser via the
    merger's field-level fill logic.

    Confidence is fixed at 0.4 to reflect the lower data richness.
    """
    lines = text.splitlines()
    items: list[ItemExtraido] = []
    current_lote: Optional[str] = None
    current_desc: Optional[str] = None
    item_num = 0  # auto-incremented since this format has no explicit item numbers

    for line in lines:
        stripped = line.strip()
        m_lote = RE_LOTE_LINE.match(stripped)
        if m_lote:
            # Flush the previous lot's item before starting a new one.
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
            # Some lines carry the description inline after the dash.
            inline_desc = m_lote.group(2).strip(" -")
            current_desc = inline_desc if inline_desc else None
            continue

        m_desc = RE_DESCRICAO.match(stripped)
        if m_desc and current_lote is not None:
            # Explicit "Descrição:" line — overrides any inline description.
            current_desc = m_desc.group(1).strip(" -")
            continue

    # Flush the last pending item after the loop ends.
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
    """Last-resort parser for sparse single-line content (Variant D).

    Handles entries like:
        MC0800538-TRAVA SEGURANCA PARA NOTEBOOK   ← code prefix stripped
        PAPEL SULFITE A4                           ← used verbatim

    Confidence is fixed at 0.2 — the lowest tier — reflecting that only the
    object description could be extracted. Quantity and unit are expected to
    come from a paired PDF attachment via the merger's field-level fill.

    Returns [] (passing control back to the caller) if:
      - The text is shorter than 3 characters.
      - The text looks like a URL or a JSON blob, not a product name.
    """
    clean = text.strip()
    if not clean or len(clean) < 3:
        return []

    # Strip product-code prefixes like "MC0800538-" before using the rest as objeto.
    m_code = RE_CODE_PREFIX.match(clean)
    objeto = m_code.group(1).strip() if m_code else clean

    # Reject strings that are clearly not item descriptions.
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
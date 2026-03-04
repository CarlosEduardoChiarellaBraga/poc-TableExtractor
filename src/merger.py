"""
Fusion layer: merges ItemExtraido lists from multiple sources.

Strategy:
  1. Source priority: edital PDF > JSON itens > relacaoitens PDF > TR PDF
  2. For each (lote, item) key, take the version with the highest confidence.
  3. Field-level merge: fill missing fields (quantidade=0, unidade="")
     from lower-priority sources.
  4. Lote/group is applied from any source that has it.
"""
from __future__ import annotations

import logging
from models import ItemExtraido, _item_str_sort_key

logger = logging.getLogger(__name__)

# Source priority order (lower index = higher priority).
# "edital" PDFs are considered the most authoritative because they are the
# official procurement specification. The JSON itens field comes next, followed
# by relacaoitens reports and term-of-reference documents.
SOURCE_PRIORITY = ["edital", "json", "relacaoitens", "termo_referencia"]


def merge_sources(
    sources: dict[str, list[ItemExtraido]],
) -> list[ItemExtraido]:
    """Merge item lists from multiple extraction sources into one canonical list.

    For every unique (lote, item) key found across all sources the function:
      1. Selects the best candidate according to SOURCE_PRIORITY, breaking ties
         by confidence score (higher wins).
      2. Fills any empty fields on the winner from lower-priority candidates
         (field-level merge) — e.g. a PDF may have the quantity while the JSON
         has the full description.

    Items are returned sorted by (lote, item_sort_key) so the output order is
    stable and numerically correct regardless of the order sources were added.

    Args:
        sources: Mapping from source name (e.g. "json", "edital") to the list
                 of items extracted from that source.  Source names not listed
                 in SOURCE_PRIORITY are treated as lowest priority (rank 99).

    Returns:
        Merged, deduplicated, sorted list of ItemExtraido.  Returns [] when
        ``sources`` is empty or all source lists are empty.
    """
    if not sources:
        return []

    # Collect every (lote, item_str) key present across all sources.
    # ``item`` is a string ("1", "1.1", etc.) so we key on the exact string
    # rather than converting to int, preserving hierarchical notation.
    all_keys: set[tuple] = set()
    for items in sources.values():
        for item in items:
            all_keys.add((item.lote, item.item))

    if not all_keys:
        return []

    merged: list[ItemExtraido] = []

    def _key_sort(k: tuple) -> tuple:
        """Sort (lote, item_str) tuples by lote string then numerically by item."""
        lote, item_str = k
        major, minor = _item_str_sort_key(item_str)
        return (lote or "", major, minor)

    for lote, item_str in sorted(all_keys, key=_key_sort):
        # Gather all versions of this (lote, item) pair, annotated with their
        # source priority rank so sorting is deterministic.
        candidates: list[tuple[int, ItemExtraido]] = []
        for source_name, items in sources.items():
            rank = SOURCE_PRIORITY.index(source_name) if source_name in SOURCE_PRIORITY else 99
            for item in items:
                if item.item == item_str and item.lote == lote:
                    candidates.append((rank, item))

        if not candidates:
            continue

        # Primary sort: source priority (lower rank = better source).
        # Secondary sort: confidence descending (higher confidence wins ties).
        candidates.sort(key=lambda x: (x[0], -x[1].confianca))
        _, best = candidates[0]

        # ── Field-level fill from lower-priority sources ──────────────────
        # The winner may have an empty quantity or unit (e.g. the JSON itens
        # field often lacks these), which a lower-priority PDF source may have.
        for _, fallback in candidates[1:]:
            if best.quantidade == 0 and fallback.quantidade > 0:
                best.quantidade = fallback.quantidade
                logger.debug("Item %s: filled quantidade from fallback", item_str)
            if not best.unidade_fornecimento and fallback.unidade_fornecimento:
                best.unidade_fornecimento = fallback.unidade_fornecimento
                logger.debug("Item %s: filled unidade from fallback", item_str)
            if best.lote is None and fallback.lote is not None:
                # Propagate lote assignment from any source that detected it.
                best.lote = fallback.lote
                logger.debug("Item %s: filled lote from fallback", item_str)
            if len(best.objeto) < 10 and len(fallback.objeto) > len(best.objeto):
                # Replace a suspiciously short description with a longer one
                # from the fallback (threshold: fewer than 10 characters).
                best.objeto = fallback.objeto
                logger.debug("Item %s: replaced short objeto from fallback", item_str)
            if best.fonte is None and fallback.fonte is not None:
                # Carry the source path forward for debug traceability.
                best.fonte = fallback.fonte

        merged.append(best)

    logger.info("Merged %d items from %d sources", len(merged), len(sources))
    return merged
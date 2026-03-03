"""
Fusion layer: merges ItemExtraido lists from multiple sources.

Strategy:
  1. Source priority: JSON itens > relacaoitens PDF > edital PDF > TR PDF
  2. For each (lote, item) key, take the version with the highest confidence.
  3. Field-level merge: fill missing fields (quantidade=0, unidade="")
     from lower-priority sources.
  4. Lote/group is applied from any source that has it.
"""
from __future__ import annotations

import logging
from models import ItemExtraido, _item_str_sort_key

logger = logging.getLogger(__name__)

# Source priority (lower index = higher priority)
#SOURCE_PRIORITY = ["json", "relacaoitens", "edital", "termo_referencia"]
SOURCE_PRIORITY = ["edital", "json", "relacaoitens", "termo_referencia"]


def merge_sources(
    sources: dict[str, list[ItemExtraido]],
) -> list[ItemExtraido]:
    """
    Merge item lists from multiple sources.

    Args:
        sources: dict mapping source name to list of ItemExtraido.
                 Source names should be keys of SOURCE_PRIORITY.

    Returns:
        Merged, deduplicated, sorted list of ItemExtraido.
    """
    if not sources:
        return []

    # Collect all (lote, item) keys across all sources.
    # item is now a str, so we key on the exact string ("1", "1.1", "2.14", etc.)
    all_keys: set[tuple] = set()
    for items in sources.values():
        for item in items:
            all_keys.add((item.lote, item.item))

    if not all_keys:
        return []

    merged: list[ItemExtraido] = []

    def _key_sort(k: tuple) -> tuple:
        lote, item_str = k
        major, minor = _item_str_sort_key(item_str)
        return (lote or "", major, minor)

    for lote, item_str in sorted(all_keys, key=_key_sort):
        # Gather candidate versions from each source
        candidates: list[tuple[int, ItemExtraido]] = []
        for source_name, items in sources.items():
            rank = SOURCE_PRIORITY.index(source_name) if source_name in SOURCE_PRIORITY else 99
            for item in items:
                if item.item == item_str and item.lote == lote:
                    candidates.append((rank, item))

        if not candidates:
            continue

        # Sort by priority rank (lower = better), then by confidence desc
        candidates.sort(key=lambda x: (x[0], -x[1].confianca))
        _, best = candidates[0]

        # Field-level fill from lower-priority sources
        for _, fallback in candidates[1:]:
            if best.quantidade == 0 and fallback.quantidade > 0:
                best.quantidade = fallback.quantidade
                logger.debug("Item %s: filled quantidade from fallback", item_str)
            if not best.unidade_fornecimento and fallback.unidade_fornecimento:
                best.unidade_fornecimento = fallback.unidade_fornecimento
                logger.debug("Item %s: filled unidade from fallback", item_str)
            if best.lote is None and fallback.lote is not None:
                best.lote = fallback.lote
                logger.debug("Item %s: filled lote from fallback", item_str)
            if len(best.objeto) < 10 and len(fallback.objeto) > len(best.objeto):
                best.objeto = fallback.objeto
                logger.debug("Item %s: replaced short objeto from fallback", item_str)
            if best.fonte is None and fallback.fonte is not None:
                best.fonte = fallback.fonte

        merged.append(best)

    logger.info("Merged %d items from %d sources", len(merged), len(sources))
    return merged
#!/usr/bin/env python3
"""
sanitize.py

Filters items inside the result JSON on a per-item basis.

Rule — an item is removed if ANY of these conditions holds:
  - ``item``                 equals "0" (known extraction garbage)
  - ``objeto``               is ""
  - ``unidade_fornecimento`` is ""
  - ``quantidade``           is 0 (or non-numeric / None)

Additional rule — de-duplication within the same edital (document):
  - Items are grouped by ("unidade_fornecimento", "objeto", "quantidade", "lote")
    (lote can be null or non-null).
  - If a group contains at least one record with a non-empty ``item``, all records
    with empty/None ``item`` are dropped.
  - If multiple records have non-empty ``item``, they are kept when their ``item``
    values differ (exact duplicates keep only the first).

Output:
  - prints ``itens_before`` and ``itens_after`` counts (global across all docs)
  - writes a new file, or overwrites the input with --inplace (creating a .bak)
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple


def norm(s: Any) -> str:
    """Coerce any value to a stripped string, treating None as empty string."""
    return ("" if s is None else str(s)).strip()


def norm_lote(x: Any) -> Any:
    """Normalize lote for dedup keys.

    Treats None and empty/whitespace as None; otherwise returns stripped string.
    """
    if x is None:
        return None
    s = norm(x)
    return None if s == "" else s


def to_int(x: Any) -> int:
    """Convert a value to int, returning 0 on failure or for falsy inputs.

    Handles the variety of types that ``quantidade`` may arrive as after JSON
    deserialisation (int, float, str with thousand-separators, bool, None).
    Only digits and a leading minus sign are retained before parsing.
    """
    if x is None:
        return 0
    if isinstance(x, bool):
        # bool is a subclass of int in Python; treat True/False as 1/0.
        return int(x)
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    s = norm(x)
    if not s:
        return 0
    # Strip everything except digits and a leading minus sign.
    cleaned = "".join(ch for ch in s if ch.isdigit() or ch == "-")
    if cleaned in ("", "-"):
        return 0
    try:
        return int(cleaned)
    except ValueError:
        return 0


def is_invalid_item(it: Dict[str, Any]) -> bool:
    """Return True when the item dict is missing any required field.

    An item is considered invalid — and should be removed — when any of the
    following is true:
      - ``item`` equals "0" (known extraction garbage)
      - ``objeto`` is empty
      - ``unidade_fornecimento`` is empty
      - ``quantidade`` resolves to 0
    """
    item = norm(it.get("item", ""))
    objeto = norm(it.get("objeto", ""))
    und = norm(it.get("unidade_fornecimento", ""))
    qtd = to_int(it.get("quantidade", 0))

    # Allow missing/empty item here; it can still be kept if it doesn't collide
    # with a better (non-empty item) record during de-duplication.
    if item == "0":
        return True
    if objeto == "":
        return True
    if und == "":
        return True
    if qtd == 0 or qtd >= 10**9:
        return True
    return False


def dedup_key(it: Dict[str, Any]) -> Tuple[Any, str, str, int]:
    """Key used to detect duplicates within the same edital/document.

    Uses normalized/trimmed strings and integer quantidade.
    lote is normalized to None when missing/empty.
    """
    lote = norm_lote(it.get("lote"))
    und = norm(it.get("unidade_fornecimento", ""))
    obj = norm(it.get("objeto", ""))
    qtd = to_int(it.get("quantidade", 0))
    return (lote, und, obj, qtd)


def norm_item_for_dedup(it: Dict[str, Any]) -> str:
    """Normalize the 'item' field for tie-breaking during de-duplication.

    Returns a stripped string; treats None as empty. Keeps original punctuation.
    The caller decides what counts as "present" (non-empty).
    """
    return norm(it.get("item", ""))


def filter_payload(payload: Any) -> Tuple[Any, int, int]:
    """Remove invalid and duplicate items from every licitação document in the payload.

    Supports two payload shapes:
      - A plain list of document dicts (the normal ``pre_resultado_final.json`` format).
      - A dict with a ``"results"`` key containing such a list (legacy wrapper).

    Items that fail ``is_invalid_item`` are dropped.
    De-duplication is done within the same document on ("unidade_fornecimento","objeto","quantidade","lote"):
      - if any record in the duplicate-group has a non-empty ``item``, all records with empty/None ``item`` are dropped
      - if multiple records have non-empty ``item``, they are kept when their ``item`` values differ
      - exact duplicates (same base key + same non-empty ``item``) keep only the first occurrence

    Non-dict items inside ``itens_extraidos`` are dropped.

    Returns:
        (filtered_payload, itens_before, itens_after) where counts are global across all docs.
    """
    wrapper = None
    wrapper_key = None

    # Detect whether the payload uses the legacy {"results": [...]} wrapper.
    if isinstance(payload, dict):
        if "results" in payload and isinstance(payload["results"], list):
            wrapper = payload
            wrapper_key = "results"
            docs = payload["results"]
        else:
            raise SystemExit("Formato inesperado: JSON dict sem 'results' list.")
    elif isinstance(payload, list):
        docs = payload
    else:
        raise SystemExit("Formato inesperado: JSON deve ser list (ou dict com 'results').")

    itens_before = 0
    itens_after = 0

    new_docs = []
    for doc in docs:
        if not isinstance(doc, dict):
            # Preserve non-dict entries unchanged (unknown format).
            new_docs.append(doc)
            continue

        items = doc.get("itens_extraidos") or []
        if not isinstance(items, list):
            new_docs.append(doc)
            continue

        itens_before += len(items)

        # De-duplication is done in two steps:
        #   1) group items by the base key (lote, unidade_fornecimento, objeto, quantidade)
        #   2) within each group:
        #        - if any record has a non-empty item, drop the empty/None item ones
        #        - keep distinct non-empty item values (but collapse exact duplicates)
        #        - if *all* items are empty/None, keep only the first record
        grouped: dict[Tuple[Any, str, str, int], List[Tuple[int, Dict[str, Any], str]]] = {}
        for pos, it in enumerate(items):
            if not isinstance(it, dict):
                continue
            if is_invalid_item(it):
                continue
            k = dedup_key(it)
            item_norm = norm_item_for_dedup(it)
            grouped.setdefault(k, []).append((pos, it, item_norm))

        keep_pos: set[int] = set()

        for _k, group in grouped.items():
            # Preserve original order (group is built in scan order)
            has_present_item = any((itm.strip() != "" and itm.strip() != "0") for (_pos, _it, itm) in group)

            if has_present_item:
                seen_items: set[str] = set()
                for pos, _it, itm in group:
                    itm2 = itm.strip()
                    if itm2 == "" or itm2 == "0":
                        continue
                    # Collapse exact duplicates where item is the same.
                    if itm2 in seen_items:
                        continue
                    seen_items.add(itm2)
                    keep_pos.add(pos)
            else:
                # All items empty/None -> keep the first occurrence only.
                keep_pos.add(group[0][0])

        kept_items: List[Any] = []
        for pos in range(len(items)):
            if pos in keep_pos:
                kept_items.append(items[pos])

        itens_after += len(kept_items)

        # Shallow-copy the doc dict so the original payload is not mutated.
        doc2 = dict(doc)
        doc2["itens_extraidos"] = kept_items
        new_docs.append(doc2)

    # Rebuild the wrapper if the input used one.
    if wrapper is not None:
        out = dict(wrapper)
        out[wrapper_key] = new_docs
        return out, itens_before, itens_after

    return new_docs, itens_before, itens_after


def main() -> None:
    """CLI entry point for standalone sanitization of a result JSON file."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=Path("pre_resultado_final.json"))
    ap.add_argument("--output", type=Path, default=Path("resultado.json"))
    ap.add_argument("--inplace", action="store_true", help="Sobrescreve input (cria .bak).")
    args = ap.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input não encontrado: {args.input}")

    payload = json.loads(args.input.read_text(encoding="utf-8"))
    out_payload, before, after = filter_payload(payload)

    if args.inplace:
        backup = args.input.with_suffix(args.input.suffix + ".bak")
        shutil.copy2(args.input, backup)
        args.input.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote inplace: {args.input} (backup: {backup})")
    else:
        args.output.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote: {args.output}")

    print(f"itens_before: {before}")
    print(f"itens_after:  {after}")
    print(f"itens_removed:{before - after}")


if __name__ == "__main__":
    main()
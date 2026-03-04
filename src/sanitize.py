#!/usr/bin/env python3
"""
sanitize.py

Filters items inside the result JSON on a per-item basis.

Rule — an item is removed if ANY of these conditions holds:
  - ``item``                is "" or "0"
  - ``objeto``              is ""
  - ``unidade_fornecimento`` is ""
  - ``quantidade``          is 0 (or non-numeric / None)

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
      - ``item`` is empty or equals "0"
      - ``objeto`` is empty
      - ``unidade_fornecimento`` is empty
      - ``quantidade`` resolves to 0

    Args:
        it: A single item dict as found in ``itens_extraidos``.

    Returns:
        True if the item should be dropped, False if it should be kept.
    """
    item = norm(it.get("item", ""))
    objeto = norm(it.get("objeto", ""))
    und = norm(it.get("unidade_fornecimento", ""))
    qtd = to_int(it.get("quantidade", 0))

    if item == "" or item == "0":
        return True
    if objeto == "":
        return True
    if und == "":
        return True
    if qtd == 0:
        return True
    return False


def filter_payload(payload: Any) -> Tuple[Any, int, int]:
    """Remove invalid items from every licitação document in the payload.

    Supports two payload shapes:
      - A plain list of document dicts (the normal ``pre_resultado_final.json``
        format produced by ``main.py``).
      - A dict with a ``"results"`` key containing such a list (legacy wrapper).

    Items that fail ``is_invalid_item`` are silently dropped. Documents that
    have no ``itens_extraidos`` list (or whose list is empty) are kept
    unchanged; only individual invalid items are removed.

    Non-dict items inside ``itens_extraidos`` are also dropped because they
    cannot be validated against the required fields.

    Args:
        payload: Deserialised JSON — either a list of docs or a ``{"results": [...]}`` dict.

    Returns:
        A tuple of (filtered_payload, itens_before, itens_after) where
        ``itens_before`` and ``itens_after`` are global counts across all docs.

    Raises:
        SystemExit: When the payload shape is not recognised.
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

        kept_items: List[Any] = []
        for it in items:
            if not isinstance(it, dict):
                # Non-dict item entries are always dropped — can't validate fields.
                continue
            if is_invalid_item(it):
                continue
            kept_items.append(it)

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
    """CLI entry point for standalone sanitization of a result JSON file.

    Reads ``pre_resultado_final.json`` (or a custom ``--input`` path), applies
    ``filter_payload``, and writes the cleaned result to ``resultado.json``
    (or a custom ``--output`` path).

    With ``--inplace``, the input file is overwritten and a ``.bak`` backup is
    created automatically before writing.
    """
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
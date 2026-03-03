#!/usr/bin/env python3
"""
sanitize.py

Filtra item-a-item dentro do resultado.json.

Regra:
- Para cada item em `itens_extraidos`, se QUALQUER um destes campos estiver “vazio”,
  o item é removido:
    - item: "" ou "0"
    - objeto: ""
    - unidade_fornecimento: ""
    - quantidade: 0 (ou não numérico / None)

Saída:
- imprime itens_before e itens_after (contagem global)
- escreve um novo arquivo (ou sobrescreve com --inplace, criando .bak)
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple


def norm(s: Any) -> str:
    return ("" if s is None else str(s)).strip()


def to_int(x: Any) -> int:
    if x is None:
        return 0
    if isinstance(x, bool):
        return int(x)
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    s = norm(x)
    if not s:
        return 0
    # mantém apenas dígitos e sinal
    cleaned = "".join(ch for ch in s if ch.isdigit() or ch == "-")
    if cleaned in ("", "-"):
        return 0
    try:
        return int(cleaned)
    except ValueError:
        return 0


def is_invalid_item(it: Dict[str, Any]) -> bool:
    item = norm(it.get("item", ""))
    objeto = norm(it.get("objeto", ""))
    und = norm(it.get("unidade_fornecimento", ""))
    qtd = to_int(it.get("quantidade", 0))

    # “vazio” aqui inclui: strings vazias, item == "0", e quantidade == 0
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
    """
    Retorna (novo_payload, itens_before, itens_after).
    Suporta:
      - payload como lista de docs
      - payload como dict wrapper com key "results" (fallback)
    """
    wrapper = None
    wrapper_key = None

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
            # se doc não é dict, mantém como está (não sabemos mexer)
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
                # item inválido (não dá pra validar campos) -> remove
                continue
            if is_invalid_item(it):
                continue
            kept_items.append(it)

        itens_after += len(kept_items)

        doc2 = dict(doc)
        doc2["itens_extraidos"] = kept_items
        new_docs.append(doc2)

    if wrapper is not None:
        out = dict(wrapper)
        out[wrapper_key] = new_docs
        return out, itens_before, itens_after

    return new_docs, itens_before, itens_after


def main() -> None:
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
"""
parsers/pdf_parser.py

PDF/DOCX attachment parser based on:
  1) table extractor (pdfplumber, docx2pdf)
  2) table-to-item parser (heuristics for licitação "Item / Quantidade / Unidade / Descrição" tables)

It produces ItemExtraido items compatible with the merger/aggregator pipeline.

Key entrypoint:
  parse_attachment(file_path, doc_type, project_root, tables_out_dir, parsed_out_dir, debug)

When debug=True, each extracted item includes `fonte` with the original attachment path
(relative to project_root) stored in the extracted table dicts.

Also offers a standalone CLI to parse an existing *_tables.json directory.

Changelog (improvements over original):
  - Hierarchical item numbers (X.Y format like 1.1, 2.14) now supported with sequential mapping.
  - Section-header rows (e.g. "1 | IMPLANTAÇÃO DO SISTEMA | (no qty/unit)") are detected and
    skipped instead of being emitted as fake items.
  - Section title text in continuation rows no longer gets appended to the previous item's objeto.
  - _extract_qty_with_header now correctly returns qty even when qty value == item_num
    (previously items like item=1 with qty=1 came out as qty=0).
  - _detect_header_indices relaxed: only item + obj columns required; qtd/und are optional.
    This handles tables where quantity or unit columns are absent or use unusual labels.
  - Header search window widened from 25 to 40 rows to handle tables with tall title areas.
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Any


def _safe_relpath(path: Path, root: Path) -> str:
    """Return path relative to root when possible; otherwise return str(path)."""
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path)


from models import ItemExtraido, ResultadoLicitacao
from extractor import process_file, write_tables_json


# =========================
# Helpers
# =========================

def _norm(x: Any) -> str:
    if x is None:
        return ""
    return re.sub(r"\s+", " ", str(x)).strip()


def _flatten_table(table: list[list[Any]]) -> list[list[str]]:
    return [[_norm(c) for c in row] for row in table or []]


def _table_text(rows: list[list[str]]) -> str:
    lines = []
    for row in rows:
        if any(row):
            lines.append(" | ".join([c for c in row if c]))
    return "\n".join(lines)


def _parse_int_ptbr(s: str) -> Optional[int]:
    s = _norm(s)
    if not s:
        return None
    s = s.replace(".", "").replace(" ", "")
    # ex: 1.125,00
    if re.match(r"^\d+,\d+$", s):
        s = s.split(",")[0]
    if re.match(r"^\d+$", s):
        try:
            return int(s)
        except ValueError:
            return None
    return None


# ── Item number parsing ────────────────────────────────────────────────────────

_RE_HIER_ITEM = re.compile(r"^(\d{1,4})\.(\d{1,4})$")
_RE_FLAT_ITEM = re.compile(r"^\d{1,5}$")


def _parse_item_id(s: str) -> Optional[tuple[int, Optional[int]]]:
    """
    Parse an item number, supporting both flat and hierarchical formats.

    Examples:
        '5'    -> (5, None)   flat item
        '01'   -> (1, None)   flat item (leading zero stripped)
        '1.1'  -> (1, 1)      hierarchical item
        '2.14' -> (2, 14)     hierarchical item

    Returns None if s does not look like an item number.
    """
    s = _norm(s)
    m = _RE_HIER_ITEM.match(s)
    if m:
        return int(m.group(1)), int(m.group(2))
    if _RE_FLAT_ITEM.match(s):
        return int(s), None
    return None


def _is_item_number(s: str) -> bool:
    """True if s is a recognised item number (flat or hierarchical)."""
    return _parse_item_id(s) is not None


def _is_section_title(text: str) -> bool:
    """
    Heuristic: text looks like a section/group header rather than a product description.
    Criteria: more than 65% of alphabetic characters are uppercase.
    """
    if not text:
        return False
    alpha = [c for c in text if c.isalpha()]
    if not alpha:
        return False
    return sum(c.isupper() for c in alpha) / len(alpha) > 0.65


def _looks_like_code(s: str) -> bool:
    s = _norm(s)
    return bool(re.fullmatch(r"[\d\.\-\/]+", s)) and len(s) >= 4


def _detect_header_indices(header_row: list[str]) -> dict[str, int]:
    """
    Identify column indices for item, qty, unit and description within a header row.

    Only 'item' and 'obj' are required for the header to be considered valid;
    'qtd' and 'und' are detected when present but are not mandatory.
    """
    hdr = [c.upper().strip() for c in header_row]
    idx: dict[str, int] = {}

    for i, c in enumerate(hdr):
        if not c:
            continue
        # Item number column
        if c in ("ITEM", "ITEM.", "Nº ITEM", "N° ITEM", "N.°", "N°", "Nº") or c.startswith("ITEM"):
            idx.setdefault("item", i)
        # Quantity column
        if "QTD" in c or "QUANTIDADE" in c or c in ("QT", "QT."):
            idx.setdefault("qtd", i)
        # Unit column
        if any(k in c for k in ["UND", "UNID", "UNIDADE"]) or c in ("UN.", "UN", "UN°"):
            idx.setdefault("und", i)
        # Description / specification column
        if any(k in c for k in ["DESCRI", "ESPECIF", "OBJETO", "SERVIÇO", "MATERIAL", "PRODUTO"]):
            idx.setdefault("obj", i)

    return idx


def _pick_obj(row: list[str], preferred_idx: int) -> str:
    candidates: list[tuple[int, str]] = []
    for j, c in enumerate(row):
        if not c:
            continue
        if _is_item_number(c):
            continue
        if _parse_int_ptbr(c) is not None:
            continue
        if "R$" in c:
            continue
        if re.search(r"[A-Za-zÀ-ú]", c):
            score = len(c)
            if j == preferred_idx:
                score += 20
            if _looks_like_code(c):
                score -= 10
            candidates.append((score, c))

    if candidates:
        return max(candidates, key=lambda x: x[0])[1]

    if preferred_idx < len(row):
        return row[preferred_idx]
    return ""


def _extract_qty_with_header(row: list[str], qtd_idx: int, item_num: int) -> int:
    """
    Extract quantity from a data row given the expected column index.

    Fix over original: the exact-position check now accepts any positive integer,
    including qty == item_num (the original code skipped those, causing qty=0 for
    items like item=1 with qty=1). The nearby fallback search still prefers values
    != item_num to avoid confusing item numbers with quantities.
    """
    # Exact position — trust it unconditionally if it holds a positive integer
    if 0 <= qtd_idx < len(row):
        q = _parse_int_ptbr(row[qtd_idx])
        if q is not None and q > 0:
            return q

    # Nearby fallback: prefer values that differ from item_num
    best_q: Optional[int] = None
    for j in range(max(0, qtd_idx - 4), min(len(row), qtd_idx + 5)):
        if j == qtd_idx:
            continue
        q = _parse_int_ptbr(row[j])
        if q is not None and q > 0:
            if q != item_num:
                return q            # confident it's a quantity
            elif best_q is None:
                best_q = q          # candidate (qty happens to equal item_num)

    return best_q if best_q is not None else 0


def _extract_unit_with_header(row: list[str], und_idx: int) -> str:
    if 0 <= und_idx < len(row):
        cand = _norm(row[und_idx])
        if cand and "R$" not in cand and not re.search(r"\d", cand):
            return cand.upper()

    for j in range(max(0, und_idx - 3), min(len(row), und_idx + 4)):
        cand = _norm(row[j])
        if cand and "R$" not in cand and not re.search(r"\d", cand) and len(cand) <= 20:
            return cand.upper()

    return ""


def _estimate_confidence(item: ItemExtraido, doc_type: str) -> float:
    base = 0.55
    if item.quantidade > 0:
        base += 0.25
    if item.unidade_fornecimento:
        base += 0.15
    if len(item.objeto) >= 20:
        base += 0.05

    # doc_type weight: relacaoitens tends to be best; TR weakest
    w = {"relacaoitens": 1.0, "edital": 0.9, "termo_referencia": 0.8}.get(doc_type, 0.85)
    return round(min(1.0, base * w), 2)


# =========================
# Metadata (heuristic)
# =========================

_PREGAO_PATTERNS = [
    re.compile(r"Preg[aã]o\s+Eletr[oô]nico\s*(n[ºo.]*)?\s*[:\-]?\s*([0-9]{1,4}\s*/\s*[0-9]{2,4}|[0-9]{1,6})", re.I),
    re.compile(r"PREG[AÃ]O\s+ELETR[OÔ]NICO\s*([0-9]{1,6}(?:/[0-9]{2,4})?)", re.I),
]


def _extract_numero_pregao(text: str) -> str:
    for pat in _PREGAO_PATTERNS:
        m = pat.search(text)
        if m:
            for g in reversed(m.groups()):
                if g and re.search(r"\d", g):
                    return _norm(g).replace(" ", "")
            return _norm(m.group(0))
    return ""


def _extract_orgao_cidade_estado(text: str) -> tuple[str, str, str]:
    orgao = ""
    cidade = ""
    estado = ""

    m = re.search(r"PREFEITURA\s+MUNICIPAL\s+DE\s+([A-ZÀ-Ú\s]+)", text, re.I)
    if m:
        city = _norm(m.group(1)).title()
        orgao = f"Prefeitura Municipal de {city}"

    m2 = re.search(r"Munic[ií]pio\s+de\s+([A-ZÀ-Ú\s]+)", text, re.I)
    if not orgao and m2:
        city = _norm(m2.group(1)).title()
        orgao = f"Município de {city}"

    m3 = re.search(r"\b([A-ZÀ-Ú][A-Za-zÀ-ú\s'´`-]{2,})/([A-Z]{2})\b", text)
    if m3:
        cidade = _norm(m3.group(1)).title()
        estado = m3.group(2).upper()

    if not cidade and orgao:
        cidade = orgao.split(" de ", 1)[-1]

    return orgao, cidade, estado


# =========================
# Parse tables → items
# =========================

def _table_looks_like_continuation(rows: list[list[str]]) -> bool:
    """
    Return True if a table appears to be a page-break continuation of a previous
    table: no recognisable header but the first non-empty row starts with an
    item number (flat or hierarchical).
    """
    for row in rows[:5]:
        if any(row):
            for c in row[:8]:
                if _parse_item_id(c) is not None:
                    return True
            return False   # first non-empty row has no item number
    return False


def _extract_items_from_tables(
    tables_json: list[dict[str, Any]],
    *,
    doc_type: str,
    debug: bool,
) -> tuple[list[ItemExtraido], dict[str, str]]:
    itens: list[ItemExtraido] = []
    anexos: set[str] = set()
    current_lote: Optional[str] = None
    last_item: Optional[ItemExtraido] = None

    # Persist the most recently seen valid header map across tables so that
    # page-break continuation tables (which repeat no header) are still parsed.
    last_header_map: Optional[dict[str, int]] = None


    for t in tables_json:
        fonte_arquivo = _norm(t.get("arquivo", ""))
        if fonte_arquivo:
            anexos.add(fonte_arquivo)

        rows = _flatten_table(t.get("dados", []))
        if not rows:
            continue

        # ── Detect lote from first few rows ───────────────────────────────
        joined = " ".join([c for r in rows[:3] for c in r if c])
        m_lote = re.search(r"\bLOTE\s+([0-9]{1,3}|ÚNICO|UNICO)\b", joined.upper())
        if m_lote:
            val = m_lote.group(1)
            current_lote = "LOTE " + ("ÚNICO" if val in ("ÚNICO", "UNICO") else val.zfill(2))

        # ── Find header row (requires at least item + obj columns) ─────────
        # Widened search window (40 rows) handles tables with large title blocks.
        header_idx: Optional[int] = None
        header_map: Optional[dict[str, int]] = None
        for ri, row in enumerate(rows[:40]):
            hm = _detect_header_indices(row)
            if "item" in hm and "obj" in hm:   # relaxed: qtd/und now optional
                header_idx = ri
                header_map = hm
                last_header_map = hm   # save for future headerless continuations
                break

        # ── Fallback: headerless continuation table ────────────────────────
        # pdfplumber splits multi-page tables at every page boundary.  Only the
        # first page carries the header row; subsequent pages contain data rows
        # only.  If we have a saved header map and this table looks like a
        # continuation (first non-empty row begins with an item number), reuse
        # the saved header so we don't silently drop those rows.
        if header_idx is None and last_header_map is not None:
            if _table_looks_like_continuation(rows):
                header_idx = -1          # sentinel: start processing from row 0
                header_map = last_header_map

        if header_idx is None or header_map is None:
            continue

        # Start of data: row after the header (or row 0 for continuations).
        start_row = 0 if header_idx == -1 else header_idx + 1

        # ── Walk data rows ────────────────────────────────────────────────
        for row in rows[start_row:]:

            # ── Look for an item number in the first 8 columns ────────────
            item_id: Optional[tuple[int, Optional[int]]] = None
            for c in row[:8]:
                parsed = _parse_item_id(c)
                if parsed is not None:
                    item_id = parsed
                    break

            if item_id is not None:
                major, minor = item_id

                # Build canonical item string.
                # Flat: "01" -> "1"  Hierarchical: "1.1", "2.14" kept as-is
                if minor is not None:
                    item_str = f"{major}.{minor}"
                else:
                    qtd_idx = header_map.get("qtd", -1)
                    und_idx = header_map.get("und", -1)
                    obj_idx = header_map.get("obj", 0)
                    qty_cell = row[qtd_idx] if 0 <= qtd_idx < len(row) else ""
                    und_cell = row[und_idx] if 0 <= und_idx < len(row) else ""
                    obj_text = _pick_obj(row, obj_idx)
                    if not qty_cell and not und_cell and _is_section_title(obj_text):
                        last_item = None
                        continue
                    item_str = str(major)

                obj = _pick_obj(row, header_map.get("obj", 0)).strip()
                qtd = _extract_qty_with_header(row, header_map.get("qtd", -1), major)
                und = _extract_unit_with_header(row, header_map.get("und", -1)).strip()

                it = ItemExtraido(
                    item=item_str,
                    objeto=obj,
                    quantidade=qtd,
                    unidade_fornecimento=und,
                    lote=current_lote,
                    fonte=(fonte_arquivo or None) if debug else None,
                )
                it.confianca = _estimate_confidence(it, doc_type)
                itens.append(it)
                last_item = it

            else:
                # ── Continuation row (no item number found) ───────────────
                # Skip completely empty rows
                if not any(c for c in row if c):
                    continue

                if last_item:
                    txt = _pick_obj(row, header_map.get("obj", 0)).strip()
                    if txt and len(txt) > 2:
                        if _is_section_title(txt):
                            # Section divider — end the current continuation chain
                            last_item = None
                        else:
                            last_item.objeto = (last_item.objeto + " " + txt).strip()

    # ── Deduplicate by (lote, item), keeping best entry ──────────────────
    best: dict[tuple, ItemExtraido] = {}
    for it in itens:
        key = (it.lote, it.item)
        cur = best.get(key)
        if cur is None:
            best[key] = it
        else:
            def _score(x: ItemExtraido) -> tuple:
                return (x.confianca, 1 if x.quantidade else 0, len(x.objeto))
            if _score(it) > _score(cur):
                best[key] = it

    final_itens = [v for v in best.values() if v.objeto or v.quantidade]
    final_itens.sort(key=lambda x: (x.lote or "", x.item_sort_key()))

    # ── Extract metadata heuristically from table text ────────────────────
    all_text = "\n".join(
        [_table_text(_flatten_table(t.get("dados", []))) for t in tables_json]
    )
    numero = _extract_numero_pregao(all_text)
    orgao, cidade, estado = _extract_orgao_cidade_estado(all_text)

    meta = {
        "numero_pregao": numero,
        "orgao": orgao,
        "cidade": cidade,
        "estado": estado,
    }
    return final_itens, meta


@dataclass
class ParsedAttachment:
    doc_type: str
    items: list[ItemExtraido]
    meta: dict[str, str]
    tables_json_path: Optional[Path] = None
    parsed_json_path: Optional[Path] = None


def parse_attachment(
    file_path: Path,
    doc_type: str,
    *,
    project_root: Path,
    tables_out_dir: Path,
    parsed_out_dir: Path,
    debug: bool = False,
) -> ParsedAttachment:
    """
    Full pipeline for one attachment:
      1) Extract tables (PDF/DOCX) to tables JSON (saved under tables_out_dir)
      2) Parse tables to items (+meta)
      3) Save parsed ResultadoLicitacao JSON under parsed_out_dir (optional but useful)
    """
    tables = process_file(file_path, project_root=project_root)

    tables_out_dir.mkdir(parents=True, exist_ok=True)
    parsed_out_dir.mkdir(parents=True, exist_ok=True)

    tables_json_path = tables_out_dir / f"{file_path.stem}_tables.json"
    write_tables_json(tables, tables_json_path)

    items, meta = _extract_items_from_tables(tables, doc_type=doc_type, debug=debug)

    parsed_json_path = parsed_out_dir / f"{file_path.stem}_resultado.json"
    res = ResultadoLicitacao(
        arquivo_json=tables_json_path.name,
        numero_pregao=meta.get("numero_pregao", ""),
        orgao=meta.get("orgao", ""),
        cidade=meta.get("cidade", ""),
        estado=meta.get("estado", ""),
        anexos_processados=[_safe_relpath(file_path, project_root)],
        itens_extraidos=items,
    )
    parsed_json_path.write_text(res.to_json(debug=debug), encoding="utf-8")

    return ParsedAttachment(
        doc_type=doc_type,
        items=items,
        meta=meta,
        tables_json_path=tables_json_path,
        parsed_json_path=parsed_json_path,
    )


# =========================
# Batch CLI: parse existing *_tables.json
# =========================

def process_tables_json_file(
    path: Path, *, doc_type: str = "edital", debug: bool = False
) -> ResultadoLicitacao:
    tables = json.loads(path.read_text(encoding="utf-8"))
    items, meta = _extract_items_from_tables(tables, doc_type=doc_type, debug=debug)
    return ResultadoLicitacao(
        arquivo_json=path.name,
        numero_pregao=meta.get("numero_pregao", ""),
        orgao=meta.get("orgao", ""),
        cidade=meta.get("cidade", ""),
        estado=meta.get("estado", ""),
        anexos_processados=list(
            {t.get("arquivo", "") for t in tables if t.get("arquivo")}
        ),
        itens_extraidos=items,
    )


def cli_main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", type=Path, required=True, help="Pasta com *_tables.json")
    ap.add_argument("--output-dir", type=Path, required=True, help="Pasta de saída")
    ap.add_argument("--pattern", type=str, default="*_tables.json")
    ap.add_argument(
        "--doc-type", type=str, default="edital",
        help="relacaoitens|edital|termo_referencia",
    )
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    for p in sorted(args.input_dir.glob(args.pattern)):
        res = process_tables_json_file(p, doc_type=args.doc_type, debug=args.debug)
        results.append(res.to_dict(debug=args.debug))
        out_file = args.output_dir / (p.stem.replace("_tables", "") + "_resultado.json")
        out_file.write_text(res.to_json(debug=args.debug), encoding="utf-8")

    consolidated = args.output_dir / "resultado_licitacoes_consolidado.json"
    consolidated.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    cli_main()
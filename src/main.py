"""
Main orchestrator for licitação item extraction.

Usage:
    python src/main.py [downloads_dir] [outputs_dir] [--debug]

downloads_dir: folder containing licitação .json files and their attachments
outputs_dir:   folder where outputs are written

Pipeline per licitação JSON:
    1) Parse `data.itens` field (primary source).
    2) Find attachments:
        - Prefer file paths listed in JSON (data.anexos, if present)
        - Also scan the conventional attachment folder: <json_path.parent>/<json_stem>/
          (this is crucial when filenames are generic like "anexo_1.pdf")
        - If still missing, fall back to a global filename index built from downloads_dir
    3) For each found PDF/DOCX attachment:
        - Extract tables (outputs/tabelas/<json_stem>/..._tables.json)
        - Parse tables into items (outputs/pdf_parsed/<json_stem>/..._resultado.json)
    4) Aggregate JSON items + attachment items (merge/deduplicate).
    5) Write:
        - outputs/<json_filename> (ResultadoLicitacao)
        - outputs/resultado.json (combined list)

Why you previously saw "no PDFs extracted":
    - The earlier version only processed attachments whose NAMES contained certain keywords
      (edital/relacaoitens/termo...) and relied strictly on JSON attachment metadata.
      Many datasets use generic names (e.g., anexo_1.pdf), so nothing matched and nothing ran.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from models import ItemExtraido, ResultadoLicitacao
from parsers.json_itens import parse_itens_field
from parsers.pdf_parser import parse_attachment, ParsedAttachment
from aggregator import aggregate_items, merge_metadata

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("orchestrator")


def _safe_relpath(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path)


def _project_root_from_downloads(downloads_dir: Path) -> Path:
    # If downloads_dir is ./downloads inside project root, project_root is its parent.
    return downloads_dir.resolve().parent


def detect_doc_type(filename: str) -> str:
    """
    Map an attachment filename to its document type.
    Used only for source labeling/priorities (merge still works with unknown types).
    """
    name = filename.lower()
    if "relacaoitens" in name or "relaçãoitens" in name:
        return "relacaoitens"
    if "termo" in name and ("refer" in name or "referên" in name):
        return "termo_referencia"
    if "edital" in name:
        return "edital"
    return "anexo"


def build_attachment_index(downloads_dir: Path) -> dict[str, list[Path]]:
    """Index all PDFs/DOCXs under downloads_dir by lowercase filename."""
    index: dict[str, list[Path]] = {}
    for p in downloads_dir.rglob("*"):
        if p.is_file() and p.suffix.lower() in [".pdf", ".docx"]:
            index.setdefault(p.name.lower(), []).append(p)
    return index


def _extract_attachment_refs(data: dict[str, Any]) -> list[str]:
    """
    Extract attachment references from known JSON shapes.

    We accept:
      - data['anexos'] as list[dict] with keys like 'nome', 'arquivo', 'filename', 'path', 'caminho'
      - data['anexos'] as list[str]
    """
    anexos = data.get("anexos", [])
    refs: list[str] = []
    if isinstance(anexos, list):
        for a in anexos:
            if isinstance(a, str):
                if a.strip():
                    refs.append(a.strip())
                continue
            if isinstance(a, dict):
                cand = (
                    a.get("nome")
                    or a.get("arquivo")
                    or a.get("filename")
                    or a.get("file_name")
                    or a.get("path")
                    or a.get("caminho")
                    or a.get("local_path")
                    or ""
                )
                cand = str(cand).strip()
                if cand:
                    refs.append(cand)
    return refs


def _resolve_attachment_path(
    ref: str,
    *,
    json_path: Path,
    attachment_dir: Path,
    downloads_dir: Path,
    project_root: Path,
    attachment_index: dict[str, list[Path]],
) -> Path | None:
    """Resolve an attachment ref to an existing local file."""
    ref = ref.strip()
    if not ref:
        return None

    # If ref looks like a path, try it relative to common roots
    if "/" in ref or "\\" in ref:
        ref_path = Path(ref)
        candidates = []
        if ref_path.is_absolute():
            candidates.append(ref_path)
        else:
            candidates.extend([
                project_root / ref_path,
                downloads_dir / ref_path,
                json_path.parent / ref_path,
                attachment_dir / ref_path,
            ])
        for c in candidates:
            if c.exists() and c.is_file():
                return c

    # Otherwise, treat it as a filename
    candidates = [
        attachment_dir / ref,
        json_path.parent / ref,
        downloads_dir / ref,
    ]
    for c in candidates:
        if c.exists() and c.is_file():
            return c

    # Global index fallback
    hits = attachment_index.get(ref.lower())
    if hits:
        return hits[0]

    return None


def discover_attachments_for_json(
    json_path: Path,
    data: dict[str, Any],
    *,
    downloads_dir: Path,
    project_root: Path,
    attachment_index: dict[str, list[Path]],
) -> list[Path]:
    """
    Find local attachments for a licitação JSON.

    Priority:
      1) resolve refs from JSON metadata (data.anexos)
      2) scan conventional attachment_dir = <json_parent>/<json_stem> for PDF/DOCX
      3) (implicit) filename index fallback in _resolve_attachment_path
    """
    attachment_dir = json_path.parent / json_path.stem
    found: list[Path] = []

    # 1) JSON refs
    refs = _extract_attachment_refs(data)
    for ref in refs:
        p = _resolve_attachment_path(
            ref,
            json_path=json_path,
            attachment_dir=attachment_dir,
            downloads_dir=downloads_dir,
            project_root=project_root,
            attachment_index=attachment_index,
        )
        if p is not None:
            found.append(p)

    # 2) scan attachment folder (covers generic names like anexo_1.pdf)
    if attachment_dir.is_dir():
        for p in attachment_dir.rglob("*"):
            if p.is_file() and p.suffix.lower() in [".pdf", ".docx"]:
                found.append(p)

    # Deduplicate while preserving order
    seen = set()
    uniq: list[Path] = []
    for p in found:
        key = str(p.resolve())
        if key not in seen:
            seen.add(key)
            uniq.append(p)

    return uniq


def process_json(
    json_path: Path,
    *,
    downloads_dir: Path,
    outputs_dir: Path,
    debug: bool,
    attachment_index: dict[str, list[Path]],
) -> ResultadoLicitacao:
    logger.info("Processing: %s", json_path.name)

    raw = json.loads(json_path.read_text(encoding="utf-8"))
    data = raw.get("data", {}) if isinstance(raw, dict) else {}

    # Intermediate folders per licitação
    tables_dir = outputs_dir / "tabelas" / json_path.stem
    parsed_dir = outputs_dir / "pdf_parsed" / json_path.stem

    # Metadata from JSON (primary)
    primary_meta = {
        "numero_pregao": (data.get("numero_pregao", "") or "").strip(),
        "orgao": (data.get("orgao", "") or "").strip(),
        "cidade": (data.get("cidade", "") or "").strip(),
        "estado": (data.get("estado", "") or "").strip(),
    }

    result = ResultadoLicitacao(
        arquivo_json=json_path.name,
        numero_pregao=primary_meta["numero_pregao"],
        orgao=primary_meta["orgao"],
        cidade=primary_meta["cidade"],
        estado=primary_meta["estado"],
    )

    # Step 1: parse JSON itens field
    raw_itens = data.get("itens", [])
    json_items = parse_itens_field(raw_itens)
    logger.info("  JSON itens → %d items", len(json_items))

    project_root = _project_root_from_downloads(downloads_dir)

    # Step 2/3: discover + process attachments
    attachments = discover_attachments_for_json(
        json_path,
        data,
        downloads_dir=downloads_dir,
        project_root=project_root,
        attachment_index=attachment_index,
    )

    if not attachments:
        logger.info("  Attachments: none found locally (no PDF/DOCX to extract)")
    else:
        logger.info("  Attachments: %d file(s) found", len(attachments))

    parsed_attachments: list[ParsedAttachment] = []
    other_sources: dict[str, list[ItemExtraido]] = {}

    for file_path in attachments:
        doc_type = detect_doc_type(file_path.name)

        logger.info("  Extract+Parse %s (%s)", file_path.name, doc_type)
        try:
            parsed = parse_attachment(
                file_path,
                doc_type,
                project_root=project_root,
                tables_out_dir=tables_dir,
                parsed_out_dir=parsed_dir,
                debug=debug,
            )
        except Exception as exc:
            logger.error("    FAILED attachment %s: %s", file_path.name, exc, exc_info=True)
            continue

        parsed_attachments.append(parsed)
        logger.info("    → %d items", len(parsed.items))

        if parsed.items:
            other_sources.setdefault(doc_type, []).extend(parsed.items)
            result.anexos_processados.append(_safe_relpath(file_path, project_root))

    # Step 4: merge metadata (fill blanks from attachments)
    fallback_metas = [p.meta for p in parsed_attachments if p.meta]
    merged_meta = merge_metadata(primary=primary_meta, fallbacks=fallback_metas)
    result.numero_pregao = merged_meta["numero_pregao"]
    result.orgao = merged_meta["orgao"]
    result.cidade = merged_meta["cidade"]
    result.estado = merged_meta["estado"]

    # Step 5: aggregate items from both sources
    merged_items = aggregate_items(
        json_items=json_items,
        other_sources=other_sources,
        debug=debug,
        json_source_label=_safe_relpath(json_path, project_root),
    )
    result.itens_extraidos = merged_items
    logger.info("  Final: %d merged items", len(merged_items))

    # Step 6: write per-JSON output
    outputs_dir.mkdir(parents=True, exist_ok=True)
    out_path = outputs_dir / json_path.name
    out_path.write_text(result.to_json(debug=debug), encoding="utf-8")
    logger.info("  Written → %s", out_path)

    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("downloads_dir", nargs="?", default="downloads")
    ap.add_argument("outputs_dir", nargs="?", default="outputs")
    ap.add_argument("--debug", action="store_true", help="Include 'fonte' in each extracted item")
    args = ap.parse_args()

    dl = Path(args.downloads_dir).resolve()
    out = Path(args.outputs_dir).resolve()

    if not dl.exists():
        logger.error("Downloads directory not found: %s", dl)
        sys.exit(1)

    out.mkdir(parents=True, exist_ok=True)

    # Discover licitação JSON files (recursive) and ignore common non-licitacao files
    json_files: list[Path] = []
    for p in dl.rglob("*.json"):
        if not p.is_file():
            continue
        name = p.name.lower()
        if name in ("archive.json", "arquive.json"):
            continue
        if name.endswith("_tables.json") or name.endswith("_resultado.json"):
            continue
        # Some datasets store lists/archives — keep rule conservative: require 'data' object
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(raw, dict) or "data" not in raw:
                continue
        except Exception:
            continue
        json_files.append(p)

    json_files = sorted(json_files)

    if not json_files:
        logger.error("No licitação JSON files found under %s", dl)
        sys.exit(1)

    logger.info("Found %d licitação JSON file(s) under %s", len(json_files), dl)

    # Build a global index of attachments (fixes cases where files are stored in unexpected subfolders)
    logger.info("Indexing attachments under %s ...", dl)
    attachment_index = build_attachment_index(dl)
    logger.info("Indexed %d unique attachment names", len(attachment_index))

    results: list[dict] = []
    for jf in json_files:
        try:
            res = process_json(
                jf,
                downloads_dir=dl,
                outputs_dir=out,
                debug=args.debug,
                attachment_index=attachment_index,
            )
            results.append(res.to_dict(debug=args.debug))
        except Exception as exc:
            logger.error("FAILED %s: %s", jf.name, exc, exc_info=True)

    combined_path = out / "resultado.json"
    combined_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Combined output → %s (%d licitações)", combined_path, len(results))


if __name__ == "__main__":
    main()

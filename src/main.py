"""
Main orchestrator for licitação item extraction.

Output layout
─────────────
outputs/
  pre_resultado_final.json                ← combined list (all licitações, pre-sanitize)
  resultado_parcial/<json_filename>       ← one file per licitação JSON
  tabelas/<json_stem>/..._tables.json
  pdf_parsed/<json_stem>/..._resultado.json
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
from sanitize import filter_payload

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("orchestrator")


def _safe_relpath(path: Path, root: Path) -> str:
    """Return ``path`` relative to ``root``, falling back to ``str(path)``."""
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path)


def _project_root_from_downloads(downloads_dir: Path) -> Path:
    """Derive the project root from the downloads directory.

    Convention: the project root is one level above the downloads directory,
    which keeps relative paths short and consistent in logs and JSON outputs.
    """
    return downloads_dir.resolve().parent


def detect_doc_type(filename: str) -> str:
    """Infer the document type from a filename using keyword heuristics.

    The returned string is used as the ``doc_type`` argument for the PDF
    parser and as the source key in the merger's priority table.

    Recognised types (in order of precedence):
      ``relacaoitens``   — "Relação de Itens" report files
      ``termo_referencia`` — Term of Reference documents
      ``edital``         — Edital (official procurement notice)
      ``anexo``          — fallback for any other attachment

    Args:
        filename: Bare filename (not full path) to inspect.

    Returns:
        One of ``"relacaoitens"``, ``"termo_referencia"``, ``"edital"``,
        or ``"anexo"``.
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
    """Index all PDF and DOCX files under ``downloads_dir`` by lowercase filename.

    The index allows O(1) lookups when resolving attachment references from
    JSON ``data.anexos`` entries that contain only a bare filename.

    Args:
        downloads_dir: Root directory to walk recursively.

    Returns:
        Dict mapping ``filename.lower()`` → list of matching absolute Paths.
        Multiple hits occur when the same filename exists in different subdirs.
    """
    index: dict[str, list[Path]] = {}
    for p in downloads_dir.rglob("*"):
        if p.is_file() and p.suffix.lower() in [".pdf", ".docx"]:
            index.setdefault(p.name.lower(), []).append(p)
    return index


def _extract_attachment_refs(data: dict[str, Any]) -> list[str]:
    """Extract raw attachment references from the ``data.anexos`` field.

    The field may contain:
      - A list of plain strings (bare filenames or relative paths).
      - A list of dicts with any of the common path keys:
        ``nome``, ``arquivo``, ``filename``, ``file_name``, ``path``,
        ``caminho``, ``local_path``.

    Args:
        data: The ``data`` object from a ConLicitação JSON document.

    Returns:
        List of non-empty reference strings, preserving order.
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
                # Try common key names in priority order.
                cand = (
                    a.get("nome") or a.get("arquivo") or a.get("filename")
                    or a.get("file_name") or a.get("path") or a.get("caminho")
                    or a.get("local_path") or ""
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
    """Resolve a raw attachment reference string to an existing file path.

    Resolution strategy (tried in order):
      1. If ``ref`` contains path separators, try it as an absolute path
         then as a relative path under several candidate directories.
      2. Try the ref as a bare filename under the attachment directory,
         the JSON's parent directory, and the downloads root.
      3. Fall back to the filename index for an unambiguous filename lookup.

    Args:
        ref:              Raw reference string from ``data.anexos``.
        json_path:        Path of the JSON file being processed.
        attachment_dir:   Conventional attachment subdirectory
                          (``<json_stem>/`` beside the JSON file).
        downloads_dir:    Root downloads directory.
        project_root:     Project root for resolving relative paths.
        attachment_index: Prebuilt filename → path index.

    Returns:
        Resolved ``Path`` if the file exists, or ``None``.
    """
    ref = ref.strip()
    if not ref:
        return None

    # Case 1: ref looks like a path (contains slashes).
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

    # Case 2: try ref as a bare filename in common locations.
    candidates = [
        attachment_dir / ref,
        json_path.parent / ref,
        downloads_dir / ref,
    ]
    for c in candidates:
        if c.exists() and c.is_file():
            return c

    # Case 3: index lookup by lowercase filename.
    hits = attachment_index.get(ref.lower())
    if hits:
        return hits[0]  # return the first hit when multiple exist
    return None


def discover_attachments_for_json(
    json_path: Path,
    data: dict[str, Any],
    *,
    downloads_dir: Path,
    project_root: Path,
    attachment_index: dict[str, list[Path]],
) -> list[Path]:
    """Find all PDF/DOCX attachments associated with a licitação JSON document.

    Two discovery mechanisms are combined:
      1. **Explicit refs**: entries in ``data.anexos`` resolved via
         ``_resolve_attachment_path``.
      2. **Directory scan**: any PDF/DOCX found recursively under the
         conventional attachment subdirectory ``<json_stem>/`` beside the JSON.

    Duplicates (same resolved absolute path) are removed while preserving order.

    Args:
        json_path:         Path to the licitação JSON file.
        data:              The ``data`` object from the JSON.
        downloads_dir:     Root downloads directory.
        project_root:      Project root for path resolution.
        attachment_index:  Prebuilt filename index.

    Returns:
        Deduplicated list of existing attachment Paths.
    """
    # The conventional location for a JSON's attachments is a same-named subdir.
    attachment_dir = json_path.parent / json_path.stem
    found: list[Path] = []

    # Explicit references from the JSON metadata.
    for ref in _extract_attachment_refs(data):
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

    # Directory scan — picks up files not listed in anexos.
    if attachment_dir.is_dir():
        for p in attachment_dir.rglob("*"):
            if p.is_file() and p.suffix.lower() in [".pdf", ".docx"]:
                found.append(p)

    # Deduplicate by resolved absolute path.
    seen: set[str] = set()
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
    """Extract, parse, merge and write results for a single licitação JSON.

    Pipeline for one document:
      1. Read the JSON and extract primary metadata.
      2. Parse ``data.itens`` via ``parse_itens_field``.
      3. Discover and parse all associated attachments.
      4. Merge metadata from JSON and attachments (primary preferred).
      5. Merge item lists from all sources via ``aggregate_items``.
      6. Write the per-licitação result under ``resultado_parcial/``.

    Args:
        json_path:         Path to the licitação JSON file.
        downloads_dir:     Root downloads directory.
        outputs_dir:       Root outputs directory.
        debug:             When True, ``fonte`` fields are written to output.
        attachment_index:  Prebuilt filename index for fast attachment lookup.

    Returns:
        A fully populated ``ResultadoLicitacao`` instance.
    """
    logger.info("Processing: %s", json_path.name)

    raw = json.loads(json_path.read_text(encoding="utf-8"))
    data = raw.get("data", {}) if isinstance(raw, dict) else {}

    # Output sub-directories scoped to this specific JSON file.
    tables_dir  = outputs_dir / "tabelas"    / json_path.stem
    parsed_dir  = outputs_dir / "pdf_parsed" / json_path.stem
    parcial_dir = outputs_dir / "resultado_parcial"

    primary_meta = {
        "numero_pregao": (data.get("numero_pregao", "") or "").strip(),
        "orgao":         (data.get("orgao",         "") or "").strip(),
        "cidade":        (data.get("cidade",        "") or "").strip(),
        "estado":        (data.get("estado",        "") or "").strip(),
    }

    # Initialise the result object with metadata from the JSON.
    result = ResultadoLicitacao(
        arquivo_json=json_path.name,
        numero_pregao=primary_meta["numero_pregao"],
        orgao=primary_meta["orgao"],
        cidade=primary_meta["cidade"],
        estado=primary_meta["estado"],
    )

    # Step 1: parse items from the JSON itens field.
    raw_itens  = data.get("itens", [])
    json_items = parse_itens_field(raw_itens)
    logger.info("  JSON itens → %d items", len(json_items))

    project_root = _project_root_from_downloads(downloads_dir)

    # Step 2: discover attachments.
    attachments = discover_attachments_for_json(
        json_path, data,
        downloads_dir=downloads_dir,
        project_root=project_root,
        attachment_index=attachment_index,
    )

    if not attachments:
        logger.info("  Attachments: none found locally")
    else:
        logger.info("  Attachments: %d file(s) found", len(attachments))

    parsed_attachments: list[ParsedAttachment] = []
    # Keyed by doc_type so the merger can apply source-priority correctly.
    other_sources: dict[str, list[ItemExtraido]] = {}

    # Step 3: extract and parse each attachment.
    for file_path in attachments:
        doc_type = detect_doc_type(file_path.name)
        logger.info("  Extract+Parse %s (%s)", file_path.name, doc_type)
        try:
            parsed = parse_attachment(
                file_path, doc_type,
                project_root=project_root,
                tables_out_dir=tables_dir,
                parsed_out_dir=parsed_dir,
                debug=debug,
            )
        except Exception as exc:
            logger.error("    FAILED %s: %s", file_path.name, exc, exc_info=True)
            continue

        parsed_attachments.append(parsed)
        logger.info("    → %d items", len(parsed.items))

        result.anexos_processados.append(_safe_relpath(file_path, project_root))

        if parsed.items:
            other_sources.setdefault(doc_type, []).extend(parsed.items)

    # Step 4: merge metadata — JSON primary, PDF fallbacks fill blanks.
    fallback_metas = [p.meta for p in parsed_attachments if p.meta]
    merged_meta = merge_metadata(primary=primary_meta, fallbacks=fallback_metas)
    result.numero_pregao = merged_meta["numero_pregao"]
    result.orgao         = merged_meta["orgao"]
    result.cidade        = merged_meta["cidade"]
    result.estado        = merged_meta["estado"]

    # Step 5: merge item lists from all sources.
    merged_items = aggregate_items(
        json_items=json_items,
        other_sources=other_sources,
        debug=debug,
        json_source_label=_safe_relpath(json_path, project_root),
    )
    result.itens_extraidos = merged_items
    logger.info("  Final: %d merged items", len(merged_items))

    # Step 6: write per-licitação partial result.
    parcial_dir.mkdir(parents=True, exist_ok=True)
    out_path = parcial_dir / json_path.name
    out_path.write_text(result.to_json(debug=debug), encoding="utf-8")
    logger.info("  Written → %s", out_path)

    return result


def main() -> None:
    """Entry point: process all licitação JSON files under the downloads directory.

    Workflow:
      1. Discover all valid licitação JSON files under ``downloads_dir``.
      2. Build a global attachment index for fast filename lookups.
      3. Process each JSON through ``process_json``.
      4. Write the combined pre-sanitize output (``pre_resultado_final.json``).
      5. Apply ``filter_payload`` to remove incomplete items and write
         the final ``resultado.json``.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("downloads_dir", nargs="?", default="downloads")
    ap.add_argument("outputs_dir",   nargs="?", default="outputs")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    dl  = Path(args.downloads_dir).resolve()
    out = Path(args.outputs_dir).resolve()

    if not dl.exists():
        logger.error("Downloads directory not found: %s", dl)
        sys.exit(1)

    out.mkdir(parents=True, exist_ok=True)

    # Discover licitação JSON files, skipping known non-data JSON files
    # (archive, table dumps, result files written by previous runs).
    json_files: list[Path] = []
    for p in dl.rglob("*.json"):
        if not p.is_file():
            continue
        name = p.name.lower()
        if name in ("archive.json", "arquive.json"):
            continue
        if name.endswith("_tables.json") or name.endswith("_resultado.json"):
            continue
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(raw, dict) or "data" not in raw:
                continue  # not a licitação JSON — skip silently
        except Exception:
            continue
        json_files.append(p)

    json_files = sorted(json_files)

    if not json_files:
        logger.error("No licitação JSON files found under %s", dl)
        sys.exit(1)

    logger.info("Found %d licitação JSON file(s) under %s", len(json_files), dl)

    # Build the global attachment index once — reused for every JSON file.
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

    # Write the combined pre-sanitize output for inspection/debugging.
    combined_path = out / "pre_resultado_final.json"
    combined_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Combined output → %s (%d licitações)", combined_path, len(results))

    # Apply sanitization: remove items with any empty required field.
    sanitized, before, after = filter_payload(results)
    resultado_path = out / "resultado.json"
    resultado_path.write_text(json.dumps(sanitized, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Sanitized output → %s (itens: %d → %d, removed: %d)", resultado_path, before, after, before - after)


if __name__ == "__main__":
    main()
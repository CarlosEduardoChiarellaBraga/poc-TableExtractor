"""
extractor.py

Extract tables from PDF (and DOCX via DOCX->PDF conversion) using pdfplumber.

Each extracted table entry has schema:
{
  "arquivo": "<path relative to project root if possible>",
  "pagina": <1-indexed>,
  "indice_tabela": <0-indexed table on page>,
  "dados": <list[list[str|None]]>
}

Important fix:
- For DOCX, tables are extracted from a temporary PDF outside the project tree.
  We still store "arquivo" as the ORIGINAL DOCX path (relative to project_root when possible),
  so downstream debug/auditing stays consistent and we never crash on Path.relative_to().
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from typing import Optional

import pdfplumber


def _safe_relpath(path: Path, root: Path) -> str:
    """Return ``path`` relative to ``root`` when possible, else return ``str(path)``.

    ``Path.relative_to`` raises ``ValueError`` when ``path`` is not under
    ``root`` (e.g. temp files in ``/tmp``). This wrapper absorbs that error
    so callers never have to guard against it.
    """
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path)


def extract_tables_from_pdf(
    pdf_path: Path,
    *,
    project_root: Path,
    arquivo_label: Optional[str] = None,
) -> list[dict]:
    """Extract all tables from a PDF file using pdfplumber.

    Iterates over every page and every table on each page, collecting each
    table as a list of rows (each row is a list of cell strings or None).

    Args:
        pdf_path:      Absolute or relative path to the PDF file to parse.
        project_root:  Used to compute the relative ``arquivo`` label stored
                       on each table dict; does not affect extraction logic.
        arquivo_label: Optional override for the ``arquivo`` field. When
                       provided (e.g. when called for a converted DOCX), the
                       label points to the original source file rather than
                       the temporary PDF so that debug output is meaningful.

    Returns:
        List of table dicts, each with keys
        ``arquivo``, ``pagina``, ``indice_tabela``, ``dados``.
        Returns an empty list if the PDF contains no tables.

    Raises:
        RuntimeError: If pdfplumber fails to open or process the file.
    """
    tables_data: list[dict] = []
    # Use the provided label or fall back to a path relative to project_root.
    arquivo_value = arquivo_label or _safe_relpath(pdf_path, project_root)

    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page_number, page in enumerate(pdf.pages, start=1):
                tables = page.extract_tables()
                for table_index, table in enumerate(tables):
                    if not table:
                        continue
                    tables_data.append(
                        {
                            "arquivo": arquivo_value,
                            "pagina": page_number,        # 1-indexed for human readability
                            "indice_tabela": table_index, # 0-indexed within the page
                            "dados": table,
                        }
                    )
    except Exception as e:
        raise RuntimeError(f"Erro ao processar {pdf_path}: {e}") from e

    return tables_data


def convert_docx_to_pdf(docx_path: Path) -> Optional[Path]:
    """Convert a DOCX file to a temporary PDF using docx2pdf.

    The converted PDF is written to a temporary directory. The caller is
    responsible for cleaning up the temp directory after use.

    Requirements:
      - ``docx2pdf`` must be installed (``pip install docx2pdf``).
      - Microsoft Word must be available (macOS / Windows only).

    Returns:
        Path to the generated PDF, or ``None`` if docx2pdf is not installed
        or the conversion fails for any reason.
    """
    try:
        from docx2pdf import convert  # type: ignore
    except Exception:
        # docx2pdf not installed — caller will skip DOCX conversion silently.
        return None

    temp_dir = tempfile.mkdtemp()
    temp_pdf_path = Path(temp_dir) / (docx_path.stem + ".pdf")
    try:
        convert(str(docx_path), str(temp_pdf_path))
        return temp_pdf_path
    except Exception:
        # Conversion failed (Word not available, file locked, etc.)
        return None


def process_file(file_path: Path, *, project_root: Path) -> list[dict]:
    """Extract all tables from a single PDF or DOCX file.

    Routing:
      - ``.pdf``  → extracted directly via pdfplumber.
      - ``.docx`` → converted to a temp PDF first, then extracted.
                    The ``arquivo`` field is always set to the original DOCX
                    path so that downstream audit trails remain consistent.

    Args:
        file_path:    Path to the file to process (.pdf or .docx).
        project_root: Used to compute relative paths in the output dicts.

    Returns:
        List of table dicts (same schema as ``extract_tables_from_pdf``).
        Returns [] for unsupported formats or failed DOCX conversions.
    """
    suffix = file_path.suffix.lower()

    if suffix == ".pdf":
        return extract_tables_from_pdf(file_path, project_root=project_root)

    if suffix == ".docx":
        temp_pdf = convert_docx_to_pdf(file_path)
        if temp_pdf and temp_pdf.exists():
            try:
                # Store the original DOCX path as the label — NOT the temp PDF path —
                # so that "arquivo" in the output refers to the real source file.
                docx_label = _safe_relpath(file_path, project_root)
                return extract_tables_from_pdf(
                    temp_pdf,
                    project_root=project_root,
                    arquivo_label=docx_label,
                )
            finally:
                # Always clean up the temp directory, even if extraction raised.
                shutil.rmtree(temp_pdf.parent, ignore_errors=True)
        return []

    # Unsupported file type — return empty rather than raising so the pipeline
    # continues processing the remaining attachments.
    return []


def write_tables_json(tables: list[dict], out_path: Path) -> None:
    """Serialize extracted tables to a JSON file.

    Creates parent directories as needed. The JSON uses UTF-8 encoding with
    ``ensure_ascii=False`` to preserve accented characters in cell values.

    Args:
        tables:   List of table dicts as produced by ``extract_tables_from_pdf``.
        out_path: Destination file path (will be created or overwritten).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(tables, ensure_ascii=False, indent=2), encoding="utf-8")


def cli_main() -> None:
    """CLI entry point: extract tables from all PDFs/DOCXs under a base directory.

    Walks the base directory recursively, processes each .pdf and .docx file,
    writes per-file ``*_tables.json`` outputs, and produces a single
    consolidated JSON file with every table from every file.

    Example:
        python -m extractor --base-dir ./downloads --out-dir ./outputs/tabelas
    """
    import argparse
    from tqdm import tqdm

    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir", type=Path, default=Path("downloads"))
    ap.add_argument("--out-dir", type=Path, default=Path("outputs") / "tabelas")
    ap.add_argument("--project-root", type=Path, default=None)
    ap.add_argument("--consolidated-name", type=str, default="licitacoes_tables.json")
    args = ap.parse_args()

    base_dir: Path = args.base_dir
    out_dir: Path = args.out_dir
    # Default project root is the parent of the downloads directory.
    project_root = args.project_root or base_dir.resolve().parent

    out_dir.mkdir(parents=True, exist_ok=True)

    consolidated: list[dict] = []
    files = list(base_dir.rglob("*"))
    for fp in tqdm(files):
        if fp.suffix.lower() not in [".pdf", ".docx"]:
            continue
        try:
            tables = process_file(fp, project_root=project_root)
        except Exception:
            tables = []
        if not tables:
            continue
        consolidated.extend(tables)
        out_file = out_dir / f"{fp.stem}_tables.json"
        write_tables_json(tables, out_file)

    consolidated_path = out_dir / args.consolidated_name
    write_tables_json(consolidated, consolidated_path)
    print(f"Total de tabelas extraídas: {len(consolidated)}")
    print(f"Consolidado: {consolidated_path}")


if __name__ == "__main__":
    cli_main()
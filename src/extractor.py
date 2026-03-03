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
    """Return path relative to root when possible; otherwise return str(path)."""
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
    """
    Extract all tables from a PDF using pdfplumber.

    Args:
        pdf_path: path to a PDF file (can be outside project_root)
        project_root: used to compute relative paths (if possible)
        arquivo_label: override for the "arquivo" field (useful for DOCX temp pdf)

    Returns:
        list of extracted table dicts
    """
    tables_data: list[dict] = []
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
                            "pagina": page_number,
                            "indice_tabela": table_index,
                            "dados": table,
                        }
                    )
    except Exception as e:
        raise RuntimeError(f"Erro ao processar {pdf_path}: {e}") from e

    return tables_data


def convert_docx_to_pdf(docx_path: Path) -> Optional[Path]:
    """
    Convert DOCX to a temporary PDF using docx2pdf.

    - If docx2pdf is not installed, returns None.
    - docx2pdf typically requires Microsoft Word installed (macOS/Windows).
    """
    try:
        from docx2pdf import convert  # type: ignore
    except Exception:
        return None

    temp_dir = tempfile.mkdtemp()
    temp_pdf_path = Path(temp_dir) / (docx_path.stem + ".pdf")
    try:
        convert(str(docx_path), str(temp_pdf_path))
        return temp_pdf_path
    except Exception:
        return None


def process_file(file_path: Path, *, project_root: Path) -> list[dict]:
    """
    Extract all tables from a single file (.pdf or .docx).

    For DOCX:
      - Converts to a temporary PDF (outside project)
      - Extracts from that PDF
      - Stores "arquivo" as the original DOCX path (relative to project_root when possible)
    """
    suffix = file_path.suffix.lower()

    if suffix == ".pdf":
        return extract_tables_from_pdf(file_path, project_root=project_root)

    if suffix == ".docx":
        temp_pdf = convert_docx_to_pdf(file_path)
        if temp_pdf and temp_pdf.exists():
            try:
                docx_label = _safe_relpath(file_path, project_root)
                return extract_tables_from_pdf(
                    temp_pdf,
                    project_root=project_root,
                    arquivo_label=docx_label,
                )
            finally:
                shutil.rmtree(temp_pdf.parent, ignore_errors=True)
        return []

    return []


def write_tables_json(tables: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(tables, ensure_ascii=False, indent=2), encoding="utf-8")


def cli_main() -> None:
    """
    CLI: extract tables from all PDFs/DOCXs inside a base dir.

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

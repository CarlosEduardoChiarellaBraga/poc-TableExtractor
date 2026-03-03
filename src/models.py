"""
Data models for structured extraction of licitação items.
Uses dataclasses (no external deps) with JSON serialization support.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional


def _item_str_sort_key(item: str) -> tuple[int, int]:
    """Numeric sort key for item strings, handling both flat and hierarchical formats.

    Examples:  "3" -> (3,0)  "03" -> (3,0)  "1.1" -> (1,1)  "2.14" -> (2,14)
    """
    parts = str(item).split(".", 1)
    try:
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        return (major, minor)
    except (ValueError, IndexError):
        return (0, 0)


@dataclass
class ItemExtraido:
    """A single extracted item from a licitacao.

    ``item`` is a string to support both flat integers ("1", "42") and
    hierarchical dot-notation numbers ("1.1", "2.14") that appear in many
    Brazilian public-procurement documents.
    Use item_sort_key() whenever items need to be sorted numerically.
    """
    item: str
    objeto: str
    quantidade: int
    unidade_fornecimento: str
    lote: Optional[str] = None
    confianca: float = 1.0  # 0.0-1.0, internal use only
    fonte: Optional[str] = None  # included only when debug=True

    def item_sort_key(self) -> tuple[int, int]:
        """Numeric sort key that correctly orders "7" < "2.14" by (major, minor)."""
        return _item_str_sort_key(self.item)

    def to_dict(self, debug: bool = False) -> dict:
        d = {
            "lote": self.lote,
            "item": self.item,
            "objeto": self.objeto,
            "quantidade": self.quantidade,
            "unidade_fornecimento": self.unidade_fornecimento,
        }
        if debug:
            d["fonte"] = self.fonte
        return d


@dataclass
class ResultadoLicitacao:
    """Full extraction result for one licitação JSON file."""
    arquivo_json: str
    numero_pregao: str
    orgao: str
    cidade: str
    estado: str
    anexos_processados: list[str] = field(default_factory=list)
    itens_extraidos: list[ItemExtraido] = field(default_factory=list)

    def to_dict(self, debug: bool = False) -> dict:
        return {
            "arquivo_json": self.arquivo_json,
            "numero_pregao": self.numero_pregao,
            "orgao": self.orgao,
            "cidade": self.cidade,
            "estado": self.estado,
            "anexos_processados": self.anexos_processados,
            "itens_extraidos": [i.to_dict(debug=debug) for i in self.itens_extraidos],
        }

    def to_json(self, indent: int = 2, debug: bool = False) -> str:
        return json.dumps(self.to_dict(debug=debug), ensure_ascii=False, indent=indent)
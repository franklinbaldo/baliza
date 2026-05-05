"""RSS 2.0 feed generation for curated PNCP watches.

Mirrors `web/src/lib/homepage-content.ts:FEATURED_QUERIES` — both copies
must stay in sync until we cross the static-only architecture boundary.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from email.utils import format_datetime
from typing import Any
from xml.etree import ElementTree as ET

CHANNEL_LINK = "https://baliza.dev"
CONTRATACAO_BASE = f"{CHANNEL_LINK}/contratacao"


# Mirror of web/src/lib/homepage-content.ts:FEATURED_QUERIES.
# `where`/`order_by` are appended to a SELECT * FROM baliza_raw.contratos
# query; `limit` caps the per-feed item count.
CURATED_WATCHES: list[dict[str, Any]] = [
    {
        "slug": "dispensas-acima-1mi",
        "title": "Dispensas acima de R$ 1 mi",
        # Canonical contratos schema exposes value as `valor_global` and
        # contract object as `objeto_contrato` (no -cao). See
        # tests/conftest.py for the full DDL.
        "where": (
            "modalidade_nome ILIKE '%Dispensa%' "
            "AND COALESCE(valor_global, valor_inicial, 0) >= 1000000"
        ),
    },
    {
        "slug": "compras-emergenciais",
        "title": "Compras emergenciais",
        "where": "objeto_contrato ILIKE '%emergencial%'",
    },
]


def _coalesce(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _to_rfc2822(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except ValueError:
            return None
    return format_datetime(dt)


def generate(contracts: Iterable[dict[str, Any]], slug: str, title: str) -> str:
    """Return an RSS 2.0 XML string for the given contract rows.

    Each row may use either the camelCase PNCP API names
    (`numeroControlePNCP`, `dataPublicacaoPncp`, `objetoContratacao`) or
    the snake_case warehouse column names (`numero_controle_pncp`,
    `data_publicacao_pncp`, `objeto_contratacao`). Rows missing the id or
    publication date are skipped silently — RSS items without a guid or
    pubDate are not actionable for subscribers.
    """
    rss = ET.Element("rss", attrib={"version": "2.0"})
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = title
    ET.SubElement(channel, "link").text = CHANNEL_LINK
    ET.SubElement(channel, "description").text = title
    ET.SubElement(channel, "language").text = "pt-BR"

    for row in contracts:
        pncp_id = _coalesce(row, "numeroControlePNCP", "numero_controle_pncp")
        published = _to_rfc2822(
            _coalesce(row, "dataPublicacaoPncp", "data_publicacao_pncp")
        )
        if not pncp_id or not published:
            continue
        objeto = _coalesce(row, "objetoContratacao", "objeto_contratacao") or str(pncp_id)
        link = f"{CONTRATACAO_BASE}?id={pncp_id}"
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = str(objeto)
        ET.SubElement(item, "link").text = link
        ET.SubElement(item, "guid", attrib={"isPermaLink": "true"}).text = link
        ET.SubElement(item, "pubDate").text = published

    return ET.tostring(rss, encoding="unicode", xml_declaration=True)

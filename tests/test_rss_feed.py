"""Tests for the curated-watch RSS 2.0 feed generator."""

from __future__ import annotations

import re
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

from baliza import rss_feed  # noqa: I001

CONTRATACAO_RE = re.compile(r"^https://baliza\.dev/contratacao\?id=")


def _sample_rows() -> list[dict[str, str]]:
    return [
        {
            "numeroControlePNCP": "00000000000191-1-000001/2024",
            "dataPublicacaoPncp": "2024-12-20T00:00:00",
            "objetoContratacao": "Aquisição de medicamentos",
        },
        {
            "numero_controle_pncp": "00000000000272-1-000002/2025",
            "data_publicacao_pncp": "2025-01-15T00:00:00",
            "objeto_contratacao": "Contratação de obras",
        },
    ]


def test_generate_emits_valid_rss_2_0() -> None:
    xml = rss_feed.generate(_sample_rows(), "dispensas-acima-1mi", "Dispensas")
    root = ET.fromstring(xml)
    assert root.tag == "rss"
    assert root.attrib["version"] == "2.0"
    channel = root.find("channel")
    assert channel is not None
    assert channel.findtext("language") == "pt-BR"
    assert channel.findtext("link") == "https://baliza.dev"
    assert channel.findtext("title") == "Dispensas"


def test_generate_one_item_per_row_with_contratacao_links() -> None:
    rows = _sample_rows()
    xml = rss_feed.generate(rows, "slug", "Title")
    items = ET.fromstring(xml).findall("./channel/item")
    assert len(items) == len(rows)
    for item in items:
        link = item.findtext("link") or ""
        assert CONTRATACAO_RE.match(link), link
        guid = item.find("guid")
        assert guid is not None
        assert guid.attrib["isPermaLink"] == "true"
        assert guid.text == link
        pub = item.findtext("pubDate")
        assert pub
        # Parses cleanly as RFC 2822.
        parsedate_to_datetime(pub)


def test_generate_skips_rows_missing_id_or_date() -> None:
    rows = [
        {"numeroControlePNCP": "x", "objetoContratacao": "no date"},
        {"dataPublicacaoPncp": "2025-01-01T00:00:00", "objetoContratacao": "no id"},
        {
            "numeroControlePNCP": "valid",
            "dataPublicacaoPncp": "2025-01-01T00:00:00",
            "objetoContratacao": "ok",
        },
    ]
    items = ET.fromstring(rss_feed.generate(rows, "s", "T")).findall("./channel/item")
    assert len(items) == 1
    assert items[0].findtext("title") == "ok"


def test_curated_watches_includes_seed_slug() -> None:
    slugs = [w["slug"] for w in rss_feed.CURATED_WATCHES]
    assert "dispensas-acima-1mi" in slugs


def test_upload_feeds_binds_against_real_contratos_schema(tmp_path, monkeypatch):
    """Regression: the upload_feeds SQL must reference columns that
    actually exist in main.contratos (see tests/conftest.py for the
    canonical DDL). A typo like `objeto_contratacao` (the schema column
    is `objeto_contrato`) would be swallowed by the per-feed except
    block, so we assert each curated slug binds and produces a feed."""
    from baliza.engine import BalizaEngine
    from baliza.ia_uploader import IAUploader

    db_file = tmp_path / "feeds.duckdb"
    engine = BalizaEngine(db_path=db_file)
    try:
        engine.con.raw_sql(
            """
            CREATE TABLE main.contratos (
                numero_controle_pncp VARCHAR PRIMARY KEY,
                modalidade_nome VARCHAR,
                valor_global DOUBLE,
                valor_inicial DOUBLE,
                data_publicacao_pncp TIMESTAMP,
                objeto_contrato VARCHAR
            )
            """
        )
        engine.con.raw_sql(
            """
            INSERT INTO main.contratos VALUES
                ('A-1', 'Dispensa de Licitação', 1500000.0, 1500000.0,
                 TIMESTAMP '2025-01-15 00:00:00', 'Aquisição emergencial de medicamentos'),
                ('B-2', 'Pregão',                  500.0,    500.0,
                 TIMESTAMP '2025-01-10 00:00:00', 'Compra rotineira')
            """
        )

        upload_calls: list[dict] = []

        def fake_upload(item_id, files, **_kwargs):
            upload_calls.append({"item_id": item_id, "files": dict(files)})

        monkeypatch.setattr("baliza.ia_uploader.ia.upload", fake_upload)

        uploader = IAUploader(engine=engine)
        published = uploader.upload_feeds("ak", "sk")

        # Both curated slugs must bind against the real schema; if the SQL
        # referenced a non-existent column, the per-feed except would
        # swallow the binder error and `published` would be empty.
        assert set(published) == {w["slug"] for w in rss_feed.CURATED_WATCHES}
        assert len(upload_calls) == 1
        assert upload_calls[0]["item_id"] == "baliza-pncp-feeds"
        for slug in published:
            assert f"feed-{slug}.xml" in upload_calls[0]["files"]
    finally:
        try:
            engine.con.disconnect()
        except Exception:
            pass

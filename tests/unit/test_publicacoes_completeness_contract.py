from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from baliza.mirror import mirror_month


def _probe_ok(*args, **kwargs):
    return {"total_pages": 1, "total_registries": 1}


def _fetch_writes_page(self, resource, start_date, end_date, page=1, *, extra_params=None):
    data = {
        "data": [
            {
                "numeroControlePNCP": f"{extra_params}-{page}",
                "dataPublicacaoPncp": "2026-08-31T12:00:00",
            }
        ],
        "totalPaginas": 1,
        "totalRegistros": 1,
    }
    self._save_raw(resource, start_date, page, data, extra_params=extra_params)
    return data


def test_publicacoes_probe_failure_aborts_before_upload(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    uploaded = []

    def probe(self, resource, start_date, end_date, *, extra_params=None):
        if extra_params == {"codigoModalidadeContratacao": 5}:
            raise TimeoutError("PNCP timed out")
        return _probe_ok()

    monkeypatch.setattr("baliza.mirror.PNCPExtractor.probe_range", probe)
    monkeypatch.setattr("baliza.mirror.PNCPExtractor.fetch_page", _fetch_writes_page)
    monkeypatch.setattr(
        "baliza.mirror.IAUploader.upload_raw_zip",
        lambda *args, **kwargs: uploaded.append(True) or True,
    )

    with pytest.raises(Exception, match="PNCP timed out"):
        mirror_month(
            date(2026, 8, 1),
            ia_access_key="x",
            ia_secret_key="y",
            resource="publicacoes",
            is_current_month=False,
        )

    assert uploaded == []


def test_publicacoes_page_failure_aborts_before_upload(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    uploaded = []

    def fetch(self, resource, start_date, end_date, page=1, *, extra_params=None):
        if extra_params == {"codigoModalidadeContratacao": 6}:
            raise TimeoutError("page fetch timed out")
        return _fetch_writes_page(
            self,
            resource,
            start_date,
            end_date,
            page,
            extra_params=extra_params,
        )

    monkeypatch.setattr("baliza.mirror.PNCPExtractor.probe_range", _probe_ok)
    monkeypatch.setattr("baliza.mirror.PNCPExtractor.fetch_page", fetch)
    monkeypatch.setattr(
        "baliza.mirror.IAUploader.upload_raw_zip",
        lambda *args, **kwargs: uploaded.append(True) or True,
    )

    with pytest.raises(Exception, match="page fetch timed out"):
        mirror_month(
            date(2026, 8, 1),
            ia_access_key="x",
            ia_secret_key="y",
            resource="publicacoes",
            is_current_month=False,
        )

    assert uploaded == []


def test_publicacoes_complete_mirror_writes_machine_readable_coverage(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr("baliza.mirror.PNCPExtractor.probe_range", _probe_ok)
    monkeypatch.setattr("baliza.mirror.PNCPExtractor.fetch_page", _fetch_writes_page)

    mirror_month(
        date(2026, 8, 1),
        ia_access_key="x",
        ia_secret_key="y",
        resource="publicacoes",
        is_current_month=False,
        dry_run=True,
    )

    coverage_path = Path("data/raw/2026-08/_coverage.json")
    assert coverage_path.exists()
    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))

    assert coverage["resource"] == "publicacoes"
    assert coverage["partition"] == "2026-08"
    assert coverage["complete"] is True
    assert coverage["page_size"] == 50
    assert len(coverage["modalidades"]) == 14
    assert all(row["complete"] is True for row in coverage["modalidades"])
    assert all(row["last_publication_date"] == "2026-08-31" for row in coverage["modalidades"])
    assert all(row["records"] == 1 for row in coverage["modalidades"])

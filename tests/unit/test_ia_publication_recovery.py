from __future__ import annotations

import json
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

from baliza.coverage import modalidade_coverage, validate_coverage
from baliza.ia_retry import PublicationError, rejection_kind, upload_with_retry
from baliza.ia_uploader import IAUploader, _sha256
from baliza.mirror import mirror_month


def response(status=200, text="", headers=None):
    result = requests.Response()
    result.status_code = status
    result._content = text.encode()
    result.headers.update(headers or {})
    return result


def http_error(status, text="", headers=None):
    return requests.HTTPError(text, response=response(status, text, headers))


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        (http_error(403), "permanent"),
        (http_error(400), "permanent"),
        (http_error(503), "temporary"),
        (http_error(429), "temporary"),
        (requests.Timeout(), "temporary"),
        (http_error(403, "total_tasks_queued exceeds global_limit"), "capacity"),
    ],
)
def test_classification(error, kind):
    assert rejection_kind(error) == kind


def test_capacity_backoff_then_success(monkeypatch):
    put = Mock(
        side_effect=[
            http_error(503, "total_tasks_queued exceeds global_limit"),
            http_error(503, "total_tasks_queued exceeds global_limit"),
            [response()],
        ]
    )
    sleep = Mock()
    monkeypatch.setattr("baliza.ia_retry.ia.upload", put)
    monkeypatch.setattr("baliza.ia_retry.time.sleep", sleep)
    upload_with_retry("item", files={"same.zip": "/same.zip"})
    assert [c.args[0] for c in sleep.call_args_list] == [60, 120]
    assert all(c.kwargs == put.call_args_list[0].kwargs for c in put.call_args_list)
    assert put.call_args.kwargs["retries"] == 0


@pytest.mark.parametrize("error", [http_error(403), http_error(400), ValueError("bad file")])
def test_permanent_failure_does_not_wait(monkeypatch, error):
    put = Mock(side_effect=error)
    sleep = Mock()
    monkeypatch.setattr("baliza.ia_retry.ia.upload", put)
    monkeypatch.setattr("baliza.ia_retry.time.sleep", sleep)
    with pytest.raises(PublicationError, match="Permanent"):
        upload_with_retry("item")
    assert put.call_count == 1
    sleep.assert_not_called()


def test_retry_after_exceeding_budget_preserves_lower_bound(monkeypatch):
    put = Mock(side_effect=http_error(429, headers={"Retry-After": "901"}))
    sleep = Mock()
    monkeypatch.setattr("baliza.ia_retry.ia.upload", put)
    monkeypatch.setattr("baliza.ia_retry.time.sleep", sleep)
    with pytest.raises(PublicationError, match="budget exhausted"):
        upload_with_retry("item")
    assert put.call_count == 1
    sleep.assert_not_called()


def test_max_attempts_and_returned_http_errors(monkeypatch):
    put = Mock(return_value=[response(503)])
    sleep = Mock()
    monkeypatch.setattr("baliza.ia_retry.ia.upload", put)
    monkeypatch.setattr("baliza.ia_retry.time.sleep", sleep)
    with pytest.raises(PublicationError, match="budget exhausted"):
        upload_with_retry("item", max_attempts=3)
    assert put.call_count == 3
    assert [c.args[0] for c in sleep.call_args_list] == [30, 60]


@pytest.fixture
def prepared(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("baliza.extractor.validate_url", lambda url: url)
    monkeypatch.setattr("baliza.extractor.httpx.Client", Mock())
    monkeypatch.setattr(
        "baliza.mirror.PNCPExtractor.probe_range",
        lambda *a, **k: {"total_pages": 1, "total_registries": 1},
    )

    def fetch(self, resource, start, end, page=1, *, extra_params=None):
        data = {
            "data": [{"dataPublicacaoPncp": "2026-08-31T12:00:00"}],
            "totalPaginas": 1,
            "totalRegistros": 1,
        }
        self._save_raw(resource, start, page, data, extra_params=extra_params)
        return data

    monkeypatch.setattr("baliza.mirror.PNCPExtractor.fetch_page", fetch)
    mirror_month(
        date(2026, 8, 1),
        ia_access_key="x",
        ia_secret_key="y",
        resource="publicacoes",
        is_current_month=False,
        dry_run=True,
    )
    return Path("data/raw/2026-08")


def test_exhaustion_preserves_zip_and_resume_never_fetches_pncp(monkeypatch, prepared):
    uploader = IAUploader()
    put = Mock(side_effect=PublicationError("capacity exhausted"))
    monkeypatch.setattr("baliza.ia_uploader.upload_with_retry", put)
    manifest = Mock()
    monkeypatch.setattr(IAUploader, "_upsert_manifest_row", manifest)
    with pytest.raises(PublicationError):
        uploader.upload_raw_zip(date(2026, 8, 1), prepared, "x", "y", resource="publicacoes")
    pending = Path("data/pending/publicacoes/2026-08/raw-publicacoes-2026-08.zip")
    original = pending.read_bytes()
    manifest.assert_not_called()
    with zipfile.ZipFile(pending) as zf:
        assert "_coverage.json" in zf.namelist()
    put.side_effect = None
    monkeypatch.setattr(
        "baliza.mirror.PNCPExtractor", Mock(side_effect=AssertionError("PNCP called"))
    )
    result = mirror_month(
        date(2026, 8, 1),
        ia_access_key="x",
        ia_secret_key="y",
        resource="publicacoes",
        is_current_month=False,
    )
    assert result["uploaded"] is True
    assert result["pages_fetched"] == 0
    assert len(original) > 0
    assert put.call_args_list[0] == put.call_args_list[1]
    assert not pending.exists()
    assert not prepared.exists()


def test_manifest_retry_does_not_reupload_zip(monkeypatch, prepared):
    put = Mock()
    monkeypatch.setattr("baliza.ia_uploader.upload_with_retry", put)
    manifest = Mock(side_effect=[RuntimeError("manifest failed"), None])
    monkeypatch.setattr(IAUploader, "_upsert_manifest_row", manifest)
    uploader = IAUploader()
    assert not uploader.upload_raw_zip(date(2026, 8, 1), prepared, "x", "y", resource="publicacoes")
    assert uploader.upload_raw_zip(date(2026, 8, 1), prepared, "x", "y", resource="publicacoes")
    assert put.call_count == 1
    assert manifest.call_count == 2
    fields = manifest.call_args.args[1]
    assert fields["raw_coverage_complete"] == "true"
    assert fields["parquet_uploaded_at"] == ""
    assert fields["parquet_url"] == ""
    assert fields["sha256"] == ""


def test_coverage_rejects_altered_missing_and_surplus_pages(prepared):
    validate_coverage(prepared, "2026-08")
    path = next(prepared.glob("publicacoes*_p1.json"))
    data = json.loads(path.read_text())
    data["data"][0]["dataPublicacaoPncp"] = "2026-08-30T00:00:00"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="does not match"):
        validate_coverage(prepared, "2026-08")


@pytest.mark.parametrize("change", ["missing", "surplus"])
def test_coverage_rejects_wrong_page_set(prepared, change):
    if change == "missing":
        next(prepared.glob("publicacoes*_p1.json")).unlink()
    else:
        (prepared / "publicacoes_codigoModalidadeContratacao1_p2.json").write_text('{}')
    with pytest.raises((ValueError, FileNotFoundError)):
        validate_coverage(prepared, "2026-08")


def test_publicacoes_204_is_confirmed_empty(monkeypatch):
    from baliza.extractor import PNCPExtractor

    monkeypatch.setattr("baliza.extractor.validate_url", lambda url: url)
    client = Mock()
    client.stream.return_value.__enter__ = Mock(return_value=Mock(status_code=204))
    client.stream.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr("baliza.extractor.httpx.Client", Mock(return_value=client))
    with PNCPExtractor(engine=None) as extractor:
        assert extractor._fetch_with_httpx("publicacoes", 1, "https://example.test", {}) == {
            "data": [], "totalPaginas": 0, "totalRegistros": 0,
        }


def test_zero_modalidades_have_valid_coverage(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("baliza.extractor.validate_url", lambda url: url)
    monkeypatch.setattr("baliza.extractor.httpx.Client", Mock())
    monkeypatch.setattr("baliza.mirror.PNCPExtractor.probe_range",
                        lambda *a, **k: {"total_pages": 0, "total_registries": 0})
    mirror_month(date(2026, 8, 1), ia_access_key="x", ia_secret_key="y",
                 resource="publicacoes", is_current_month=False, dry_run=True)
    coverage = validate_coverage(Path("data/raw/2026-08"), "2026-08")
    assert len(coverage["modalidades"]) == 14
    assert all(row["records"] == 0 for row in coverage["modalidades"])


def test_open_snapshot_stays_pending_after_rollover(monkeypatch):
    from baliza.mirror import _pending_mirror_months

    monkeypatch.setattr("baliza.mirror.read_manifest_from_ia", lambda: [{
        "table_name": "publicacoes", "data_particao": "2026-08", "raw_zip_url": "zip",
        "raw_partition_closed": "false",
    }])
    monkeypatch.setattr("baliza.mirror.current_partition_start", lambda _: date(2026, 9, 1))
    monkeypatch.setattr("baliza.mirror.previous_partition_start", lambda _: date(2026, 8, 1))
    assert date(2026, 8, 1) in _pending_mirror_months(date(2026, 8, 1), None, resource="publicacoes")


def test_incremental_full_pages_can_have_older_total_metadata(tmp_path):
    combo = {"codigoModalidadeContratacao": 1}
    first = [{"numeroControlePNCP": str(i)} for i in range(50)]
    last = [{"numeroControlePNCP": "50"}]
    for page, records, total in [(1, first, 50), (2, last, 51)]:
        (tmp_path / f"publicacoes_codigoModalidadeContratacao1_p{page}.json").write_text(
            json.dumps({"data": records, "totalRegistros": total, "totalPaginas": page}))
    assert modalidade_coverage(tmp_path, combo, 2, 51)["records"] == 51
    # A shifted/duplicated page must not pass merely because the counts match.
    path = tmp_path / "publicacoes_codigoModalidadeContratacao1_p2.json"
    path.write_text(json.dumps({"data": [{"numeroControlePNCP": "0"}]}))
    with pytest.raises(ValueError, match="Duplicate"):
        modalidade_coverage(tmp_path, combo, 2, 51)


def test_build_rejects_legacy_zip_before_engine(monkeypatch, tmp_path):
    from baliza.builder import build_month

    archive = tmp_path / "legacy.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("publicacoes_p1.json", '{"data": []}')
    monkeypatch.setattr("baliza.builder._download_raw_zip", lambda *args: archive)
    engine = Mock(side_effect=AssertionError("engine started"))
    monkeypatch.setattr("baliza.builder.BalizaEngine", engine)
    with pytest.raises(FileNotFoundError, match="_coverage.json"):
        build_month(
            date(2026, 8, 1),
            ia_access_key="x",
            ia_secret_key="y",
            resource="publicacoes",
            manifest=[
                {
                    "table_name": "publicacoes",
                    "data_particao": "2026-08",
                    "raw_zip_url": "https://example.test/raw.zip",
                        "raw_zip_sha256": _sha256(archive),
                }
            ],
        )
    engine.assert_not_called()

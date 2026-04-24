"""BDD step definitions for sync.feature.

Seams:
- httpx.Client.stream → patched to return a canned PNCP response.
- baliza.ia_uploader.{try_,}read_manifest_from_ia → patched via
  `mock_ia_manifest` to avoid HTTP and simulate empty / populated manifests.
- internetarchive.upload → patched via `mock_ia_upload` to record invocations.
- monkeypatch.chdir(tmp_path) so `upload_month`'s `shutil.rmtree("data/raw")`
  and `shutil.rmtree("data/daily")` touch only the scenario's scratch dir.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, Mock

from pytest_bdd import given, parsers, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()


def _mock_stream_ctx(body: dict):
    """Build a context-manager mock mirroring httpx.Client.stream()."""
    resp = Mock()
    resp.status_code = 200
    resp.raise_for_status = Mock()

    payload = json.dumps(body).encode("utf-8")

    def _iter_bytes():
        yield payload

    resp.iter_bytes = _iter_bytes

    cm = MagicMock()
    cm.__enter__ = Mock(return_value=resp)
    cm.__exit__ = Mock(return_value=None)
    return cm


def _contratos_page_body() -> dict:
    """Minimal valid PNCP response: five contracts, single page."""
    rows = []
    for i in range(5):
        rows.append(
            {
                "numeroControlePNCP": f"23000-1-{i:03d}/2023",
                "anoContrato": 2023,
                "sequencialContrato": i,
                "valorInicial": 1000.0 + i,
                "objetoContrato": f"Test contract {i}",
                "orgaoEntidade": {
                    "cnpj": f"{i:014d}",
                    "razaoSocial": f"Org {i}",
                    "poderId": "E",
                    "esferaId": "F",
                },
                "niFornecedor": f"FORN-{i:05d}",
                "tipoPessoa": "PJ",
                "nomeRazaoSocialFornecedor": f"Fornecedor {i}",
            }
        )
    return {
        "data": rows,
        "totalPaginas": 1,
        "totalRegistros": len(rows),
        "numeroPagina": 1,
        "paginasRestantes": 0,
        "empty": False,
    }


@scenario(
    "../features/sync.feature",
    "Sync uploads a pending month to Internet Archive",
)
def test_sync_uploads_month():
    pass


@scenario(
    "../features/sync.feature",
    "Sync skips months already in the manifest",
)
def test_sync_skips_uploaded():
    pass


@scenario(
    "../features/sync.feature",
    "Sync in dry-run mode extracts but does not upload",
)
def test_sync_dry_run():
    pass


@scenario(
    "../features/sync.feature",
    "Sync surfaces an error when IA keys are missing",
)
def test_sync_missing_keys():
    pass


# ── Given ────────────────────────────────────────────────────────────────────


@given("PNCP returns a single page of contracts", target_fixture="pncp_stream_mock")
def mock_pncp_stream(monkeypatch):
    import httpx

    def fake_stream(self, method, url, **kwargs):
        return _mock_stream_ctx(_contratos_page_body())

    monkeypatch.setattr(httpx.Client, "stream", fake_stream)


@given("IA credentials are configured")
def set_ia_keys(monkeypatch):
    monkeypatch.setenv("IA_ACCESS_KEY", "fake-access-key")
    monkeypatch.setenv("IA_SECRET_KEY", "fake-secret-key")


@given("IA credentials are missing")
def clear_ia_keys(monkeypatch):
    monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IA_SECRET_KEY", raising=False)
    monkeypatch.delenv("IAS3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IAS3_SECRET_KEY", raising=False)


@given("the IA manifest lists every pending month")
def manifest_lists_every_month(mock_ia_manifest):
    from datetime import date

    from baliza.constants import RESOURCE_CONTRATOS, known_data_start_month

    today = date.today()
    rows = []
    first_month = known_data_start_month(RESOURCE_CONTRATOS)
    for year in range(first_month.year, today.year + 1):
        last_month = 12 if year < today.year else today.month
        first_month_for_year = first_month.month if year == first_month.year else 1
        for m in range(first_month_for_year, last_month + 1):
            month = f"{year}-{m:02d}"
            rows.append(
                {
                    "data_particao": month,
                    "table_name": RESOURCE_CONTRATOS,
                    "parquet_url": f"https://example.invalid/contratos-{month}.parquet",
                }
            )
    mock_ia_manifest(rows)


# ── When ─────────────────────────────────────────────────────────────────────


def _isolate_cwd(monkeypatch, tmp_path: Path) -> None:
    """Ensure file I/O (raw cache, uploader cleanup) stays within tmp_path."""
    monkeypatch.chdir(tmp_path)


@when(
    "I run baliza sync --force-month 2023-01 with IA mocked",
    target_fixture="sync_result",
)
def run_sync_happy(
    monkeypatch,
    tmp_path: Path,
    pncp_stream_mock,
    mock_ia_upload,
    mock_ia_manifest,
):
    # No months in manifest → force-month path bypasses the diff but
    # upload_month still reads the manifest to append its row.
    mock_ia_manifest([])
    _isolate_cwd(monkeypatch, tmp_path)
    result = runner.invoke(
        app,
        [
            "sync",
            "--force-month",
            "2023-01",
            "--no-curl",
            "--workers",
            "1",
            "--no-consolidate",
            "--duckdb",
            str(tmp_path / "sync.duckdb"),
        ],
    )
    return {"result": result, "uploads": mock_ia_upload, "tmp_path": tmp_path}


@when(
    "I run baliza sync without --force-month",
    target_fixture="sync_result",
)
def run_sync_no_force(monkeypatch, tmp_path: Path, mock_ia_upload):
    _isolate_cwd(monkeypatch, tmp_path)
    result = runner.invoke(
        app,
        [
            "sync",
            "--no-curl",
            "--workers",
            "1",
            "--no-consolidate",
            "--duckdb",
            str(tmp_path / "sync.duckdb"),
        ],
    )
    return {"result": result, "uploads": mock_ia_upload, "tmp_path": tmp_path}


@when(
    "I run baliza sync --force-month 2023-01 --dry-run",
    target_fixture="sync_result",
)
def run_sync_dry_run(
    monkeypatch,
    tmp_path: Path,
    pncp_stream_mock,
    mock_ia_upload,
    mock_ia_manifest,
):
    mock_ia_manifest([])
    _isolate_cwd(monkeypatch, tmp_path)
    result = runner.invoke(
        app,
        [
            "sync",
            "--force-month",
            "2023-01",
            "--dry-run",
            "--no-curl",
            "--workers",
            "1",
            "--no-consolidate",
            "--duckdb",
            str(tmp_path / "sync.duckdb"),
        ],
    )
    return {"result": result, "uploads": mock_ia_upload, "tmp_path": tmp_path}


@when(
    "I run baliza sync --force-month 2023-01 without dry-run",
    target_fixture="sync_result",
)
def run_sync_missing_keys(monkeypatch, tmp_path: Path, mock_ia_upload):
    _isolate_cwd(monkeypatch, tmp_path)
    result = runner.invoke(
        app,
        [
            "sync",
            "--force-month",
            "2023-01",
            "--no-curl",
            "--workers",
            "1",
            "--no-consolidate",
            "--duckdb",
            str(tmp_path / "sync.duckdb"),
        ],
    )
    return {"result": result, "uploads": mock_ia_upload, "tmp_path": tmp_path}


# ── Then ─────────────────────────────────────────────────────────────────────


@then("the sync command should exit successfully")
def check_sync_success(sync_result):
    result = sync_result["result"]
    assert result.exit_code == 0, f"exit={result.exit_code}\n{result.stdout}"


@then(parsers.parse('the sync command should fail with "{fragment}"'))
def check_sync_failure(sync_result, fragment: str):
    result = sync_result["result"]
    assert result.exit_code != 0, f"expected failure, got exit=0\n{result.stdout}"
    assert fragment in result.stdout, f"{fragment!r} not in:\n{result.stdout}"


@then(parsers.parse('the IA upload mock should have been called for item "{item_id}"'))
def check_upload_called(sync_result, item_id: str):
    uploads = sync_result["uploads"]
    matching = [u for u in uploads if u["item_id"] == item_id]
    assert matching, f"no upload for {item_id!r}; got items={[u['item_id'] for u in uploads]}"


@then(parsers.parse('the uploaded files for "{item_id}" should include "{filename}"'))
def check_upload_contains_file(sync_result, item_id: str, filename: str):
    # upload_month pushes raw-YYYY-MM.zip alongside the per-table parquets;
    # item_id alone would pass if parquet export silently failed and only
    # the raw archive made it up.
    matching = [u for u in sync_result["uploads"] if u["item_id"] == item_id]
    assert matching, f"no upload for {item_id!r}"
    all_files: list[str] = []
    for u in matching:
        all_files.extend(u["files"].keys())
    assert filename in all_files, f"{filename!r} not in uploaded files for {item_id!r}: {all_files}"


@then("the IA upload mock should not have been called")
def check_no_upload(sync_result):
    uploads = sync_result["uploads"]
    assert uploads == [], f"expected zero uploads, got: {[u['item_id'] for u in uploads]}"


@then(parsers.parse("the raw JSON cache for {month} should exist"))
def check_raw_cache_exists(sync_result, month: str):
    tmp_path: Path = sync_result["tmp_path"]
    raw_dir = tmp_path / "data" / "raw" / month
    assert raw_dir.exists(), f"raw cache dir {raw_dir} does not exist"
    files = list(raw_dir.glob("*.json"))
    assert files, f"raw cache dir {raw_dir} is empty"

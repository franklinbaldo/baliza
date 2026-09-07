from __future__ import annotations

from datetime import date, timedelta

from typer.testing import CliRunner

from baliza import cli_simple as cli


runner = CliRunner()
_SHA = "a" * 64
_ARCHIVE_PARQUET = "https://archive.org/download/test/canonical.parquet"
_ARCHIVE_RAW = "https://archive.org/download/test/raw.zip"


def _previous_month(today: date) -> date:
    return (today.replace(day=1) - timedelta(days=1)).replace(day=1)


def _run_doctor(monkeypatch, rows: list[dict[str, str]], resource: str, start: str):
    monkeypatch.setattr(cli, "read_manifest_from_ia", lambda: rows)
    return runner.invoke(cli.app, ["doctor", "--resource", resource, "--start", start])


def test_doctor_allows_current_monthly_partition_awaiting_build(monkeypatch):
    today = date.today()
    label = today.strftime("%Y-%m")
    result = _run_doctor(
        monkeypatch,
        [
            {
                "table_name": "contratos",
                "data_particao": label,
                "file_type": "monthly_canonical",
                "raw_zip_url": _ARCHIVE_RAW,
                "parquet_url": "",
                "sha256": "",
                "uf_sigla": "",
            }
        ],
        "contratos",
        today.replace(day=1).isoformat(),
    )

    assert result.exit_code == 0, result.stdout
    assert "missing sha256" not in result.stdout
    assert "missing parquet_url" not in result.stdout


def test_doctor_keeps_closed_monthly_partition_fail_closed(monkeypatch):
    previous = _previous_month(date.today())
    label = previous.strftime("%Y-%m")
    result = _run_doctor(
        monkeypatch,
        [
            {
                "table_name": "contratos",
                "data_particao": label,
                "file_type": "monthly_canonical",
                "raw_zip_url": _ARCHIVE_RAW,
                "parquet_url": "",
                "sha256": "",
                "uf_sigla": "",
            }
        ],
        "contratos",
        previous.isoformat(),
    )

    assert result.exit_code == 1
    assert f"contratos/{label}: missing sha256" in result.stdout
    assert f"contratos/{label}: missing parquet_url" in result.stdout


def test_doctor_accepts_complete_closed_monthly_partition(monkeypatch):
    previous = _previous_month(date.today())
    label = previous.strftime("%Y-%m")
    result = _run_doctor(
        monkeypatch,
        [
            {
                "table_name": "contratos",
                "data_particao": label,
                "file_type": "monthly_canonical",
                "raw_zip_url": _ARCHIVE_RAW,
                "parquet_url": _ARCHIVE_PARQUET,
                "sha256": _SHA,
                "uf_sigla": "",
            }
        ],
        "contratos",
        previous.isoformat(),
    )

    assert result.exit_code == 0, result.stdout


def test_doctor_inspects_current_annual_canonical_from_registry(monkeypatch):
    year = date.today().year
    label = str(year)
    result = _run_doctor(
        monkeypatch,
        [
            {
                "table_name": "pca",
                "data_particao": label,
                "file_type": "annual_canonical",
                "raw_zip_url": _ARCHIVE_RAW,
                "parquet_url": "",
                "sha256": "",
                "uf_sigla": "",
            }
        ],
        "pca",
        f"{year}-01-01",
    )

    assert result.exit_code == 1
    assert f"pca/{label}: missing sha256" in result.stdout
    assert f"pca/{label}: missing parquet_url" in result.stdout

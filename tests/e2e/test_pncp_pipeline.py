from __future__ import annotations

from datetime import UTC
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

from typer.testing import CliRunner

from baliza import cli
from baliza.pipelines import pncp

runner = CliRunner()


def test_load_pncp_config_resolves_convert_callable() -> None:
    config = pncp.load_pncp_config()
    incremental = config["resources"][0]["endpoint"]["incremental"]
    convert = incremental["convert"]
    assert callable(convert)
    assert convert("2024-01-15T10:00:00Z") == "20240115"
    assert len(convert(None)) == 8


def test_pncp_source_passes_config_to_rest_api() -> None:
    with patch("baliza.pipelines.pncp.rest_api_source") as mocked:
        source = pncp.pncp_source()
        mocked.assert_called_once()
        assert source is mocked.return_value
        config = mocked.call_args.kwargs["config"]
        assert config["client"]["base_url"].startswith("https://")


def test_pncp_source_applies_lookback(monkeypatch) -> None:
    captured: dict[str, dict] = {}

    def fake_rest_api_source(*, config: dict) -> MagicMock:
        captured["config"] = config
        return MagicMock()

    monkeypatch.setattr(pncp, "rest_api_source", fake_rest_api_source)

    source = pncp.pncp_source(lookback_days=2)

    assert isinstance(source, MagicMock)
    incremental = captured["config"]["resources"][0]["endpoint"]["incremental"]
    assert incremental["lag"] == 2 * 24 * 60 * 60


def test_run_pncp_uses_duckdb_destination(tmp_path: Path) -> None:
    fake_pipeline = MagicMock()
    fake_run = MagicMock()
    fake_pipeline.run.return_value = fake_run

    with (
        patch("baliza.pipelines.pncp.dlt.pipeline", return_value=fake_pipeline) as pipeline_mock,
        patch("baliza.pipelines.pncp.dlt.destinations.duckdb", return_value="duck") as duckdb_mock,
        patch("baliza.pipelines.pncp.pncp_source", return_value="source") as source_mock,
    ):
        pipeline, run = pncp.run_pncp(
            duckdb_path=tmp_path / "baliza.duckdb",
            dataset="demo",
            lookback_days=5,
            range_start="2024-01-01",
            range_end="2024-01-31",
            pipeline_name="custom",
        )

    duckdb_mock.assert_called_once()
    pipeline_mock.assert_called_once_with(
        pipeline_name="custom",
        destination="duck",
        dataset_name="demo",
    )
    source_mock.assert_called_once_with(
        None,
        lookback_days=5,
        range_start="2024-01-01",
        range_end="2024-01-31",
        tracker=ANY,
    )
    fake_pipeline.run.assert_called_once_with("source")
    assert pipeline is fake_pipeline
    assert run is fake_run


def test_cli_extract_emits_json(monkeypatch) -> None:
    run_info = MagicMock()
    run_info.asdict.return_value = {"rows": 10}
    monkeypatch.setattr(cli, "run_pncp", MagicMock(return_value=(MagicMock(), run_info)))

    result = runner.invoke(cli.app, ["extract", "--dataset", "cli_demo", "--lookback-days", "2"])

    assert result.exit_code == 0
    assert '"rows": 10' in result.stdout


def test_cli_backfill_invokes_pipeline_per_month(monkeypatch) -> None:
    calls = []

    def fake_run_pncp(**kwargs):
        calls.append(kwargs)
        run_info = MagicMock()
        run_info.asdict.return_value = {"rows": 5}
        return MagicMock(), run_info

    monkeypatch.setattr(cli, "run_pncp", fake_run_pncp)

    result = runner.invoke(cli.app, ["backfill", "2024-01", "2024-03"])

    assert result.exit_code == 0
    assert len(calls) == 3
    first_call = calls[0]
    assert first_call["pipeline_name"] == pncp.BACKFILL_PIPELINE_NAME
    assert first_call["range_start"].month == 1
    assert first_call["range_start"].tzinfo == UTC
    assert "2024-03" in result.stdout

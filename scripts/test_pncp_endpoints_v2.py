"""Regression test around the run_pncp helper."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from baliza.pipelines import pncp


def test_run_pncp_uses_duckdb(tmp_path: Path) -> None:
    fake_pipeline = MagicMock()
    fake_run = MagicMock()
    fake_pipeline.run.return_value = fake_run

    with (
        patch("baliza.pipelines.pncp.dlt.pipeline", return_value=fake_pipeline) as pipeline_mock,
        patch("baliza.pipelines.pncp.dlt.destinations.duckdb", return_value="duck") as duckdb_mock,
        patch("baliza.pipelines.pncp.pncp_source", return_value="source") as source_mock,
    ):
        pipeline, run_info = pncp.run_pncp(duckdb_path=tmp_path / "db.duckdb", dataset="test")

    duckdb_mock.assert_called_once_with(str(tmp_path / "db.duckdb"))
    pipeline_mock.assert_called_once_with(
        pipeline_name="baliza_pncp",
        destination="duck",
        dataset_name="test",
    )
    source_mock.assert_called_once()
    fake_pipeline.run.assert_called_once_with("source")
    assert pipeline is fake_pipeline
    assert run_info is fake_run

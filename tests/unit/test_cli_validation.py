from __future__ import annotations

from typer.testing import CliRunner

from baliza import cli

runner = CliRunner()


def test_backfill_rejects_invalid_month_format() -> None:
    result = runner.invoke(cli.app, ["backfill", "202401", "2024-02"])
    assert result.exit_code != 0
    assert "YYYY-MM" in result.stderr


def test_backfill_requires_start_before_end() -> None:
    result = runner.invoke(cli.app, ["backfill", "2024-03", "2024-01"])
    assert result.exit_code != 0
    assert "before or equal" in result.stderr

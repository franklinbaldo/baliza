from datetime import datetime

import pytest
from typer.testing import CliRunner

from src.baliza.cli_simple import app
from src.baliza.engine import BalizaEngine
from src.baliza.extractor import PNCPExtractor
from src.baliza.resources.specs import PNCPResource

runner = CliRunner()


def test_pncpresource_rejects_unsafe_name():
    """Charset safety is enforced at registration time, not at every call site.

    Anything matching ``[a-zA-Z0-9_]+`` is allowed; everything else
    (slashes, dots, semicolons, spaces) raises before the dataclass
    is even constructed, so the registry can never contain a name
    that's unsafe to interpolate into paths or URLs.
    """
    for hostile in ("../secret", "contratos; rm -rf /", "with space", "double..dots"):
        with pytest.raises(ValueError, match="Invalid resource_name"):
            PNCPResource(
                resource_name=hostile,
                fetch=None,  # type: ignore[arg-type]
                raw_dataset=None,  # type: ignore[arg-type]
                entities=[],
                canonical_tables=[],
            )


def test_extractor_rejects_unregistered_resource(tmp_path):
    """Unregistered resource names raise ValueError before any I/O happens."""
    engine = BalizaEngine(tmp_path / "test.duckdb")
    extractor = PNCPExtractor(engine)

    with pytest.raises(ValueError, match="unknown resource"):
        extractor.fetch_page(
            resource="../secret", start_date=datetime.now(), end_date=datetime.now()
        )


def test_cli_verify_rejects_command_injection():
    """The CLI surfaces the registry rejection for hostile resource args."""
    result = runner.invoke(
        app,
        [
            "verify",
            "--resource",
            "contratos; rm -rf /",
            "--start",
            "2023-01-01",
            "--end",
            "2023-01-31",
        ],
    )
    assert result.exit_code != 0, f"Expected non-zero exit code. Output was: {result.output}"
    assert "unknown resource" in result.output, (
        f"Expected 'unknown resource' in output. Output was: {result.output}"
    )


def test_cli_verify_rejects_path_traversal():
    """Path-traversal attempts in --resource are rejected by the registry."""
    result = runner.invoke(
        app, ["verify", "--resource", "../secret", "--start", "2023-01-01", "--end", "2023-01-01"]
    )
    assert result.exit_code != 0, f"Expected non-zero exit code. Output was: {result.output}"
    assert "unknown resource" in result.output, (
        f"Expected 'unknown resource' in output. Output was: {result.output}"
    )

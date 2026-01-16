from __future__ import annotations

import json
from typer.testing import CliRunner

from baliza.cli import app
from causaganha.schemas.manifest import ParquetSchema

runner = CliRunner()


def test_manifest_export():
    """Verify that the manifest export command runs and produces valid JSON."""
    # When
    result = runner.invoke(app, ["manifest", "export", "--limit", "5"])

    # Then
    assert result.exit_code == 0, result.stdout

    try:
        output = json.loads(result.stdout)
    except json.JSONDecodeError:
        assert False, "The command output is not valid JSON."

    assert isinstance(output, list)
    assert len(output) == 5

    # Verify that each object in the output conforms to the ParquetSchema
    for item in output:
        ParquetSchema.model_validate(item)

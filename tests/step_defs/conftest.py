import pytest
from typer.testing import CliRunner

@pytest.fixture
def run_result():
    return {}

@pytest.fixture
def runner():
    return CliRunner()

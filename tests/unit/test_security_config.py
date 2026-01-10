import pytest
import shutil
import sys
from baliza.pipelines.pncp import _import_from_string, load_pncp_config


def test_load_config_restricts_arbitrary_imports(tmp_path):
    """Test that load_pncp_config refuses to import non-baliza modules."""

    config_content = """
resources:
  - name: "contratos"
    endpoint:
      incremental:
        convert: "os.system"
"""
    config_file = tmp_path / "malicious.yml"
    config_file.write_text(config_content)

    with pytest.raises(ValueError, match="is not allowed"):
        load_pncp_config(config_file)


def test_import_from_string_allows_baliza():
    """Test that _import_from_string allows baliza modules."""
    # This should succeed
    func = _import_from_string("baliza.utils.dates.to_pncp_window")
    assert callable(func)


def test_import_from_string_blocks_stdlib():
    """Test that _import_from_string blocks stdlib modules."""
    with pytest.raises(ValueError, match="is not allowed"):
        _import_from_string("os.system")


def test_import_from_string_blocks_other_packages():
    """Test that _import_from_string blocks other packages."""
    with pytest.raises(ValueError, match="is not allowed"):
        _import_from_string("json.loads")


def test_import_from_string_blocks_reexported_modules():
    """Test that _import_from_string blocks re-exported modules from baliza namespace."""
    # baliza.cli imports shutil, so baliza.cli.shutil exists
    # but we should not be able to import it
    with pytest.raises(ValueError, match="Importing module 'shutil' via 'baliza.cli.shutil' is forbidden"):
        _import_from_string("baliza.cli.shutil")

def test_import_from_string_blocks_reexported_classes():
    """Test that _import_from_string blocks re-exported classes from baliza namespace."""
    # baliza.cli imports Panel from rich.panel
    with pytest.raises(ValueError, match="Importing object 'Panel' from 'baliza.cli' is forbidden"):
        _import_from_string("baliza.cli.Panel")

    # baliza.cli imports Path from pathlib
    with pytest.raises(ValueError, match="Importing object 'Path' from 'baliza.cli' is forbidden"):
        _import_from_string("baliza.cli.Path")

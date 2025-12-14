import pytest

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

import pytest
from baliza.utils import validate_identifier, validate_resource_path

def test_validate_identifier_valid():
    assert validate_identifier("valid_name") == "valid_name"
    assert validate_identifier("valid123") == "valid123"
    assert validate_identifier("_valid") == "_valid"

def test_validate_identifier_invalid():
    with pytest.raises(ValueError):
        validate_identifier("invalid-name")
    with pytest.raises(ValueError):
        validate_identifier("invalid name")
    with pytest.raises(ValueError):
        validate_identifier("invalid;drop")

def test_validate_resource_path_valid():
    assert validate_resource_path("contratos") == "contratos"
    assert validate_resource_path("contratacoes/publicacao") == "contratacoes/publicacao"
    assert validate_resource_path("atas-registro-preco") == "atas-registro-preco"
    assert validate_resource_path("my_resource_1") == "my_resource_1"
    assert validate_resource_path("a/b/c") == "a/b/c"

def test_validate_resource_path_invalid():
    # Path traversal attempts
    with pytest.raises(ValueError, match="must not contain '..'"):
        validate_resource_path("../admin")
    with pytest.raises(ValueError, match="must not contain '..'"):
        validate_resource_path("contratos/../atas")
    with pytest.raises(ValueError, match="must not contain '..'"):
        validate_resource_path("dir/..")

    # Invalid characters
    with pytest.raises(ValueError, match="Invalid resource path"):
        validate_resource_path("contratos?id=1")
    with pytest.raises(ValueError, match="Invalid resource path"):
        validate_resource_path("contratos#frag")
    with pytest.raises(ValueError, match="Invalid resource path"):
        validate_resource_path("contratos;drop")

import pytest

from baliza.sql_loader import SQLLoader


def test_sql_loader_basic(tmp_path):
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()
    sample = sql_dir / "sample.sql"
    sample.write_text("SELECT * FROM table WHERE id = $id")

    loader = SQLLoader(sql_dir)
    query = loader.load("sample.sql", id=1)
    assert "1" in query
    # Second load uses cache
    query2 = loader.load("sample.sql", id=2)
    assert query2.startswith("SELECT")


def test_sql_loader_missing_param(tmp_path):
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()
    sample = sql_dir / "sample.sql"
    sample.write_text("SELECT $var")

    loader = SQLLoader(sql_dir)
    with pytest.raises(ValueError):
        loader.load("sample.sql")


def test_sql_loader_missing_file(tmp_path):
    loader = SQLLoader(tmp_path)
    with pytest.raises(FileNotFoundError):
        loader.load("absent.sql")

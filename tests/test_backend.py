import pytest
from src.baliza.backend import connect, load_sql_file
from src.baliza.config import settings


def test_load_sql_file_success():
    content = load_sql_file("init_schema.sql")
    assert "CREATE SCHEMA" in content


def test_load_sql_file_missing():
    with pytest.raises(FileNotFoundError):
        load_sql_file("missing.sql")


def test_connect_uses_duckdb_connect(mocker):
    fake_conn = mocker.Mock()
    mocked_connect = mocker.patch("ibis.duckdb.connect", return_value=fake_conn)
    conn = connect()
    mocked_connect.assert_called_once_with(settings.database_path)
    assert conn is fake_conn
    assert fake_conn.raw_sql.call_count >= 4

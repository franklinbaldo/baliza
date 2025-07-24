import pytest
from unittest import mock
from src.baliza import backend
from src.baliza.config import settings


def test_load_sql_file_success():
    content = backend.load_sql_file("init_schema.sql")
    assert "CREATE SCHEMA" in content


def test_load_sql_file_missing():
    with pytest.raises(FileNotFoundError):
        backend.load_sql_file("missing.sql")


def test_connect_uses_duckdb_connect(mocker):
    fake_conn = mocker.Mock()
    mocked_connect = mocker.patch("ibis.duckdb.connect", return_value=fake_conn)
    conn = backend.connect()
    mocked_connect.assert_called_once_with(settings.DATABASE_PATH)
    assert conn is fake_conn
    assert fake_conn.raw_sql.call_count >= 4

"""BDD step definitions for data_quality.feature."""

import httpx
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner
from baliza.cli_simple import app
import duckdb

runner = CliRunner()

@scenario("../features/data_quality.feature", "Primary key uniqueness is maintained")
def test_pk_uniqueness():
    pass

@scenario("../features/data_quality.feature", "Schema validation for contracts")
def test_schema_validation():
    pass

# --- Givens ---

@given("some records already exist in the data store", target_fixture="existing_records")
def some_records_exist(db_path, monkeypatch):
    # Mock API to return 1 record
    def mock_get(self_client, url, **kwargs):
        resp = httpx.Response(200, json={
            "data": [{"numeroControlePNCP": "CONT-1", "dataPublicacao": "2024-01-01T10:00:00"}],
            "totalPaginas": 1
        })
        resp._request = httpx.Request("GET", url)
        return resp

    monkeypatch.setattr(httpx.Client, "get", mock_get)
    runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])

    with duckdb.connect(str(db_path)) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count == 1
    return ["CONT-1"]

# --- Whens ---

@when("I extract records that overlap with the existing data", target_fixture="overlap_result")
def extract_overlapping(db_path, monkeypatch):
    # Mock API to return same record + a new one
    def mock_get(self_client, url, **kwargs):
        resp = httpx.Response(200, json={
            "data": [
                {"numeroControlePNCP": "CONT-1", "dataPublicacao": "2024-01-01T10:00:00"},
                {"numeroControlePNCP": "CONT-2", "dataPublicacao": "2024-01-01T11:00:00"}
            ],
            "totalPaginas": 1
        })
        resp._request = httpx.Request("GET", url)
        return resp

    monkeypatch.setattr(httpx.Client, "get", mock_get)
    return runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])

@when("I extract data for a specific date", target_fixture="extract_result")
def extract_specific_date(db_path, monkeypatch):
    def mock_get(self_client, url, **kwargs):
        resp = httpx.Response(200, json={
            "data": [{"numeroControlePNCP": "CONT-X", "dataPublicacao": "2024-01-01T10:00:00"}],
            "totalPaginas": 1
        })
        resp._request = httpx.Request("GET", url)
        return resp
    monkeypatch.setattr(httpx.Client, "get", mock_get)
    return runner.invoke(app, ["extract", "--start", "2024-01-01", "--end", "2024-01-01", "--duckdb", str(db_path)])

# --- Thens ---

@then("the data store should not contain any duplicate primary keys")
def check_no_duplicates(db_path):
    with duckdb.connect(str(db_path)) as con:
        dupes = con.execute("SELECT numeroControlePNCP, COUNT(*) FROM baliza_raw.contratos GROUP BY 1 HAVING COUNT(*) > 1").fetchall()
        assert len(dupes) == 0

@then("the total record count should be the sum of new and unique old records")
def check_total_count(db_path):
    with duckdb.connect(str(db_path)) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count == 2 # CONT-1 and CONT-2

@then("the contracts table should have the expected schema")
def check_schema(db_path):
    with duckdb.connect(str(db_path)) as con:
        cols = con.execute("DESCRIBE baliza_raw.contratos").fetchall()
        col_names = [col[0] for col in cols]
        assert "numeroControlePNCP" in col_names
        assert "dataPublicacao" in col_names

@then('essential fields like "numeroControlePNCP" should not be null')
def check_not_null(db_path):
    with duckdb.connect(str(db_path)) as con:
        nulls = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos WHERE numeroControlePNCP IS NULL").fetchone()[0]
        assert nulls == 0

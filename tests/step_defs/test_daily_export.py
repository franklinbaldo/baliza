"""BDD step definitions for daily_export.feature.

Calls DailyExporter.export() directly. The factory fixture lives in
conftest (`make_db_with_contracts`) so the sync/extractor scenarios can
reuse the same schema if needed.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from pytest_bdd import given, parsers, scenario, then, when

from baliza.daily_exporter import DailyExporter

# =============================================================================
# Scenario: Daily export creates all required files (Tier 0)
# =============================================================================


@pytest.mark.tier0
@scenario("../features/daily_export.feature", "Daily export creates all required files")
def test_daily_export_creates_files():
    pass


@given(
    parsers.parse("a DuckDB database with contracts for {date_str}"),
    target_fixture="db_with_contracts",
)
def db_with_contracts(make_db_with_contracts, date_str: str) -> dict:
    return make_db_with_contracts(date_str, count=50)


@when(
    parsers.parse("I export daily parquet for {date_str}"),
    target_fixture="export_daily_result",
)
def run_export_daily(db_with_contracts, tmp_path: Path, date_str: str) -> dict:
    # Feature-file filenames live under `data/daily/` — route them under the
    # scenario's tmp_path so the assertions can check absolute paths.
    output_dir = tmp_path / "data" / "daily"
    exporter = DailyExporter(db_with_contracts["db_path"], "baliza_raw")
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    stats = exporter.export(target, output_dir)
    return {
        **db_with_contracts,
        "output_dir": output_dir,
        "stats": stats,
        "tmp_path": tmp_path,
    }


@then("the following files should exist:")
def check_files_exist(export_daily_result, datatable):
    tmp_path: Path = export_daily_result["tmp_path"]
    # First row is the header "| file |"
    expected = [row[0] for row in datatable[1:]]
    missing = [p for p in expected if not (tmp_path / p).exists()]
    assert not missing, f"Missing files: {missing}"


# =============================================================================
# Scenario: Contratos parquet has correct schema (Tier 0)
# =============================================================================


@pytest.mark.tier0
@scenario("../features/daily_export.feature", "Contratos parquet has correct schema")
def test_contratos_schema():
    pass


@given(parsers.parse("a daily export for {date_str}"), target_fixture="daily_export")
def daily_export(make_db_with_contracts, tmp_path: Path, date_str: str) -> dict:
    ctx = make_db_with_contracts(date_str, count=10)
    output_dir = tmp_path / "data" / "daily"
    exporter = DailyExporter(ctx["db_path"], "baliza_raw")
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    stats = exporter.export(target, output_dir)
    return {**ctx, "output_dir": output_dir, "stats": stats, "tmp_path": tmp_path}


@when("I read contratos.parquet", target_fixture="contratos_parquet")
def read_contratos_parquet(daily_export) -> dict:
    parquet_file = daily_export["output_dir"] / daily_export["date_str"] / "contratos.parquet"
    table = pq.read_table(parquet_file)
    return {**daily_export, "parquet_table": table}


@then("it should have columns:")
def check_parquet_schema(contratos_parquet, datatable):
    schema = contratos_parquet["parquet_table"].schema
    # datatable[0] is the header row: | column | type |
    for name, expected_type in datatable[1:]:
        assert name in schema.names, f"Column '{name}' not found; have {schema.names}"
        actual = str(schema.field(name).type)
        assert expected_type in actual, (
            f"Column '{name}' has type '{actual}', expected substring '{expected_type}'"
        )


# =============================================================================
# Scenario: Orgaos parquet is deduplicated per day (Tier 1)
# =============================================================================


@pytest.mark.tier1
@scenario("../features/daily_export.feature", "Orgaos parquet is deduplicated per day")
def test_orgaos_deduplicated():
    pass


@given(
    parsers.parse(
        "a DuckDB database with {contract_count:d} contracts from {org_count:d} unique orgs"
    ),
    target_fixture="db_with_orgs",
)
def db_with_multiple_orgs(
    make_db_with_contracts, tmp_path: Path, contract_count: int, org_count: int
) -> dict:
    ctx = make_db_with_contracts("2023-01-15", count=contract_count)
    output_dir = tmp_path / "data" / "daily"
    return {
        **ctx,
        "output_dir": output_dir,
        "tmp_path": tmp_path,
        "contract_count": contract_count,
        "org_count": org_count,
    }


@when("I run the daily export", target_fixture="exported_orgs")
def export_orgs_parquet(db_with_orgs) -> dict:
    exporter = DailyExporter(db_with_orgs["db_path"], "baliza_raw")
    stats = exporter.export(date(2023, 1, 15), db_with_orgs["output_dir"])
    return {**db_with_orgs, "stats": stats}


@then(parsers.parse("orgaos.parquet should have at least {count:d} row"))
def check_orgaos_row_count(exported_orgs, count: int):
    parquet_file = exported_orgs["output_dir"] / "2023-01-15" / "orgaos.parquet"
    table = pq.read_table(parquet_file)
    assert table.num_rows >= count, f"Expected ≥{count} rows, got {table.num_rows}"


@then("each org should have contratos_no_dia count")
def check_contratos_count(exported_orgs):
    parquet_file = exported_orgs["output_dir"] / "2023-01-15" / "orgaos.parquet"
    table = pq.read_table(parquet_file)
    assert "contratos_no_dia" in table.schema.names
    for val in table.column("contratos_no_dia"):
        assert val.as_py() > 0, "All orgs should have at least 1 contract"


# =============================================================================
# Scenario: Foreign keys are valid (Tier 1)
# =============================================================================


@pytest.mark.tier1
@scenario("../features/daily_export.feature", "Foreign keys are valid")
def test_foreign_keys_valid():
    pass


@when("I join contratos with orgaos on cnpj_orgao", target_fixture="join_result")
def join_contratos_orgaos(daily_export):
    day_dir = daily_export["output_dir"] / daily_export["date_str"]
    contratos_file = day_dir / "contratos.parquet"
    orgaos_file = day_dir / "orgaos.parquet"

    with duckdb.connect(":memory:") as con:
        rows = con.execute(
            f"""
            SELECT c.numero_controle_pncp, c.cnpj_orgao, o.cnpj
            FROM '{contratos_file}' c
            LEFT JOIN '{orgaos_file}' o ON c.cnpj_orgao = o.cnpj
            """
        ).fetchall()

    return {**daily_export, "join_result": rows}


@then("all contracts should have matching orgs")
def check_all_contracts_have_orgs(join_result):
    for numero, cnpj_orgao, org_cnpj in join_result["join_result"]:
        assert org_cnpj is not None, f"Contract {numero} has no matching org"
        assert cnpj_orgao == org_cnpj, f"CNPJ mismatch for {numero}"


@then("no orphan contracts should exist")
def check_no_orphans(join_result):
    day_dir = join_result["output_dir"] / join_result["date_str"]
    contratos_file = day_dir / "contratos.parquet"
    orgaos_file = day_dir / "orgaos.parquet"

    with duckdb.connect(":memory:") as con:
        orphans = con.execute(
            f"""
            SELECT COUNT(*)
            FROM '{contratos_file}' c
            LEFT JOIN '{orgaos_file}' o ON c.cnpj_orgao = o.cnpj
            WHERE o.cnpj IS NULL
            """
        ).fetchone()[0]
    assert orphans == 0, f"Found {orphans} orphaned contracts"


# =============================================================================
# Scenario: Metadata file contains stats (Tier 2)
# =============================================================================


@pytest.mark.tier2
@scenario("../features/daily_export.feature", "Metadata file contains stats")
def test_metadata_contains_stats():
    pass


@given(
    parsers.parse("a daily export with {count:d} contracts"),
    target_fixture="export_with_metadata",
)
def export_with_metadata(make_db_with_contracts, tmp_path: Path, count: int) -> dict:
    ctx = make_db_with_contracts("2023-01-15", count=count)
    output_dir = tmp_path / "data" / "daily"
    exporter = DailyExporter(ctx["db_path"], "baliza_raw")
    stats = exporter.export(date(2023, 1, 15), output_dir)
    return {**ctx, "output_dir": output_dir, "stats": stats, "count": count}


@when("I read _metadata.json", target_fixture="metadata")
def read_metadata(export_with_metadata) -> dict:
    metadata_file = export_with_metadata["output_dir"] / "2023-01-15" / "_metadata.json"
    with open(metadata_file) as f:
        metadata = json.load(f)
    return {**export_with_metadata, "metadata": metadata}


@then(parsers.parse('schema_version should be "{version}"'))
def check_schema_version(metadata, version: str):
    assert metadata["metadata"]["schema_version"] == version


@then(parsers.parse("tables.contratos.row_count should be {count:d}"))
def check_row_count(metadata, count: int):
    actual = metadata["metadata"]["tables"]["contratos"]["row_count"]
    assert actual == count, f"Expected row_count {count}, got {actual}"


@then(parsers.parse('data_particao should be "{date_str}"'))
def check_data_particao(metadata, date_str: str):
    actual = metadata["metadata"]["data_particao"]
    assert actual == date_str, f"Expected '{date_str}', got '{actual}'"

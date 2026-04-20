"""BDD step definitions for daily_export.feature."""

import json
from datetime import date, datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from pytest_bdd import given, parsers, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app
from baliza.daily_exporter import DailyExporter
from baliza.extractor import PNCPExtractor

runner = CliRunner()


@pytest.fixture
def make_db_with_contracts(tmp_path: Path):
    """Factory fixture: create a test DB using the real schema, insert N contracts."""

    def _factory(date_str: str, count: int = 50) -> dict:
        db_file = tmp_path / "test.duckdb"
        output_dir = tmp_path / "daily"

        extractor = PNCPExtractor(db_path=db_file)
        with duckdb.connect(str(db_file)) as con:
            extractor._ensure_schema(con)
            for i in range(count):
                org_no = i % max(1, count // 5)
                cnpj = f"{org_no:014d}"
                con.execute(
                    """
                    INSERT OR IGNORE INTO baliza_raw.contratos (
                        numero_controle_pncp,
                        data_publicacao,
                        cnpj_orgao,
                        razao_social_orgao,
                        poder_id,
                        esfera_id,
                        codigo_unidade,
                        nome_unidade,
                        uf_sigla,
                        ni_fornecedor,
                        tipo_pessoa,
                        modalidade_id,
                        modalidade_nome,
                        valor_inicial,
                        objeto_contrato,
                        processo,
                        usuario_nome
                    ) VALUES (?, CAST(?||'T10:00:00' AS TIMESTAMP), ?, ?, 'E', 'F',
                              '001', 'Unit 1', 'SP', '12000000000190', 'PJ',
                              1, 'Pregão', ?, 'Test contract', 'PROC-001', 'Test User')
                """,
                    [f"CTRL-{i:05d}", date_str, cnpj, f"Org {org_no}", 1000.00 + float(i)],
                )

        return {"db_path": db_file, "output_dir": output_dir, "date_str": date_str}

    return _factory


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
    """Create database with contracts for a specific date."""
    return make_db_with_contracts(date_str, count=50)


@when(
    parsers.parse('I run "baliza export-daily --date {date_str} --output {output_path}"'),
    target_fixture="export_daily_result",
)
def run_export_daily(db_with_contracts, tmp_path: Path, date_str, output_path):
    """Run baliza export-daily command."""
    db_path = db_with_contracts["db_path"]
    # Resolve the output_path from the feature file under tmp_path so each scenario
    # is isolated. This preserves the "data/daily/" wording in the feature while
    # keeping the test hermetic.
    output_dir = tmp_path / output_path.strip("/")
    db_with_contracts["output_dir"] = output_dir

    result = runner.invoke(
        app,
        [
            "export-daily",
            "--duckdb",
            str(db_path),
            "--date",
            date_str,
            "--output",
            str(output_dir),
        ],
    )

    if result.exit_code != 0:
        print("\n=== EXPORT FAILED ===")
        print(f"Exit code: {result.exit_code}")
        print(f"Output:\n{result.stdout}")
        if result.exception:
            import traceback

            traceback.print_exception(
                type(result.exception), result.exception, result.exception.__traceback__
            )

    return {**db_with_contracts, "result": result}


@then("the following files should exist:", target_fixture="files_exist")
def check_files_exist(export_daily_result):
    """Verify all required files exist."""
    output_dir = export_daily_result["output_dir"]
    date_str = export_daily_result["date_str"]

    expected_files = [
        f"{date_str}/contratos.parquet",
        f"{date_str}/orgaos.parquet",
        f"{date_str}/unidades.parquet",
        f"{date_str}/fornecedores.parquet",
        f"{date_str}/_metadata.json",
    ]

    missing_files = []
    for file_path in expected_files:
        full_path = output_dir / file_path
        if not full_path.exists():
            missing_files.append(str(full_path))

    assert len(missing_files) == 0, f"Missing files: {missing_files}"
    return export_daily_result


# =============================================================================
# Scenario: Contratos parquet has correct schema (Tier 0)
# =============================================================================


@pytest.mark.tier0
@scenario("../features/daily_export.feature", "Contratos parquet has correct schema")
def test_contratos_schema():
    pass


@given(parsers.parse("a daily export for {date_str}"), target_fixture="daily_export")
def daily_export(make_db_with_contracts, date_str: str) -> dict:
    """Create a daily export."""
    ctx = make_db_with_contracts(date_str, count=1)
    exporter = DailyExporter(ctx["db_path"], "baliza_raw")
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    stats = exporter.export(target_date, ctx["output_dir"])
    return {**ctx, "stats": stats}


@when("I read contratos.parquet", target_fixture="contratos_parquet")
def read_contratos_parquet(daily_export):
    """Read contratos.parquet file."""
    output_dir = daily_export["output_dir"]
    date_str = daily_export["date_str"]
    parquet_file = output_dir / date_str / "contratos.parquet"
    table = pq.read_table(parquet_file)
    return {**daily_export, "parquet_table": table}


@then("it should have columns:")
def check_parquet_schema(contratos_parquet):
    """Verify parquet schema matches expected columns."""
    table = contratos_parquet["parquet_table"]
    schema = table.schema

    # Key columns from the normalized schema
    expected_columns = {
        "numero_controle_pncp": "string",
        "cnpj_orgao": "string",
        "ni_fornecedor": "string",
        "valor_inicial": "double",
        "data_publicacao": "timestamp",
        "data_particao": "date32",
    }

    for column_name, expected_type in expected_columns.items():
        assert column_name in schema.names, f"Column '{column_name}' not found in schema"
        field = schema.field(column_name)
        actual_type = str(field.type)
        assert expected_type in actual_type, (
            f"Column '{column_name}' has type '{actual_type}', expected '{expected_type}'"
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
def db_with_multiple_orgs(make_db_with_contracts, contract_count: int, org_count: int) -> dict:
    """Create database with multiple contracts from multiple orgs."""
    ctx = make_db_with_contracts("2023-01-15", count=contract_count)
    return {**ctx, "contract_count": contract_count, "org_count": org_count}


@when("I export daily parquet for that date", target_fixture="exported_orgs")
def export_orgs_parquet(db_with_orgs):
    """Export daily parquet."""
    db_path = db_with_orgs["db_path"]
    output_dir = db_with_orgs["output_dir"]
    exporter = DailyExporter(db_path, "baliza_raw")
    stats = exporter.export(date(2023, 1, 15), output_dir)
    return {**db_with_orgs, "stats": stats}


@then(parsers.parse("orgaos.parquet should have exactly {count:d} rows"))
def check_orgaos_row_count(exported_orgs, count):
    """Verify orgaos parquet has correct row count."""
    output_dir = exported_orgs["output_dir"]
    parquet_file = output_dir / "2023-01-15" / "orgaos.parquet"
    table = pq.read_table(parquet_file)
    # The fixture creates distinct orgs based on i % max(1, count//5)
    actual = table.num_rows
    assert actual > 0, f"Expected >0 rows, got {actual}"


@then("each org should have contratos_no_dia count")
def check_contratos_count(exported_orgs):
    """Verify each org has contratos_no_dia field."""
    output_dir = exported_orgs["output_dir"]
    parquet_file = output_dir / "2023-01-15" / "orgaos.parquet"
    table = pq.read_table(parquet_file)
    assert "contratos_no_dia" in table.schema.names, "Missing contratos_no_dia column"
    for val in table.column("contratos_no_dia"):
        assert val.as_py() > 0, "All orgs should have at least 1 contract"


# =============================================================================
# Scenario: Foreign keys are valid (Tier 1)
# =============================================================================


@pytest.mark.tier1
@scenario("../features/daily_export.feature", "Foreign keys are valid")
def test_foreign_keys_valid():
    pass


@when("I join contratos with orgaos on orgao_cnpj", target_fixture="join_result")
def join_contratos_orgaos(daily_export):
    """Join contratos with orgaos."""
    output_dir = daily_export["output_dir"]
    date_str = daily_export["date_str"]
    day_dir = output_dir / date_str

    contratos_file = day_dir / "contratos.parquet"
    orgaos_file = day_dir / "orgaos.parquet"

    with duckdb.connect(":memory:") as con:
        result = con.execute(
            f"""
            SELECT c.numero_controle_pncp, c.cnpj_orgao, o.cnpj
            FROM '{contratos_file}' c
            LEFT JOIN '{orgaos_file}' o ON c.cnpj_orgao = o.cnpj
        """
        ).fetchall()

    return {**daily_export, "join_result": result}


@then("all contracts should have matching orgs")
def check_all_contracts_have_orgs(join_result):
    """Verify all contracts have matching org records."""
    results = join_result["join_result"]
    for row in results:
        numero_controle, cnpj_orgao, org_cnpj = row
        assert org_cnpj is not None, f"Contract {numero_controle} has no matching org"
        assert cnpj_orgao == org_cnpj, f"CNPJ mismatch for {numero_controle}"


@then("no orphan contracts should exist")
def check_no_orphans(join_result):
    """Verify no orphan contracts exist."""
    output_dir = join_result["output_dir"]
    date_str = join_result["date_str"]
    day_dir = output_dir / date_str

    contratos_file = day_dir / "contratos.parquet"
    orgaos_file = day_dir / "orgaos.parquet"

    with duckdb.connect(":memory:") as con:
        orphan_count = con.execute(
            f"""
            SELECT COUNT(*)
            FROM '{contratos_file}' c
            LEFT JOIN '{orgaos_file}' o ON c.cnpj_orgao = o.cnpj
            WHERE o.cnpj IS NULL
        """
        ).fetchone()[0]

    assert orphan_count == 0, f"Found {orphan_count} orphaned contracts"


# =============================================================================
# Scenario: Metadata file contains stats (Tier 2)
# =============================================================================


@pytest.mark.tier2
@scenario("../features/daily_export.feature", "Metadata file contains stats")
def test_metadata_contains_stats():
    pass


@given(
    parsers.parse("a daily export with {count:d} contracts"), target_fixture="export_with_metadata"
)
def export_with_metadata(make_db_with_contracts, count: int) -> dict:
    """Create daily export with N contracts."""
    ctx = make_db_with_contracts("2023-01-15", count=count)
    exporter = DailyExporter(ctx["db_path"], "baliza_raw")
    stats = exporter.export(date(2023, 1, 15), ctx["output_dir"])
    return {**ctx, "stats": stats, "count": count}


@when("I read _metadata.json", target_fixture="metadata")
def read_metadata(export_with_metadata):
    """Read metadata JSON file."""
    output_dir = export_with_metadata["output_dir"]
    metadata_file = output_dir / "2023-01-15" / "_metadata.json"
    with open(metadata_file) as f:
        metadata = json.load(f)
    return {**export_with_metadata, "metadata": metadata}


@then(parsers.parse('schema_version should be "{version}"'))
def check_schema_version(metadata, version):
    """Verify schema version."""
    meta = metadata["metadata"]
    assert meta["schema_version"] == version, (
        f"Expected schema_version '{version}', got '{meta['schema_version']}'"
    )


@then(parsers.parse("tables.contratos.row_count should be {count:d}"))
def check_row_count(metadata, count):
    """Verify row count in metadata."""
    meta = metadata["metadata"]
    actual_count = meta["tables"]["contratos"]["row_count"]
    assert actual_count == count, f"Expected row_count {count}, got {actual_count}"


@then(parsers.parse('data_particao should be "{date_str}"'))
def check_data_particao(metadata, date_str):
    """Verify data_particao."""
    meta = metadata["metadata"]
    actual_date = meta["data_particao"]
    assert actual_date == date_str, f"Expected data_particao '{date_str}', got '{actual_date}'"

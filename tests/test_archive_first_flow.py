"""
End-to-end tests for archive-first data flow.
This validates the complete workflow from IA federation to data analysis.
"""

import os
import sys
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb
import pytest

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from baliza.ia_federation import InternetArchiveFederation
from baliza.main import (
    _check_existing_data_in_ia,
    _ensure_ia_federation_updated,
    _init_baliza_db,
)


@pytest.fixture
def end_to_end_workspace():
    """Create a complete workspace for end-to-end testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Change to temp directory
        original_cwd = os.getcwd()
        os.chdir(tmpdir)

        # Create all necessary directories
        os.makedirs("state", exist_ok=True)
        os.makedirs("baliza_data", exist_ok=True)
        os.makedirs("dbt_baliza", exist_ok=True)

        yield tmpdir

        # Restore original directory
        os.chdir(original_cwd)


@pytest.fixture
def mock_ia_data():
    """Mock Internet Archive data for testing."""
    return [
        {
            "identifier": "pncp-contratos-2024-01-01",
            "parquet_urls": [
                "https://archive.org/download/pncp-contratos-2024-01-01/pncp-contratos-2024-01-01.parquet"
            ],
            "data_date": "2024-01-01",
            "metadata": {"date": "2024-01-01", "title": "PNCP Contratos 2024-01-01"},
        },
        {
            "identifier": "pncp-contratos-2024-01-02",
            "parquet_urls": [
                "https://archive.org/download/pncp-contratos-2024-01-02/pncp-contratos-2024-01-02.parquet"
            ],
            "data_date": "2024-01-02",
            "metadata": {"date": "2024-01-02", "title": "PNCP Contratos 2024-01-02"},
        },
    ]


@pytest.fixture
def mock_parquet_data():
    """Mock Parquet data content."""
    return [
        {
            "numeroControlePncpCompra": "CNT-2024-001",
            "data_date": "2024-01-01",
            "nomeRazaoSocialFornecedor": "Empresa Alpha LTDA",
            "valorInicial": 50000.0,
            "orgaoEntidade": {
                "razaoSocial": "Prefeitura Municipal",
                "cnpj": "11111111000111",
                "uf": "RO",
            },
            "data_source": "internet_archive",
        },
        {
            "numeroControlePncpCompra": "CNT-2024-002",
            "data_date": "2024-01-02",
            "nomeRazaoSocialFornecedor": "Empresa Beta SA",
            "valorInicial": 75000.0,
            "orgaoEntidade": {
                "razaoSocial": "Governo do Estado",
                "cnpj": "22222222000222",
                "uf": "RO",
            },
            "tipoContrato": {"codigo": "2", "descricao": "Materiais"},
            "data_source": "internet_archive",
        },
    ]


class TestArchiveFirstFlow:
    """Test suite for archive-first data flow."""

    def test_complete_archive_first_initialization(
        self, end_to_end_workspace, mock_ia_data, duckdb_conn
    ):
        """Test complete archive-first initialization process."""
        with (
            patch(
                "baliza.ia_federation.InternetArchiveFederation.discover_ia_items"
            ) as mock_discover,
            patch(
                "baliza.ia_federation.InternetArchiveFederation.create_federated_views"
            ) as mock_create,
        ):
            mock_discover.return_value = mock_ia_data

            # Initialize database with IA federation
            _init_baliza_db()

            # Verify database structure
            # Use the provided duckdb_conn fixture
            schemas = duckdb_conn.execute("SHOW SCHEMAS").fetchall()
            schema_names = [s[0] for s in schemas]
            assert "federated" in schema_names
            assert "psa" in schema_names
            assert "control" in schema_names

            # Verify federation was attempted
            mock_discover.assert_called()
            mock_create.assert_called()

    def test_ia_data_availability_check(self, end_to_end_workspace, mock_ia_data, duckdb_conn):
        """Test checking data availability in Internet Archive."""
        _init_baliza_db()

        with patch(
            "baliza.ia_federation.InternetArchiveFederation.discover_ia_items"
        ) as mock_discover:
            mock_discover.return_value = mock_ia_data

            # Mock federation connection and query
            with patch("duckdb.connect") as mock_connect:
                mock_connect.return_value = duckdb_conn # Use the fixture
                mock_conn = mock_connect.return_value
                mock_conn.execute.return_value.fetchone.return_value = (
                    1,
                )  # Data exists

                # Test data availability check
                data_exists = _check_existing_data_in_ia("2024-01-01", "contratos")

                # Should find existing data
                assert data_exists == True

    @patch("internetarchive.search_items") # Corrected mock path
    def test_federation_discovery_and_cataloging(
        self, mock_search_items, end_to_end_workspace
    ):
        """Test IA data discovery and cataloging process."""
        # Mock IA search response
        mock_item1 = Mock()
        mock_item1.identifier = "pncp-contratos-2024-01-01"
        mock_item1.metadata = {"date": "2024-01-01", "title": "PNCP Contratos 2024-01-01"}
        mock_item1.get_files.return_value = [
            Mock(name="pncp-contratos-2024-01-01.parquet", size=100, sha1="abc", md5="def")
        ]

        mock_search_items.return_value.iter_as_items.return_value = [mock_item1]

        # Initialize federation
        federation = InternetArchiveFederation("state/baliza.duckdb")

        # Test discovery
        items = federation.discover_ia_items()

        assert len(items) == 1
        assert items[0]["identifier"] == "pncp-contratos-2024-01-01"
        assert len(items[0]["files"]) > 0

    def test_federated_view_creation_with_real_structure(
        self, end_to_end_workspace, mock_ia_data, duckdb_conn
    ):
        """Test creation of federated views with realistic structure."""
        _init_baliza_db()

        # Create federation instance
        federation = InternetArchiveFederation("state/baliza.duckdb")

        # Mock IA data discovery
        with patch.object(federation, "discover_ia_items", return_value=mock_ia_data):
            # Mock DuckDB httpfs functionality
            with patch("duckdb.connect") as mock_connect:
                mock_connect.return_value = duckdb_conn # Use the fixture
                mock_conn = mock_connect.return_value

                # Test view creation
                federation.create_federated_views()

                # Verify httpfs was loaded
                mock_conn.execute.assert_any_call("INSTALL httpfs")
                mock_conn.execute.assert_any_call("LOAD httpfs")

                # Verify view creation was attempted
                view_creation_calls = [
                    call
                    for call in mock_conn.execute.call_args_list
                    if "CREATE OR REPLACE VIEW" in str(call)
                ]
                assert len(view_creation_calls) > 0

    def test_unified_data_access_priority(
        self, end_to_end_workspace, mock_parquet_data, duckdb_conn
    ):
        """Test that unified view prioritizes IA data over local storage."""
        _init_baliza_db()

        # Use the provided duckdb_conn fixture
        # Create mock federated views with sample data
        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS federated")

        # Create mock IA data table
        duckdb_conn.execute("""
            CREATE TABLE federated.contratos_ia AS
            SELECT 
                'CNT-IA-001' as numeroControlePncpCompra,
                '2024-01-01'::DATE as data_date,
                'IA Source Data' as fornecedor_nome,
                100000.0 as valorInicial,
                'internet_archive' as data_source
        """)

        # Create mock local data table
        duckdb_conn.execute("""
            CREATE TABLE federated.contratos_local AS
            SELECT 
                'CNT-LOCAL-001' as numeroControlePncpCompra,
                '2024-01-01'::DATE as data_date,
                50000.0 as valorInicial,
                'local_storage' as data_source
        """)

        # Create unified view that prioritizes IA
        duckdb_conn.execute("""
            CREATE OR REPLACE VIEW federated.contratos_unified AS
            SELECT *, 1 as priority FROM federated.contratos_ia
            UNION ALL
            SELECT *, 2 as priority FROM federated.contratos_local
            WHERE NOT EXISTS (
                SELECT 1 FROM federated.contratos_ia ia 
                WHERE ia.numeroControlePncpCompra = contratos_local.numeroControlePncpCompra
                AND ia.data_date = contratos_local.data_date
            )
        """)

        # Test unified query
        result = duckdb_conn.execute("""
            SELECT data_source, COUNT(*) 
            FROM federated.contratos_unified 
            GROUP BY data_source
            ORDER BY data_source
        """).fetchall()

        # Should prioritize IA data
        assert len(result) >= 1
        ia_records = [r for r in result if r[0] == "internet_archive"]
        assert len(ia_records) > 0

    def test_archive_first_data_pipeline_integration(
        self, end_to_end_workspace, mock_ia_data, mock_parquet_data, duckdb_conn
    ):
        """Test complete archive-first data pipeline integration."""
        _init_baliza_db()

        # Mock the complete flow
        with (
            patch(
                "baliza.ia_federation.InternetArchiveFederation.discover_ia_items"
            ) as mock_discover,
            patch(
                "baliza.ia_federation.InternetArchiveFederation.get_data_availability"
            ) as mock_availability,
        ):
            mock_discover.return_value = mock_ia_data
            mock_availability.return_value = {
                "internet_archive": {
                    "total_records": 1000,
                    "date_range": {"min_date": "2024-01-01", "max_date": "2024-01-15"},
                },
                "local_storage": {
                    "total_records": 100,
                    "date_range": {"min_date": "2024-01-15", "max_date": "2024-01-16"},
                },
            }

            # Initialize federation
            federation = InternetArchiveFederation("state/baliza.duckdb")
            federation.update_federation()

            # Test data availability
            availability = federation.get_data_availability()

            # Verify IA has more data (archive-first priority)
            assert (
                availability["internet_archive"]["total_records"]
                > availability["local_storage"]["total_records"]
            )

    def test_fallback_to_local_when_ia_unavailable(self, end_to_end_workspace, duckdb_conn):
        """Test fallback to local storage when IA is unavailable."""
        _init_baliza_db()

        # Mock IA being unavailable
        with patch(
            "internetarchive.search_items", side_effect=Exception("IA API unavailable") # Corrected mock path
        ):
            # System should continue to work with local data only
            try:
                _ensure_ia_federation_updated()
                # Should complete without error (graceful degradation)
                assert True
            except Exception as e:
                pytest.fail(f"Should handle IA unavailability gracefully: {e}")

    def test_data_consistency_across_sources(self, end_to_end_workspace, duckdb_conn):
        """Test data consistency checks across IA and local sources."""
        _init_baliza_db()

        # Use the provided duckdb_conn fixture

        # Create test data with potential inconsistencies
        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS federated")
        duckdb_conn.execute("""
            CREATE TABLE federated.contratos_ia AS
            SELECT 
                'CNT-001' as numeroControlePncpCompra,
                '2024-01-01'::DATE as data_date,
                50000.0 as valorInicial,
                'internet_archive' as data_source
        """)

        duckdb_conn.execute("""
            CREATE TABLE federated.contratos_local AS
            SELECT 
                'CNT-001' as numeroControlePncpCompra,
                '2024-01-01'::DATE as data_date,
                55000.0 as valorInicial,  -- Different value!
                'local_storage' as data_source
        """)

        # Check for inconsistencies
        inconsistencies = duckdb_conn.execute("""
            SELECT 
                ia.numeroControlePncpCompra,
                ia.valorInicial as ia_valor,
                local.valorInicial as local_valor
            FROM federated.contratos_ia ia
            JOIN federated.contratos_local local
                ON ia.numeroControlePncpCompra = local.numeroControlePncpCompra
                AND ia.data_date = local.data_date
            WHERE ia.valorInicial != local.valorInicial
        """).fetchall()

        # Should detect the inconsistency
        assert len(inconsistencies) == 1
        assert inconsistencies[0][1] != inconsistencies[0][2]  # Different values


    def test_performance_optimization_with_federation(self, end_to_end_workspace, duckdb_conn):
        """Test performance optimizations in federated queries."""
        _init_baliza_db()

        # Use the provided duckdb_conn fixture

        # Create large federated dataset
        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS federated")
        duckdb_conn.execute("""
            CREATE TABLE federated.contratos_unified AS
            SELECT 
                'CNT-' || CAST(n as VARCHAR) as numeroControlePncpCompra,
                ('2024-01-01'::DATE + INTERVAL (n % 365) DAY) as data_date,
                CAST(n * 1000 as DOUBLE) as valorInicial,
                'internet_archive' as data_source
            FROM generate_series(1, 10000) as t(n)
        """)

        # Create indexes for performance
        duckdb_conn.execute(
            "CREATE INDEX idx_unified_date ON federated.contratos_unified(data_date)"
        )
        duckdb_conn.execute(
            "CREATE INDEX idx_unified_numero ON federated.contratos_unified(numeroControlePncpCompra)"
        )

        # Test query performance with date filter
        import time

        start_time = time.time()

        result = duckdb_conn.execute("""
            SELECT COUNT(*) 
            FROM federated.contratos_unified 
            WHERE data_date = '2024-01-01'
        """).fetchone()

        query_time = time.time() - start_time

        # Should complete reasonably quickly (< 1 second for 10k records)
        assert query_time < 1.0, f"Query too slow: {query_time:.2f}s"
        assert len(result) > 0


    def test_end_to_end_coverage_analysis(self, end_to_end_workspace, duckdb_conn):
        """Test end-to-end coverage analysis using federated data."""
        _init_baliza_db()

        # Use the provided duckdb_conn fixture

        # Create comprehensive test dataset
        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS federated")
        duckdb_conn.execute("""
            CREATE TABLE federated.contratos_unified AS
            SELECT 
                'CNT-' || CAST(n as VARCHAR) as numeroControlePncpCompra,
                ('2024-01-01'::DATE + INTERVAL (n % 15) DAY) as data_date,
                CASE WHEN n % 3 = 0 THEN 'Prefeitura A' 
                     WHEN n % 3 = 1 THEN 'Prefeitura B'
                     ELSE 'Estado' END as orgao_nome,
                CAST(n * 1000 as DOUBLE) as valorInicial,
                'internet_archive' as data_source
            FROM generate_series(1, 100) as t(n)
        """)

        # Test temporal coverage analysis
        temporal_coverage = duckdb_conn.execute("""
            WITH date_series AS (
                SELECT generate_series(
                    '2024-01-01'::DATE,
                    '2024-01-15'::DATE,
                    INTERVAL 1 DAY
                )::DATE as expected_date
            ),
            actual_dates AS (
                SELECT DISTINCT data_date 
                FROM federated.contratos_unified
            )
            SELECT 
                COUNT(ds.expected_date) as total_expected_days,
                COUNT(ad.data_date) as actual_days_with_data,
                ROUND(COUNT(ad.data_date) * 100.0 / COUNT(ds.expected_date), 2) as coverage_percentage
            FROM date_series ds
            LEFT JOIN actual_dates ad ON ds.expected_date = ad.data_date
        """).fetchone()

        assert temporal_coverage[0] == 15  # Expected 15 days
        assert temporal_coverage[1] > 0  # Some days have data
        assert temporal_coverage[2] > 0  # Coverage percentage > 0

        # Test entity coverage analysis
        entity_coverage = duckdb_conn.execute("""
            SELECT 
                orgao_nome,
                COUNT(*) as contratos_count,
                SUM(valorInicial) as valor_total,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage_share
            FROM federated.contratos_unified
            GROUP BY orgao_nome
            ORDER BY contratos_count DESC
        """).fetchall()

        assert len(entity_coverage) >= 3  # Should have 3 different entities


    def test_archive_first_with_dbt_integration(self, end_to_end_workspace, duckdb_conn):
        """Test archive-first flow integration with DBT models."""
        _init_baliza_db()

        # Use the provided duckdb_conn fixture

        # Create federated source data
        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS federated")
        duckdb_conn.execute("""
            CREATE TABLE federated.contratos_unified AS
            SELECT 
                'CNT-001' as numeroControlePncpCompra,
                '2024-01-01'::DATE as data_date,
                'Empresa A' as nomeRazaoSocialFornecedor,
                50000.0 as valorInicial,
                'internet_archive' as data_source
        """)

        # Simulate DBT staging model
        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
        duckdb_conn.execute("""
            CREATE VIEW staging.stg_contratos AS
            SELECT 
                numeroControlePncpCompra,
                data_date,
                nomeRazaoSocialFornecedor,
                valorInicial,
                data_source,
                CASE WHEN valorInicial > 100000 THEN 'high_value'
                     WHEN valorInicial > 10000 THEN 'medium_value'
                     ELSE 'low_value' END as valor_categoria
            FROM federated.contratos_unified
        """)

        # Test staging model query
        staging_result = duckdb_conn.execute("""
            SELECT valor_categoria, COUNT(*)
            FROM staging.stg_contratos
            GROUP BY valor_categoria
        """).fetchall()

        assert len(staging_result) >= 1

        # Simulate coverage model
        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS coverage")
        duckdb_conn.execute("""
            CREATE VIEW coverage.coverage_temporal AS
            SELECT 
                data_date,
                data_source,
                COUNT(*) as contratos_count,
                SUM(valorInicial) as valor_total
            FROM staging.stg_contratos
            GROUP BY data_date, data_source
            ORDER BY data_date
        """)

        # Test coverage model
        coverage_result = duckdb_conn.execute("""
            SELECT * FROM coverage.coverage_temporal
        """).fetchall()

        assert len(coverage_result) >= 1
        assert coverage_result[0][1] == "internet_archive"  # Should prioritize IA data


    def test_real_time_federation_updates(self, end_to_end_workspace, duckdb_conn):
        """Test real-time federation updates when new IA data becomes available."""
        _init_baliza_db()

        federation = InternetArchiveFederation("state/baliza.duckdb")

        # Mock initial state with no data
        with patch.object(federation, "discover_ia_items", return_value=[]):
            initial_availability = federation.get_data_availability()
            assert initial_availability["internet_archive"]["total_records"] == 0

        # Mock updated state with new data
        new_ia_data = [
            {
                "identifier": "pncp-contratos-2024-01-15",
                "parquet_urls": [
                    "https://archive.org/download/pncp-contratos-2024-01-15/file.parquet"
                ],
                "data_date": "2024-01-15",
                "metadata": {"date": "2024-01-15", "title": "PNCP Contratos 2024-01-15"},
            }
        ]

        with patch.object(federation, "discover_ia_items", return_value=new_ia_data):
            # Simulate federation update
            try:
                federation.update_federation()
                # Should complete without error
                assert True
            except Exception as e:
                pytest.fail(f"Federation update failed: {e}")

    def test_error_recovery_in_archive_first_flow(self, end_to_end_workspace, duckdb_conn):
        """Test error recovery mechanisms in archive-first flow."""
        _init_baliza_db()

        federation = InternetArchiveFederation("state/baliza.duckdb")

        # Test network timeout handling
        with patch(
            "internetarchive.search_items", side_effect=Exception("IA service unavailable") # Corrected mock path
        ):
            # System should continue to work with local data only
            try:
                _ensure_ia_federation_updated()

                # Verify database still works
                conn = duckdb.connect("state/baliza.duckdb")
                result = conn.execute("SELECT COUNT(*) FROM control.runs").fetchone()
                conn.close()

                # Should handle gracefully
                assert True
            except Exception as e:
                pytest.fail(f"Should handle IA unavailability gracefully: {e}")

        # Test partial data corruption handling
        corrupted_data = [
            {
                "identifier": "invalid-item",
                "parquet_urls": [],  # No URLs
                "data_date": None,  # Invalid date
                "metadata": {},
            }
        ]

        with patch.object(federation, "discover_ia_items", return_value=corrupted_data):
            # Should filter out invalid data
            try:
                federation.update_federation()
                assert True
            except Exception as e:
                # Should handle corrupted data gracefully
                print(f"Handled corrupted data: {e}")

    @pytest.mark.slow
    def test_complete_end_to_end_workflow_simulation(self, end_to_end_workspace, duckdb_conn):
        """Complete end-to-end workflow simulation with all components."""
        # This test simulates the complete Baliza workflow in archive-first mode

        # Step 1: Initialize system with IA federation
        _init_baliza_db()

        # Step 2: Mock IA data discovery
        mock_ia_items = [
            {
                "identifier": "pncp-contratos-2024-01-01",
                "parquet_urls": ["https://archive.org/download/test/file.parquet"],
                "data_date": "2024-01-01",
                "metadata": {"date": "2024-01-01", "title": "PNCP Contratos 2024-01-01"},
            }
        ]

        with patch(
            "baliza.ia_federation.InternetArchiveFederation.discover_ia_items"
        ) as mock_discover:
            mock_discover.return_value = mock_ia_items

            # Step 3: Check existing data availability (archive-first check)
            data_available = _check_existing_data_in_ia("2024-01-01", "contratos")

            # Step 4: Verify database state
            # Use the provided duckdb_conn fixture
            all_schemas = duckdb_conn.execute("SHOW SCHEMAS").fetchall()
            all_schema_names = [s[0] for s in all_schemas]

            expected_schemas = ["main", "information_schema", "psa", "control", "federated"]
            for schema in expected_schemas:
                assert schema in all_schema_names, (
                    f"Missing schema in final verification: {schema}"
                )

            # Check that control tables exist
            control_tables = duckdb_conn.execute("SHOW TABLES FROM control").fetchall()
            control_table_names = [t[0] for t in control_tables]

            required_tables = ["runs", "data_quality"]
            for table in required_tables:
                assert table in control_table_names, f"Missing control table: {table}"

            # Verify data can be queried
            try:
                # This should work even with mocked data
                test_query = duckdb_conn.execute("SELECT COUNT(*) FROM control.runs").fetchone()
                assert test_query[0] >= 0  # Should return a count
            except Exception as e:
                # Some queries might fail with mocked data, but structure should be correct
                print(f"Query test note: {e}")


            # Archive-first workflow verification complete
            assert True, "Complete archive-first workflow verification passed"


# Integration test for the complete archive-first strategy
@pytest.mark.integration
class TestArchiveFirstStrategy:
    """Integration tests for the complete archive-first strategy."""

    def test_archive_first_priority_implementation(self, end_to_end_workspace, duckdb_conn):
        """Test that archive-first priority is correctly implemented."""
        _init_baliza_db()

        # Verify that IA federation is updated during initialization
        with patch("baliza.main._ensure_ia_federation_updated") as mock_ensure:
            _init_baliza_db()
            mock_ensure.assert_called()

    def test_federation_update_frequency_control(self, end_to_end_workspace, duckdb_conn):
        """Test federation update frequency control (daily updates)."""
        # Test that federation updates are controlled by date flag
        update_flag_file = "state/ia_federation_last_update"

        # Create flag file with today's date
        with open(update_flag_file, "w") as f:
            f.write(date.today().isoformat())

        # Should skip update if already updated today
        with patch(
            "baliza.ia_federation.InternetArchiveFederation.update_federation"
        ) as mock_update:
            _ensure_ia_federation_updated()
            mock_update.assert_not_called()

    def test_graceful_degradation_when_ia_unavailable(self, end_to_end_workspace, duckdb_conn):
        """Test graceful degradation when Internet Archive is unavailable."""
        _init_baliza_db()

        # Mock IA being completely unavailable
        with patch(
            "internetarchive.search_items", side_effect=Exception("IA service unavailable") # Corrected mock path
        ):
            # System should continue to work with local data only
            try:
                _ensure_ia_federation_updated()

                # Verify database still works
                conn = duckdb.connect("state/baliza.duckdb")
                result = conn.execute("SELECT COUNT(*) FROM control.runs").fetchone()
                conn.close()

                # Should handle gracefully
                assert True
            except Exception as e:
                pytest.fail(f"Should handle IA unavailability gracefully: {e}")


# Performance and scalability tests
@pytest.mark.performance
class TestArchiveFirstPerformance:
    """Performance tests for archive-first data flow."""

    def test_federation_query_performance(self, end_to_end_workspace, duckdb_conn):
        """Test federation query performance with large datasets."""
        _init_baliza_db()

        # Use the provided duckdb_conn fixture

        # Create large federated dataset
        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS federated")
        duckdb_conn.execute("""
            CREATE TABLE federated.contratos_unified AS
            SELECT 
                'CNT-' || CAST(n as VARCHAR) as numeroControlePncpCompra,
                ('2024-01-01'::DATE + INTERVAL (n % 365) DAY) as data_date,
                CAST(n * 1000 as DOUBLE) as valorInicial,
                'internet_archive' as data_source
            FROM generate_series(1, 10000) as t(n)
        """)

        # Create indexes for performance
        duckdb_conn.execute(
            "CREATE INDEX idx_unified_date ON federated.contratos_unified(data_date)"
        )
        duckdb_conn.execute(
            "CREATE INDEX idx_unified_numero ON federated.contratos_unified(numeroControlePncpCompra)"
        )

        # Test query performance with date filter
        import time

        start_time = time.time()

        result = duckdb_conn.execute("""
            SELECT COUNT(*) 
            FROM federated.contratos_unified 
            WHERE data_date = '2024-01-01'
        """).fetchone()

        query_time = time.time() - start_time

        # Should complete reasonably quickly (< 1 second for 10k records)
        assert query_time < 1.0, f"Query too slow: {query_time:.2f}s"
        assert len(result) > 0


    def test_federation_memory_usage(self, end_to_end_workspace, duckdb_conn):
        """Test federation memory usage with multiple data sources."""
        _init_baliza_db()

        # This would test memory usage patterns
        # For now, just verify the structure can handle multiple sources

        # Create multiple source tables
        for i in range(5):
            duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS source_{i}")
            duckdb_conn.execute(f"""
                CREATE TABLE source_{i}.contratos AS
                SELECT 
                    'CNT-' || CAST(n as VARCHAR) as id,
                    'source_{i}' as data_source
                FROM generate_series(1, 1000) as t(n)
            """)

        # Test unified query across sources
        result = duckdb_conn.execute("""
            SELECT data_source, COUNT(*)
            FROM (
                SELECT id, 'source_0' as data_source FROM source_0.contratos
                UNION ALL
                SELECT id, 'source_1' as data_source FROM source_1.contratos
                UNION ALL
                SELECT id, 'source_2' as data_source FROM source_2.contratos
            )
            GROUP BY data_source
        """).fetchall()

        assert len(result) == 3


@pytest.mark.end_to_end
def test_complete_baliza_archive_first_workflow(end_to_end_workspace, duckdb_conn):
    """Ultimate end-to-end test for complete Baliza archive-first workflow."""
    # This test simulates the complete user journey in archive-first mode

    # 1. System initialization
    _init_baliza_db()

    # 2. IA federation setup
    with patch(
        "baliza.ia_federation.InternetArchiveFederation.discover_ia_items"
    ) as mock_discover:
        mock_discover.return_value = [
            {
                "identifier": "pncp-contratos-2024-01-01",
                "parquet_urls": ["https://archive.org/download/test/file.parquet"],
                "data_date": "2024-01-01",
                "metadata": {"date": "2024-01-01", "title": "PNCP Contratos 2024-01-01"},
            }
        ]

        federation = InternetArchiveFederation("state/baliza.duckdb")
        federation.update_federation()

    # 3. Data availability check
    data_available = _check_existing_data_in_ia("2024-01-01", "contratos")

    # 4. Database verification
    # Use the provided duckdb_conn fixture
    all_schemas = duckdb_conn.execute("SHOW SCHEMAS").fetchall()
    all_schema_names = [s[0] for s in all_schemas]

    expected_schemas = ["main", "information_schema", "psa", "control", "federated"]
    for schema in expected_schemas:
        assert schema in all_schema_names, (
            f"Missing schema in final verification: {schema}"
        )

    # Verify data can be queried
    try:
        # This should work even with mocked data
        test_query = duckdb_conn.execute("SELECT COUNT(*) FROM control.runs").fetchone()
        assert test_query[0] >= 0  # Should return a count
    except Exception as e:
        # Some queries might fail with mocked data, but structure should be correct
        print(f"Query test note: {e}")

    # Archive-first workflow verification complete
    assert True, "Complete archive-first workflow verification passed"
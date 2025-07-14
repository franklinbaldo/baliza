import os
import sys
import tempfile
from pathlib import Path
import gc

import pytest
import duckdb # Import duckdb

# Add src to Python path for all tests
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (may take several seconds)"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "performance: marks tests as performance tests")
    config.addinivalue_line(
        "markers", "end_to_end: marks tests as complete end-to-end tests"
    )


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--run-slow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests",
    )
    parser.addoption(
        "--run-performance",
        action="store_true",
        default=False,
        help="run performance tests",
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on command line options."""
    if not config.getoption("--run-slow"):
        skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if not config.getoption("--run-integration"):
        skip_integration = pytest.mark.skip(
            reason="need --run-integration option to run"
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)

    if not config.getoption("--run-performance"):
        skip_performance = pytest.mark.skip(
            reason="need --run-performance option to run"
        )
        for item in items:
            if "performance" in item.keywords:
                item.add_marker(skip_performance)


@pytest.fixture(scope="session")
def project_root():
    """Get the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def temp_baliza_workspace():
    """Create a temporary workspace that mimics the Baliza project structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Change to temporary directory
        original_cwd = os.getcwd()
        os.chdir(tmpdir)

        # Create project structure
        dirs_to_create = [
            "src/baliza",
            "state",
            "baliza_data",
            "dbt_baliza/models/coverage",
            "dbt_baliza/models/staging",
            "dbt_baliza/models/sources",
            "notebooks",
            ".github/workflows",
            "tests",
        ]

        for dir_path in dirs_to_create:
            os.makedirs(dir_path, exist_ok=True)

        yield tmpdir

        # Restore original directory
        os.chdir(original_cwd)

        # Ensure all DuckDB connections are closed and garbage collected
        gc.collect()


@pytest.fixture
def duckdb_conn():
    """Provides a DuckDB in-memory connection for testing."""
    conn = duckdb.connect(database=':memory:')
    yield conn
    conn.close()
    gc.collect()


@pytest.fixture
def mock_environment_variables():
    """Provide mock environment variables for testing."""
    return {
        "IA_ACCESS_KEY": "test_access_key",
        "IA_SECRET_KEY": "test_secret_key",
        "BALIZA_DATE": "2024-01-15",
    }


@pytest.fixture
def sample_pncp_data():
    """Provide sample PNCP data for testing."""
    return {
        "data": [
            {
                "numeroControlePncpCompra": "12345-2024-001",
                "anoContrato": 2024,
                "dataAssinatura": "20240115",
                "niFornecedor": "12345678000195",
                "nomeRazaoSocialFornecedor": "Empresa Teste LTDA",
                "objetoContrato": "Prestação de serviços de TI",
                "valorInicial": 50000.00,
                "valorGlobal": 50000.00,
                "orgaoEntidade": {
                    "razaoSocial": "Prefeitura Municipal",
                    "cnpj": "11111111000111",
                    "uf": "RO",
                },
                "tipoContrato": {"codigo": "1", "descricao": "Serviços"},
            },
            {
                "numeroControlePncpCompra": "12345-2024-002",
                "anoContrato": 2024,
                "dataAssinatura": "20240116",
                "niFornecedor": "98765432000123",
                "nomeRazaoSocialFornecedor": "Outra Empresa SA",
                "objetoContrato": "Fornecimento de materiais",
                "valorInicial": 75000.00,
                "valorGlobal": 75000.00,
                "orgaoEntidade": {
                    "razaoSocial": "Governo do Estado",
                    "cnpj": "22222222000222",
                    "uf": "RO",
                },
                "tipoContrato": {"codigo": "2", "descricao": "Materiais"},
            },
        ],
        "totalRegistros": 2,
        "totalPaginas": 1,
        "paginaAtual": 1,
    }


@pytest.fixture
def sample_ia_items():
    """Provide sample Internet Archive items for testing."""
    return [
        {
            "identifier": "pncp-contratos-2024-01-15",
            "parquet_urls": [
                "https://archive.org/download/pncp-contratos-2024-01-15/file.parquet"
            ],
            "data_date": "2024-01-15",
            "metadata": {
                "date": "2024-01-15",
                "title": "PNCP Contratos 2024-01-15",
                "collection": "opensource",
            },
        }
    ]

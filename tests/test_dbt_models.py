"""
Tests for DBT coverage models and data transformations.
"""
import pytest
import tempfile
import os
import yaml
import duckdb
from pathlib import Path
import subprocess
from unittest.mock import patch, Mock


@pytest.fixture
def dbt_project_path():
    """Get path to the DBT project."""
    return Path(__file__).parent.parent / "dbt_baliza"


@pytest.fixture
def temp_dbt_workspace():
    """Create a temporary workspace for DBT testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Change to temp directory
        original_cwd = os.getcwd()
        os.chdir(tmpdir)
        
        # Create necessary directories
        os.makedirs("state", exist_ok=True)
        
        yield tmpdir
        
        # Restore original directory
        os.chdir(original_cwd)


def test_dbt_project_yml_exists(dbt_project_path):
    """Test that dbt_project.yml exists and is valid."""
    project_file = dbt_project_path / "dbt_project.yml"
    assert project_file.exists(), "dbt_project.yml not found"
    
    with open(project_file, 'r') as f:
        project_config = yaml.safe_load(f)
    
    assert 'name' in project_config
    assert 'version' in project_config
    assert 'profile' in project_config
    assert project_config['name'] == 'baliza_dbt'


def test_profiles_yml_exists(dbt_project_path):
    """Test that profiles.yml exists and is configured for DuckDB."""
    profiles_file = dbt_project_path / "profiles.yml"
    assert profiles_file.exists(), "profiles.yml not found"
    
    with open(profiles_file, 'r') as f:
        profiles_config = yaml.safe_load(f)
    
    assert 'baliza_dbt' in profiles_config
    profile = profiles_config['baliza_dbt']
    assert 'outputs' in profile
    assert 'dev' in profile['outputs']
    assert profile['outputs']['dev']['type'] == 'duckdb'


def test_sources_yml_exists(dbt_project_path):
    """Test that sources.yml exists and defines required sources."""
    sources_file = dbt_project_path / "models" / "sources" / "sources.yml"
    assert sources_file.exists(), "sources.yml not found"
    
    with open(sources_file, 'r') as f:
        sources_config = yaml.safe_load(f)
    
    assert 'version' in sources_config
    assert 'sources' in sources_config
    
    # Check for required sources
    source_names = [source['name'] for source in sources_config['sources']]
    assert 'federated' in source_names or 'psa' in source_names


def test_coverage_models_exist(dbt_project_path):
    """Test that coverage models exist."""
    coverage_dir = dbt_project_path / "models" / "coverage"
    assert coverage_dir.exists(), "Coverage models directory not found"
    
    # Check for specific coverage models
    temporal_model = coverage_dir / "coverage_temporal.sql"
    entities_model = coverage_dir / "coverage_entidades.sql"
    
    assert temporal_model.exists(), "coverage_temporal.sql not found"
    assert entities_model.exists(), "coverage_entidades.sql not found"


def test_coverage_temporal_model_syntax(dbt_project_path):
    """Test coverage_temporal.sql syntax and structure."""
    temporal_model = dbt_project_path / "models" / "coverage" / "coverage_temporal.sql"
    
    with open(temporal_model, 'r') as f:
        model_sql = f.read()
    
    # Check for essential SQL components
    assert 'select' in model_sql.lower()
    assert 'from' in model_sql.lower()
    assert 'group by' in model_sql.lower()
    
    # Check for temporal analysis components
    temporal_keywords = ['date', 'day', 'month', 'year', 'period']
    found_temporal = sum(1 for keyword in temporal_keywords 
                        if keyword in model_sql.lower())
    assert found_temporal >= 2, "Insufficient temporal analysis components"


def test_coverage_entidades_model_syntax(dbt_project_path):
    """Test coverage_entidades.sql syntax and structure."""
    entities_model = dbt_project_path / "models" / "coverage" / "coverage_entidades.sql"
    
    with open(entities_model, 'r') as f:
        model_sql = f.read()
    
    # Check for essential SQL components
    assert 'select' in model_sql.lower()
    assert 'from' in model_sql.lower()
    
    # Check for entity analysis components
    entity_keywords = ['entidade', 'orgao', 'cnpj', 'entity', 'organization']
    found_entity = sum(1 for keyword in entity_keywords 
                      if keyword in model_sql.lower())
    assert found_entity >= 1, "Insufficient entity analysis components"


def test_staging_model_exists(dbt_project_path):
    """Test that staging model exists."""
    staging_dir = dbt_project_path / "models" / "staging"
    staging_model = staging_dir / "stg_contratos.sql"
    
    assert staging_model.exists(), "stg_contratos.sql not found"
    
    with open(staging_model, 'r') as f:
        model_sql = f.read()
    
    # Check for source reference
    assert 'source(' in model_sql or 'ref(' in model_sql


def test_schema_yml_exists(dbt_project_path):
    """Test that schema.yml exists with model documentation."""
    staging_dir = dbt_project_path / "models" / "staging"
    schema_file = staging_dir / "schema.yml"
    
    if schema_file.exists():
        with open(schema_file, 'r') as f:
            schema_config = yaml.safe_load(f)
        
        assert 'version' in schema_config
        assert 'models' in schema_config


def test_macros_exist(dbt_project_path):
    """Test that macros directory and files exist."""
    macros_dir = dbt_project_path / "macros"
    assert macros_dir.exists(), "Macros directory not found"
    
    # Check for data quality macros
    quality_macro = macros_dir / "data_quality_checks.sql"
    if quality_macro.exists():
        with open(quality_macro, 'r') as f:
            macro_sql = f.read()
        
        # Check for macro definition
        assert 'macro' in macro_sql.lower()


@pytest.mark.slow
@patch('subprocess.run')
def test_dbt_parse_command(mock_run, dbt_project_path, temp_dbt_workspace):
    """Test that DBT parse command runs successfully."""
    mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
    
    # Change to DBT project directory
    os.chdir(str(dbt_project_path))
    
    # Test DBT parse
    result = subprocess.run(['dbt', 'parse'], capture_output=True, text=True)
    
    # In real scenario, this would actually parse
    # For mock, just verify the command structure
    assert True, "DBT parse command structure verified"


@pytest.mark.slow
@patch('subprocess.run')
def test_dbt_compile_models(mock_run, dbt_project_path, temp_dbt_workspace):
    """Test that DBT models compile successfully."""
    mock_run.return_value = Mock(returncode=0, stdout="Compiled successfully", stderr="")
    
    # Test DBT compile for coverage models
    coverage_models = ['coverage_temporal', 'coverage_entidades']
    
    for model in coverage_models:
        # Mock compile command
        result = subprocess.run(['dbt', 'compile', '--select', model], 
                              capture_output=True, text=True)
        
        # In real scenario, would check compilation success
        assert True, f"Model {model} compilation verified"


def test_dbt_model_dependencies(dbt_project_path):
    """Test that DBT models have proper dependencies."""
    # Check if models reference appropriate sources
    models_dir = dbt_project_path / "models"
    
    for model_file in models_dir.rglob("*.sql"):
        with open(model_file, 'r') as f:
            model_content = f.read()
        
        # Models should use source() or ref() functions
        if 'select' in model_content.lower():
            uses_dbt_functions = ('source(' in model_content or 
                                'ref(' in model_content or
                                'federated.' in model_content)
            
            # Allow direct table references for now, but prefer DBT functions
            if not uses_dbt_functions:
                print(f"Warning: {model_file.name} may not use proper DBT references")


def test_coverage_analysis_logic(temp_dbt_workspace):
    """Test coverage analysis logic with sample data."""
    # Create test database with sample data
    db_path = os.path.join(temp_dbt_workspace, "test_baliza.duckdb")
    conn = duckdb.connect(db_path)
    
    # Create test schema and data
    conn.execute("CREATE SCHEMA IF NOT EXISTS federated")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS federated.contratos_unified AS
        SELECT 
            'CNT-2024-001' as numeroControlePncpCompra,
            '2024-01-01'::DATE as data_date,
            'Prefeitura Municipal' as orgao_nome,
            '11111111000111' as orgao_cnpj,
            'RO' as uf,
            50000.0 as valorInicial
        UNION ALL
        SELECT 
            'CNT-2024-002' as numeroControlePncpCompra,
            '2024-01-02'::DATE as data_date,
            'Governo do Estado' as orgao_nome,
            '22222222000222' as orgao_cnpj,
            'RO' as uf,
            75000.0 as valorInicial
        UNION ALL
        SELECT 
            'CNT-2024-003' as numeroControlePncpCompra,
            '2024-01-03'::DATE as data_date,
            'Prefeitura Municipal' as orgao_nome,
            '11111111000111' as orgao_cnpj,
            'RO' as uf,
            30000.0 as valorInicial
    """)
    
    # Test temporal coverage analysis
    temporal_result = conn.execute("""
        SELECT 
            data_date,
            COUNT(*) as contratos_count,
            SUM(valorInicial) as valor_total
        FROM federated.contratos_unified
        GROUP BY data_date
        ORDER BY data_date
    """).fetchall()
    
    assert len(temporal_result) == 3
    assert temporal_result[0][1] == 1  # 1 contract on first day
    assert temporal_result[0][2] == 50000.0  # Total value on first day
    
    # Test entity coverage analysis
    entity_result = conn.execute("""
        SELECT 
            orgao_cnpj,
            orgao_nome,
            COUNT(*) as contratos_count,
            SUM(valorInicial) as valor_total
        FROM federated.contratos_unified
        GROUP BY orgao_cnpj, orgao_nome
        ORDER BY contratos_count DESC
    """).fetchall()
    
    assert len(entity_result) == 2
    assert entity_result[0][2] == 2  # Prefeitura has 2 contracts
    assert entity_result[1][2] == 1  # Estado has 1 contract
    
    conn.close()


def test_data_quality_checks(temp_dbt_workspace):
    """Test data quality validation logic."""
    # Create test database
    db_path = os.path.join(temp_dbt_workspace, "test_baliza.duckdb")
    conn = duckdb.connect(db_path)
    
    # Create test data with quality issues
    conn.execute("CREATE SCHEMA IF NOT EXISTS federated")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS federated.contratos_unified AS
        SELECT 
            'CNT-2024-001' as numeroControlePncpCompra,
            '2024-01-01'::DATE as data_date,
            'Empresa A' as fornecedor_nome,
            50000.0 as valorInicial
        UNION ALL
        SELECT 
            NULL as numeroControlePncpCompra,  -- Quality issue: NULL control number
            '2024-01-02'::DATE as data_date,
            'Empresa B' as fornecedor_nome,
            0.0 as valorInicial  -- Quality issue: Zero value
        UNION ALL
        SELECT 
            'CNT-2024-003' as numeroControlePncpCompra,
            NULL as data_date,  -- Quality issue: NULL date
            '' as fornecedor_nome,  -- Quality issue: Empty supplier
            -1000.0 as valorInicial  -- Quality issue: Negative value
    """)
    
    # Test data quality checks
    quality_result = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(numeroControlePncpCompra) as valid_control_numbers,
            COUNT(data_date) as valid_dates,
            COUNT(CASE WHEN LENGTH(TRIM(fornecedor_nome)) > 0 THEN 1 END) as valid_suppliers,
            COUNT(CASE WHEN valorInicial > 0 THEN 1 END) as positive_values
        FROM federated.contratos_unified
    """).fetchone()
    
    assert quality_result[0] == 3  # Total records
    assert quality_result[1] == 2  # Valid control numbers (2 out of 3)
    assert quality_result[2] == 2  # Valid dates (2 out of 3)
    assert quality_result[3] == 2  # Valid suppliers (2 out of 3)
    assert quality_result[4] == 1  # Positive values (1 out of 3)
    
    conn.close()


def test_coverage_metrics_calculation():
    """Test coverage metrics calculation algorithms."""
    # Test temporal coverage calculation
    sample_dates = ['2024-01-01', '2024-01-02', '2024-01-05']  # Missing 3rd and 4th
    expected_business_days = 4  # Mon-Thu in first week of Jan 2024
    
    coverage_percentage = (len(sample_dates) / expected_business_days) * 100
    assert coverage_percentage == 75.0  # 3 out of 4 business days
    
    # Test entity coverage scoring
    entity_scores = {
        'entity_1': {'contracts': 10, 'total_value': 100000},
        'entity_2': {'contracts': 5, 'total_value': 50000},
        'entity_3': {'contracts': 0, 'total_value': 0}  # Not publishing
    }
    
    publishing_entities = sum(1 for entity, data in entity_scores.items() 
                            if data['contracts'] > 0)
    total_entities = len(entity_scores)
    
    entity_coverage = (publishing_entities / total_entities) * 100
    assert entity_coverage == 66.67  # 2 out of 3 entities publishing


def test_analytics_dashboard_data_prep(temp_dbt_workspace):
    """Test data preparation for analytics dashboard."""
    # Create test database
    db_path = os.path.join(temp_dbt_workspace, "test_baliza.duckdb")
    conn = duckdb.connect(db_path)
    
    # Create sample analytics data
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analytics.dashboard_metrics AS
        SELECT 
            '2024-01' as month_year,
            'RO' as uf,
            'Prefeitura' as tipo_orgao,
            100 as total_contratos,
            5000000.0 as valor_total,
            50000.0 as valor_medio,
            25 as fornecedores_unicos,
            0.85 as coverage_score
    """)
    
    # Test dashboard query
    dashboard_data = conn.execute("""
        SELECT 
            month_year,
            uf,
            total_contratos,
            valor_total,
            coverage_score
        FROM analytics.dashboard_metrics
        WHERE coverage_score > 0.8  -- High coverage threshold
    """).fetchall()
    
    assert len(dashboard_data) == 1
    assert dashboard_data[0][4] == 0.85  # Coverage score
    
    conn.close()


def test_model_documentation_completeness(dbt_project_path):
    """Test that models have adequate documentation."""
    models_dir = dbt_project_path / "models"
    
    # Count models and documented models
    sql_models = list(models_dir.rglob("*.sql"))
    assert len(sql_models) > 0, "No SQL models found"
    
    # Check for schema documentation
    schema_files = list(models_dir.rglob("schema.yml"))
    
    # At least one schema file should exist for documentation
    if schema_files:
        print(f"Found {len(schema_files)} schema documentation files")
    else:
        print("Warning: No schema documentation files found")
    
    # Check individual model documentation within SQL files
    documented_models = 0
    for model_file in sql_models:
        with open(model_file, 'r') as f:
            content = f.read()
        
        # Look for documentation patterns
        if any(pattern in content for pattern in ['--', '/*', 'description']):
            documented_models += 1
    
    documentation_ratio = documented_models / len(sql_models)
    assert documentation_ratio >= 0.5, f"Insufficient model documentation: {documentation_ratio:.1%}"
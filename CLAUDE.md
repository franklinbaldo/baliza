# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Essential Commands

**Always use `uv` for package management and command execution:**

```bash
# Install dependencies and setup environment
uv sync

# Run CLI commands
uv run baliza sync                    # Incremental sync from PNCP API
uv run baliza backfill 20240101 20240331  # Historical data backfill
uv run baliza status                  # Check pipeline configuration
uv run baliza info                    # Show available resources

# Development commands
uv run python -m pytest              # Run tests
uv run ruff check                     # Lint code
uv run ruff format                    # Format code
uv run mypy src/                      # Type checking

# Run single test
uv run python -m pytest tests/test_pipeline.py::TestPipelineConfig::test_yaml_config_exists_and_valid
```

## Architecture Overview

**BALIZA** is a Brazilian public procurement data extraction tool that interfaces with the PNCP (Portal Nacional de Contratações Públicas) API using **DLT (Data Load Tool)** as the core pipeline engine.

### Core Architecture Components

1. **DLT Pipeline Engine** (`src/baliza/pipeline.py`):
   - Main extraction logic using `rest_api_source` from DLT
   - Pydantic models for schema validation and data quality
   - Multi-modality strategy for PNCP API endpoints
   - Automatic resource expansion for different procurement types (modalidades)

2. **Pydantic Models** (`src/baliza/models.py`):
   - Complete data models for all PNCP API responses
   - Field validators for data quality (e.g., `coerce_situacao_compra_id`)
   - Nested models for complex data structures (orgaoEntidade, unidadeOrgao)

3. **Configuration System**:
   - `config/pncp_resources.yaml`: Endpoint definitions and parameters
   - `.dlt/config.toml`: DLT pipeline optimizations and performance settings
   - `.dlt/secrets.toml`: API base URL and credentials (not in repo)

4. **CLI Interface** (`src/baliza/cli.py`):
   - Typer-based CLI with intuitive commands
   - Sync (incremental) vs backfill (historical) modes
   - Built-in validation and error handling

### Multi-Modality Strategy

The PNCP API has **13 procurement modalities** (modalidades 1-13). The pipeline automatically handles this:

- **Endpoints requiring modality parameter**: Automatically expands to 13 separate resources
  - `contratacoes_publicacao` → `contratacoes_publicacao_mod1` through `mod13`
  - `contratacoes_atualizacao` → `contratacoes_atualizacao_mod1` through `mod13`

- **Endpoints not requiring modality**: Single resource fetches all modalities
  - `contratos`, `atas`, `pca_usuario`, etc.

This ensures 100% complete data extraction without unnecessary API calls.

### Key Data Flow

```
PNCP API → DLT REST Source → Pydantic Validation → Nested Hints Processing → DuckDB/Parquet
```

1. **Extraction**: DLT `rest_api_source` handles pagination, retries, and state management
2. **Validation**: Pydantic models ensure data quality and type safety
3. **Schema Management**: Nested hints control how complex objects become tables
4. **Storage**: Default to DuckDB with Parquet export for analysis

### Critical Integration Points

- **Resource-to-Model Mapping**: `RESOURCE_PYDANTIC_MAPPING` in `pipeline.py` connects YAML resources to Pydantic models
- **Nested Table Control**: `_get_nested_hints_for_model()` defines how nested objects become separate tables
- **Schema Contracts**: Configured for data evolution (`columns: "evolve"`) to handle API changes
- **Incremental Loading**: Uses `dataAtualizacao` cursors with proper lag settings

### Configuration Files Structure

- `config/pncp_resources.yaml`: Defines API endpoints, parameters, and incremental settings
- `.dlt/config.toml`: Performance tuning, retry policies, and destination settings
- `.dlt/secrets.toml`: Contains `base_url = "https://pncp.gov.br/api/consulta"`

### Testing Strategy

- **Smoke tests**: Verify configuration validity and API compatibility
- **Resource expansion tests**: Ensure modality logic works correctly
- **Mock-based**: Tests use mocked API responses to avoid external dependencies

### Performance Optimizations

- **Parallel processing**: 4 workers for normalization, threaded DuckDB
- **File rotation**: 50k items per file for parallel loading
- **State management**: Compressed state with 14-day hash trimming
- **Memory limits**: 2GB DuckDB, 4MB file chunks, 10k buffer items

## Development Notes

- All Pydantic models include comprehensive docstrings with JSON examples
- Field validators handle API inconsistencies (e.g., integer vs string IDs)
- The pipeline automatically logs Pydantic schema configuration for debugging
- Use `max_table_nesting: 2` to control nested table depth
- Schema contracts allow data evolution while maintaining type safety
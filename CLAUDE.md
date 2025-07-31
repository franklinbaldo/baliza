# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Essential Commands

**Always use `uv` for package management and command execution:**

```bash
# Install dependencies and setup environment
uv sync

# Main command - Autonomous and idempotent pipeline
uv run baliza run                     # Process all months since 2021-01 automatically
uv run baliza run --full-rebuild      # Reprocess all months from scratch
uv run baliza run --exclude-modalidades "2,7,8"  # Exclude specific modalidades

# Status and information commands
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

1. **Intelligent Pipeline Orchestrator** (`src/baliza/pipeline.py`):
   - Single autonomous `run_intelligent_pipeline()` function
   - Programmatically configures DLT state backend via `dlt.config`
   - Month-by-month processing with database-backed state management
   - Automatic calculation of months to process (2021-01 to last complete month)
   - Built-in resumability and idempotence

2. **DLT Pipeline Engine** (`src/baliza/pipeline.py`):
   - Main extraction logic using `rest_api_source` from DLT
   - Pydantic models for schema validation and data quality
   - Multi-modality strategy for PNCP API endpoints
   - Automatic resource expansion for different procurement types (modalidades)

3. **Pydantic Models** (`src/baliza/models.py`):
   - Complete data models for all PNCP API responses
   - Field validators for data quality (e.g., `coerce_situacao_compra_id`)
   - Nested models for complex data structures (orgaoEntidade, unidadeOrgao)

4. **Python-First Configuration**:
   - All critical configuration is set programmatically in Python code
   - State backend dynamically configured to match data destination
   - `config/pncp_resources.yaml`: Endpoint definitions and parameters
   - `.dlt/secrets.toml`: API base URL and credentials (not in repo)
   - No manual `.dlt/config.toml` editing required

5. **Simplified CLI Interface** (`src/baliza/cli.py`):
   - Single primary command: `baliza run`
   - Legacy commands hidden but maintained for compatibility
   - Built-in validation and error handling
   - Zero-configuration operation for end users

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
Autonomous Orchestrator → Month Selection → State Check → PNCP API → DLT REST Source → Pydantic Validation → Database Storage
```

1. **Orchestration**: Intelligent pipeline calculates months to process and checks database state
2. **Extraction**: DLT `rest_api_source` handles pagination, retries, and API communication  
3. **Validation**: Pydantic models ensure data quality and type safety
4. **State Management**: Progress stored in database alongside data for perfect consistency
5. **Storage**: Default to DuckDB with Parquet export for analysis

### Autonomous State Management

The pipeline uses DLT's built-in state management with programmatic configuration:

- **Database-Backed State**: `dlt.config["state.backend"] = destination` automatically configured
- **Month Tracking**: `pipeline.state["completed_months"]` contains list of processed months (format: "YYYYMM")
- **Atomic Updates**: State is updated after each successful month processing
- **Perfect Resumability**: Interrupted pipelines automatically continue from last completed month
- **Production Ready**: State lives in database alongside data, perfect for containers and schedulers

### Critical Integration Points

- **Resource-to-Model Mapping**: `RESOURCE_PYDANTIC_MAPPING` in `pipeline.py` connects YAML resources to Pydantic models
- **Intelligent Orchestration**: `run_intelligent_pipeline()` is the single entry point for all data processing
- **Month Calculation**: `_get_months_to_process()` determines work needed from 2021-01 to last complete month
- **State Backend Config**: Programmatically set via `dlt.config` to match data destination
- **Schema Contracts**: Configured for data evolution (`columns: "evolve"`) to handle API changes

### Configuration Structure

**Python-First Configuration (Zero manual config required):**
- **State Backend**: Automatically configured via `dlt.config["state.backend"] = destination`
- **Pipeline Settings**: All critical settings defined programmatically in `run_intelligent_pipeline()`

**File-Based Configuration (minimal):**
- `config/pncp_resources.yaml`: API endpoint definitions and parameters
- `.dlt/secrets.toml`: Contains `base_url = "https://pncp.gov.br/api/consulta"` (only file users need to create)
- `.dlt/config.toml`: Optional performance tuning (not required for basic operation)

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
# Repository Cleanup Plan - Extract & Export to Parquet Only

**Goal**: Streamline repository to contain only what's needed for PNCP data extraction and Parquet export.

## Current State Analysis

### Core Components (KEEP & REORGANIZE) ✅
```
src/baliza/
├── extraction/
│   ├── config.py          # 🔄 RENAMED from pncp_config.py - API configuration
│   ├── pipeline.py        # 🔄 RENAMED from pncp.py - extraction functions
│   └── gap_detector.py    # ✅ NEW - smart gap detection for incremental loading
├── schemas.py             # 🔄 RENAMED from enums.py - PNCP schemas & enums
├── models.py              # 🔄 MOVED from legacy/ - Pydantic response models  
├── utils.py               # 🔄 MOVED from legacy/utils/ - hash & utility functions
├── settings.py            # 🔄 RENAMED from config.py - app settings
├── cli.py                 # Entry points for extraction
└── __init__.py
```

### Legacy Components (DELETE) ❌
```
src/baliza/
├── flows/                 # ❌ Prefect flows - no longer needed
├── legacy/
│   ├── sql/              # ❌ Old SQL schemas - dlt handles schema
│   ├── utils/
│   │   ├── circuit_breaker.py  # ❌ dlt has built-in retries
│   │   ├── endpoints.py        # ❌ Replaced by pncp_config.py
│   │   ├── http_client.py      # ❌ dlt REST API source handles HTTP
│   │   └── io.py              # ❌ File I/O utilities not needed
│   └── validators.py     # ❌ Pydantic models handle validation
├── metrics/              # ❌ Data quality - dlt has built-in metrics
├── transforms/           # ❌ Ibis transforms - focus on extraction only
├── types.py             # ❌ Type definitions - using Pydantic
├── ui/                  # ❌ UI components not needed
├── backend.py           # ❌ DuckDB backend - dlt handles destinations
├── logger.py            # ❌ Custom logging - dlt has built-in logging
└── pncp_schemas.py      # ❌ Duplicate of models.py
```

### Test Components (SIMPLIFY) 🔄
```
tests/
├── e2e/
│   └── test_pncp_pipeline.py  # 🔄 Update for new dlt pipeline
├── test_backend.py            # ❌ Remove - no custom backend
├── test_endpoints.py          # ❌ Remove - using dlt built-ins
└── test_http_client.py        # ❌ Remove - using dlt built-ins
```

### Documentation (KEEP/UPDATE) 📝
```
docs/
├── dlt-migration-analysis.md  # ✅ Keep - documents migration
├── cleanup-plan.md           # ✅ This file
└── README.md                 # 🔄 Update with simplified usage
```

## Cleanup Actions

### Phase 1: Reorganize & Rename for Production
```bash
# Create new extraction module
mkdir -p src/baliza/extraction/

# Move and rename pipeline components with better names
mv src/baliza/pipelines/pncp_config.py src/baliza/extraction/config.py
mv src/baliza/pipelines/pncp.py src/baliza/extraction/pipeline.py
mv src/baliza/pipelines/gap_detector.py src/baliza/extraction/gap_detector.py

# Move and rename core components
mv src/baliza/legacy/enums.py src/baliza/schemas.py      # Better name: schemas
mv src/baliza/legacy/models.py src/baliza/models.py      # Keep: models
mv src/baliza/legacy/utils/hash_utils.py src/baliza/utils.py  # Keep: utils
mv src/baliza/config.py src/baliza/settings.py          # Better name: settings

# Remove obsolete structure
rm -rf src/baliza/legacy/
rm -rf src/baliza/pipelines/
```

### Phase 2: Update Imports with Better Names
Update all import statements with cleaner, production-ready names:
```python
# OLD imports (migration artifacts)
from baliza.legacy.enums import ModalidadeContratacao, PncpEndpoint
from baliza.legacy.utils.hash_utils import hash_sha256
from baliza.pipelines.pncp_config import create_pncp_rest_config
from baliza.pipelines.pncp import run_priority_extraction

# NEW imports (clean production names)
from baliza.schemas import ContractType, Endpoint  # Better enum names
from baliza.utils import hash_sha256
from baliza.extraction.config import create_api_config
from baliza.extraction.pipeline import extract_priority_data
```

Files to update:
- `src/baliza/extraction/config.py` (renamed from pncp_config.py)
- `src/baliza/extraction/pipeline.py` (renamed from pncp.py)
- `src/baliza/settings.py` (renamed from config.py)
- `src/baliza/cli.py`
- `tests/e2e/test_extraction_pipeline.py` (renamed test file)

### Phase 3: Revamp CLI - Intuitive & Minimal Commands

**Design Principles:**
- **Single purpose**: Only data extraction to Parquet
- **Intuitive defaults**: Most common use cases work with minimal flags
- **Smart date handling**: Natural language and flexible formats
- **Clear output**: Progress indicators and helpful messages

**NEW CLI Design:**
```python
# Main command: baliza extract [options]
@app.command()
def extract(
    # Smart date options (pick one) - DEFAULT: backfill everything
    backfill_all: bool = typer.Option(True, "--backfill/--no-backfill", help="Extract all available historical data (default)"),
    days: int = typer.Option(None, "--days", "-d", help="Extract last N days (overrides backfill)"),
    date_input: str = typer.Option(None, "--date", help="Date (YYYY-MM-DD, YYYY-MM) (overrides backfill)"),
    date_range: str = typer.Option(None, "--range", "-r", help="Date range (YYYY-MM:YYYY-MM) (overrides backfill)"),
    
    # Data selection
    types: str = typer.Option("all", "--types", "-t", help="Data types: all,contracts,publications,agreements"),
    
    # Output options  
    output: Path = typer.Option("data/", "--output", "-o", help="Output directory"),
    
    # Utility flags
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be extracted")
):
    """Extract PNCP data to Parquet files. By default, backfills all available historical data."""
```

**REMOVE obsolete commands:**
- `baliza run` (Prefect flows)
- `baliza init` (database setup)  
- `baliza doctor` (health checks)
- `baliza ui` (Prefect UI)
- `baliza reset` (database management)
- `baliza query` (SQL queries)
- `baliza verify` (data integrity)
- `baliza transform` (staging/marts)

### Phase 4: Remove Obsolete Code
```bash
# Remove entire obsolete infrastructure (legacy/ already moved)
rm -rf src/baliza/flows/
rm -rf src/baliza/metrics/
rm -rf src/baliza/transforms/
rm -rf src/baliza/ui/
rm src/baliza/backend.py
rm src/baliza/logger.py
rm src/baliza/pncp_schemas.py
rm src/baliza/types.py

# Remove obsolete tests
rm tests/test_backend.py
rm tests/test_endpoints.py  
rm tests/test_http_client.py
```

### Phase 2: Simplify CLI
Update `src/baliza/cli.py` to only include:
- `baliza extract --date-range YYYYMMDD:YYYYMMDD --output parquet/`
- `baliza extract --latest --output parquet/`
- Remove all Prefect flow commands
- Remove database management commands
- Remove UI commands

### Phase 3: Update Configuration
Simplify `src/baliza/config.py`:
- Keep only ENDPOINT_CONFIG
- Keep only PNCP API settings
- Remove DuckDB settings (dlt handles destinations)
- Remove circuit breaker settings
- Remove UI settings

### Phase 4: Streamline Dependencies
Update `pyproject.toml`:

**Remove:**
- `prefect` and all Prefect dependencies
- `ibis-framework` (not needed for extraction only)
- `polars` (dlt handles data processing)
- `msgpack` (not used in streamlined version)
- `psutil` (monitoring not needed)

**Keep:**
- `dlt>=1.14.1` 
- `pydantic>=2.0`
- `pydantic-settings`
- `httpx` (dlt dependency)
- `typer` (for CLI)

### Phase 8: Final Clean Structure
```
baliza/
├── src/baliza/
│   ├── extraction/
│   │   ├── __init__.py
│   │   ├── config.py         # API configuration (clean names)
│   │   └── pipeline.py       # Extraction functions (clean names)
│   ├── __init__.py
│   ├── cli.py                # Simplified CLI
│   ├── settings.py           # App settings (ENDPOINT_CONFIG)
│   ├── schemas.py            # Business schemas & enums (clean names)
│   ├── models.py             # Pydantic response models
│   └── utils.py              # Hash & utility functions
├── tests/
│   ├── __init__.py
│   └── e2e/
│       ├── __init__.py
│       └── test_extraction.py # Simplified extraction tests
├── docs/
│   ├── cleanup-plan.md
│   ├── migration-analysis.md  # Renamed for clarity
│   └── README.md
├── pyproject.toml           # Minimal dependencies
└── uv.lock
```

## Simplified Usage After Cleanup

### Installation
```bash
uv sync
```

### Extract to Parquet (Intuitive CLI)
```bash
# Smart defaults - BACKFILL ALL HISTORICAL DATA (new default behavior)
baliza extract

# Override backfill with specific time periods  
baliza extract --days 30                    # Last 30 days only
baliza extract --date 2025-01              # Entire January 2025 only
baliza extract --date 2025-01-15           # Single day only
baliza extract --range 2025-01:2025-03     # January to March only

# Disable backfill explicitly
baliza extract --no-backfill --days 7      # Just last 7 days

# Specific data types (still backfills all historical data for selected types)
baliza extract --types contracts           # All historical contracts
baliza extract --types contracts,agreements # All historical contracts & agreements

# Custom output location (still backfills everything)
baliza extract --output /my/data/path      # All data to custom directory

# Dry run to see what would be extracted (shows full backfill scope)
baliza extract --dry-run                   # See all available historical data
baliza extract --range 2025-01:2025-03 --dry-run  # See specific range

# Get help and info
baliza --help                              # Command help
baliza info                                # Available data types
baliza version                             # Version info
```

### CLI Examples with Real Use Cases
```bash
# Data analyst: Get ALL historical contracts (default backfill behavior)
baliza extract --types contracts

# Data analyst: Get ONLY recent contracts (override backfill)
baliza extract --no-backfill --days 30 --types contracts

# Compliance team: Get specific month for reporting  
baliza extract --date 2025-01 --output reports/january/

# Research: Get comprehensive historical data (leverages default backfill)
baliza extract --verbose

# Research: Get specific date range only
baliza extract --range 2024-06:2024-12 --verbose

# Quick check: See full scope of available historical data
baliza extract --dry-run

# Quick check: See what specific range would include
baliza extract --days 7 --dry-run
```

### Python API (Clean Production Names)
```python
from baliza.extraction.pipeline import extract_data
from baliza.schemas import ContractType, DataType
from datetime import date, timedelta

# DEFAULT: Backfill all historical data (new behavior)
result = extract_data(
    output_dir="data/",
    data_types=["contracts", "agreements"],
    backfill_all=True  # Default value
)

# Override backfill with specific dates
result = extract_data(
    start_date="2025-01-01",
    end_date="2025-01-31", 
    output_dir="data/",
    data_types=["contracts", "agreements"],
    backfill_all=False  # Explicit override
)

# Use business enums (Portuguese names for manual verification)
modalidade = ModalidadeContratacao.PREGAO_ELETRONICO  # Matches PNCP manual
endpoint = Endpoint.CONTRATOS  # Matches API documentation

# Smart date handling (when not using backfill)
last_week = date.today() - timedelta(days=7)
result = extract_data(
    start_date=last_week.isoformat(),
    end_date=date.today().isoformat(),
    output_dir="data/recent/",
    backfill_all=False
)
```

## Benefits of Cleanup

1. **🎯 Focused Purpose**: Only extraction and parquet export
2. **📉 Reduced Complexity**: ~70% fewer files and dependencies
3. **🚀 Faster Setup**: Minimal dependencies, quicker installation
4. **🧹 Maintainable**: Clear separation of concerns, no "legacy" folders
5. **📦 Lightweight**: Smaller package size
6. **🔧 Simple CLI**: Single intuitive command with smart defaults
7. **🏗️ Clean Architecture**: Business logic in main module with clear names
8. **🌍 Professional Naming**: English names, clear business meaning
9. **📁 Logical Organization**: `extraction/` module groups related functionality
10. **🎯 User-Friendly**: Natural language dates, helpful defaults, progress indicators

## Migration Safety

- **Keep git history**: All removed code remains in git history
- **Tag current version**: `git tag v1.0-full-features` before cleanup
- **Test after each phase**: Ensure extraction still works
- **Document removed features**: Note what was removed and why

## Success Criteria

- ✅ Single intuitive CLI command with smart defaults
- ✅ No Prefect dependencies
- ✅ No custom HTTP client code
- ✅ No database management code
- ✅ Extraction to Parquet works
- ✅ All PNCP endpoints supported
- ✅ Business logic promoted to main module (no legacy/ folder)
- ✅ Clean import paths (`from baliza.schemas import ContractType`)
- ✅ Professional English naming for classes/functions
- ✅ Logical module organization (`extraction/` grouping)
- ✅ Test suite covers extraction only

## Naming Convention Guidelines

### Enum Names (Keep Portuguese for Manual Verification)
- `ModalidadeContratacao` ✅ KEEP - matches PNCP manual exactly
- `PREGAO_ELETRONICO` ✅ KEEP - official legal terminology  
- `CONCORRENCIA_ELETRONICA` ✅ KEEP - official legal terminology
- `AmparoLegal` ✅ KEEP - legal basis references
- Only rename containers: `PncpEndpoint` → `Endpoint` (class name, not values)

### Function Names (Verb-Object Pattern)
- `pncp_source()` → `create_extraction_source()`
- `run_priority_extraction()` → `extract_priority_data()`
- `get_default_config()` → `get_development_config()`

### Module Names (Business Domain)
- `pncp_config.py` → `config.py` (in extraction/ module)
- `enums.py` → `schemas.py` (includes enums + schemas)
- `config.py` → `settings.py` (app-level settings)

### CLI Command Design
- **One main command**: `baliza extract` (not multiple subcommands)
- **Smart defaults**: `baliza extract` works without flags (last 7 days)
- **Natural language**: `--days 30`, `--date 2025-01`, `--range 2025-01:2025-03`
- **Clear options**: `--types contracts` instead of `--endpoints contratacoes_publicacao`
- **Helpful utilities**: `--dry-run`, `--verbose`, `baliza info`

## CLI Design Philosophy

### Before (Complex & Technical)
```bash
# Multiple confusing commands
baliza run --latest                           # What does "run" mean?
baliza extract --mes 2024-01                # Portuguese flag names
baliza transform --mes 2024-01              # Separate transform step?
baliza query "SELECT * FROM raw.audit_log"  # SQL knowledge required
baliza reset --force                        # Dangerous database operations
baliza doctor                               # What does this check?
```

### After (Simple & Intuitive)  
```bash
# One clear command with smart defaults
baliza extract                              # Just works - BACKFILLS ALL HISTORICAL DATA
baliza extract --days 30                   # Natural language (overrides backfill)
baliza extract --date 2025-01             # Flexible date formats (overrides backfill)
baliza extract --types contracts          # Clear business terms (still backfills all)
baliza info                               # Helper information
```

### Key Improvements
1. **🎯 Single Purpose**: One command for one job - data extraction
2. **🧠 Smart Defaults**: `baliza extract` backfills ALL historical data by default - perfect for 90% of use cases  
3. **🌍 Natural Language**: `--days 30` instead of `--mes 2024-01`
4. **📋 Manual Compliance**: Enum values kept in Portuguese for PNCP manual verification
5. **🗃️ Complete by Default**: No partial data - backfill ensures comprehensive historical coverage
6. **🔒 Safe**: No database management or destructive operations
7. **📋 Self-Documenting**: `baliza info` shows available options
8. **🚀 Fast**: No complex setup or configuration files needed

**Result**: Professional, maintainable repository with intuitive CLI, clear English naming, and logical organization for PNCP data extraction to Parquet files.
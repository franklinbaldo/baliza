# Extraction Dashboard

Monitor the PNCP data extraction pipeline status.

## Quick Status

| Metric | Command |
|--------|---------|
| Buffer stats | `baliza buffer-stats` |
| Verify coverage | `baliza verify --resource contratos --start YYYY-MM-DD --end YYYY-MM-DD` |

## Pipeline Architecture

```mermaid
flowchart LR
    subgraph Extraction
        API[PNCP API] -->|httpx| Buffer[(DuckDB Buffer)]
        Buffer -->|checkpoint| State[(baliza_state)]
    end

    subgraph Export
        Buffer -->|D-7| Daily[Daily Parquet]
        Daily --> IA[Internet Archive]
    end

    subgraph Cleanup
        IA -->|success| Cleanup[Buffer Cleanup]
        Cleanup --> Buffer
    end
```

## Data Flow

### 1. Extraction Phase
- **Continuous extraction** runs every 30 minutes
- **D-1 priority**: Yesterday's data is always extracted first
- **Per-page checkpointing**: Resumes from last page on timeout
- Data stored in `baliza_raw.contratos` table

### 2. Stability Window (D-7)
- Data waits 7 days before export
- Allows late-arriving records to be included
- Prevents exporting incomplete days

### 3. Daily Export
- Self-contained parquet packages per day
- Relational structure: `contratos`, `orgaos`, `unidades`
- Includes `_metadata.json` with schema version

### 4. Internet Archive Upload
- Daily packages uploaded to `baliza-pncp-YYYY-MM-DD` items
- Buffer cleaned after successful upload
- State tables preserved for tracking

## Monitoring Commands

### Check Buffer Status
```bash
baliza buffer-stats --duckdb baliza.duckdb
```

Output includes:
- Total rows in buffer
- Dates in buffer
- Dates uploaded to IA
- Pending checkpoints (incomplete extractions)

### Verify Coverage
```bash
baliza verify --resource contratos --start 2024-01-01 --end 2024-01-31
```

Shows:
- Complete coverage confirmation
- Gap detection with missing date ranges

### Export Specific Date
```bash
baliza export-daily --date 2024-01-15 --output ./data/daily
```

Creates:
```
data/daily/2024-01-15/
├── contratos.parquet
├── orgaos.parquet
├── unidades.parquet
└── _metadata.json
```

## State Tables

### `baliza_state.coverage`
Tracks extraction windows:
| Column | Description |
|--------|-------------|
| resource | Resource type (contratos) |
| window_start | Start of extraction window |
| window_end | End of extraction window |
| status | complete/failed |
| total_paginas | Pages fetched |
| rows_extracted | Total rows |

### `baliza_state.extraction_checkpoint`
Resume points for incomplete extractions:
| Column | Description |
|--------|-------------|
| resource | Resource type |
| extraction_date | Date being extracted |
| current_page | Last completed page |
| total_pages | Total pages expected |
| rows_extracted | Rows so far |

### `baliza_state.uploaded_to_ia`
Internet Archive upload tracking:
| Column | Description |
|--------|-------------|
| item_id | IA item identifier |
| extraction_date | Date of data |
| uploaded_at | Upload timestamp |
| file_count | Number of files |
| total_rows | Rows uploaded |

## GitHub Actions Workflows

### `continuous-extract.yml`
- **Schedule**: Every 30 minutes
- **Purpose**: Keep data fresh
- **Priority**: D-1 (yesterday first)

### `historical-backfill.yml`
- **Schedule**: Every 30 minutes
- **Purpose**: Fill historical gaps
- **Batch size**: 5 days per run
- **Features**: Export to IA, buffer cleanup

## Alert Thresholds

| Condition | Action |
|-----------|--------|
| Pending checkpoints > 3 | Check for API issues |
| No extraction for 2+ hours | Check workflow status |
| Buffer > 100 days | Increase export frequency |
| IA upload failures | Check credentials |

## Troubleshooting

### Extraction stuck on a page
```bash
# Check checkpoint status
baliza buffer-stats

# Resume will happen automatically on next run
# Or manually trigger extraction for that date
baliza extract --start YYYY-MM-DD --end YYYY-MM-DD
```

### Missing data for a date
```bash
# Verify the gap
baliza verify --resource contratos --start YYYY-MM-DD --end YYYY-MM-DD

# Extract the missing date
baliza extract --start YYYY-MM-DD --end YYYY-MM-DD
```

### Export failed
```bash
# Try exporting manually
baliza export-daily --date YYYY-MM-DD --output ./data/daily

# Check the metadata
cat data/daily/YYYY-MM-DD/_metadata.json
```

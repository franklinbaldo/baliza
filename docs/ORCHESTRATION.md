# Baliza Orchestration Guide

**Target Audience:** Developers building `baliza-site` or other automation systems that consume the Baliza CLI

**Last Updated:** 2026-01-19

## Overview

This guide documents the stable interface contract for the Baliza CLI, enabling reliable orchestration in GitHub Actions, CI/CD pipelines, and automated workflows.

**Key Principle:** Baliza is designed to be a **reliable, predictable CLI tool** that can be safely automated without human intervention.

## Table of Contents

1. [CLI Contract](#cli-contract)
2. [Installation in CI/CD](#installation-in-cicd)
3. [Command Reference](#command-reference)
4. [Exit Codes](#exit-codes)
5. [JSON Output Mode](#json-output-mode)
6. [State Management](#state-management)
7. [Error Handling](#error-handling)
8. [GitHub Actions Examples](#github-actions-examples)
9. [Docker Usage](#docker-usage)
10. [Best Practices](#best-practices)

---

## CLI Contract

### Stability Guarantees

Baliza follows **semantic versioning** (SemVer):

```
MAJOR.MINOR.PATCH
  │     │     │
  │     │     └─ Bug fixes (backwards compatible)
  │     └─────── New features (backwards compatible)
  └───────────── Breaking changes (requires migration)
```

**Current Version:** 0.x (Pre-1.0)
- Minor versions may include breaking changes
- Check CHANGELOG.md before upgrading
- Pin to specific version in production

**Post-1.0 Guarantees:**
- CLI flags will not be removed without deprecation
- Exit codes will remain stable
- JSON output schema will be versioned
- Breaking changes only in major versions

### Command Stability

| Command | Stability | Notes |
|---------|-----------|-------|
| `baliza extract` | ✅ Stable | Core command, will not change |
| `baliza backfill` | ✅ Stable | Core command, will not change |
| `baliza export` | ✅ Stable | Core command, will not change |
| `baliza verify` | ✅ Stable | Core command, will not change |
| `baliza state show` | ✅ Stable | Core command, will not change |
| `baliza state gaps` | ✅ Stable | Core command, will not change |
| `baliza state history` | ✅ Stable | Core command, will not change |

### Dependencies

**Required:**
- Python 3.11+
- ~500MB disk space for DuckDB
- Internet connection to PNCP API

**Optional:**
- Internet Archive credentials (for `export --ia-upload`)

---

## Installation in CI/CD

### Option 1: uvx (Recommended)

Execute directly from GitHub without cloning:

```bash
# Latest from main branch
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --version

# Specific version (when tagged)
uvx --from "git+https://github.com/franklinbaldo/baliza@v0.4.0" baliza --version

# Specific commit
uvx --from "git+https://github.com/franklinbaldo/baliza@abc1234" baliza extract
```

**Advantages:**
- ✅ No installation step required
- ✅ Isolated environment automatically
- ✅ Easy version pinning
- ✅ Works in any CI environment

### Option 2: pip install (PyPI - Future)

```bash
# When published to PyPI
pip install baliza==0.4.0
baliza extract
```

### Option 3: Docker (Planned)

```bash
docker run ghcr.io/franklinbaldo/baliza:v0.4.0 extract
```

---

## Command Reference

### baliza extract

**Purpose:** Incremental extraction from PNCP API

```bash
baliza extract [OPTIONS]
```

**Options:**
```
--lookback-days INTEGER    Days to look back for updates [default: 3]
--config PATH              Path to declarative source configuration
--duckdb PATH              Path to DuckDB database file [default: baliza.duckdb]
--dataset TEXT             Destination dataset name [default: baliza_raw]
```

**Behavior:**
- Automatically detects gaps using StateManager
- Processes windows sequentially
- Saves state after each successful window
- Safe to interrupt (Ctrl+C) - will resume later

**Example:**
```bash
baliza extract --lookback-days 5 --duckdb ./data/baliza.duckdb
```

### baliza backfill

**Purpose:** Deterministic historical data consolidation

```bash
baliza backfill START_MONTH END_MONTH [OPTIONS]
```

**Arguments:**
```
START_MONTH    Start month in YYYY-MM format
END_MONTH      End month in YYYY-MM format
```

**Options:**
```
--config PATH    Path to declarative source configuration
--duckdb PATH    Path to DuckDB database file [default: baliza.duckdb]
--dataset TEXT   Destination dataset name [default: baliza_raw]
```

**Behavior:**
- Processes full months deterministically
- Does not reuse incremental state
- Idempotent (safe to run multiple times)
- Uses separate pipeline instance

**Example:**
```bash
baliza backfill 2024-01 2024-12
```

### baliza export

**Purpose:** Export DuckDB data to Parquet files

```bash
baliza export [OPTIONS]
```

**Options:**
```
--table TEXT         Table name to export [required]
--out PATH           Output directory [default: ./data]
--duckdb PATH        Path to DuckDB database file [default: baliza.duckdb]
--start DATE         Start date filter (YYYY-MM-DD)
--end DATE           End date filter (YYYY-MM-DD)
--ia-upload          Upload to Internet Archive (requires IA credentials)
--ia-identifier TEXT Internet Archive item identifier
```

**Behavior:**
- Exports to Hive-partitioned Parquet (year=YYYY/month=MM/)
- Creates directory if it doesn't exist
- Overwrites existing files
- Optionally uploads to Internet Archive

**Example:**
```bash
baliza export --table contratos --out ./parquet --start 2024-01-01 --end 2024-12-31
```

### baliza verify

**Purpose:** Verify data coverage and integrity

```bash
baliza verify [OPTIONS]
```

**Options:**
```
--resource TEXT    Resource name [required]
--duckdb PATH      Path to DuckDB database file [default: baliza.duckdb]
--start DATE       Start date filter (YYYY-MM-DD)
--end DATE         End date filter (YYYY-MM-DD)
```

**Behavior:**
- Queries coverage table for gaps
- Fetches page 1 from API to detect suspect windows
- Shows human-readable summary with icons
- Non-zero exit if gaps found

**Example:**
```bash
baliza verify --resource contratos --start 2024-01-01 --end 2024-12-31
```

### baliza state commands

**Purpose:** Inspect extraction state

```bash
baliza state show --resource TEXT [OPTIONS]
baliza state gaps --resource TEXT [OPTIONS]
baliza state history --resource TEXT [OPTIONS]
```

**Behavior:**
- Read-only operations
- Show coverage summary, gaps, or run history
- Human-readable output with Rich formatting
- Always exits with 0 (informational only)

---

## Exit Codes

Baliza uses standard Unix exit codes for reliable automation:

| Exit Code | Meaning | When to Retry | Example |
|-----------|---------|---------------|---------|
| **0** | Success | N/A | Extraction completed successfully |
| **1** | General error | Yes (transient) | Network timeout, API error |
| **2** | Invalid arguments | No (fix command) | Missing required flag, invalid date format |
| **64** | Data usage error | No (fix data) | DuckDB file corrupted |
| **70** | Internal error | Yes (bug) | Unexpected exception |

**Exit Code Details:**

### Exit Code 0: Success

```bash
baliza extract --lookback-days 3
echo $?
# Output: 0
```

**Meaning:** Command completed successfully
- All windows processed
- No errors encountered
- State persisted correctly

### Exit Code 1: General Error

```bash
baliza extract --lookback-days 3
# PNCP API returns 500 error
echo $?
# Output: 1
```

**Meaning:** Operation failed due to external issue
- Network error
- API timeout
- Rate limiting

**Retry Strategy:** Exponential backoff recommended

### Exit Code 2: Invalid Arguments

```bash
baliza extract --invalid-flag
echo $?
# Output: 2
```

**Meaning:** User provided invalid input
- Unknown flag
- Invalid date format
- Missing required argument

**Retry Strategy:** Do not retry, fix the command

### Exit Code 64: Data Error

```bash
baliza extract
# DuckDB file is corrupted
echo $?
# Output: 64
```

**Meaning:** Data integrity issue
- Corrupted database file
- Invalid state
- Disk full

**Retry Strategy:** Investigate and repair, then retry

### Exit Code 70: Internal Error

```bash
baliza extract
# Unexpected Python exception
echo $?
# Output: 70
```

**Meaning:** Bug in Baliza CLI
- Unhandled exception
- Logic error

**Retry Strategy:** Report issue, may retry after investigation

---

## JSON Output Mode

All commands support `--json` flag for machine-readable output:

```bash
baliza extract --json
```

### JSON Schema: extract

```json
{
  "status": "success" | "partial" | "failed",
  "command": "extract",
  "timestamp": "2024-01-19T12:00:00Z",
  "windows": {
    "total": 10,
    "processed": 10,
    "failed": 0
  },
  "rows": {
    "extracted": 50000,
    "deduplicated": 49500
  },
  "duration_seconds": 125.5,
  "errors": []
}
```

### JSON Schema: export

```json
{
  "status": "success",
  "command": "export",
  "timestamp": "2024-01-19T12:00:00Z",
  "table": "contratos",
  "rows_exported": 50000,
  "files_created": 12,
  "total_size_bytes": 26542080,
  "output_path": "./data/contratos",
  "partitions": [
    {"year": 2024, "month": 1, "rows": 4200, "file": "year=2024/month=01/data.parquet"},
    {"year": 2024, "month": 2, "rows": 4100, "file": "year=2024/month=02/data.parquet"}
  ]
}
```

### JSON Schema: verify

```json
{
  "status": "complete" | "has_gaps",
  "command": "verify",
  "timestamp": "2024-01-19T12:00:00Z",
  "resource": "contratos",
  "coverage": {
    "total_windows": 365,
    "complete": 350,
    "incomplete": 5,
    "suspect": 3,
    "missing": 7,
    "percentage": 95.9
  },
  "gaps": [
    {
      "start": "2024-01-15",
      "end": "2024-01-17",
      "reason": "missing",
      "windows": 3
    }
  ]
}
```

---

## State Management

### State File Location

Baliza stores state in the DuckDB database:

```
baliza.duckdb
├── baliza_raw schema
│   └── contratos table      # Extracted data
└── baliza_state schema
    ├── runs table           # Run history
    └── cobertura table      # Coverage tracking
```

**State Tables:**

#### baliza_state.runs
```sql
SELECT * FROM baliza_state.runs LIMIT 1;
-- Columns: run_id, resource, start_time, end_time, status, total_rows
```

#### baliza_state.cobertura
```sql
SELECT * FROM baliza_state.cobertura WHERE resource = 'contratos' LIMIT 1;
-- Columns: resource, data_inicio, data_fim, status, total_paginas, hash, created_at
```

### Concurrent Execution

**⚠️ WARNING:** Baliza uses file-based locking to prevent concurrent execution.

```bash
# Process 1
baliza extract &

# Process 2 (will fail immediately)
baliza extract
# Error: Another extraction is in progress (PID 12345)
# Exit code: 1
```

**For parallel processing:** Use separate DuckDB files
```bash
baliza extract --duckdb ./data/run1.duckdb &
baliza extract --duckdb ./data/run2.duckdb &
```

---

## Error Handling

### Network Errors

Baliza automatically retries transient network errors:

**Retry Strategy:**
```
Attempt 1: immediate
Attempt 2: wait 1s
Attempt 3: wait 2s
Attempt 4: wait 4s
Attempt 5: wait 8s
Give up: exit 1
```

**User Action:** None required, retries are automatic

### API Rate Limiting

When PNCP API returns HTTP 429:
- Baliza respects `Retry-After` header
- Pauses before retry
- Logs rate limit event

**User Action:** None required, handled automatically

### Disk Space

If disk becomes full during export:
- Partial files are cleaned up
- Database remains intact
- Exit code 64

**User Action:** Free disk space and retry

### Database Corruption

If DuckDB file becomes corrupted:
- Clear error message displayed
- Suggests backup restoration
- Exit code 64

**User Action:**
1. Restore from backup, OR
2. Delete corrupted file and re-extract

---

## GitHub Actions Examples

### Example 1: Daily Incremental Extraction

```yaml
name: Daily Data Extraction

on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM UTC
  workflow_dispatch:      # Manual trigger

jobs:
  extract:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout (for storing DuckDB)
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install uv
        run: curl -LsSf https://astral.sh/uv/install.sh | sh

      - name: Restore DuckDB state from cache
        uses: actions/cache@v4
        with:
          path: baliza.duckdb
          key: baliza-state-${{ github.run_number }}
          restore-keys: baliza-state-

      - name: Extract latest data
        id: extract
        run: |
          uvx --from "git+https://github.com/franklinbaldo/baliza@v0.4.0" \
            baliza extract --lookback-days 3 --json > extract-result.json

          # Parse JSON result
          cat extract-result.json | jq .

          # Set outputs for later steps
          echo "rows=$(jq -r .rows.extracted extract-result.json)" >> $GITHUB_OUTPUT
          echo "status=$(jq -r .status extract-result.json)" >> $GITHUB_OUTPUT

      - name: Verify coverage
        run: |
          uvx --from "git+https://github.com/franklinbaldo/baliza@v0.4.0" \
            baliza verify --resource contratos --json > verify-result.json

          cat verify-result.json | jq .

      - name: Export to Parquet
        if: steps.extract.outputs.status == 'success'
        run: |
          uvx --from "git+https://github.com/franklinbaldo/baliza@v0.4.0" \
            baliza export --table contratos --out ./data/contratos --json > export-result.json

          cat export-result.json | jq .

      - name: Upload Parquet files
        if: steps.extract.outputs.status == 'success'
        uses: actions/upload-artifact@v4
        with:
          name: parquet-${{ github.run_number }}
          path: data/contratos/**/*.parquet
          retention-days: 90

      - name: Create summary
        run: |
          echo "## Extraction Summary" >> $GITHUB_STEP_SUMMARY
          echo "- **Rows extracted:** ${{ steps.extract.outputs.rows }}" >> $GITHUB_STEP_SUMMARY
          echo "- **Status:** ${{ steps.extract.outputs.status }}" >> $GITHUB_STEP_SUMMARY
          echo "- **Timestamp:** $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $GITHUB_STEP_SUMMARY
```

### Example 2: Monthly Backfill

```yaml
name: Monthly Backfill

on:
  workflow_dispatch:
    inputs:
      start_month:
        description: 'Start month (YYYY-MM)'
        required: true
      end_month:
        description: 'End month (YYYY-MM)'
        required: true

jobs:
  backfill:
    runs-on: ubuntu-latest

    steps:
      - name: Install uv
        run: curl -LsSf https://astral.sh/uv/install.sh | sh

      - name: Run backfill
        run: |
          uvx --from "git+https://github.com/franklinbaldo/baliza@v0.4.0" \
            baliza backfill \
            ${{ github.event.inputs.start_month }} \
            ${{ github.event.inputs.end_month }} \
            --json > backfill-result.json

          cat backfill-result.json | jq .

      - name: Export backfilled data
        run: |
          uvx --from "git+https://github.com/franklinbaldo/baliza@v0.4.0" \
            baliza export --table contratos --out ./backfill-data

      - name: Create GitHub Release
        uses: softprops/action-gh-release@v1
        with:
          files: backfill-data/**/*.parquet
          tag_name: backfill-${{ github.event.inputs.start_month }}-${{ github.event.inputs.end_month }}
          name: Backfill ${{ github.event.inputs.start_month }} to ${{ github.event.inputs.end_month }}
```

### Example 3: Error Handling with Retry

```yaml
name: Resilient Extraction

on:
  schedule:
    - cron: '0 */6 * * *'  # Every 6 hours

jobs:
  extract:
    runs-on: ubuntu-latest

    steps:
      - name: Extract with retry
        uses: nick-fields/retry@v2
        with:
          timeout_minutes: 30
          max_attempts: 3
          retry_wait_seconds: 300  # 5 minutes between retries
          command: |
            uvx --from "git+https://github.com/franklinbaldo/baliza@v0.4.0" \
              baliza extract --lookback-days 3 --json

          # Only retry on exit code 1 (transient errors)
          retry_on_exit_code: 1

      - name: Notify on failure
        if: failure()
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.create({
              owner: context.repo.owner,
              repo: context.repo.repo,
              title: 'Extraction failed after retries',
              body: 'The automated extraction failed. Please investigate.',
              labels: ['automated', 'extraction-failure']
            })
```

---

## Docker Usage

### Dockerfile (Planned)

```dockerfile
FROM python:3.11-slim

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:$PATH"

# Install baliza
RUN uvx --from "git+https://github.com/franklinbaldo/baliza@v0.4.0" baliza --version

# Set working directory
WORKDIR /data

# Default command
ENTRYPOINT ["uvx", "--from", "git+https://github.com/franklinbaldo/baliza@v0.4.0", "baliza"]
CMD ["--help"]
```

### Usage

```bash
# Build image
docker build -t baliza:latest .

# Run extraction
docker run -v $(pwd)/data:/data baliza:latest extract --lookback-days 3

# Run export
docker run -v $(pwd)/data:/data baliza:latest export --table contratos --out /data/parquet
```

---

## Best Practices

### 1. Version Pinning

Always pin to a specific version in production:

```yaml
# Good
uvx --from "git+https://github.com/franklinbaldo/baliza@v0.4.0" baliza extract

# Bad (uses latest, may break)
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract
```

### 2. State Management

Store DuckDB file persistently:

```yaml
# Good: Use cache or artifact storage
- uses: actions/cache@v4
  with:
    path: baliza.duckdb
    key: baliza-state-${{ github.run_number }}

# Bad: No state persistence (re-extracts everything every time)
- run: baliza extract
```

### 3. Error Handling

Check exit codes and handle failures:

```bash
# Good
if baliza extract --json > result.json; then
  echo "Success"
  cat result.json | jq .
else
  exit_code=$?
  echo "Failed with exit code $exit_code"
  if [ $exit_code -eq 1 ]; then
    echo "Transient error, will retry"
  fi
fi

# Bad (ignores errors)
baliza extract || true
```

### 4. Resource Limits

Set reasonable timeouts and resource limits:

```yaml
# Good
jobs:
  extract:
    runs-on: ubuntu-latest
    timeout-minutes: 60  # Fail if extraction takes > 1 hour
```

### 5. Monitoring

Always capture and log JSON output:

```yaml
# Good
- run: |
    baliza extract --json | tee extract.json
    cat extract.json | jq .rows.extracted
```

### 6. Verification

Always verify after extraction:

```bash
# Good
baliza extract --lookback-days 3
baliza verify --resource contratos

# Bad (no verification)
baliza extract
```

---

## Troubleshooting

### Issue: "Another extraction is in progress"

**Cause:** Lock file exists from previous run

**Solution:**
```bash
# Check for running processes
ps aux | grep baliza

# If no process, remove lock manually
rm baliza.duckdb.lock
```

### Issue: "DuckDB file corrupted"

**Cause:** Disk full, hard shutdown, or bug

**Solution:**
```bash
# Restore from backup
cp baliza.duckdb.backup baliza.duckdb

# OR start fresh
rm baliza.duckdb
baliza extract --lookback-days 30
```

### Issue: Extraction is very slow

**Cause:** Large lookback window or network issues

**Solution:**
```bash
# Reduce lookback
baliza extract --lookback-days 1

# Check network connectivity to PNCP
curl -I https://pncp.gov.br/api/consulta/v1/contratos
```

---

## Support

**Issues:** https://github.com/franklinbaldo/baliza/issues
**Discussions:** https://github.com/franklinbaldo/baliza/discussions
**Changelog:** https://github.com/franklinbaldo/baliza/blob/main/CHANGELOG.md

For `baliza-site` specific issues, use the baliza-site repository.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 0.4.0 | 2026-01-19 | Initial orchestration guide |

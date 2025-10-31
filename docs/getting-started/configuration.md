# Configuration

Baliza uses a declarative configuration approach based on YAML files. The main configuration file is located at `src/baliza/config/pncp.yml`.

## Configuration File Structure

The declarative pipeline configuration is stored in `src/baliza/config/pncp.yml`. Here you can adjust:

- Default pagination parameters (`tamanhoPagina=500`, `pagina=1`)
- Initial/final dates used by incremental extraction (`initial_value`, `lookback_days` via CLI) always converted to `AAAAMMDD`
- Standard response mapping (`data`, `totalPaginas`, etc.), preserving `numeroControlePNCP` as the textual primary key

## API Endpoints

The public PNCP API is at `https://pncp.gov.br/api/consulta` and returns an envelope with:

- `data` - Array of records
- `totalRegistros` - Total number of records
- `totalPaginas` - Total number of pages
- `numeroPagina` - Current page number
- `paginasRestantes` - Remaining pages
- `empty` - Whether the response is empty

Baliza's configuration consumes these fields, treating `204 No Content` responses as empty windows (without error) and always respecting `tamanhoPagina ≤ 500`.

## Custom Configuration

To use a custom configuration, provide the path via `--config`:

```bash
uv run baliza extract --config configs/pncp-custom.yml
```

## Command-Line Options

### Global Options

```bash
baliza --help
```

### Extract Options

```bash
baliza extract \
  --duckdb /path/to/file.duckdb \
  --dataset baliza_raw \
  --lookback-days 7 \
  --config configs/pncp-custom.yml
```

Options:

- `--duckdb PATH` - Specifies the DuckDB destination file
- `--dataset NAME` - Specifies the schema inside DuckDB (default: `baliza_raw`)
- `--lookback-days N` - Looks back N days from the last saved cursor when building the incremental window
- `--config PATH` - Path to custom configuration file

### Export Options

```bash
baliza export \
  --table contratos \
  --out data/contratos \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

Options:

- `--table NAME` - Table name to export
- `--out PATH` - Output directory for Parquet files
- `--start-date YYYY-MM-DD` - Start date for export range
- `--end-date YYYY-MM-DD` - End date for export range

This delimits the exported range and creates `data/<resource>/ano=YYYY/mes=MM/*.parquet`.

### Backfill Options

```bash
baliza backfill 2024-01 2024-12 \
  --duckdb /path/to/file.duckdb \
  --dataset baliza_raw
```

Arguments:

- First argument: Start month in `YYYY-MM` format
- Second argument: End month in `YYYY-MM` format

### Verify Options

```bash
baliza verify \
  --output report.json \
  --sequencia
```

Options:

- `--output PATH` - Path to save the JSON report
- `--sequencia` - Activates the audit of `sequencialCompra`/`sequencialContrato`

## Incremental Policy

### Daily Lookback

Every execution of `baliza extract` looks back (by default) three days from the saved cursor, opening `dataInicial`/`dataFinal` windows in this interval. Since the public API does not have a global update filter, this redundancy ensures capture of recent rectifications.

### Monthly Backfill

The `baliza backfill <YYYY-MM> <YYYY-MM>` command re-executes entire months in sequence, reusing the same DuckDB. This covers late rectifications and consolidates historical data.

### Coverage Manifest

In addition to using `write_disposition=merge`, Baliza records `totalPaginas`, item count, and hashes per page to audit windows and identify missing pages.

### Future Integrations

The current design separates client configuration, allowing integration with the authenticated API (Integration Manual) in an alternative mode if it becomes necessary to leverage `dataAtualizacaoGlobal` filters in the future.

## Export Configuration

### Bronze in DuckDB

Raw data remains in `baliza.duckdb` within the `baliza_raw` dataset.

### Partitioned Parquet

`baliza export` generates `data/<resource>/ano=YYYY/mes=MM/*.parquet` from a domain date column (in the case of contracts, the PNCP publication date; in its absence, the best available proxy is used and documented in the CLI).

### Incremental Consumption

Parquet files follow the same primary key used in DuckDB, preserving the official `numeroControlePNCP` mask.

## Environment Variables

Currently, Baliza does not use environment variables for configuration. All configuration is done through YAML files and command-line options.

## Next Steps

- [Quick Start](quickstart.md) - Run your first extraction
- [API Reference](../api/cli.md) - Detailed API documentation

# Integration Tests with pytest-vcr

This directory contains integration tests for the Baliza CLI that use **pytest-vcr** to record and replay HTTP interactions with the PNCP API.

## Why VCR?

VCR (Video Cassette Recorder) for HTTP:
- ✅ **Fast tests** - Replays recorded responses instead of making real API calls
- ✅ **Reliable** - Tests don't fail due to network issues or API downtime
- ✅ **Reproducible** - Same responses every time
- ✅ **Works offline** - No internet connection needed once cassettes are recorded
- ✅ **CI/CD friendly** - No API rate limits or IP blocks

## How It Works

1. **First run**: Test makes real HTTP request → Response is recorded to a "cassette" file (YAML)
2. **Subsequent runs**: Test replays the recorded response → No real HTTP request

## Directory Structure

```
tests/integration/
├── README.md                           # This file
├── __init__.py
├── test_pncp_api_simple.py            # Simple API tests
├── test_pncp_real_api.py              # Full pipeline tests (disabled - API blocked)
└── ../cassettes/                       # VCR cassette files (YAML)
    ├── test_pncp_api_get_contratos_single_day.yaml
    ├── test_pncp_api_pagination.yaml
    └── ...
```

## Running Integration Tests

### With Existing Cassettes (No API Calls)

```bash
# Run all integration tests (uses recorded cassettes)
pytest tests/integration/

# Run specific test
pytest tests/integration/test_pncp_api_simple.py::test_pncp_api_get_contratos_single_day -v
```

**Note**: These tests will be FAST (no real HTTP calls) if cassettes exist.

### Recording New Cassettes

To record new HTTP interactions:

```bash
# 1. Delete old cassettes
rm -rf tests/cassettes/*.yaml

# 2. Run tests (will make real API calls and record responses)
pytest tests/integration/

# 3. Commit the new cassettes
git add tests/cassettes/
git commit -m "chore: update VCR cassettes"
```

**Important**: Recording requires:
- Internet connection
- Access to PNCP API (not blocked by firewall/IP)
- May take longer (real HTTP requests)

## Current Status

⚠️ **API Access Issue**: The PNCP API is currently returning `403 Forbidden` in some environments (CI/CD, certain IPs). This prevents recording new cassettes.

**Solutions**:
1. **Record locally**: Run tests on your local machine to record cassettes
2. **Use existing cassettes**: If cassettes exist, tests will work fine
3. **Mock-based tests**: Use unit tests in `tests/e2e/` (don't require real API)

## Configuration

VCR is configured in `tests/conftest.py`:

```python
@pytest.fixture(scope="module")
def vcr_config():
    return {
        "record_mode": "once",  # Record once, then always replay
        "match_on": ["uri", "method"],  # Match by URL and method
        "filter_headers": [
            ("authorization", "REDACTED"),  # Don't record auth headers
            ("cookie", "REDACTED"),
        ],
        "cassette_library_dir": "tests/cassettes",
        "decode_compressed_response": True,  # Handle gzip
    }
```

### Record Modes

- `once` (default): Record if cassette doesn't exist, replay if it does
- `new_episodes`: Record new interactions, replay existing ones
- `none`: Never record (fail if cassette missing)
- `all`: Always record (overwrite cassettes)

Change mode by setting `record_mode` in `vcr_config`.

## Writing VCR Tests

### Simple Example

```python
import httpx
import pytest

@pytest.mark.vcr()  # <-- This decorator enables VCR
def test_pncp_api():
    response = httpx.get("https://pncp.gov.br/api/consulta/v1/contratos", params={
        "dataInicial": "20241001",
        "dataFinal": "20241001",
        "pagina": 1,
        "tamanhoPagina": 10,
    })

    assert response.status_code == 200
    data = response.json()
    assert "data" in data
```

**What happens**:
1. First run: Makes real GET request → Saves response to `tests/cassettes/test_pncp_api.yaml`
2. Next runs: Replays saved response → No HTTP request

### Testing Baliza CLI

```python
import pytest
from baliza.pipelines import pncp

@pytest.mark.vcr()
def test_baliza_extract():
    """Test real Baliza extract with VCR recording."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"

        # This will make HTTP requests on first run
        # Then replay from cassette on subsequent runs
        pipeline, run_info = pncp.run_pncp(
            duckdb_path=db_path,
            dataset="test",
            range_start="2024-10-01",
            range_end="2024-10-01",
        )

        assert run_info is not None
        # Verify data was loaded...
```

## Troubleshooting

### "Cassette not found" Error

**Problem**: Cassette file doesn't exist.

**Solution**:
```bash
# Run test to record cassette
pytest tests/integration/test_file.py::test_name -v

# Check cassette was created
ls tests/cassettes/
```

### "403 Forbidden" When Recording

**Problem**: PNCP API blocks the IP or user-agent.

**Solutions**:
1. Record from a different network/environment
2. Use VPN
3. Update User-Agent header
4. Use existing cassettes (don't re-record)

### Cassette Doesn't Match Request

**Problem**: Test makes different request than recorded.

**Solution**:
```bash
# Re-record cassette
rm tests/cassettes/problematic_test.yaml
pytest tests/integration/test_file.py::problematic_test -v
```

### Tests Slow Despite Cassettes

**Problem**: Tests still making HTTP calls.

**Check**:
1. Cassettes exist in `tests/cassettes/`
2. Test has `@pytest.mark.vcr()` decorator
3. Request URL/params match cassette exactly

## Best Practices

### ✅ Do

- **Commit cassettes** to version control (they're part of the test suite)
- **Use small responses** (limit `tamanhoPagina` in tests)
- **Filter sensitive data** (configured in `vcr_config`)
- **Test edge cases** (empty responses, errors, pagination)
- **Document cassette date** (know when to re-record)

### ❌ Don't

- **Don't record production secrets** (use `filter_headers`)
- **Don't use huge datasets** in tests (keep cassettes small)
- **Don't rely on current dates** (use fixed dates like "2024-10-01")
- **Don't skip cassette commits** (other developers need them)

## Re-recording Strategy

When to re-record cassettes:

1. **API schema changes** - PNCP adds/removes fields
2. **Bug investigation** - Verify current API behavior
3. **Periodic refresh** - Every 3-6 months to catch API changes

How to re-record specific tests:

```bash
# Re-record one test
rm tests/cassettes/test_pncp_api_pagination.yaml
pytest tests/integration/test_pncp_api_simple.py::test_pncp_api_pagination -v

# Re-record all tests
rm -rf tests/cassettes/*.yaml
pytest tests/integration/ -v
```

## Alternative: Manual Cassettes

If API access is blocked, you can create cassettes manually:

```bash
# Make request with curl
curl "https://pncp.gov.br/api/consulta/v1/contratos?dataInicial=20241001&dataFinal=20241001&pagina=1&tamanhoPagina=10" \
  -H "Accept: application/json" \
  -H "User-Agent: baliza/0.1" \
  > response.json

# Convert to VCR YAML format (see existing cassettes for structure)
# Place in tests/cassettes/test_name.yaml
```

## References

- **VCRpy**: https://vcrpy.readthedocs.io/
- **pytest-vcr**: https://pytest-vcr.readthedocs.io/
- **PNCP API Docs**: https://pncp.gov.br/api/consulta

---

**Questions?** See full project docs at [`/docs/`](../../docs/)

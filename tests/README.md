# 🧪 Baliza E2E Test Suite

**ADR**: This project uses **E2E tests only** - no unit tests, no mocks for validation.

---

## 🎯 Testing Philosophy

**E2E tests are the single source of truth**. We test the complete system behavior:
- Real API calls to PNCP
- Real database operations with DuckDB
- Real file system operations
- Real CLI interface testing

**No unit tests, no mocks** - we trust the system works as a whole.

---

## 📋 Current Structure

```
tests/
├── conftest.py          # E2E test configuration
├── README.md           # This file
└── (future E2E tests)  # Coming soon
```

---

## 🚀 Running E2E Tests

### **Current E2E Testing** (Manual)
```bash
# Test extraction with real API
uv run baliza extract --start-date 2024-01-01 --end-date 2024-01-01

# Test statistics
uv run baliza stats

# Test MCP server
uv run baliza mcp
```

### **Future Automated E2E Tests**
```bash
# Run all E2E tests (when implemented)
uv run pytest tests/ -m e2e

# Run slow E2E tests
uv run pytest tests/ -m "e2e and slow"

# Run specific E2E test
uv run pytest tests/test_e2e_extraction.py::test_extract_real_data
```

---

## 🔧 E2E Test Implementation Plan

### **Planned E2E Tests**
1. **`test_e2e_extraction.py`** - Test complete extraction workflow
2. **`test_e2e_cli.py`** - Test CLI commands end-to-end
3. **`test_e2e_database.py`** - Test database operations with real data
4. **`test_e2e_mcp.py`** - Test MCP server functionality

### **E2E Test Structure**
```python
import pytest
from datetime import date

@pytest.mark.e2e
@pytest.mark.slow
async def test_extract_real_data():
    """E2E test: Extract real data from PNCP API"""
    # Test with 1 day only to keep it fast
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 1)
    
    # This hits the real API, real database, everything
    result = await extractor.extract_data(start_date, end_date)
    
    # Validate E2E behavior
    assert result["total_records_extracted"] >= 0
    assert result["successful_requests"] > 0
    assert result["failed_requests"] == 0
```

---

## ✅ E2E Test Validation

### **What E2E Tests Validate**
- ✅ **Real API Integration**: PNCP endpoints work correctly
- ✅ **Database Operations**: DuckDB schema and data storage
- ✅ **Error Handling**: Network failures, API errors, rate limits
- ✅ **CLI Interface**: Command-line functionality
- ✅ **Data Processing**: JSON parsing, data transformation
- ✅ **File Operations**: Database files, logs, results
- ✅ **Performance**: Extraction speed, memory usage
- ✅ **Async Operations**: Concurrent request handling

### **What E2E Tests Don't Test**
- ❌ **Unit-level functions** - We trust the implementation
- ❌ **Mocked scenarios** - Only real interactions matter
- ❌ **Isolated components** - Only full system behavior counts

---

## 🎯 Benefits of E2E-First Approach

1. **Real Confidence**: Tests prove the system actually works
2. **No False Positives**: Mocks can't lie about real API behavior
3. **Simple Maintenance**: No complex mock setup and maintenance
4. **User-Centric**: Tests validate what users actually experience
5. **Integration Validation**: Catches integration issues immediately

---

## 🔧 E2E Test Guidelines

### **Test Data Strategy**
- Use **real PNCP API** for all tests
- Test with **short date ranges** (1 day) for speed
- Use **known good dates** that have data
- Accept **network dependencies** as part of E2E reality

### **Test Stability**
- Use `tenacity` for retry logic on network failures
- Set appropriate timeouts for API calls
- Handle rate limiting gracefully
- Test in CI with real API (accept occasional failures)

### **Test Organization**
- Mark all tests with `@pytest.mark.e2e`
- Mark slow tests with `@pytest.mark.slow`
- Use descriptive test names: `test_extract_contracts_for_single_day`
- Focus on **user workflows**, not technical implementation

---

## 🚀 Next Steps

1. **Implement E2E Test Suite** - Create automated E2E tests
2. **Add Tenacity Retry Logic** - Make tests resilient to network issues
3. **Setup CI/CD Pipeline** - Run E2E tests on every merge
4. **Monitor Test Stability** - Track and improve test reliability

---

**Philosophy**: *"E2E tests are the source of truth. Everything else is just implementation detail."*
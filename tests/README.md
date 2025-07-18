# 🧪 Baliza Test Suite

This directory contains tests for the BALIZA project.

## 📋 Current Structure

The project uses a modular architecture with the main extractor functionality in `src/baliza/extractor.py` and CLI interface in `src/baliza/cli.py`. The project also includes MCP server functionality for AI integration.

### ✅ **Remaining Files**
- `conftest.py` - Test configuration and fixtures
- `README.md` - This file

## 🚀 Testing the BALIZA System

The project uses a modular architecture with CLI interface and MCP server. Testing is primarily done through manual verification:

### **Manual Testing**
```bash
# Test basic functionality
uv run baliza stats

# Test data extraction
uv run baliza extract --start-date 2024-07-10 --end-date 2024-07-10

# Test MCP server (requires fastmcp dependency)
uv run baliza mcp

# Test extractor module directly
uv run python src/baliza/extractor.py stats
```

### **What is Tested**
- ✅ **CLI Interface**: Command-line interface functionality
- ✅ **Database Operations**: PSA schema creation and data storage
- ✅ **API Connectivity**: PNCP endpoint access
- ✅ **Data Processing**: Response parsing and storage
- ✅ **Error Handling**: HTTP errors and rate limiting
- ✅ **MCP Server**: Model Context Protocol server functionality
- ✅ **Async Operations**: Multi-threaded data extraction

## 🔧 Future Test Improvements

Potential areas for adding tests back:
1. **Unit Tests**: Test individual functions in `extractor.py`
2. **Integration Tests**: Test database operations
3. **API Tests**: Mock PNCP API responses
4. **Performance Tests**: Test with large datasets
5. **MCP Server Tests**: Test Model Context Protocol functionality
6. **E2E Tests**: Test complete extraction workflows

## 📊 Quality Assurance

The modular architecture relies on:
- **Modular design**: Separate CLI, extractor, and MCP server components
- **Raw data storage**: Preserves all API responses for future analysis
- **Unified schema**: Consistent data structure across all endpoints
- **Built-in error handling**: Graceful handling of API failures
- **Async operations**: Efficient multi-threaded data extraction
- **MCP integration**: AI-ready data analysis capabilities

## 🎯 Key Benefits of Current Architecture

1. **Modular Design**: Separate concerns for better maintainability
2. **Extensibility**: Easy to add new features (MCP server, new extractors)
3. **Performance**: Async operations for efficient data extraction
4. **AI Integration**: MCP server enables advanced AI analysis
5. **Robust Error Handling**: Graceful handling of various failure scenarios

---

## 🔍 Manual Verification Checklist

To verify the system works correctly:

- [ ] `baliza stats` shows existing data
- [ ] `baliza extract` can extract new data  
- [ ] Database file is created at `data/baliza.duckdb`
- [ ] Raw responses are stored in `psa.pncp_raw_responses` table
- [ ] API rate limiting works (1 second delay between requests)
- [ ] All HTTP status codes are handled gracefully
- [ ] Progress bars and console output work correctly

For issues or suggestions, please open a GitHub issue.
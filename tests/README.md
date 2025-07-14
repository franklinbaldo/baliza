# 🧪 Baliza Test Suite

This directory contains tests for the simplified Baliza project.

## 📋 Current Structure

The project has been refactored to use a simplified architecture with a single script. Most tests have been removed as they were for the old complex architecture.

### ✅ **Remaining Files**
- `conftest.py` - Test configuration and fixtures
- `README.md` - This file

## 🚀 Testing the Simplified Script

Since the project now uses a single self-contained script, testing is primarily done through manual verification:

### **Manual Testing**
```bash
# Test basic functionality
uv run baliza stats

# Test data extraction
uv run baliza extract --start-date 2024-07-10 --end-date 2024-07-10

# Test script directly
uv run python src/baliza/simple_pncp_extractor.py stats
```

### **What is Tested**
- ✅ **Script Execution**: Basic command-line interface
- ✅ **Database Operations**: PSA schema creation and data storage
- ✅ **API Connectivity**: PNCP endpoint access
- ✅ **Data Processing**: Response parsing and storage
- ✅ **Error Handling**: HTTP errors and rate limiting

## 🔧 Future Test Improvements

Potential areas for adding tests back:
1. **Unit Tests**: Test individual functions in `simple_pncp_extractor.py`
2. **Integration Tests**: Test database operations
3. **API Tests**: Mock PNCP API responses
4. **Performance Tests**: Test with large datasets

## 📊 Quality Assurance

The simplified architecture relies on:
- **Self-contained script**: Reduces complexity and potential failure points
- **Raw data storage**: Preserves all API responses for future analysis
- **Unified schema**: Consistent data structure across all endpoints
- **Built-in error handling**: Graceful handling of API failures

## 🎯 Key Benefits of Simplified Architecture

1. **Reduced Test Complexity**: Single script is easier to test
2. **Better Maintainability**: Less code means fewer bugs
3. **Increased Reliability**: Fewer dependencies and moving parts
4. **Simplified Deployment**: Single script deployment

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
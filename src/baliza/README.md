# BALIZA - Simplified PNCP Data Extractor

This directory contains the Baliza Python package source code.

## Structure

- `simple_pncp_extractor.py`: Simplified PNCP data extractor (main application)

## Usage

Extract data from PNCP endpoints:

```bash
# Extract data for a specific date
python -m baliza.simple_pncp_extractor extract --start-date 2024-07-10 --end-date 2024-07-10

# View extraction statistics
python -m baliza.simple_pncp_extractor stats

# Or use the installed command
baliza extract --start-date 2024-07-10 --end-date 2024-07-10
baliza stats
```

## Features

- **Simplified Architecture**: Single script that handles all PNCP endpoints
- **Comprehensive Coverage**: Extracts data from all 12 authentication-free PNCP endpoints
- **Raw Data Storage**: Stores all responses with complete metadata
- **Unified Schema**: Single PSA table for all data types
- **Rate Limiting**: Built-in rate limiting to avoid API throttling
- **Progress Tracking**: Rich console interface with progress bars

## Database Schema

All data is stored in a single table `psa.pncp_raw_responses` with complete request and response metadata:

- URL used
- Request parameters
- Response code
- Response content
- Timestamp
- Endpoint metadata
# Baliza Analytics

Advanced analytics and fraud detection for PNCP government contract data from the [Baliza](../../) project.

## Overview

This project consumes clean, structured data from the core Baliza project to perform sophisticated analysis including:

- **Fraud Detection**: Algorithmic scoring of suspicious contract patterns (0-100 scale)
- **Anomaly Detection**: Machine learning identification of unusual procurement behaviors  
- **Supplier Risk Analysis**: Assessment of supplier concentration and risk patterns
- **Compliance Analysis**: Verification against procurement law requirements

## Architecture

```
Baliza (Core)           →    Baliza Analytics (This Project)
├── PSA Data Collection →    ├── Data Connector
├── DuckDB Storage     →    ├── Fraud Detection Algorithms  
└── Clean Data APIs    →    ├── ML Anomaly Detection
                           ├── Risk Scoring
                           └── Reports & Dashboards
```

## Installation

```bash
cd sandbox/baliza-analytics
pip install -e .
```

## Usage

### CLI Interface

```bash
# Show dataset statistics
baliza-analytics stats

# Analyze contracts for suspicious patterns
baliza-analytics analyze --start-date 2024-01-01 --min-score 60

# Filter by state and minimum value
baliza-analytics analyze --uf RO,SP --min-value 1000000

# Supplier risk analysis
baliza-analytics suppliers --limit 50

# Generate interactive marimo report (opens in browser)
baliza-analytics report --output fraud_analysis --port 2718
```

### Python API

```python
from baliza_analytics import BalizaDataConnector, ContractFraudDetector

# Connect to Baliza data
connector = BalizaDataConnector()
contracts = connector.get_contracts(start_date='2024-01-01')

# Detect suspicious patterns
detector = ContractFraudDetector()
suspicious = detector.get_suspicious_contracts(contracts, min_score=70)

# Generate comprehensive report
report = detector.generate_fraud_report(contracts)
print(f"Found {report['summary']['suspicious_contracts']} suspicious contracts")
```

## Fraud Detection Algorithm

### Scoring Factors (0-100 scale):

1. **High Value Contracts** (20 points): Contracts above 99th percentile
2. **Unusual Duration** (15 points): Extremely long (>3x median) or short (<30 days) contracts
3. **Supplier Frequency** (15 points): Same supplier with >10 or >20 contracts  
4. **Round Number Values** (10 points): Values ending in exact multiples of R$ 10,000
5. **Weekend Signatures** (5 points): Contracts signed on weekends
6. **Emergency Keywords** (10 points): "Emergency", "urgent" terms in contract object
7. **Monopoly Patterns** (15 points): Agency-supplier pairs with >5 contracts

### Machine Learning Anomalies:

- **Isolation Forest**: Detects outliers in contract value and duration patterns
- **Feature Engineering**: Normalized numerical features for ML analysis
- **Contamination Rate**: Assumes ~10% anomaly rate in dataset

## Risk Analysis

### Supplier Risk Indicators:

- **Geographic Concentration**: Operating in only one state
- **Agency Concentration**: Contracting with ≤2 agencies
- **High Volume**: Contract count above 95th percentile
- **Risk Score**: Weighted combination (0-75 scale)

## Data Interface

This project connects to Baliza via DuckDB federation:

```python
# Automatic connection to ../../../state/baliza.duckdb
connector = BalizaDataConnector()

# Or specify custom path
connector = BalizaDataConnector('/path/to/baliza.duckdb')
```

### Available Data Fields:

- **Identifiers**: `contrato_id`, `orgao_cnpj`, `fornecedor_ni`
- **Financial**: `valor_global_brl`, `valor_inicial_brl`
- **Temporal**: `data_assinatura`, `duracao_contrato_dias`
- **Geographic**: `uf_sigla`, `municipio_nome`
- **Quality Flags**: `flag_valor_invalido`, `flag_datas_incompletas`

## Example Results

### Typical Fraud Report:
```
📊 Fraud Detection Summary
┌─────────────────────┬──────────┐
│ Total Contracts     │ 50,000   │
│ Suspicious Contracts│ 2,150    │
│ Suspicion Rate      │ 4.30%    │
│ ML Anomalies        │ 890      │
│ High Risk Suppliers │ 45       │
└─────────────────────┴──────────┘
```

### Suspicious Contract Example:
- **Score**: 85/100
- **Agency**: Secretaria Municipal de Obras - Porto Velho/RO  
- **Supplier**: Construtora XYZ Ltda
- **Value**: R$ 5.8M (round number, high value)
- **Issues**: Weekend signature + Emergency keywords + Supplier frequency

## Interactive Reports (Marimo)

The project includes beautiful interactive reports built with [Marimo](https://marimo.io/):

### Fraud Analysis Report (`fraud_analysis.py`)

- **📊 Interactive Dashboard**: Filter by date, state, contract value
- **📈 Real-time Charts**: Risk distribution, score histograms, temporal analysis  
- **🔥 Suspicious Contracts Table**: Top suspicious contracts with details
- **💰 Value vs Score Analysis**: Scatter plots and correlations
- **📅 Timeline Visualization**: Monthly trends and patterns

**To run:**
```bash
baliza-analytics report --output fraud_analysis
# Opens at http://localhost:2718
```

### Features:
- **Reactive UI**: Dynamic filtering and real-time updates
- **Beautiful Visualizations**: Plotly charts with hover details
- **Statistical Tables**: Sortable and paginated data views
- **Export Capabilities**: Download charts and data
- **Mobile Responsive**: Works on tablets and phones

## Future Enhancements

- **Additional Marimo Reports**: Supplier network analysis, compliance dashboards
- **Time Series Analysis**: Trend detection and forecasting
- **Network Analysis**: Relationship mapping between agencies and suppliers  
- **Alert System**: Automated notifications for high-risk contracts
- **API Endpoints**: REST API for external integrations

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Code formatting
ruff format .

# Type checking  
mypy src/
```

This project maintains separation of concerns from the core Baliza data collection system while providing powerful analytical capabilities for transparency and oversight.
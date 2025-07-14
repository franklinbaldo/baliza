# PNCP Endpoint Integration Plan

## Current Status
- ✅ BALIZA currently backs up: `/v1/contratos` (4,163 records/day)
- ❌ NOT backing up: 11 Contratações modalidades (5,810+ records/day)
- ❌ NOT backing up: Atas endpoints (157,232+ records/day)

## Integration Requirements

### 1. Endpoint Configuration Extension
Add all discovered endpoints to the `PNCP_ENDPOINTS` configuration:

```python
PNCP_ENDPOINTS = {
    # Existing
    "contratos_publicacao": {
        "api_path": "/v1/contratos",
        "file_prefix": "pncp-contratos-publicacao",
    },
    
    # New Contratações endpoints (by modalidade)
    "contratacoes_leilao_eletronico": {
        "api_path": "/v1/contratacoes/publicacao", 
        "file_prefix": "pncp-contratacoes-leilao-eletronico",
        "modalidade": 1
    },
    "contratacoes_concurso": {
        "api_path": "/v1/contratacoes/publicacao",
        "file_prefix": "pncp-contratacoes-concurso", 
        "modalidade": 3
    },
    "contratacoes_concorrencia_eletronica": {
        "api_path": "/v1/contratacoes/publicacao",
        "file_prefix": "pncp-contratacoes-concorrencia-eletronica",
        "modalidade": 4
    },
    "contratacoes_pregao_eletronico": {
        "api_path": "/v1/contratacoes/publicacao",
        "file_prefix": "pncp-contratacoes-pregao-eletronico", 
        "modalidade": 6
    },
    "contratacoes_dispensa": {
        "api_path": "/v1/contratacoes/publicacao",
        "file_prefix": "pncp-contratacoes-dispensa",
        "modalidade": 8
    },
    "contratacoes_inexigibilidade": {
        "api_path": "/v1/contratacoes/publicacao",
        "file_prefix": "pncp-contratacoes-inexigibilidade",
        "modalidade": 9
    },
    
    # Atas endpoints
    "atas_periodo": {
        "api_path": "/v1/atas",
        "file_prefix": "pncp-atas-periodo",
    },
    "atas_atualizacao": {
        "api_path": "/v1/atas/atualizacao", 
        "file_prefix": "pncp-atas-atualizacao",
    },
}
```

### 2. Database Schema Updates
Create separate tables for each data type:

```sql
-- Contratações (procurement processes)
CREATE TABLE IF NOT EXISTS psa.contratacoes_raw (
    -- Metadata
    baliza_extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    baliza_data_date DATE,
    baliza_endpoint VARCHAR,
    baliza_modalidade INTEGER,
    
    -- PNCP fields
    numeroControlePNCP VARCHAR,
    modalidadeId INTEGER,
    modalidadeNome VARCHAR,
    orgaoEntidade_json VARCHAR,
    valorTotalEstimado DOUBLE,
    -- ... other fields
    raw_data_json VARCHAR
);

-- Atas (price registration records)  
CREATE TABLE IF NOT EXISTS psa.atas_raw (
    -- Metadata
    baliza_extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    baliza_data_date DATE,
    baliza_endpoint VARCHAR,
    
    -- PNCP fields
    numeroControlePNCPAta VARCHAR,
    numeroAtaRegistroPreco VARCHAR,
    orgaoEntidade_json VARCHAR,
    -- ... other fields
    raw_data_json VARCHAR
);
```

### 3. Client Wrapper Extensions
Extend `PNCPClient` to support all endpoints:

```python
def fetch_contratacoes_data(self, modalidade: int, data_inicial: str, data_final: str, pagina: int, tamanho_pagina: int):
    """Fetch contratações data for specific modalidade."""
    
def fetch_atas_data(self, data_inicial: str, data_final: str, pagina: int, tamanho_pagina: int):
    """Fetch atas data."""
    
def fetch_atas_atualizacao_data(self, data_inicial: str, data_final: str, pagina: int, tamanho_pagina: int):
    """Fetch atas updates data."""
```

### 4. Rate Limiting Strategy
- Implement delays between endpoint calls (observed HTTP 429 errors)
- Stagger different endpoints across time
- Consider priority ordering (largest datasets first)

## Implementation Priority

### Phase 1: High-Value Endpoints (Immediate)
1. **Atas Período** (157,232 records/day) - Largest dataset
2. **Dispensa** (2,998 records/day) - Most common procurement type
3. **Pregão Eletrônico** (1,603 records/day) - Primary bidding method

### Phase 2: Medium-Value Endpoints  
4. **Inexigibilidade** (871 records/day)
5. **Atas Atualização** (1,290 records/day)
6. **Concorrência** (205 records/day)

### Phase 3: Low-Value Endpoints
7. All remaining modalidades (< 100 records/day each)

## Estimated Impact
- **Current backup**: ~4,163 records/day
- **After integration**: ~172,000+ records/day (42x increase!)
- **Storage impact**: ~10x increase in parquet files
- **Internet Archive**: Much richer dataset covering full procurement lifecycle

## Next Steps
1. Update endpoint configuration
2. Extend database schema  
3. Update client wrapper methods
4. Modify main backup loop
5. Test with rate limiting
6. Deploy and monitor
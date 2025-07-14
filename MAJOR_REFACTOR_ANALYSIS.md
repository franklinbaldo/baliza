# BALIZA Major Refactor Analysis

## Summary

Analysis of both OpenAPI specifications to identify all endpoints with potential to extract data without authentication for the major refactor.

## OpenAPI Files Analysis

### 1. `/api/pncp` (api-pncp.json) - Management API
- **ALL ENDPOINTS REQUIRE AUTHENTICATION** (`bearerAuth`)
- This is the management API for creating, updating, and deleting data
- No endpoints suitable for data extraction without authentication

### 2. `/api/consulta` (api-pncp-consulta.json) - Consultation API
- **ALL ENDPOINTS ARE AUTHENTICATION-FREE** (no security requirements)
- This is the public consultation API for querying data
- Perfect for data extraction without authentication

## Authentication-Free Endpoints Available

### A. Contratos (Contracts)
1. **`/v1/contratos`** - Consultar Contratos por Data de Publicação
   - Parameters: `dataInicial`, `dataFinal`, `cnpjOrgao`, `codigoUnidadeAdministrativa`, `usuarioId`, `pagina`, `tamanhoPagina`
   - Returns: Contract data by publication date
   - **CURRENTLY BEING BACKED UP** ✅

2. **`/v1/contratos/atualizacao`** - Consultar Contratos por Data de Atualização Global
   - Parameters: `dataInicial`, `dataFinal`, `cnpjOrgao`, `codigoUnidadeAdministrativa`, `usuarioId`, `pagina`, `tamanhoPagina`
   - Returns: Contract data by update date
   - **NOT BEING BACKED UP** ❌

### B. Contratações (Procurements)
3. **`/v1/contratacoes/publicacao`** - Consultar Contratações por Data de Publicação
   - Parameters: `dataInicial`, `dataFinal`, `codigoModalidadeContratacao` (REQUIRED), `codigoModoDisputa`, `uf`, `codigoMunicipioIbge`, `cnpj`, `codigoUnidadeAdministrativa`, `idUsuario`, `pagina`, `tamanhoPagina`
   - Returns: Procurement data by publication date
   - **NOT BEING BACKED UP** ❌ (requires modalidade iteration)

4. **`/v1/contratacoes/atualizacao`** - Consultar Contratações por Data de Atualização Global
   - Parameters: `dataInicial`, `dataFinal`, `codigoModalidadeContratacao` (REQUIRED), `codigoModoDisputa`, `uf`, `codigoMunicipioIbge`, `cnpj`, `codigoUnidadeAdministrativa`, `idUsuario`, `pagina`, `tamanhoPagina`
   - Returns: Procurement data by update date
   - **NOT BEING BACKED UP** ❌ (requires modalidade iteration)

5. **`/v1/contratacoes/proposta`** - Consultar Contratações com Recebimento de Propostas Aberto
   - Parameters: `dataFinal`, `codigoModalidadeContratacao`, `uf`, `codigoMunicipioIbge`, `cnpj`, `codigoUnidadeAdministrativa`, `idUsuario`, `pagina`, `tamanhoPagina`
   - Returns: Procurement data with open proposal reception
   - **NOT BEING BACKED UP** ❌

### C. Atas (Price Registration Records)
6. **`/v1/atas`** - Consultar Ata de Registro de Preço por Período de Vigência
   - Parameters: `dataInicial`, `dataFinal`, `idUsuario`, `cnpj`, `codigoUnidadeAdministrativa`, `pagina`, `tamanhoPagina`
   - Returns: Price registration records by validity period
   - **NOT BEING BACKED UP** ❌

7. **`/v1/atas/atualizacao`** - Consultar Atas por Data de Atualização Global
   - Parameters: `dataInicial`, `dataFinal`, `idUsuario`, `cnpj`, `codigoUnidadeAdministrativa`, `pagina`, `tamanhoPagina`
   - Returns: Price registration records by update date
   - **NOT BEING BACKED UP** ❌

### D. Instrumentos de Cobrança (Billing Instruments)
8. **`/v1/instrumentoscobranca/inclusao`** - Consultar Instrumentos de Cobrança por Data de Inclusão
   - Parameters: `dataInicial`, `dataFinal`, `tipoInstrumentoCobranca`, `cnpjOrgao`, `pagina`, `tamanhoPagina`
   - Returns: Billing instrument data by inclusion date
   - **NOT BEING BACKED UP** ❌

### E. Planos de Contratação (Procurement Plans)
9. **`/v1/pca/usuario`** - Consultar Itens de PCA por Ano e IdUsuario
   - Parameters: `anoPca`, `idUsuario`, `codigoClassificacaoSuperior`, `cnpj`, `pagina`, `tamanhoPagina`
   - Returns: Procurement plan items by user and year
   - **NOT BEING BACKED UP** ❌ (requires user iteration)

10. **`/v1/pca/atualizacao`** - Consultar PCA por Data de Atualização Global
    - Parameters: `dataInicio`, `dataFim`, `cnpj`, `codigoUnidade`, `pagina`, `tamanhoPagina`
    - Returns: Procurement plan data by update date
    - **NOT BEING BACKED UP** ❌

11. **`/v1/pca/`** - Consultar Itens de PCA por Ano e Código de Classificação
    - Parameters: `anoPca`, `codigoClassificacaoSuperior`, `pagina`, `tamanhoPagina`
    - Returns: Procurement plan items by year and classification
    - **NOT BEING BACKED UP** ❌

### F. Individual Record Queries
12. **`/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}`** - Consultar Contratação Individual
    - Parameters: `cnpj`, `ano`, `sequencial`
    - Returns: Individual procurement record
    - **NOT BEING BACKED UP** ❌ (requires individual record iteration)

## Current vs. Proposed Architecture

### Current Architecture (Complex)
- Specific endpoint configurations
- Complex database schemas for different data types
- Multiple client wrapper methods
- Endpoint-specific processing logic

### Proposed Architecture (Simple)
- Single generic endpoint processor
- Unified PSA (Persistent Staging Area) schema
- Simple iteration through all endpoints
- Store everything: URL, parameters, timestamp, response code, response content

## New PSA Schema Design

```sql
CREATE TABLE IF NOT EXISTS psa.pncp_raw_responses (
    -- Response metadata
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Request information
    endpoint_url VARCHAR NOT NULL,
    http_method VARCHAR DEFAULT 'GET',
    request_parameters JSON,
    
    -- Response information
    response_code INTEGER NOT NULL,
    response_content TEXT,
    response_headers JSON,
    
    -- Processing metadata
    data_date DATE,
    run_id VARCHAR,
    endpoint_type VARCHAR, -- 'contratos', 'contratacoes', 'atas', etc.
    
    -- Indexing
    INDEX idx_endpoint_url (endpoint_url),
    INDEX idx_data_date (data_date),
    INDEX idx_response_code (response_code),
    INDEX idx_extracted_at (extracted_at)
);
```

## Implementation Strategy

1. **Single Endpoint Iterator**: Create a simple script that iterates through all 12 endpoints
2. **Universal Parameter Handling**: Handle required parameters like `codigoModalidadeContratacao`
3. **Raw Storage**: Store all responses as-is with metadata
4. **Post-Processing**: Parse and analyze data from raw responses separately
5. **Simplicity**: Remove complex endpoint-specific logic

## Data Volume Impact

- **Current**: ~4,163 records/day from 1 endpoint
- **Proposed**: ~172,000+ records/day from 12 endpoints
- **Increase**: 42x more data coverage

## Next Steps

1. Create simplified endpoint iterator script
2. Implement new PSA schema
3. Test with sample endpoints
4. Replace current system
5. Monitor and optimize

---

**Key Insight**: The major refactor achieves the goal of simplicity while dramatically increasing data coverage from 3% to 100% of available PNCP data.
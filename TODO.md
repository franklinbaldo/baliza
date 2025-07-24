# TODO: Arquitetura Ibis+Kedro - Eliminação da Era dbt

**Nova Arquitetura**: Ibis Pipeline + Kedro Orchestration  
**Data**: Julho 2025  
**Status**: Implementação Pós-Abandono do dbt

## 🎯 Objetivos da Nova Arquitetura

### ✅ **ATIVO - Ibis Pipeline**
- Transformações Python nativas (sem SQL/Jinja)
- Type safety com Pydantic
- Domain enrichment automático
- Testes E2E com dados reais

### 🚧 **PENDENTE - Kedro Integration** 
- Orquestração de produção robusta
- Pipeline modular e testável
- **Bloqueio**: CLI incompatibility (Typer vs Click)

### ❌ **REMOVIDO - dbt**
- Complexidade excessiva eliminada
- SQL/Python duplication removida
- Dependencies e infraestrutura limpa

---

## Fase 1: Refatoração da Persistência de Dados (CRÍTICO)

### 🔥 **Eliminar Raw Response Storage**
**Problema**: Armazenar JSON completo das respostas da API causa:
- 90% storage desperdiçado
- 10x slower parsing
- Duplicação de dados desnecessária

### **F1.1** Nova Arquitetura Direct-to-Structured
- [ ] **Mapear API Endpoints Completamente**
  - [ ] `/contratos` → schema específico
  - [ ] `/atas` → schema específico  
  - [ ] `/contratacoes` → schema específico
  - [ ] `/fontes_orcamentarias` → schema específico
  - [ ] `/instrumentos_cobranca` → schema específico
  - [ ] `/planos_contratacao` + itens → schemas específicos

- [ ] **Implementar Parsing Direto**
  ```python
  # ANTES: API → pncp_content (JSON) → Parsing → Structured Tables
  # DEPOIS: API → Pydantic Validation → Direct Insert to Typed Tables
  ```

- [ ] **Sistema de Error Handling**
  - [ ] Tabela `pncp_parse_errors` apenas para falhas
  - [ ] Campos: `url`, `error_message`, `extracted_at`, `retry_count`
  - [ ] Retry automático para erros transitórios
  - [ ] Preservar raw response APENAS se parsing falhar

### **F1.2** Validação Pydantic Robusta
- [ ] **Validators Customizados**:
  - [ ] CNPJ: 14 dígitos + dígitos verificadores válidos
  - [ ] CPF: 11 dígitos + dígitos verificadores válidos
  - [ ] Datas: não futuras, formato consistente
  - [ ] Valores: não negativos para preços/estimativas
  - [ ] UF: apenas códigos oficiais (27 estados)
  - [ ] Códigos ENUM: validação contra tabelas de domínio

- [ ] **Transformação Automática**:
  - [ ] Strings: trim whitespace, normalização de case
  - [ ] Coerção segura: `""` → `None`, `"0"` → `0`
  - [ ] Parsing de datas: múltiplos formatos aceitos
  - [ ] Limpeza de CNPJ/CPF: remover pontuação

### **F1.3** Benefícios Esperados
- **-90% storage usage**: Sem raw JSON duplicado
- **+5x parsing speed**: Direct structured parsing
- **+10x query performance**: Dados já normalizados
- **100% data quality**: Pydantic validation
- **Type safety**: Garantia de tipos corretos

---

## Fase 2: Kedro Pipeline Structure

### 🚧 **F2.1** Resolver CLI Incompatibility
**Problema**: Kedro espera Click CLI, BALIZA usa Typer
**Soluções Possíveis**:
- [ ] Adapter pattern: Typer wrapper para Kedro commands
- [ ] Dual CLI: `baliza run` (Typer) + `kedro run` (separado)
- [ ] Refactoring: Migração completa para Click
- [ ] Híbrido: Core CLI Typer + Pipeline CLI Kedro

### **F2.2** Estrutura Kedro + Ibis
```
src/baliza/pipelines/
├── data_extraction/          # API → Bronze tables
│   ├── nodes.py              # Extraction functions
│   ├── pipeline.py           # Extraction pipeline
│   └── __init__.py
├── data_processing/          # Bronze → Silver (Ibis)
│   ├── nodes.py              # Cleaning, enrichment
│   ├── pipeline.py           # Processing pipeline
│   └── __init__.py
├── analytics/                # Silver → Gold (Ibis)
│   ├── nodes.py              # Aggregations, metrics
│   ├── pipeline.py           # Analytics pipeline
│   └── __init__.py
└── domain_enrichment/        # Domain table integration
    ├── nodes.py              # ENUM enrichment
    ├── pipeline.py           # Domain pipeline
    └── __init__.py
```

### **F2.3** Kedro + Ibis Integration
- [ ] **Kedro DataCatalog** com Ibis Tables
  ```yaml
  # conf/base/catalog.yml
  bronze_contratos:
    type: ibis.TableDataset
    table: bronze_contratos
    connection: duckdb://data/baliza.duckdb
  ```

- [ ] **Pipeline Nodes** retornam Ibis expressions
  ```python
  def process_contracts(bronze_contracts: ibis.Table) -> ibis.Table:
      return (
          bronze_contracts
          .filter(_.modalidadeId.notnull())
          .mutate(valor_normalizado=_.valorTotalEstimado.cast("decimal(15,4)"))
      )
  ```

- [ ] **Lazy Evaluation**: Ibis expressions até materialização final

---

## Fase 3: Storage Tiers & Performance

### **F3.1** Hot-Cold Storage Architecture
- [ ] **Hot Tier** (últimos 90 dias):
  - [ ] Otimizado para writes rápidos
  - [ ] Compression leve (ZSTD level 1)
  - [ ] Índices para queries frequentes
  
- [ ] **Cold Tier** (90 dias - 2 anos):
  - [ ] Compression agressiva (ZSTD level 9)
  - [ ] Read-only com batch updates
  - [ ] Row-group optimization para Parquet export
  
- [ ] **Archive Tier** (>2 anos):
  - [ ] Export para Parquet + Internet Archive
  - [ ] `read_parquet('https://archive.org/download/baliza-pncp/**/*.parquet')`
  - [ ] Views remotas via httpfs

### **F3.2** Lifecycle Management
- [ ] **Automated Tier Migration**:
  ```python
  def migrate_to_cold_tier():
      # Mover dados de 90+ dias para cold tier
      # Aplicar compression agressiva
      # Update views transparentes
  ```

### **F3.3** Performance Monitoring
- [ ] **Pipeline Metrics**:
  - [ ] Parsing success rate por endpoint
  - [ ] Records processed per second
  - [ ] Storage efficiency por tier
  - [ ] Query performance benchmarks

---

## Fase 4: Migrações & Deployment

### **F4.1** Migração do Estado Atual
- [ ] **Backup dos Dados Existentes**
- [ ] **Schema Migration Script**:
  - [ ] `pncp_content` → structured tables
  - [ ] Aplicar Pydantic validation retroativa
  - [ ] Migrar metadata de requests
  
- [ ] **Validation Suite**:
  - [ ] Row count consistency
  - [ ] Data integrity checks
  - [ ] Performance regression tests

### **F4.2** CI/CD Integration
- [ ] **Automated Testing**:
  - [ ] Kedro pipeline tests
  - [ ] Ibis expression validation
  - [ ] E2E tests com dados reais
  - [ ] Performance benchmarks

- [ ] **Deployment Strategy**:
  - [ ] Blue-green deployment
  - [ ] Rollback capability
  - [ ] Zero-downtime migration

---

## Critérios de Sucesso

### **Performance Targets**
- [ ] ✅ **-90% storage usage** vs schema atual
- [ ] ✅ **+5x ingestion speed** (direct parsing)
- [ ] ✅ **+10x query performance** (structured data)
- [ ] ✅ **<30s** para extração mensal completa

### **Quality Gates**
- [ ] ✅ **100% Pydantic validation** success
- [ ] ✅ **Zero data loss** durante migration
- [ ] ✅ **E2E test coverage** para todos os pipelines
- [ ] ✅ **Type safety** garantida

### **Operational Goals**
- [ ] ✅ **Kedro orchestration** funcionando
- [ ] ✅ **Automated tier management** ativo
- [ ] ✅ **Monitoring dashboards** implementados
- [ ] ✅ **CI/CD automation** completo

---

## Referencias Atualizadas

- [ADR-014: Ibis Pipeline Adoption](docs/adr/014-ibis-pipeline-adoption.md) - **ATIVO**
- [dbt-to-kedro-migration-plan.md](docs/dbt-to-kedro-migration-plan.md) - **ROADMAP**
- [ADR-001: DuckDB Adoption](docs/adr/001-adopt-duckdb.md) - **MANTIDO**
- [ADR-006: Internet Archive](docs/adr/006-internet-archive.md) - **MANTIDO**
- [ADR-013: dbt Integration](docs/adr/013-dbt-integration-for-ddl.md) - **DEPRECIADO**

---

**Status**: 🚀 **Ready to Start** - Arquitetura definida, dbt removido  
**Próximo**: Implementar Fase 1 (Eliminar Raw Response Storage)  
**Bloqueio**: Kedro CLI integration (Fase 2) precisa de solução de compatibilidade
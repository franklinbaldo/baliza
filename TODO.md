# TODO: Database Optimization Implementation

Este arquivo contém tarefas atômicas para implementar o [Plano de Otimização da Base de Dados](docs/database-optimization-plan.md).

## Legenda
- 🔥 **CRÍTICO** - Deve ser feito primeiro, bloqueia outras tarefas
- ⚡ **ALTO** - Impacto significativo na performance/arquitetura  
- 📊 **MÉDIO** - Melhorias importantes mas não críticas
- 🧹 **BAIXO** - Limpeza e otimizações menores

---

## Fase 1: Diagnóstico e Preparação

### 🔥 Análise da Documentação Oficial PNCP
**Referência**: ADR-011, Plano Seção 1.1

- [ ] **F1.1** Criar script `scripts/audit_pncp_compliance.py`
  - [ ] Extrair todos os ENUMs do `docs/openapi/MANUAL-PNCP-CONSULSTAS-VERSAO-1.md`
  - [ ] Parsear `docs/openapi/api-pncp-consulta.json` para tipos precisos
  - [ ] Comparar schema atual vs oficial
  - [ ] Gerar relatório `docs/pncp-compliance-audit.md`

- [ ] **F1.2** Mapear campos atuais para schema oficial
  - [ ] Identificar campos faltantes no BALIZA
  - [ ] Identificar campos não-oficiais que devemos remover
  - [ ] Listar todos os campos com tipos incorretos
  - [ ] Documentar breaking changes necessários

- [ ] **F1.3** Extrair códigos oficiais das 17 tabelas de domínio
  - [ ] Modalidade de Contratação (13 valores)
  - [ ] Situação da Contratação (4 valores)  
  - [ ] UF (27 valores)
  - [ ] Natureza Jurídica (47 valores)
  - [ ] Salvar como `sql/seeds/pncp_domain_tables.sql`

### 🔥 Inventário de SQL Inline
**Referência**: ADR-009, Plano Seção 1.2

- [ ] **F1.4** Escanear código Python para SQL inline
  - [ ] Usar regex para encontrar strings SQL em `src/baliza/`
  - [ ] Classificar por tipo: SELECT, INSERT, UPDATE, DELETE, DDL
  - [ ] Medir complexidade (single table vs joins)
  - [ ] Gerar `docs/sql-inventory.md` com mapeamento completo

- [ ] **F1.5** Analisar performance atual
  - [ ] Executar `pragma_storage_info()` em todas as tabelas
  - [ ] Documentar tamanhos atuais e padrões de compressão
  - [ ] Identificar tabelas com maior potencial de otimização
  - [ ] Baseline para comparação pós-otimização

---

## Fase 2: Extração e Reorganização SQL

### ⚡ Estrutura de Diretórios SQL
**Referência**: ADR-009, Plano Seção 2.1

- [ ] **F2.1** Criar estrutura de diretórios
```bash
mkdir -p sql/{ddl,dml/{inserts,updates,deletes},analytics,maintenance,migrations}
```

- [ ] **F2.2** Implementar SQLLoader
  - [ ] Criar `src/baliza/sql_loader.py` conforme spec do plano
  - [ ] Adicionar cache de arquivos SQL
  - [ ] Implementar parametrização segura com `string.Template`
  - [ ] Adicionar validação de parâmetros obrigatórios
  - [ ] Testes unitários para SQLLoader

### ⚡ Migração de SQL Inline
**Referência**: ADR-009

- [ ] **F2.3** Extrair queries de inserção
  - [ ] `src/baliza/pncp_writer.py` → `sql/dml/inserts/pncp_content.sql`
  - [ ] `src/baliza/pncp_writer.py` → `sql/dml/inserts/pncp_requests.sql`  
  - [ ] Adicionar headers padronizados com metadata
  - [ ] Converter hard-coded values para parâmetros

- [ ] **F2.4** Extrair queries analíticas
  - [ ] Dashboard queries → `sql/analytics/deduplication_stats.sql`
  - [ ] Performance queries → `sql/analytics/endpoint_performance.sql`
  - [ ] Storage queries → `sql/analytics/storage_efficiency.sql`

- [ ] **F2.5** Extrair queries de manutenção
  - [ ] Cleanup queries → `sql/maintenance/cleanup_old_data.sql`
  - [ ] Optimization → `sql/maintenance/optimize_compression.sql`
  - [ ] Archiving → `sql/maintenance/export_to_cold_storage.sql`

- [ ] **F2.6** Refatorar código Python
  - [ ] Substituir SQL inline por `sql_loader.load()`
  - [ ] Adicionar error handling para arquivos SQL missing
  - [ ] Manter parâmetros consistentes
  - [ ] Testes para garantir equivalência funcional

---

## Fase 3: Nova DDL com dbt

### 🔥 Setup dbt
**Referência**: ADR-013, Plano Seção 3

- [ ] **F3.1** Configurar projeto dbt
  - [ ] Executar `dbt init dbt_baliza` 
  - [ ] Configurar `dbt_project.yml` com target DuckDB
  - [ ] Setup profiles em `~/.dbt/profiles.yml`
  - [ ] Testar conexão: `dbt debug`

- [ ] **F3.2** Estrutura do projeto dbt
  - [ ] Criar diretórios `models/{bronze,silver,gold,marts}/`
  - [ ] Criar `macros/{compression,validation,optimization}/`
  - [ ] Configurar `schema.yml` para cada layer
  - [ ] Setup `packages.yml` com dbt-utils

### ⚡ Implementar ENUMs Oficiais
**Referência**: ADR-011

- [ ] **F3.3** Criar ENUMs como seeds
  - [ ] `seeds/modalidade_contratacao.csv` (13 valores oficiais)
  - [ ] `seeds/situacao_contratacao.csv` (4 valores oficiais)
  - [ ] `seeds/uf_brasil.csv` (27 estados)
  - [ ] `seeds/natureza_juridica.csv` (47 códigos)
  - [ ] Todos os 17 domain tables do manual PNCP

- [ ] **F3.4** Gerar CREATE TYPE statements
  - [ ] Macro `create_enum_from_seed()` 
  - [ ] SQL: `CREATE TYPE modalidade_contratacao AS ENUM (...)`
  - [ ] Aplicar em hook `on-run-start`
  - [ ] Validation que enum values existem nos seeds

### ⚡ Bronze Layer (Raw Data)
**Referência**: ADR-013, ADR-010 (compression)

- [ ] **F3.5** Modelo bronze_pncp_raw
  - [ ] Schema identical to current pncp_content table
  - [ ] `materialized: table`
  - [ ] Global ZSTD compression: `SET default_compression='zstd'`
  - [ ] Sem transformações, apenas staging

- [ ] **F3.6** Modelo bronze_pncp_requests  
  - [ ] Schema para tabela de requests/metadata
  - [ ] Campos: url_path, response_time, extracted_at, etc.
  - [ ] Mesma estratégia de compressão

### ⚡ Silver Layer (Cleaned & Validated)
**Referência**: ADR-011 (schema compliance), ADR-013

- [ ] **F3.7** Modelo silver_pncp_contratacoes
  - [ ] Aplicar ENUMs oficiais: `modalidade_contratacao::modalidade_contratacao_enum`
  - [ ] Campos com tipos precisos: `DECIMAL(15,4)` para valores
  - [ ] `VARCHAR(14)` para CNPJs, `VARCHAR(11)` para CPFs
  - [ ] Data validation tests

- [ ] **F3.8** Modelo silver_pncp_orgaos_entidades
  - [ ] Hierarquia oficial: Órgão → Unidade Administrativa
  - [ ] Campos: CNPJ, razaoSocial, poderId, esferaId
  - [ ] Normalização de dados de entidades

- [ ] **F3.9** Modelo silver_pncp_contratos
  - [ ] Contratos/Atas vinculados a Contratações
  - [ ] numeroControlePNCPContrato, valores, datas
  - [ ] Foreign keys para integridade referencial

### 📊 Gold Layer (Business Logic)
**Referência**: ADR-013

- [ ] **F3.10** Modelo gold_contratacoes_analytics
  - [ ] Agregações por período, modalidade, órgão
  - [ ] Métricas: total_valores, quantidade_contratos, média_por_modalidade
  - [ ] Views otimizadas para dashboard

- [ ] **F3.11** Modelo gold_deduplication_efficiency
  - [ ] Análise de conteúdo duplicado
  - [ ] Storage savings, compression ratios
  - [ ] Métricas para otimização

### 📊 Macros dbt
**Referência**: ADR-010 (heuristics), ADR-011 (enum drift)

- [ ] **F3.12** Macro enum_drift_detection
  - [ ] Comparar ENUMs atuais vs seeds oficiais  
  - [ ] `SELECT enum_range() EXCEPT SELECT code FROM seed`
  - [ ] Alert quando novos valores aparecem na API
  - [ ] Auto-add com `ADD VALUE IF NOT EXISTS`

- [ ] **F3.13** Macro compression_config
  - [ ] Aplicar `SET default_compression='zstd'` globalmente
  - [ ] Evitar --strict em hot tables
  - [ ] `CHECKPOINT` after bulk operations

- [ ] **F3.14** Macro data_quality_tests
  - [ ] CNPJ format validation (14 digits)
  - [ ] CPF format validation (11 digits)
  - [ ] Required fields não-nulos
  - [ ] Foreign key integrity

---

## Fase 4: Hot-Cold Storage Tiers

### ⚡ Configuração de Tiers
**Referência**: ADR-012, Plano Seção 4

- [ ] **F4.1** Implementar Hot Tier (últimos 90 dias)
  - [ ] Modelo dbt `bronze_pncp_hot` com filtro de data
  - [ ] Otimizado para writes: sem --strict compression
  - [ ] `materialized: table` para performance máxima
  - [ ] Incremental updates baseados em extracted_at

- [ ] **F4.2** Implementar Cold Tier (90 dias - 2 anos)
  - [ ] Modelo `bronze_pncp_cold` 
  - [ ] Compression agressiva + row-group optimization
  - [ ] Read-only com batch updates
  - [ ] `materialized: table` com compression otimizada

- [ ] **F4.3** Implementar Archive Tier (>2 anos)  
  - [ ] Export para Parquet: `ROW_GROUP_SIZE 500k`
  - [ ] Upload para S3/cloud storage
  - [ ] Views externas: `read_parquet('s3://baliza-archive/**/*.parquet')`
  - [ ] Sem cópia local dos dados arquivados

### 📊 Automação de Lifecycle
**Referência**: ADR-012

- [ ] **F4.4** Script tier_management.py
  - [ ] Daily job para mover dados entre tiers
  - [ ] `DELETE FROM hot WHERE extracted_at < CURRENT_DATE - 90`
  - [ ] `INSERT INTO cold SELECT * FROM hot WHERE ...`
  - [ ] `CHECKPOINT` após cada tier move

- [ ] **F4.5** Transparent views
  - [ ] `VIEW pncp_all AS SELECT * FROM hot UNION ALL SELECT * FROM cold UNION ALL SELECT * FROM archive`
  - [ ] Query optimizer routes automaticamente
  - [ ] Maintains API compatibility

### 📊 Otimizações Específicas por Tier

- [ ] **F4.6** Hot tier optimizations
  - [ ] `ON CONFLICT DO NOTHING` para avoid rollbacks
  - [ ] Minimal indexing para fast writes
  - [ ] ZSTD application em nightly CHECKPOINT apenas

- [ ] **F4.7** Cold tier optimizations  
  - [ ] Row-group tuning apenas aqui (não no hot)
  - [ ] Aggressive compression post-checkpoint
  - [ ] Rebuild indexes após tier migration

- [ ] **F4.8** Archive tier via httpfs
  - [ ] Install/config httpfs extension
  - [ ] Test S3 connectivity e permissions
  - [ ] Benchmark query performance vs local storage
  - [ ] Fallback strategy se S3 unavailable

---

## Fase 5: Validação e Performance

### ⚡ Testes de Integridade
**Referência**: ADR-011 (compliance), ADR-013 (dbt tests)

- [ ] **F5.1** Data validation suite
  - [ ] dbt tests para schema compliance
  - [ ] Row count validation (old vs new schema)
  - [ ] Data integrity tests (foreign keys, constraints)
  - [ ] Business rule validation

- [ ] **F5.2** Migration testing
  - [ ] Script de migração com rollback capability
  - [ ] Parallel validation: old vs new queries
  - [ ] Performance regression testing
  - [ ] Data loss detection

### 📊 Performance Benchmarking
**Referência**: Feedback "Benchmarks automatizados"

- [ ] **F5.3** Automated benchmark suite
  - [ ] `scripts/benchmarks.py`
  - [ ] ① Ingest de 1M linhas  
  - [ ] ② Três queries típicas (analytics)
  - [ ] ③ Storage efficiency (`pragma_storage_info`)
  - [ ] Fail PR se regressão > 5%

- [ ] **F5.4** Compression effectiveness
  - [ ] Before/after storage comparison
  - [ ] Query performance comparison  
  - [ ] Memory usage analysis
  - [ ] Target: -70% storage, +2-3x throughput

### 🧹 CI/CD Integration

- [ ] **F5.5** GitHub Actions mutex
  - [ ] Single-writer enforcement para DuckDB
  - [ ] Workflow concurrency control  
  - [ ] Lockfile mechanism via artifacts
  - [ ] Prevent WAL corruption

- [ ] **F5.6** Automated quality gates
  - [ ] `dbt test` em todo PR
  - [ ] Performance benchmarks automáticos
  - [ ] SQL linting com sqlfmt
  - [ ] Schema change detection

---

## Fase 6: Deploy e Monitoring

### 📊 Migration Strategy
**Referência**: Breaking changes em ADR-011

- [ ] **F6.1** Legacy compatibility
  - [ ] Branch `legacy-reader` com views para novo schema
  - [ ] Backward compatibility layer
  - [ ] Notebooks antigos continue funcionando
  - [ ] Phased deprecation plan

- [ ] **F6.2** Zero-downtime migration
  - [ ] Blue-green deployment strategy
  - [ ] Parallel write durante transition
  - [ ] Validation em ambos schemas  
  - [ ] Rollback plan se problemas

### 🧹 Operational Excellence

- [ ] **F6.3** Monitoring e alertas
  - [ ] Disk usage por tier
  - [ ] Query performance metrics
  - [ ] Enum drift alerts
  - [ ] Data freshness monitoring

- [ ] **F6.4** Documentation
  - [ ] Atualizar README com novo workflow
  - [ ] Migration guide para users
  - [ ] Performance tuning guide
  - [ ] Troubleshooting guide

---

## Critérios de Sucesso

**Performance Targets**:
- [ ] ✅ **-70% storage usage** vs schema atual
- [ ] ✅ **+2-3x query throughput** para analytical workloads  
- [ ] ✅ **<30s** para extractação mensal completa
- [ ] ✅ **100% PNCP schema compliance**

**Quality Gates**:
- [ ] ✅ **Zero data loss** durante migration
- [ ] ✅ **100% test coverage** para dbt models
- [ ] ✅ **Automated benchmarks** passing em CI
- [ ] ✅ **Legacy compatibility** maintained

**Operational Goals**:
- [ ] ✅ **Single-writer safety** em GitHub Actions
- [ ] ✅ **Automated tier management** functioning
- [ ] ✅ **Enum drift detection** alerting
- [ ] ✅ **Performance monitoring** dashboards

---

## Referencias

- [Plano de Otimização da Base de Dados](docs/database-optimization-plan.md)
- [ADR-009: SQL Extraction Strategy](docs/adr/009-sql-extraction-strategy.md)  
- [ADR-010: Compression Heuristics First](docs/adr/010-compression-heuristics-first.md)
- [ADR-011: Official PNCP Schema Compliance](docs/adr/011-official-pncp-schema-compliance.md)
- [ADR-012: Hot-Cold Storage Tiers](docs/adr/012-hot-cold-storage-tiers.md)  
- [ADR-013: dbt Integration for DDL](docs/adr/013-dbt-integration-for-ddl.md)

---
**Status**: ⏳ Aguardando início da implementação  
**Estimativa Total**: 15-20 dias para implementação completa  
**Dependências Críticas**: F1.1-F1.3 devem ser completados antes de F2.x
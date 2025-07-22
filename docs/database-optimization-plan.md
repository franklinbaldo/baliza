# Plano de Otimização e Refatoração da Base de Dados BALIZA

## Contexto e Objetivos

Este plano visa otimizar radicalmente a arquitetura de dados BALIZA através de:

1. **Extração de SQL inline** para arquivos `.sql` organizados
2. **Quebra de compatibilidade controlada** para implementar melhores práticas
3. **DDL robusta** com compressão inteligente e tipos otimizados
4. **Arquivamento em tiers** (quente/frio) para eficiência de armazenamento

## Análise da Documentação Oficial PNCP

### Fontes de Verdade para Schema

**Documentos Analisados**:
- `docs/openapi/MANUAL-PNCP-CONSULSTAS-VERSAO-1.md` - Manual oficial com tabelas de domínio
- `docs/openapi/api-pncp-consulta.json` - Schema OpenAPI oficial com tipos precisos

**Insights Críticos Descobertos**:

1. **Tabelas de Domínio Oficiais** - O manual define 17 tabelas de domínio com códigos precisos:
   - Modalidade de Contratação: 13 valores (Leilão Eletrônico=1, Pregão Eletrônico=6, etc)
   - Situação da Contratação: 4 valores (Divulgada=1, Revogada=2, Anulada=3, Suspensa=4)
   - UF: 27 valores oficiais (AC, AL, AP, ..., TO)
   - Natureza Jurídica: 47 códigos específicos (1015=Executivo Federal, etc)

2. **Campos Obrigatórios vs Opcionais** - OpenAPI define precisamente:
   - **Obrigatórios**: dataInicial, dataFinal, codigoModalidadeContratacao, pagina
   - **Opcionais**: uf, codigoMunicipioIbge, cnpj, codigoUnidadeAdministrativa, idUsuario

3. **Tipos de Dados Precisos**:
   - Decimais com **até 4 casas**: `DECIMAL(15,4)` para valores monetários
   - Textos com limites: objetoCompra `VARCHAR(5120)`, numeroCompra `VARCHAR(50)`
   - Códigos CNPJ sempre 14 dígitos, CPF 11 dígitos
   - Datas em formato específico: AAAAMMDD para consultas

4. **Estrutura Hierárquica Oficial**:
   ```
   Órgão/Entidade (CNPJ + razaoSocial + poderId + esferaId)
   └── Unidade Administrativa (codigoUnidade + nomeUnidade + municipio + UF)
       └── Contratação (numeroControlePNCP + modalidade + situação)
           └── Contratos/Atas (numeroControlePNCPContrato + valores + datas)
   ```

## Fase 1: Diagnóstico e Mapeamento Orientado pela Documentação Oficial (1-2 dias)

### 1.1 Auditoria da Base Atual vs Schema Oficial

**Objetivo**: Comparar implementação atual com documentação oficial PNCP

**Ações**:
```bash
# Executar diagnóstico orientado pela documentação oficial
baliza database-audit --compare-with-official-schema --output docs/database-audit-report.md
```

**Métricas a Coletar**:
- `pragma_storage_info()` para cada tabela
- **Conformidade com tabelas de domínio oficiais**: Verificar se usamos os códigos corretos
- **Validação de tipos**: Comparar tipos atuais vs OpenAPI schema
- **Campos faltantes**: Identificar campos oficiais não capturados
- **Oportunidades de ENUMs**: Mapear campos categóricos para ENUMs oficiais

### 1.2 Inventário de SQL Inline

**Arquivo**: `docs/sql-inventory.md`

**Tarefa**: Escanear todo código Python e mapear:
- Localização de cada SQL inline
- Complexidade (simples SELECT vs multi-table JOIN)
- Dependências entre queries
- Candidatas para stored procedures/views

```python
# Exemplo de mapeamento
SQL_INVENTORY = {
    "src/baliza/pncp_writer.py:123": {
        "type": "INSERT",
        "complexity": "simple", 
        "target_file": "sql/inserts/pncp_content.sql"
    },
    "src/baliza/dashboard.py:273": {
        "type": "SELECT",
        "complexity": "analytics",
        "target_file": "sql/analytics/deduplication_stats.sql"
    }
}
```

## Fase 2: Extração e Reorganização SQL (2-3 dias)

### 2.1 Estrutura de Diretórios SQL

```
sql/
├── ddl/
│   ├── 001_drop_legacy_tables.sql
│   ├── 002_create_schemas.sql
│   ├── 003_create_core_tables.sql
│   ├── 004_create_indexes.sql
│   └── 005_create_views.sql
├── dml/
│   ├── inserts/
│   │   ├── pncp_content.sql
│   │   └── pncp_requests.sql
│   ├── updates/
│   └── deletes/
├── analytics/
│   ├── deduplication_stats.sql
│   ├── endpoint_performance.sql
│   └── storage_efficiency.sql
├── maintenance/
│   ├── cleanup_old_data.sql
│   ├── optimize_compression.sql
│   └── export_to_cold_storage.sql
└── migrations/
    ├── v1_to_v2_breaking_changes.sql
    └── README.md
```

### 2.2 Template SQL Padrão

Cada arquivo SQL seguirá este template:

```sql
-- File: sql/analytics/deduplication_stats.sql
-- Purpose: Calculate storage efficiency from content deduplication
-- Author: BALIZA Database Refactor
-- Created: 2025-01-XX
-- Dependencies: psa.pncp_content table
-- Performance: ~100ms on 1M records

-- Parameters (to be replaced by calling code):
-- ${SCHEMA_NAME} - Target schema (default: psa)
-- ${DATE_FILTER} - Optional date range filter

WITH dedup_analysis AS (
    SELECT
        COUNT(*) as unique_content,
        SUM(reference_count) as total_references,
        COUNT(CASE WHEN reference_count > 1 THEN 1 END) as deduplicated,
        SUM(content_size_bytes) as actual_size,
        SUM(content_size_bytes * reference_count) as theoretical_size
    FROM ${SCHEMA_NAME}.pncp_content
    WHERE 1=1
    ${DATE_FILTER}
)
SELECT 
    unique_content,
    total_references,
    deduplicated,
    ROUND(deduplicated::FLOAT / unique_content * 100, 1) as dedup_rate_pct,
    actual_size,
    theoretical_size,
    (theoretical_size - actual_size) as bytes_saved,
    ROUND((theoretical_size - actual_size)::FLOAT / theoretical_size * 100, 1) as storage_efficiency_pct
FROM dedup_analysis;
```

### 2.3 Utilitário de Carregamento SQL

**Arquivo**: `src/baliza/sql_loader.py`

```python
from pathlib import Path
from typing import Dict, Optional
import string

class SQLLoader:
    """Carrega e parametriza queries SQL de arquivos externos."""
    
    def __init__(self, sql_root: Path = Path("sql")):
        self.sql_root = sql_root
        self._cache: Dict[str, str] = {}
    
    def load(self, query_path: str, **params) -> str:
        """Carrega SQL file e substitui parâmetros."""
        if query_path not in self._cache:
            full_path = self.sql_root / query_path
            self._cache[query_path] = full_path.read_text(encoding='utf-8')
        
        template = string.Template(self._cache[query_path])
        return template.safe_substitute(**params)
```

## Fase 3: Nova DDL Robusta com Integração dbt (3-4 dias)

### 3.1 Arquitetura Medallion Otimizada

**Princípios de Design**:
1. **Compatibilidade total com dbt** - DDL gerada via dbt models
2. **Arquitetura Medallion** aprimorada (Bronze → Silver → Gold)
3. **ENUMs para dados categóricos** (economia de até 90%)
4. **Compressão por coluna** específica
5. **Tipos compactos** onde apropriado
6. **Separação tier quente/frio** com materializações dbt

### 3.2 Estrutura dbt + DDL Otimizada

**Nova Estrutura de Projeto dbt**:
```
dbt_baliza/
├── models/
│   ├── bronze/           # Raw data staging
│   │   ├── _bronze_schema.yml
│   │   ├── bronze_pncp_raw_hot.sql      # Last 90 days
│   │   ├── bronze_pncp_raw_cold.sql     # Historical data
│   │   └── bronze_pncp_content_optimized.sql
│   ├── silver/           # Business logic
│   │   ├── _silver_schema.yml
│   │   ├── silver_contratos_enhanced.sql
│   │   ├── silver_atas_enhanced.sql
│   │   └── silver_contratacoes_enhanced.sql
│   └── gold/             # Analytics ready
│       ├── _gold_schema.yml
│       ├── gold_procurement_analytics_optimized.sql
│       └── gold_agency_performance.sql
├── macros/
│   ├── compression/
│   │   ├── apply_column_compression.sql
│   │   ├── create_optimized_table.sql
│   │   └── setup_enum_types.sql
│   ├── optimization/
│   │   ├── partition_by_month.sql
│   │   ├── create_strategic_indexes.sql
│   │   └── compress_and_checkpoint.sql
│   └── archival/
│       ├── export_to_cold_storage.sql
│       └── tier_migration.sql
└── seeds/
    ├── enum_definitions.csv
    └── compression_config.csv
```

### 3.3 Bronze Layer: Staging Conforme Schema Oficial PNCP

**Princípios do Mapeamento**:
1. **Fidelidade ao Schema OpenAPI**: Todos os tipos seguem exatamente a especificação oficial
2. **Tabelas de Domínio como ENUMs**: Usar códigos oficiais do manual PNCP
3. **Validação de Integridade**: Constraints baseadas nos limites oficiais
4. **Normalização Hierárquica**: Estrutura orgão → unidade → contratação → contrato

**Arquivo**: `dbt_baliza/models/bronze/bronze_pncp_raw_hot.sql`

```sql
-- Bronze Layer: Hot Tier (Last 90 days)
-- Materialization: table with optimized compression

{{ config(
    materialized='table',
    pre_hook="{{ setup_enum_types() }}",
    post_hook="{{ apply_table_compression('bronze_pncp_raw_hot') }}"
) }}

WITH raw_requests AS (
    SELECT 
        {{ generate_uuid() }} as request_id,
        endpoint_name::endpoint_type as endpoint_name,
        url_path,
        extracted_at,
        response_code::SMALLINT as response_code,
        CASE 
            WHEN response_code = 200 THEN 'success'::response_status
            WHEN response_code = 404 THEN 'not_found'::response_status
            WHEN response_code = 429 THEN 'rate_limit'::response_status
            WHEN response_code >= 500 THEN 'server_error'::response_status
            ELSE 'other'::response_status
        END as response_status,
        total_records,
        content_hash,
        file_size_bytes,
        COALESCE(processing_time_ms, 0)::SMALLINT as processing_time_ms
    FROM {{ source('raw', 'pncp_requests') }}
    WHERE extracted_at >= CURRENT_DATE - INTERVAL '90 days'
)

SELECT * FROM raw_requests
```

**Arquivo**: `dbt_baliza/models/bronze/_bronze_schema.yml`

```yaml
version: 2

sources:
  - name: raw
    description: Raw PNCP data from extractor
    tables:
      - name: pncp_requests
        description: Raw API requests and responses
        columns:
          - name: endpoint_name
            tests:
              - accepted_values:
                  values: ['contratos_publicacao', 'atas_publicacao', 'contratacoes_publicacao']
          - name: content_hash
            tests:
              - unique
              - not_null

models:
  - name: bronze_pncp_raw_hot
    description: "Optimized hot tier storage (last 90 days) with compression"
    config:
      materialized: table
      post_hook: |
        ALTER TABLE {{ this }} SET COMPRESSION zstd;
        {{ apply_column_compression([
          ('url_path', 'fsst'),
          ('total_records', 'bitpacking'),
          ('file_size_bytes', 'bitpacking'),
          ('processing_time_ms', 'bitpacking')
        ]) }}
    tests:
      - dbt_utils.expression_is_true:
          expression: "extracted_at >= CURRENT_DATE - INTERVAL '90 days'"
    
  - name: bronze_pncp_content_optimized
    description: "Deduplicated content with structured parsing"
    config:
      materialized: table
      post_hook: |
        {{ apply_column_compression([
          ('content_data', 'fsst'),
          ('content_size_bytes', 'bitpacking'),
          ('reference_count', 'bitpacking'),
          ('contract_id', 'dictionary'),
          ('agency_code', 'dictionary'),
          ('supplier_cnpj', 'dictionary')
        ]) }}
```

### 3.4 Macros dbt para Otimização

**Arquivo**: `dbt_baliza/macros/compression/setup_enum_types.sql`

```sql
{% macro setup_enum_types() %}
  {% if execute %}
    -- ENUMs baseados na documentação oficial PNCP
    
    -- 5.2 Modalidade de Contratação (Manual PNCP)
    CREATE TYPE IF NOT EXISTS modalidade_contratacao AS ENUM (
        '1',   -- Leilão - Eletrônico
        '2',   -- Diálogo Competitivo  
        '3',   -- Concurso
        '4',   -- Concorrência - Eletrônica
        '5',   -- Concorrência - Presencial
        '6',   -- Pregão - Eletrônico
        '7',   -- Pregão - Presencial
        '8',   -- Dispensa de Licitação
        '9',   -- Inexigibilidade
        '10',  -- Manifestação de Interesse
        '11',  -- Pré-qualificação
        '12',  -- Credenciamento
        '13'   -- Leilão - Presencial
    );

    -- 5.5 Situação da Contratação (Manual PNCP)  
    CREATE TYPE IF NOT EXISTS situacao_contratacao AS ENUM (
        '1',   -- Divulgada no PNCP
        '2',   -- Revogada
        '3',   -- Anulada
        '4'    -- Suspensa
    );

    -- 5.3 Modo de Disputa (Manual PNCP)
    CREATE TYPE IF NOT EXISTS modo_disputa AS ENUM (
        '1',   -- Aberto
        '2',   -- Fechado
        '3',   -- Aberto-Fechado
        '4',   -- Dispensa Com Disputa
        '5',   -- Não se aplica
        '6'    -- Fechado-Aberto
    );

    -- UF oficial (27 valores)
    CREATE TYPE IF NOT EXISTS uf_type AS ENUM (
        'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
        'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
    );

    -- 5.1 Instrumento Convocatório (Manual PNCP)
    CREATE TYPE IF NOT EXISTS instrumento_convocatorio AS ENUM (
        '1',   -- Edital
        '2',   -- Aviso de Contratação Direta
        '3'    -- Ato que autoriza a Contratação Direta
    );

    -- Tipo de Pessoa (OpenAPI)
    CREATE TYPE IF NOT EXISTS tipo_pessoa AS ENUM (
        'PJ',  -- Pessoa Jurídica
        'PF',  -- Pessoa Física  
        'PE'   -- Pessoa Estrangeira
    );

    -- 5.9 Tipo de Contrato (Manual PNCP) - principais
    CREATE TYPE IF NOT EXISTS tipo_contrato AS ENUM (
        '1',   -- Contrato (termo inicial)
        '2',   -- Comodato
        '3',   -- Arrendamento
        '4',   -- Concessão
        '5',   -- Termo de Adesão
        '6',   -- Convênio
        '7',   -- Empenho
        '8',   -- Outros
        '9',   -- TED
        '10',  -- ACT
        '11',  -- Termo de Compromisso
        '12'   -- Carta Contrato
    );

    -- Status de resposta da API (interno)
    CREATE TYPE IF NOT EXISTS response_status AS ENUM (
        'success',      -- 200
        'not_found',    -- 404  
        'rate_limit',   -- 429
        'server_error', -- 500+
        'timeout',      -- timeout
        'other'         -- outros códigos
    );
  {% endif %}
{% endmacro %}
```

**Arquivo**: `dbt_baliza/macros/compression/apply_column_compression.sql`

```sql
{% macro apply_column_compression(column_specs) %}
  {% if execute %}
    {% for column_name, compression_type in column_specs %}
      ALTER TABLE {{ this }} 
      ALTER COLUMN {{ column_name }} 
      SET COMPRESSION {{ compression_type }};
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro apply_table_compression(table_name, compression='zstd') %}
  {% if execute %}
    ALTER TABLE {{ this }} SET COMPRESSION {{ compression }};
    CHECKPOINT;  -- Force compression to take effect
  {% endif %}
{% endmacro %}
```

**Arquivo**: `dbt_baliza/macros/optimization/create_strategic_indexes.sql`

```sql
{% macro create_strategic_indexes(table_name, index_specs) %}
  {% if execute %}
    {% for index_name, columns in index_specs %}
      CREATE INDEX IF NOT EXISTS {{ index_name }}
      ON {{ table_name }}({{ columns | join(', ') }});
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro create_performance_indexes() %}
  {{ create_strategic_indexes('bronze_pncp_raw_hot', [
    ('idx_hot_endpoint_date', ['endpoint_name', 'extracted_at DESC']),
    ('idx_hot_hash', ['content_hash']),
    ('idx_hot_status', ['response_status', 'extracted_at DESC'])
  ]) }}
{% endmacro %}
```

### 3.5 Silver Layer: Estrutura Conforme Schema Oficial PNCP

**Arquivo**: `dbt_baliza/models/silver/silver_contratacoes_normalized.sql`

```sql
{{ config(
    materialized='table',
    post_hook=[
      "{{ apply_table_compression(this, 'zstd') }}",
      "{{ create_contratacao_indexes() }}"
    ]
) }}

-- Normalização conforme estrutura oficial PNCP
WITH contratacoes_parsed AS (
    SELECT
        content_hash,
        
        -- Campos obrigatórios (conforme OpenAPI)
        JSON_EXTRACT_STRING(content_data, '$.numeroControlePNCP') as numero_controle_pncp,
        JSON_EXTRACT_STRING(content_data, '$.numeroCompra') as numero_compra,
        JSON_EXTRACT(content_data, '$.anoCompra')::INTEGER as ano_compra,
        JSON_EXTRACT_STRING(content_data, '$.processo') as processo,
        
        -- Modalidade (referência oficial Manual 5.2)
        JSON_EXTRACT(content_data, '$.modalidadeId')::modalidade_contratacao as modalidade_id,
        JSON_EXTRACT_STRING(content_data, '$.modalidadeNome') as modalidade_nome,
        
        -- Situação (referência oficial Manual 5.5) 
        JSON_EXTRACT(content_data, '$.situacaoCompraId')::situacao_contratacao as situacao_compra_id,
        JSON_EXTRACT_STRING(content_data, '$.situacaoCompraNome') as situacao_compra_nome,
        
        -- Valores monetários (precisão oficial: até 4 casas decimais)
        JSON_EXTRACT(content_data, '$.valorTotalEstimado')::DECIMAL(18,4) as valor_total_estimado,
        JSON_EXTRACT(content_data, '$.valorTotalHomologado')::DECIMAL(18,4) as valor_total_homologado,
        
        -- Datas (formato ISO)
        JSON_EXTRACT_STRING(content_data, '$.dataPublicacaoPncp')::TIMESTAMP as data_publicacao_pncp,
        JSON_EXTRACT_STRING(content_data, '$.dataAberturaProposta')::TIMESTAMP as data_abertura_proposta,
        JSON_EXTRACT_STRING(content_data, '$.dataEncerramentoProposta')::TIMESTAMP as data_encerramento_proposta,
        
        -- Textos com limites oficiais
        LEFT(JSON_EXTRACT_STRING(content_data, '$.objetoCompra'), 5120) as objeto_compra,
        LEFT(JSON_EXTRACT_STRING(content_data, '$.informacaoComplementar'), 5120) as informacao_complementar,
        
        -- Flags booleanas
        JSON_EXTRACT(content_data, '$.srp')::BOOLEAN as srp,
        
        -- Órgão/Entidade (estrutura hierárquica oficial)
        JSON_EXTRACT_STRING(content_data, '$.orgaoEntidade.cnpj') as orgao_cnpj,
        LEFT(JSON_EXTRACT_STRING(content_data, '$.orgaoEntidade.razaoSocial'), 255) as orgao_razao_social,
        JSON_EXTRACT_STRING(content_data, '$.orgaoEntidade.poderId') as poder_id,
        JSON_EXTRACT_STRING(content_data, '$.orgaoEntidade.esferaId') as esfera_id,
        
        -- Unidade Administrativa (estrutura hierárquica oficial)
        JSON_EXTRACT_STRING(content_data, '$.unidadeOrgao.codigoUnidade') as codigo_unidade,
        LEFT(JSON_EXTRACT_STRING(content_data, '$.unidadeOrgao.nomeUnidade'), 255) as nome_unidade,
        JSON_EXTRACT_STRING(content_data, '$.unidadeOrgao.ufSigla')::uf_type as uf_sigla,
        JSON_EXTRACT_STRING(content_data, '$.unidadeOrgao.municipioNome') as municipio_nome,
        JSON_EXTRACT(content_data, '$.unidadeOrgao.codigoIbge')::INTEGER as codigo_ibge,
        
        -- Amparo Legal (estrutura oficial)
        JSON_EXTRACT(content_data, '$.amparoLegal.codigo')::INTEGER as amparo_legal_codigo,
        JSON_EXTRACT_STRING(content_data, '$.amparoLegal.nome') as amparo_legal_nome,
        
        -- Metadata
        c.first_seen_at,
        c.reference_count
        
    FROM {{ ref('bronze_pncp_content_optimized') }} c
    WHERE JSON_EXTRACT_STRING(content_data, '$.numeroControlePNCP') IS NOT NULL
    AND JSON_VALID(content_data)
),

validated_contratacoes AS (
    SELECT 
        *,
        
        -- Validações conforme documentação oficial
        CASE 
            WHEN LENGTH(orgao_cnpj) = 14 THEN orgao_cnpj 
            ELSE NULL 
        END as cnpj_validado,
        
        -- Categorização de valores (para analytics)
        CASE 
            WHEN valor_total_estimado >= 1000000.00 THEN 'alto_valor'
            WHEN valor_total_estimado >= 100000.00 THEN 'medio_valor'
            WHEN valor_total_estimado > 0 THEN 'baixo_valor'
            ELSE 'sem_valor'
        END as categoria_valor,
        
        -- Dimensões temporais para analytics
        DATE_TRUNC('month', data_publicacao_pncp) as mes_publicacao,
        DATE_TRUNC('year', data_publicacao_pncp) as ano_publicacao,
        
        -- Performance/quality metrics
        CASE WHEN reference_count > 1 THEN TRUE ELSE FALSE END as is_duplicate_content,
        
        -- Duração do período de propostas (em horas)
        CASE 
            WHEN data_abertura_proposta IS NOT NULL 
            AND data_encerramento_proposta IS NOT NULL 
            THEN EXTRACT(EPOCH FROM (data_encerramento_proposta - data_abertura_proposta)) / 3600.0
            ELSE NULL 
        END as duracao_propostas_horas
        
    FROM contratacoes_parsed
    WHERE numero_controle_pncp IS NOT NULL
)

SELECT * FROM validated_contratacoes
```

### 3.6 Gold Layer: Analytics com Compressão Máxima

**Arquivo**: `dbt_baliza/models/gold/gold_procurement_analytics_optimized.sql`

```sql
{{ config(
    materialized='table',
    post_hook=[
      "{{ apply_table_compression(this, 'zstd') }}",
      "{{ apply_column_compression([
        ('total_contratos', 'bitpacking'),
        ('valor_total', 'bitpacking'), 
        ('valor_medio', 'bitpacking'),
        ('orgao_razao_social', 'dictionary'),
        ('uf', 'dictionary'),
        ('modalidade_contratacao', 'dictionary')
      ]) }}"
    ]
) }}

WITH monthly_analytics AS (
    SELECT 
        mes_publicacao,
        ano_publicacao,
        uf::uf_type as uf,  -- Use ENUM for compression
        orgao_razao_social,
        modalidade_contratacao,
        valor_tier,
        
        -- Aggregated metrics with optimal types
        COUNT(*)::INTEGER as total_contratos,
        SUM(valor_inicial)::DECIMAL(18,2) as valor_total,
        AVG(valor_inicial)::DECIMAL(15,2) as valor_medio,
        MEDIAN(valor_inicial)::DECIMAL(15,2) as valor_mediano,
        
        -- Efficiency metrics
        COUNT(CASE WHEN is_duplicate_content THEN 1 END)::INTEGER as contratos_duplicados,
        SUM(reference_count)::INTEGER as total_referencias,
        
        -- Data quality score
        (COUNT(CASE WHEN orgao_cnpj IS NOT NULL 
                    AND valor_inicial > 0 
                    AND data_publicacao IS NOT NULL 
                THEN 1 END)::FLOAT / COUNT(*) * 100)::DECIMAL(5,2) as data_quality_score
        
    FROM {{ ref('silver_contratos_enhanced') }}
    WHERE data_publicacao >= '2021-01-01'
    GROUP BY 1,2,3,4,5,6
),

ranked_agencies AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY mes_publicacao, uf 
            ORDER BY valor_total DESC
        ) as ranking_mensal_uf,
        
        -- Moving averages for trend analysis
        AVG(valor_total) OVER (
            PARTITION BY orgao_razao_social, uf
            ORDER BY mes_publicacao
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::DECIMAL(18,2) as valor_media_movel_3m
        
    FROM monthly_analytics
)

SELECT * FROM ranked_agencies
WHERE total_contratos >= 5  -- Filter noise
```

### 3.7 Integração com Pipeline Python

**Arquivo**: `src/baliza/dbt_integration.py`

```python
from pathlib import Path
from typing import Dict, List, Optional
import subprocess
import duckdb

class DbtDuckDBManager:
    """Gerencia integração entre pipeline Python e transformações dbt."""
    
    def __init__(self, dbt_project_path: Path, db_path: Path):
        self.dbt_project_path = dbt_project_path
        self.db_path = db_path
    
    def run_optimized_transformations(self, models: Optional[List[str]] = None) -> Dict:
        """Executa transformações dbt com otimizações."""
        cmd = ["dbt", "run"]
        
        if models:
            cmd.extend(["--models"] + models)
        
        # Add performance flags
        cmd.extend([
            "--vars", "{'compression_enabled': true}",
            "--project-dir", str(self.dbt_project_path)
        ])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            self._run_post_transformation_optimization()
            
        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    
    def _run_post_transformation_optimization(self):
        """Otimizações pós-dbt para máxima compressão."""
        with duckdb.connect(str(self.db_path)) as conn:
            # Force checkpoint para aplicar compressão
            conn.execute("CHECKPOINT;")
            
            # Collect statistics para query planner
            conn.execute("ANALYZE;")
            
            # Export para cold storage se necessário
            self._export_old_data_to_cold_storage(conn)
    
    def get_compression_report(self) -> Dict:
        """Relatório de eficiência da compressão."""
        with duckdb.connect(str(self.db_path)) as conn:
            tables = [
                'bronze_pncp_raw_hot',
                'silver_contratos_enhanced', 
                'gold_procurement_analytics_optimized'
            ]
            
            report = {}
            for table in tables:
                try:
                    result = conn.execute(f"""
                        SELECT * FROM pragma_storage_info('{table}')
                    """).fetchall()
                    report[table] = result
                except Exception as e:
                    report[table] = f"Error: {e}"
                    
            return report
```

### 3.8 Estratégia de Migração Incremental

**Arquivo**: `dbt_baliza/macros/migration/incremental_migration.sql`

```sql
{% macro migrate_legacy_to_optimized(legacy_table, target_model, batch_size=100000) %}
  
  {% set migration_status_query %}
    SELECT COALESCE(MAX(migrated_batch_id), 0) as last_batch
    FROM migration_log 
    WHERE target_table = '{{ target_model }}'
  {% endset %}
  
  {% if execute %}
    {% set last_batch = run_query(migration_status_query).columns[0].values()[0] %}
    
    INSERT INTO {{ target_model }}
    SELECT * FROM (
      {{ get_legacy_data_batch(legacy_table, last_batch + 1, batch_size) }}
    );
    
    -- Log migration progress
    INSERT INTO migration_log (
      target_table, 
      migrated_batch_id, 
      migrated_at,
      records_migrated
    ) 
    SELECT 
      '{{ target_model }}',
      {{ last_batch + 1 }},
      CURRENT_TIMESTAMP,
      {{ batch_size }};
      
  {% endif %}
{% endmacro %}
```

### 3.9 Indexes Estratégicos via dbt

```sql
-- sql/ddl/004_create_indexes.sql

-- Hot data indexes (performance critical)
CREATE INDEX idx_requests_hot_endpoint_date 
ON psa.pncp_requests_hot(endpoint_name, extracted_at DESC);

CREATE INDEX idx_requests_hot_hash 
ON psa.pncp_requests_hot(content_hash);

-- Analytics indexes
CREATE INDEX idx_content_agency_value 
ON psa.pncp_content_optimized(agency_code, contract_value DESC)
WHERE contract_value IS NOT NULL;

-- Specialized index for deduplication queries
CREATE INDEX idx_content_ref_count 
ON psa.pncp_content_optimized(reference_count DESC, content_size_bytes DESC);
```

## Fase 4: Estratégia de Compressão e Arquivamento (2-3 dias)

### 4.1 Pipeline de Otimização Automática

**Arquivo**: `src/baliza/optimization_pipeline.py`

```python
class DatabaseOptimizer:
    """Executa otimizações automáticas baseadas em métricas."""
    
    def analyze_compression_opportunities(self) -> Dict[str, Any]:
        """Usa pragma_storage_info para identificar alvos."""
        
    def apply_column_compression(self, table: str, recommendations: Dict):
        """Aplica algoritmos específicos por coluna."""
        
    def migrate_to_cold_storage(self, cutoff_date: datetime):
        """Move dados antigos para tier frio em Parquet ZSTD."""
        
    def reclaim_space(self):
        """Executa CHECKPOINT e cleanup."""
```

### 4.2 Rotina de Arquivamento Inteligente

```sql
-- sql/maintenance/export_to_cold_storage.sql

-- Export old requests to cold storage (Parquet v2 + ZSTD)
COPY (
    SELECT * FROM psa.pncp_requests_hot 
    WHERE extracted_at < CURRENT_DATE - INTERVAL '90 days'
) TO 's3://baliza-archive/{year}-{month}/requests.parquet' (
    PARQUET_VERSION v2,
    COMPRESSION zstd,
    ROW_GROUP_SIZE 500000
);

-- Move to cold table and delete from hot
INSERT INTO psa.pncp_requests_cold 
SELECT * FROM psa.pncp_requests_hot 
WHERE extracted_at < CURRENT_DATE - INTERVAL '90 days';

DELETE FROM psa.pncp_requests_hot 
WHERE extracted_at < CURRENT_DATE - INTERVAL '90 days';

-- Force cleanup
CHECKPOINT;
```

## Fase 5: Migração e Testes (2-3 dias)

### 5.1 Script de Migração Segura

```sql
-- sql/migrations/v1_to_v2_breaking_changes.sql

-- BREAKING CHANGE NOTICE:
-- This migration breaks compatibility with BALIZA v1.x
-- Backup your data before proceeding

BEGIN TRANSACTION;

-- Step 1: Create new optimized schema
\i sql/ddl/002_create_schemas.sql
\i sql/ddl/003_create_core_tables.sql

-- Step 2: Migrate existing data with optimizations
INSERT INTO psa.pncp_requests_hot (
    request_id,
    endpoint_name,
    url_path,
    extracted_at,
    response_code,
    response_status,
    total_records,
    content_hash,
    file_size_bytes,
    processing_time_ms
)
SELECT 
    gen_random_uuid(),
    endpoint_name::endpoint_type,
    url_path,
    extracted_at,
    response_code::SMALLINT,
    CASE 
        WHEN response_code = 200 THEN 'success'::response_status
        WHEN response_code = 404 THEN 'not_found'::response_status
        WHEN response_code = 429 THEN 'rate_limit'::response_status
        WHEN response_code >= 500 THEN 'server_error'::response_status
        ELSE 'other'::response_status
    END,
    total_records,
    content_hash,
    file_size_bytes,
    COALESCE(processing_time_ms, 0)::SMALLINT
FROM legacy.pncp_requests
WHERE extracted_at >= CURRENT_DATE - INTERVAL '90 days';

-- Step 3: Archive old data to cold storage immediately
-- (older than 90 days goes straight to cold tier)

COMMIT;
```

### 5.2 Testes de Performance

**Arquivo**: `tests/test_database_optimization.py`

```python
def test_compression_ratios():
    """Verifica que compressão está funcionando."""
    
def test_query_performance():
    """Benchmark queries antes/depois da otimização."""
    
def test_storage_efficiency():
    """Valida economia de espaço em disco."""
```

## Cronograma e Entregáveis

| Fase | Duração | Entregáveis | Critério de Sucesso |
|------|---------|-------------|-------------------|
| 1 | 1-2 dias | `docs/database-audit-report.md`, `docs/sql-inventory.md` | Diagnóstico completo do estado atual |
| 2 | 2-3 dias | Estrutura `sql/`, `SQLLoader` class | Todo SQL extraído e parametrizado |
| 3 | 3-4 dias | Nova DDL com ENUMs e compressão | Schema otimizado criado |
| 4 | 2-3 dias | Pipeline de arquivamento | Tier quente/frio funcionando |
| 5 | 2-3 dias | Scripts de migração, testes | Migração segura validada |

**Total: 10-15 dias úteis**

## Métricas de Sucesso

### Antes vs Depois

| Métrica | Estado Atual | Meta Pós-Otimização | Método de Medição |
|---------|--------------|-------------------|-------------------|
| Tamanho DB | TBD | -40% a -80% | `pragma_storage_info()` |
| Tempo query analytics | TBD | -50% a -70% | Benchmark suite |
| Consultas por segundo | TBD | +100% a +200% | Load testing |
| Tempo de backup | TBD | -60% a -80% | Backup pipeline |

### Qualidade do Código

- ✅ Zero SQL inline no código Python
- ✅ 100% das queries em arquivos `.sql` versionados
- ✅ Cobertura de testes para todas as queries críticas
- ✅ Documentação completa da nova arquitetura

## Riscos e Mitigações

| Risco | Probabilidade | Impacto | Mitigação |
|-------|---------------|---------|-----------|
| Perda de dados na migração | Baixa | Alto | Backup completo + dry-run + rollback plan |
| Performance pior que esperado | Média | Médio | Benchmark incremental + rollback |
| Queries quebradas | Alta | Alto | Test suite abrangente + gradual rollout |
| Incompatibilidade com código existente | Alta | Alto | Wrapper classes + deprecation warnings |

## Conclusão

Este plano implementa uma refatoração ambiciosa mas necessária da arquitetura BALIZA. A combinação de **SQL extraído**, **DDL otimizada** e **arquivamento inteligente** resultará em:

- **Melhor manutenibilidade** (SQL versionado e testável)
- **Performance superior** (compressão otimizada + tipos corretos)
- **Eficiência de armazenamento** (tier quente/frio + ENUMs)
- **Escalabilidade futura** (arquitetura preparada para crescimento)

*Sapere aude* - mediremos cada otimização e só avançaremos se os ganhos justificarem o esforço.
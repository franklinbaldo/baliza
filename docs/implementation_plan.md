# Plano de Implementação: Baliza

**Versão: 3.0 (Revisão Claude Code - Arquitetura Otimizada)**

## 1. Objetivo

Reimplementar o pipeline de extração, transformação e carga (ETL) de dados do Portal Nacional de Contratações Públicas (PNCP). O objetivo é criar um sistema de engenharia de dados moderno, resiliente, performático e observável, empacotado como uma ferramenta de linha de comando (CLI) intuitiva e pronta para produção, com arquitetura simplificada e implementação incremental.

**Tecnologias Principais:**
- **Orquestração:** Prefect OSS (com UI web completa)
- **Manipulação de Dados:** Ibis
- **Banco de Dados / Engine:** DuckDB
- **CLI:** Typer
- **Testes:** Pytest, pytest-duckdb, Hypothesis

## 2. Arquitetura e Conceitos Fundamentais

### 2.1. Arquitetura de Dados: Raw → Staging → Marts (Simplificada)

O pipeline seguirá uma arquitetura de três camadas otimizada para simplicidade e performance.

1.  **Raw (Dados Brutos Unificados):**
    *   **Propósito:** Armazenamento unificado e eficiente de todas as requisições API com auditoria completa.
    *   **Schema Único:**
        ```sql
        CREATE TABLE raw.api_requests (
          request_id         UUID PRIMARY KEY,
          ingestion_date     DATE NOT NULL,
          endpoint           TEXT NOT NULL,
          http_status        SMALLINT NOT NULL,
          etag               TEXT,
          payload_sha256     VARCHAR(64) NOT NULL,
          payload_size       INT NOT NULL,
          payload_compressed BLOB NOT NULL,  -- JSON comprimido com ZSTD nativo DuckDB
          collected_at       TIMESTAMPTZ NOT NULL,
          UNIQUE(endpoint, collected_at)
        );

        -- Índices para performance
        CREATE INDEX idx_ingestion_date ON raw.api_requests(ingestion_date);
        CREATE INDEX idx_payload_sha256 ON raw.api_requests(payload_sha256);
        ```
    *   **Vantagens da Unificação:**
        - Elimina complexidade de JOINs
        - Aproveita compressão nativa DuckDB (ZSTD)
        - Deduplicação opcional via UPSERT
        - Integridade referencial garantida

2.  **Staging (Views Materializadas Seletivas):**
    *   **Propósito:** Transformações otimizadas com materialização inteligente.
    *   **Estratégia Híbrida:**
        - Views simples permanecem não-materializadas
        - Views complexas/custosas são materializadas sob demanda
        - Cache inteligente baseado em frequência de acesso
    *   **Formato:** Views Ibis com metadados de materialização

3.  **Marts (Particionamento Lógico DuckDB):**
    *   **Propósito:** Modelos analíticos otimizados para consulta.
    *   **Estratégia de Particionamento:**
        ```sql
        -- Particionamento lógico via WHERE clauses (DuckDB native)
        CREATE TABLE marts.contratos_mes AS
        SELECT * FROM staging.contratos_cleaned
        WHERE data_publicacao >= '2024-01-01'
        ORDER BY orgao, data_publicacao;
        ```
    *   **Clustering:** Ordenação por colunas de alta seletividade

### 2.2. Error Recovery e Resilência (NOVA SEÇÃO)

#### Circuit Breaker Pattern
```python
@dataclass
class CircuitBreaker:
    failure_threshold: int = 5
    recovery_timeout: int = 300  # 5 minutos
    state: str = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
```

#### Retry Granular com Backoff Exponencial
```python
@task(
    retries=3,
    retry_delay_seconds=60,
    retry_condition_fn=lambda task, task_run, state:
        isinstance(state.result(), (httpx.HTTPStatusError, httpx.TimeoutException))
)
def fetch_pncp_endpoint(endpoint: str, attempt: int = 0):
    """Fetch com retry inteligente e backoff exponencial"""
    if attempt > 0:
        delay = min((2 ** attempt) + random.uniform(0, 1), 300)
        time.sleep(delay)

    # Lógica de requisição com circuit breaker
```

#### State Management para Recovery
- Checkpoints incrementais em `data/checkpoints/`
- Recovery de falhas por endpoint específico
- CLI commands para retry manual: `baliza retry-failed`

### 2.3. Schema Evolution e Drift Detection (MELHORADA)

#### Validação Estrutural
```python
# Fingerprinting de schema para detecção de mudanças
def generate_schema_fingerprint(response_json: dict) -> str:
    """Gera fingerprint estrutural do JSON"""
    structure = extract_json_structure(response_json)
    return hashlib.sha256(json.dumps(structure, sort_keys=True).encode()).hexdigest()
```

#### Versionamento de API
```python
# config.py
SCHEMA_VERSIONS = {
    "1.0": "sha256:abc123...",  # Expected schema fingerprint
    "1.1": "sha256:def456...",
}
```

### 2.4. Observabilidade e Métricas (OTIMIZADA)

#### Métricas Essenciais
```sql
CREATE TABLE meta.pipeline_metrics (
  metric_id       UUID PRIMARY KEY,
  flow_name       TEXT NOT NULL,
  task_name       TEXT NOT NULL,
  execution_time  TIMESTAMPTZ NOT NULL,
  duration_ms     INT NOT NULL,
  records_processed INT NOT NULL,
  bytes_processed BIGINT NOT NULL,
  success_rate    FLOAT NOT NULL,
  error_details   JSON
);
```

#### Alerting Simples
- Logs estruturados (JSON) para fácil parsing
- GitHub Actions para monitoring básico
- Webhook notifications (Slack/Discord) opcionais

## 3. Experiência do Usuário (CLI Melhorada)

### Comandos Básicos
```bash
# Setup e diagnóstico
baliza init                    # Inicialização
baliza doctor                  # Health check
baliza ui                      # Prefect UI (http://localhost:4200)

# ETL Operations
baliza run --latest            # Último mês
baliza run --mes 2024-01       # Mês específico
baliza extract --dia 2024-01-15  # Extração específica
baliza transform --mes 2024-01   # Só transformação

# Data Management
baliza query "SELECT COUNT(*) FROM raw.api_requests"
baliza dump-catalog            # Schema export
baliza reset --force          # Database reset
```

### Comandos de Recovery (NOVOS)
```bash
# Recovery e debugging
baliza retry-failed 2024-01 --max-retries 3
baliza recovery-status 2024-01
baliza backup                  # Backup antes de ops destrutivas
baliza dry-run --mes 2024-01  # Preview sem executar
baliza rollback --to-checkpoint abc123
```

### Comandos de Verificação (SIMPLIFICADOS)
```bash
# Verificação de integridade (local primeiro, S3 depois)
baliza verify --range 2024-01-01:2024-03-31
baliza verify --sample 0.1
baliza fetch-payload <sha256>
```

## 4. Fases da Implementação (REFORMULADAS)

### **Fase 2A: Core MVP (2-3 semanas)**
**Objetivo:** Pipeline básico funcional

1. **CLI Foundation**
   - Typer setup com comandos básicos
   - Config management (Pydantic Settings)
   - DuckDB connection (Ibis backend)

2. **Raw Layer Simplificado**
   - Schema único `raw.api_requests`
   - Basic extraction flow (sem S3, sem verify)
   - HTTPX com rate limiting básico

3. **Testing Foundation**
   - pytest setup
   - Basic unit tests
   - CI/CD pipeline

**Entregável:** `baliza extract --mes 2024-01` funcionando

### **Fase 2B: ETL Completo (3-4 semanas)**
**Objetivo:** Pipeline end-to-end

1. **Staging Layer**
   - Ibis views definition
   - Basic transformations
   - View materialization logic

2. **Marts Layer**
   - Parquet output
   - Logical partitioning
   - Query optimization

3. **CLI Integration**
   - All commands working
   - Error handling
   - Progress indicators

**Entregável:** `baliza run --latest` produzindo marts analisáveis

### **Fase 2C: Production Features (2-3 semanas)**
**Objetivo:** Resilência e observabilidade

1. **Error Recovery**
   - Circuit breaker implementation
   - Retry mechanisms
   - Checkpoint system

2. **Monitoring**
   - Structured logging
   - Basic metrics collection
   - Health checks

3. **Data Quality**
   - Schema validation
   - Data profiling
   - Anomaly detection

**Entregável:** Sistema robusto para produção

### **Fase 3: Advanced Features (2-4 semanas)**
**Objetivo:** Features avançadas opcionais

1. **Integrity Verification**
   - `baliza verify` implementation
   - Divergence detection
   - S3 integration para archives

2. **Performance Optimization**
   - Query optimization
   - Caching strategies
   - Memory management

3. **Operational Excellence**
   - Comprehensive monitoring
   - Automated alerting
   - Documentation

## 5. Estrutura de Diretórios (ATUALIZADA)

```
/
├── data/
│   ├── baliza.duckdb              # Database principal
│   ├── checkpoints/               # Recovery checkpoints
│   └── archives/                  # Local archives (antes S3)
├── docs/
│   ├── implementation_plan.md
│   └── openapi/
├── src/
│   └── baliza/
│       ├── __init__.py
│       ├── backend.py             # DuckDB + Ibis connection
│       ├── cli.py                 # CLI interface
│       ├── config.py              # Settings management
│       ├── enums.py
│       ├── pncp_schemas.py
│       ├── flows/
│       │   ├── __init__.py
│       │   ├── raw.py             # Extraction logic
│       │   ├── staging.py         # Transformation views
│       │   ├── marts.py           # Mart materialization
│       │   └── recovery.py        # Error recovery flows
│       ├── utils/
│       │   ├── __init__.py
│       │   ├── circuit_breaker.py # Resilience patterns
│       │   ├── schema_validation.py
│       │   └── checkpoints.py     # State management
│       └── metrics/
│           ├── __init__.py
│           └── data_quality.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
└── pyproject.toml
```

## 6. Esquemas de Dados (SIMPLIFICADOS)

```sql
-- Raw Layer: Tabela única otimizada
CREATE TABLE raw.api_requests (
  request_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ingestion_date     DATE NOT NULL,
  endpoint           TEXT NOT NULL,
  http_status        SMALLINT NOT NULL,
  etag               TEXT,
  payload_sha256     VARCHAR(64) NOT NULL,
  payload_size       INT NOT NULL,
  payload_compressed BLOB NOT NULL,
  collected_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Constraints
  UNIQUE(endpoint, collected_at),
  CHECK(http_status >= 100 AND http_status < 600),
  CHECK(payload_size > 0)
);

-- Índices para performance
CREATE INDEX idx_ingestion_date ON raw.api_requests(ingestion_date);
CREATE INDEX idx_payload_sha256 ON raw.api_requests(payload_sha256);
CREATE INDEX idx_endpoint ON raw.api_requests(endpoint);

-- Metadata: Métricas simplificadas
CREATE TABLE meta.execution_log (
  execution_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  flow_name       TEXT NOT NULL,
  start_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  end_time        TIMESTAMPTZ,
  status          TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed')),
  records_processed INT DEFAULT 0,
  error_message   TEXT,
  metadata        JSON
);

-- Metadata: Schema evolution tracking
CREATE TABLE meta.schema_versions (
  version_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  api_version     TEXT NOT NULL,
  schema_fingerprint VARCHAR(64) NOT NULL,
  detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  is_supported    BOOLEAN NOT NULL DEFAULT TRUE,
  migration_notes TEXT
);

-- Recovery: Failed operations (local archive antes S3)
CREATE TABLE meta.failed_requests (
  failure_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  original_endpoint TEXT NOT NULL,
  failure_reason  TEXT NOT NULL,
  failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  retry_count     INT DEFAULT 0,
  last_retry_at   TIMESTAMPTZ,
  resolved        BOOLEAN DEFAULT FALSE,
  archived_payload BLOB  -- Local antes S3
);
```

## 7. Tecnical Debt Mitigation

### Decisões de Arquitetura
1. **Schema único vs. separado**: Único venceu por simplicidade
2. **Views materializadas vs. não-materializadas**: Híbrido para flexibilidade
3. **S3 vs. local archive**: Local primeiro, S3 como upgrade
4. **Foreign Keys**: Enabled por padrão para integridade

### Performance Considerations
```python
# DuckDB otimizations
PRAGMA threads=8;
PRAGMA memory_limit='4GB';
PRAGMA temp_directory='data/tmp';
```

### Future-Proofing
- Plugin architecture preparada (`baliza.plugins`)
- API interface hooks (`baliza.api`)
- Schema migration framework
- Horizontal scaling considerations (DuckDB → ClickHouse path)

## 8. Definition of Done

### Fase 2A (MVP)
- [ ] CLI básica instalável (`pip install -e .`)
- [ ] `baliza extract --mes 2024-01` funciona
- [ ] Testes unitários > 80% coverage
- [ ] Documentação básica atualizada

### Fase 2B (ETL Completo)
- [ ] `baliza run --latest` produz marts
- [ ] Performance < 5min para mês completo
- [ ] Error handling robusto
- [ ] Integration tests passando

### Fase 2C (Production Ready)
- [ ] Recovery automático de falhas
- [ ] Monitoring e alerting
- [ ] Load testing executado
- [ ] Security review completo

---

**Este plano v3.0 incorpora todas as sugestões de arquitetura, priorizando simplicidade, resilência e implementação incremental. O foco é em MVP primeiro, com path claro para produção.**

# TODO: Arquitetura Ibis+Kedro - Implementação Detalhada

**Nova Arquitetura**: Ibis Pipeline + Kedro Orchestration  
**Data**: Julho 2025  
**Status**: Roadmap Técnico Detalhado Pós-Abandono do dbt

## 🎯 Contexto Arquitetural

### ✅ **ATIVO - Ibis Pipeline**
- **Onde**: `pipelines_legacy/ibis_*.py` (será movido para `src/baliza/pipelines/`)
- **Status**: Implementado mas precisa integração com Kedro
- Transformações Python nativas (sem SQL/Jinja)
- Type safety com Pydantic (`src/baliza/pncp_schemas.py`)
- Domain enrichment automático via ENUMs (`src/baliza/enums.py`)

### 🚧 **PENDENTE - Kedro Integration** 
- **Problema**: CLI incompatibility (Typer vs Click)
- **Arquivo Bloqueado**: `src/baliza/cli.py` (linha 19: `app = typer.Typer()`)
- Orquestração de produção robusta
- Pipeline modular e testável

### ❌ **REMOVIDO - dbt**
- Complexidade excessiva eliminada (2703 linhas removidas)
- SQL/Python duplication removida
- Dependencies limpas do `pyproject.toml`

---

## Fase 1: Refatoração da Persistência de Dados (CRÍTICO)

### 🔥 **F1.1: Eliminar Raw Response Storage**

#### **Por que é Crítico:**
Atualmente o sistema armazena JSON completo das respostas da API, causando:
- **90% storage desperdiçado**: Dados duplicados entre `pncp_content` e tabelas estruturadas
- **10x slower parsing**: JSON deserialization desnecessária
- **Complexidade**: Dois layers de transformação (JSON → Python → DB)

#### **Arquivos Afetados:**

**1. `src/baliza/pncp_writer.py`** (PRINCIPAL):
```python
# PROBLEMA ATUAL (linha ~150+):
async def write_pncp_content(self, content_data):
    """Persiste JSON completo - DESPERDIÇA STORAGE"""
    sql = SQL_LOADER.load("pncp_content", params={
        "content_json": json.dumps(response_data),  # ← PROBLEMA: JSON completo
        "content_size_bytes": len(json_string),     # ← Bytes desperdiçados
        "response_json": json.dumps(full_response)  # ← Mais duplicação
    })
    
# SOLUÇÃO: Direct-to-Structured
async def write_structured_data(self, endpoint_name: str, validated_data: List[BaseModel]):
    """Escreve direto para tabelas tipadas"""
    table_name = f"bronze_{endpoint_name}"
    structured_records = [record.model_dump() for record in validated_data]
    # Insert direto sem JSON intermediário
```

**2. `src/baliza/extractor.py`** (linha ~200+):
```python
# PROBLEMA ATUAL: Two-step process
async def extract_and_store(self, request_info):
    response = await self.client.fetch(request_info.url)
    
    # PROBLEMA: Persiste JSON primeiro
    await self.writer.write_pncp_content({
        'response_json': response.text,  # ← Storage desperdiçado
        'url': request_info.url
    })
    
    # Depois parse separadamente
    parsed_data = self.parser.parse(response.text)
    
# SOLUÇÃO: Single-step direct parsing
async def extract_and_validate(self, request_info):
    response = await self.client.fetch(request_info.url)
    
    try:
        # Parse + validate direto com Pydantic
        validated_records = self.parse_endpoint_response(
            endpoint_name=request_info.endpoint,
            response_text=response.text
        )
        
        # Write direto para tabela tipada
        await self.writer.write_structured_data(
            endpoint_name=request_info.endpoint,
            validated_data=validated_records
        )
        
        # Success tracking apenas
        await self.writer.log_request_success(request_info, len(validated_records))
        
    except ValidationError as e:
        # Error handling: preservar apenas failures
        await self.writer.write_parse_error({
            'url': request_info.url,
            'error_message': str(e),
            'response_raw': response.text  # ← Apenas se parsing falhar
        })
```

**3. `src/baliza/pncp_schemas.py`** (EXPANDIR):
```python
# ADICIONAR: Schemas para todos os endpoints
class ContratacaoResponseSchema(BaseModel):
    """Mapeamento direto do endpoint /contratacoes"""
    numeroControlePNCP: str
    anoContratacao: int = Field(ge=2000, le=2050)
    dataPublicacaoPNCP: date
    modalidadeId: int
    valorTotalEstimado: Optional[Decimal] = Field(decimal_places=4)
    orgaoEntidade: OrgaoEntidadeDTO
    unidadeOrgao: UnidadeOrgaoDTO
    
    @field_validator('numeroControlePNCP')
    def validate_controle_pncp(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('numeroControlePNCP cannot be empty')
        return v.strip()
    
    @field_validator('modalidadeId')
    def validate_modalidade(cls, v):
        from .enums import ModalidadeContratacao
        valid_ids = [m.value for m in ModalidadeContratacao]
        if v not in valid_ids:
            raise ValueError(f'Invalid modalidadeId: {v}. Must be one of {valid_ids}')
        return v

class ContratosResponseSchema(BaseModel):
    """Mapeamento direto do endpoint /contratos"""
    # Campos específicos de contratos...
    
class AtasResponseSchema(BaseModel):
    """Mapeamento direto do endpoint /atas"""
    # Campos específicos de atas...
```

#### **Tarefas Específicas:**

- [ ] **F1.1.1** Criar schemas Pydantic para todos endpoints:
  - [ ] `ContratacaoResponseSchema` (baseado em `/contratacoes`)
  - [ ] `ContratosResponseSchema` (baseado em `/contratos`) 
  - [ ] `AtasResponseSchema` (baseado em `/atas`)
  - [ ] `FontesOrcamentariasResponseSchema`
  - [ ] `InstrumentosCobrancaResponseSchema`
  - [ ] `PlanosContratacaoResponseSchema`

- [ ] **F1.1.2** Refatorar `pncp_writer.py`:
  - [ ] Remover `write_pncp_content()` 
  - [ ] Adicionar `write_structured_data(endpoint_name, validated_data)`
  - [ ] Criar `write_parse_error(error_info)` para falhas
  - [ ] Atualizar SQL em `sql/dml/inserts/` para tabelas específicas

- [ ] **F1.1.3** Refatorar `extractor.py`:
  - [ ] Remover two-step process (JSON → Parse)
  - [ ] Implementar direct validation com Pydantic
  - [ ] Error handling que preserva raw response apenas em falhas

### 🛡️ **F1.2: Validação Pydantic Robusta**

#### **Por que é Crítico:**
Data quality issues custam 10x mais para corrigir depois da ingestão. Pydantic validation garante:
- **Type safety**: Dados sempre no formato correto
- **Business rules**: CNPJs válidos, datas não futuras, valores não negativos
- **Debugging**: Mensagens de erro claras quando dados não batem

#### **Arquivo Principal: `src/baliza/pncp_schemas.py`**

```python
# ADICIONAR: Validators customizados
import re
from typing import Optional
from pydantic import field_validator, model_validator

class BaseValidator:
    @staticmethod
    def validate_cnpj(cnpj: str) -> str:
        """Valida CNPJ com dígitos verificadores"""
        # Remove formatação
        cnpj = re.sub(r'[^0-9]', '', cnpj)
        
        if len(cnpj) != 14:
            raise ValueError(f'CNPJ must have 14 digits, got {len(cnpj)}')
        
        # Algoritmo de validação de dígitos verificadores
        def calc_digit(cnpj_digits, weights):
            sum_prod = sum(int(digit) * weight for digit, weight in zip(cnpj_digits, weights))
            remainder = sum_prod % 11
            return 0 if remainder < 2 else 11 - remainder
        
        # Validação do primeiro dígito verificador
        weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        digit1 = calc_digit(cnpj[:12], weights1)
        
        # Validação do segundo dígito verificador  
        weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        digit2 = calc_digit(cnpj[:13], weights2)
        
        if int(cnpj[12]) != digit1 or int(cnpj[13]) != digit2:
            raise ValueError(f'Invalid CNPJ check digits: {cnpj}')
        
        return cnpj

# Aplicar em schemas:
class OrgaoEntidadeDTO(BaseModel):
    cnpj: str
    razao_social: str
    poder_id: str
    esfera_id: str
    
    @field_validator('cnpj')
    def validate_cnpj_field(cls, v):
        return BaseValidator.validate_cnpj(v)
    
    @field_validator('razao_social')
    def validate_razao_social(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('razao_social cannot be empty')
        return v.strip().upper()  # Normalização
```

#### **Tarefas Específicas:**

- [ ] **F1.2.1** Implementar validators customizados:
  - [ ] `validate_cnpj()`: 14 dígitos + dígitos verificadores
  - [ ] `validate_cpf()`: 11 dígitos + dígitos verificadores  
  - [ ] `validate_date_not_future()`: Datas não podem ser futuras
  - [ ] `validate_positive_decimal()`: Valores não negativos
  - [ ] `validate_uf_code()`: Apenas códigos oficiais (27 estados)

- [ ] **F1.2.2** Integrar com ENUMs existentes:
  - [ ] Validator para `modalidadeId` contra `ModalidadeContratacao`
  - [ ] Validator para `situacaoId` contra `SituacaoContratacao`
  - [ ] Cross-reference com `src/baliza/enums.py`

### 🗄️ **F1.3: Nova Estrutura de Tabelas**

#### **Por que é Crítico:**
Storage schema atual mistura dados de diferentes endpoints numa tabela gigante `pncp_content`. Nova estrutura:
- **Queries 10x faster**: Tabelas específicas por endpoint
- **Type safety**: Colunas com tipos corretos vs JSON genérico
- **Indexes eficientes**: Cada tabela pode ter índices específicos

#### **Arquivos Afetados:**

**1. `sql/ddl/` (CRIAR NOVOS):**
```sql
-- sql/ddl/bronze_contratacoes.sql
CREATE TABLE IF NOT EXISTS bronze_contratacoes (
    numeroControlePNCP VARCHAR(50) PRIMARY KEY,
    anoContratacao INTEGER NOT NULL,
    dataPublicacaoPNCP DATE NOT NULL,
    modalidadeId INTEGER NOT NULL,
    valorTotalEstimado DECIMAL(15,4),
    
    -- Campos orgao (desnormalizados para performance)
    orgao_cnpj VARCHAR(14) NOT NULL,
    orgao_razao_social VARCHAR(255) NOT NULL,
    orgao_poder_id VARCHAR(10),
    orgao_esfera_id VARCHAR(10),
    
    -- Campos unidade
    unidade_codigo VARCHAR(20),
    unidade_nome VARCHAR(255),
    unidade_uf_sigla VARCHAR(2),
    unidade_municipio_nome VARCHAR(100),
    
    -- Metadata
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    month_partition VARCHAR(7) GENERATED ALWAYS AS (strftime('%Y-%m', dataPublicacaoPNCP)) STORED
);

-- Índices para queries frequentes
CREATE INDEX IF NOT EXISTS idx_contratacoes_month ON bronze_contratacoes(month_partition);
CREATE INDEX IF NOT EXISTS idx_contratacoes_modalidade ON bronze_contratacoes(modalidadeId);
CREATE INDEX IF NOT EXISTS idx_contratacoes_orgao ON bronze_contratacoes(orgao_cnpj);
```

**2. `sql/dml/inserts/` (ATUALIZAR):**
```sql
-- sql/dml/inserts/bronze_contratacoes.sql
INSERT INTO bronze_contratacoes (
    numeroControlePNCP,
    anoContratacao,
    dataPublicacaoPNCP,
    modalidadeId,
    valorTotalEstimado,
    orgao_cnpj,
    orgao_razao_social,
    orgao_poder_id,
    orgao_esfera_id,
    unidade_codigo,
    unidade_nome,
    unidade_uf_sigla,
    unidade_municipio_nome
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (numeroControlePNCP) DO NOTHING;
```

#### **Tarefas Específicas:**

- [ ] **F1.3.1** Criar DDL para todas as tabelas bronze:
  - [ ] `sql/ddl/bronze_contratacoes.sql`
  - [ ] `sql/ddl/bronze_contratos.sql`
  - [ ] `sql/ddl/bronze_atas.sql`
  - [ ] `sql/ddl/bronze_fontes_orcamentarias.sql`
  - [ ] `sql/ddl/bronze_instrumentos_cobranca.sql`
  - [ ] `sql/ddl/bronze_planos_contratacao.sql`

- [ ] **F1.3.2** Atualizar `pncp_writer.py` para usar novas tabelas:
  - [ ] Remover dependência de `pncp_content`
  - [ ] Implementar insert direto para tabelas específicas
  - [ ] Batch inserts para performance

- [ ] **F1.3.3** Implementar tabela de error handling:
```sql
-- sql/ddl/pncp_parse_errors.sql
CREATE TABLE IF NOT EXISTS pncp_parse_errors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url VARCHAR(500) NOT NULL,
    endpoint_name VARCHAR(50) NOT NULL,
    error_message TEXT NOT NULL,
    response_raw TEXT, -- Apenas para debugging
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    resolved_at TIMESTAMP NULL
);
```

---

## Fase 2: Kedro Pipeline Structure

### 🚧 **F2.1: Resolver CLI Incompatibility**

#### **Por que é Crítico:**
Kedro espera Click CLI mas BALIZA usa Typer. Sem resolver isso, não conseguimos usar Kedro orchestration.

#### **Arquivo Principal: `src/baliza/cli.py`**

```python
# PROBLEMA ATUAL (linha 19):
app = typer.Typer(no_args_is_help=False)  # ← Typer CLI

# OPÇÃO 1: Hybrid approach
import click
from kedro.framework.cli import KedroCLI

app = typer.Typer(no_args_is_help=False)

# Adicionar comandos Kedro como subcomandos
@app.command("kedro")
def kedro_command(
    command: str = typer.Argument(..., help="Kedro command to run"),
    args: List[str] = typer.Argument(None, help="Additional arguments")
):
    """Run Kedro commands through Typer interface"""
    # Delegate to Kedro CLI
    kedro_cli = KedroCLI()
    kedro_cli.main([command] + (args or []))

# OPÇÃO 2: Adapter pattern
class KedroTyperAdapter:
    """Adapter to run Kedro pipelines from Typer CLI"""
    
    def __init__(self):
        from kedro.framework.session import KedroSession
        self.session_class = KedroSession
    
    def run_pipeline(self, pipeline_name: str = "default"):
        """Run Kedro pipeline from Typer command"""
        with self.session_class.create() as session:
            session.run(pipeline_name=pipeline_name)
            return session.load_context().catalog

@app.command("transform")
def transform_data(
    pipeline: str = typer.Option("default", help="Pipeline to run")
):
    """Transform data using Kedro pipeline"""
    adapter = KedroTyperAdapter()
    adapter.run_pipeline(pipeline)
```

#### **Tarefas Específicas:**

- [ ] **F2.1.1** Implementar adapter pattern:
  - [ ] Criar `KedroTyperAdapter` class em `src/baliza/cli.py`
  - [ ] Manter interface Typer para usuários
  - [ ] Delegar execução para Kedro internamente

- [ ] **F2.1.2** Testar compatibilidade:
  - [ ] Verificar se `kedro run` funciona através do adapter
  - [ ] Manter backward compatibility dos comandos existentes

### 🏗️ **F2.2: Estrutura Kedro + Ibis**

#### **Por que é Importante:**
Kedro fornece orchestração robusta, mas precisamos integrar com transformações Ibis. A estrutura permite:
- **Modularidade**: Pipelines específicos por domínio
- **Testabilidade**: Cada node pode ser testado isoladamente  
- **Paralelização**: Kedro executa nodes em paralelo automaticamente

#### **Arquivos a Criar:**

**1. `src/baliza/pipelines/data_extraction/nodes.py`:**
```python
"""Extraction nodes for Kedro pipeline"""
import ibis
from typing import Dict, List
from baliza.extractor import AsyncPNCPExtractor
from baliza.pncp_schemas import ContratacaoResponseSchema

def extract_contratacoes_month(month: str) -> ibis.Table:
    """Extract contratacoes for specific month
    
    Args:
        month: YYYY-MM format
    
    Returns:
        Ibis table with extracted and validated data
    """
    import asyncio
    
    extractor = AsyncPNCPExtractor()
    
    # Run async extraction
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(
        extractor.extract_month_endpoint(
            endpoint="contratacoes",
            month=month
        )
    )
    
    # Return as Ibis table for downstream processing
    con = ibis.duckdb.connect("data/baliza.duckdb")
    return con.table("bronze_contratacoes")

def validate_extraction_quality(bronze_table: ibis.Table) -> Dict[str, int]:
    """Data quality validation for extracted data
    
    Returns metrics about data quality
    """
    con = bronze_table._find_backend()
    
    total_records = bronze_table.count().execute()
    null_cnpj = bronze_table.filter(
        bronze_table.orgao_cnpj.isnull()
    ).count().execute()
    
    future_dates = bronze_table.filter(
        bronze_table.dataPublicacaoPNCP > ibis.today()
    ).count().execute()
    
    return {
        "total_records": total_records,
        "null_cnpj_count": null_cnpj,
        "future_dates_count": future_dates,
        "quality_score": ((total_records - null_cnpj - future_dates) / total_records) * 100
    }
```

**2. `src/baliza/pipelines/data_processing/nodes.py`:**
```python
"""Data processing nodes - Bronze to Silver transformations"""
import ibis
from baliza.enums import ModalidadeContratacao, SituacaoContratacao

def enrich_contratacoes_with_domains(bronze_contratacoes: ibis.Table) -> ibis.Table:
    """Enrich contratacoes with domain table descriptions
    
    Transforms codes to human-readable descriptions using ENUMs
    """
    con = bronze_contratacoes._find_backend()
    
    # Create modalidade lookup table from enum
    modalidade_data = [
        {"modalidade_id": m.value, "modalidade_nome": m.name.replace("_", " ")}
        for m in ModalidadeContratacao
    ]
    modalidade_lookup = con.create_table("temp_modalidade_lookup", modalidade_data, overwrite=True)
    
    # Enrich with domain data
    enriched = (
        bronze_contratacoes
        .left_join(
            modalidade_lookup,
            bronze_contratacoes.modalidadeId == modalidade_lookup.modalidade_id
        )
        .select(
            bronze_contratacoes,
            modalidade_lookup.modalidade_nome
        )
        .mutate(
            # Normalized values
            valorTotalEstimado=_.valorTotalEstimado.cast("decimal(15,4)"),
            orgao_razao_social_clean=_.orgao_razao_social.strip().upper(),
            
            # Derived fields
            is_high_value=_.valorTotalEstimado > 100000,
            year_month=_.dataPublicacaoPNCP.strftime("%Y-%m")
        )
    )
    
    return enriched

def create_silver_fact_contratacoes(enriched_contratacoes: ibis.Table) -> ibis.Table:
    """Create silver fact table with business logic applied"""
    
    return (
        enriched_contratacoes
        .filter(
            # Business rules
            _.dataPublicacaoPNCP.notnull(),
            _.valorTotalEstimado >= 0,  # No negative values
            _.orgao_cnpj.notnull()
        )
        .select([
            # Keys
            _.numeroControlePNCP.name("contratacao_key"),
            _.orgao_cnpj.name("orgao_key"),
            
            # Dimensions
            _.anoContratacao,
            _.modalidadeId,
            _.modalidade_nome,
            _.orgao_razao_social_clean.name("orgao_nome"),
            _.unidade_uf_sigla.name("uf"),
            _.unidade_municipio_nome.name("municipio"),
            
            # Facts
            _.valorTotalEstimado.name("valor_estimado"),
            _.dataPublicacaoPNCP.name("data_publicacao"),
            _.is_high_value,
            _.year_month,
            
            # Metadata
            _.extracted_at
        ])
    )
```

**3. `conf/base/catalog.yml`:**
```yaml
# Kedro data catalog configuration
bronze_contratacoes:
  type: ibis.TableDataset
  table: bronze_contratacoes
  connection:
    backend: duckdb
    database: data/baliza.duckdb

silver_fact_contratacoes:
  type: ibis.TableDataset  
  table: silver_fact_contratacoes
  connection:
    backend: duckdb
    database: data/baliza.duckdb

gold_contratacoes_analytics:
  type: ibis.TableDataset
  table: gold_contratacoes_analytics
  connection:
    backend: duckdb
    database: data/baliza.duckdb

# Parameters for extraction
extraction_params:
  type: MemoryDataSet
  data:
    concurrency: 10
    force_db: false
    endpoints:
      - contratacoes
      - contratos
      - atas
```

#### **Tarefas Específicas:**

- [ ] **F2.2.1** Criar estrutura de pipelines:
  - [ ] `src/baliza/pipelines/data_extraction/`
  - [ ] `src/baliza/pipelines/data_processing/`  
  - [ ] `src/baliza/pipelines/analytics/`
  - [ ] `src/baliza/pipelines/domain_enrichment/`

- [ ] **F2.2.2** Implementar nodes core:
  - [ ] Extraction nodes (API → Bronze)
  - [ ] Processing nodes (Bronze → Silver)
  - [ ] Analytics nodes (Silver → Gold)
  - [ ] Quality validation nodes

- [ ] **F2.2.3** Configurar Kedro DataCatalog:
  - [ ] `conf/base/catalog.yml`
  - [ ] `conf/base/parameters.yml`  
  - [ ] Conexões Ibis + DuckDB

---

## Fase 3: Storage Tiers & Performance

### 🗄️ **F3.1: Hot-Cold Storage Architecture**

#### **Por que é Importante:**
Dados de licitações têm padrões de acesso claros:
- **Hot (0-90 dias)**: Queries frequentes, análises de tendências atuais
- **Cold (90 dias - 2 anos)**: Análises históricas, comparações anuais
- **Archive (>2 anos)**: Compliance, auditorias, pesquisa acadêmica

Performance gains esperados:
- **5x faster hot queries**: Menor dataset, índices otimizados
- **60% storage reduction**: Compression agressiva no cold tier
- **Infinite scale**: Archive tier no Internet Archive

#### **Arquivos a Criar:**

**1. `sql/ddl/storage_tiers.sql`:**
```sql
-- Hot tier: Últimos 90 dias, otimizado para writes e queries frequentes
CREATE TABLE IF NOT EXISTS hot_contratacoes (
    LIKE bronze_contratacoes INCLUDING ALL
) PARTITION BY RANGE (dataPublicacaoPNCP);

-- Cold tier: 90 dias - 2 anos, compression agressiva
CREATE TABLE IF NOT EXISTS cold_contratacoes (
    LIKE bronze_contratacoes INCLUDING ALL
) PARTITION BY RANGE (dataPublicacaoPNCP);

-- Views transparentes que unem todos os tiers
CREATE OR REPLACE VIEW all_contratacoes AS
SELECT * FROM hot_contratacoes
UNION ALL
SELECT * FROM cold_contratacoes
UNION ALL
SELECT * FROM read_parquet('https://archive.org/download/baliza-pncp/archive_contratacoes_*.parquet');

-- Índices otimizados por tier
CREATE INDEX IF NOT EXISTS idx_hot_contratacoes_data 
ON hot_contratacoes(dataPublicacaoPNCP DESC); -- Hot: ordenação DESC para recent first

CREATE INDEX IF NOT EXISTS idx_cold_contratacoes_year_modalidade 
ON cold_contratacoes(EXTRACT(year FROM dataPublicacaoPNCP), modalidadeId); -- Cold: análises anuais
```

**2. `src/baliza/storage_manager.py`:**
```python
"""Storage tier management and lifecycle automation"""
import ibis
from datetime import date, timedelta
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class StorageTierManager:
    """Manages data lifecycle across Hot/Cold/Archive tiers"""
    
    def __init__(self, con: ibis.BaseBackend):
        self.con = con
        self.hot_cutoff = date.today() - timedelta(days=90)
        self.cold_cutoff = date.today() - timedelta(days=730)  # 2 years
    
    def migrate_to_cold_tier(self) -> dict:
        """Move data from Hot to Cold tier with aggressive compression"""
        
        # Identify records to migrate
        hot_table = self.con.table("hot_contratacoes")
        records_to_migrate = hot_table.filter(
            hot_table.dataPublicacaoPNCP < self.hot_cutoff
        )
        
        migration_count = records_to_migrate.count().execute()
        
        if migration_count == 0:
            return {"migrated_records": 0, "status": "no_data_to_migrate"}
        
        # Insert into cold tier with compression
        self.con.execute("""
            SET default_compression='zstd';
            SET compression_level=9;
            
            INSERT INTO cold_contratacoes 
            SELECT * FROM hot_contratacoes 
            WHERE dataPublicacaoPNCP < ?
        """, [self.hot_cutoff])
        
        # Delete from hot tier after successful migration
        self.con.execute("""
            DELETE FROM hot_contratacoes 
            WHERE dataPublicacaoPNCP < ?
        """, [self.hot_cutoff])
        
        # Optimize storage
        self.con.execute("CHECKPOINT;")
        
        logger.info(f"Migrated {migration_count} records to cold tier")
        
        return {
            "migrated_records": migration_count,
            "hot_cutoff_date": self.hot_cutoff,
            "status": "success"
        }
    
    def export_to_archive_tier(self) -> dict:
        """Export cold data to Parquet for Internet Archive"""
        
        cold_table = self.con.table("cold_contratacoes")
        archive_candidates = cold_table.filter(
            cold_table.dataPublicacaoPNCP < self.cold_cutoff
        )
        
        export_count = archive_candidates.count().execute()
        
        if export_count == 0:
            return {"exported_records": 0, "status": "no_data_to_export"}
        
        # Export by year for optimal file sizes
        years_to_export = archive_candidates.select(
            ibis.extract(archive_candidates.dataPublicacaoPNCP, "year").name("year")
        ).distinct().execute()["year"].tolist()
        
        exported_files = []
        
        for year in years_to_export:
            year_data = archive_candidates.filter(
                ibis.extract(_.dataPublicacaoPNCP, "year") == year
            )
            
            parquet_path = f"data/archive/contratacoes_{year}.parquet"
            Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Export to Parquet with optimization
            year_data.to_parquet(
                parquet_path,
                compression="zstd",
                row_group_size=100000  # Optimize for Internet Archive
            )
            
            exported_files.append(parquet_path)
        
        return {
            "exported_records": export_count,
            "exported_files": exported_files,
            "cold_cutoff_date": self.cold_cutoff,
            "status": "success"
        }
    
    def get_storage_stats(self) -> dict:
        """Get storage statistics across all tiers"""
        
        hot_stats = self.con.execute("""
            SELECT 
                COUNT(*) as record_count,
                MIN(dataPublicacaoPNCP) as oldest_date,
                MAX(dataPublicacaoPNCP) as newest_date
            FROM hot_contratacoes
        """).fetchone()
        
        cold_stats = self.con.execute("""
            SELECT 
                COUNT(*) as record_count,
                MIN(dataPublicacaoPNCP) as oldest_date,
                MAX(dataPublicacaoPNCP) as newest_date
            FROM cold_contratacoes
        """).fetchone()
        
        return {
            "hot_tier": {
                "records": hot_stats[0],
                "date_range": f"{hot_stats[1]} to {hot_stats[2]}"
            },
            "cold_tier": {
                "records": cold_stats[0], 
                "date_range": f"{cold_stats[1]} to {cold_stats[2]}"
            },
            "cutoff_dates": {
                "hot_cutoff": self.hot_cutoff,
                "cold_cutoff": self.cold_cutoff
            }
        }
```

#### **Tarefas Específicas:**

- [ ] **F3.1.1** Implementar Hot Tier:
  - [ ] Tabelas particionadas por data
  - [ ] Índices otimizados para queries recentes
  - [ ] Compression leve (ZSTD level 1)

- [ ] **F3.1.2** Implementar Cold Tier:
  - [ ] Compression agressiva (ZSTD level 9)
  - [ ] Índices para análises históricas
  - [ ] Read-only com batch updates

- [ ] **F3.1.3** Archive Tier Integration:
  - [ ] Export para Parquet otimizado
  - [ ] Views remotas via httpfs
  - [ ] Internet Archive upload automation

### 📊 **F3.2: Performance Monitoring**

#### **Por que é Importante:**
Sistema em produção precisa observability para:
- **Detectar degradação**: Queries ficando lentas
- **Capacity planning**: Quando adicionar storage tiers
- **Business insights**: Parsing success rate por endpoint

#### **Arquivo: `src/baliza/monitoring.py`:**
```python
"""Performance monitoring and metrics collection"""
import time
import ibis
from datetime import datetime, timedelta
from typing import Dict, List
from dataclasses import dataclass

@dataclass
class PipelineMetrics:
    """Metrics collected during pipeline execution"""
    total_time: float
    records_processed: int
    parsing_success_rate: float
    storage_efficiency: float
    domain_enrichment_rate: float
    errors_count: int

class PerformanceMonitor:
    """Monitors pipeline performance and storage efficiency"""
    
    def __init__(self, con: ibis.BaseBackend):
        self.con = con
    
    def collect_extraction_metrics(self, endpoint: str, date_range: tuple) -> Dict:
        """Collect metrics about extraction performance"""
        
        # Success rate
        total_requests = self.con.execute("""
            SELECT COUNT(*) FROM pncp_requests 
            WHERE endpoint_name = ? AND extracted_at BETWEEN ? AND ?
        """, [endpoint, date_range[0], date_range[1]]).fetchone()[0]
        
        successful_requests = self.con.execute("""
            SELECT COUNT(*) FROM pncp_requests 
            WHERE endpoint_name = ? AND response_code = 200 
            AND extracted_at BETWEEN ? AND ?
        """, [endpoint, date_range[0], date_range[1]]).fetchone()[0]
        
        # Parsing errors
        parsing_errors = self.con.execute("""
            SELECT COUNT(*) FROM pncp_parse_errors 
            WHERE endpoint_name = ? AND extracted_at BETWEEN ? AND ?
        """, [endpoint, date_range[0], date_range[1]]).fetchone()[0]
        
        return {
            "endpoint": endpoint,
            "date_range": date_range,
            "total_requests": total_requests,
            "successful_requests": successful_requests,
            "success_rate": (successful_requests / total_requests) * 100 if total_requests > 0 else 0,
            "parsing_errors": parsing_errors,
            "error_rate": (parsing_errors / successful_requests) * 100 if successful_requests > 0 else 0
        }
    
    def measure_query_performance(self, query: str, description: str) -> Dict:
        """Measure query execution time and resource usage"""
        
        start_time = time.time()
        start_memory = self._get_memory_usage()
        
        result = self.con.execute(query)
        row_count = len(result.fetchall()) if hasattr(result, 'fetchall') else 0
        
        execution_time = time.time() - start_time
        memory_used = self._get_memory_usage() - start_memory
        
        return {
            "description": description,
            "execution_time_seconds": execution_time,
            "memory_used_mb": memory_used,
            "rows_processed": row_count,
            "throughput_rows_per_sec": row_count / execution_time if execution_time > 0 else 0
        }
    
    def storage_efficiency_report(self) -> Dict:
        """Analyze storage efficiency across tiers"""
        
        # Storage sizes by tier
        hot_size = self.con.execute("""
            SELECT pg_size_pretty(pg_total_relation_size('hot_contratacoes'))
        """).fetchone()
        
        cold_size = self.con.execute("""
            SELECT pg_size_pretty(pg_total_relation_size('cold_contratacoes'))
        """).fetchone()
        
        # Compression ratios
        compression_stats = self.con.execute("""
            SELECT 
                table_name,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                (pg_total_relation_size(schemaname||'.'||tablename) / pg_relation_size(schemaname||'.'||tablename)) as compression_ratio
            FROM pg_tables 
            WHERE tablename LIKE '%_contratacoes'
        """).fetchall()
        
        return {
            "tier_sizes": {
                "hot": hot_size[0] if hot_size else "0 bytes",
                "cold": cold_size[0] if cold_size else "0 bytes"
            },
            "compression_stats": [
                {
                    "table": row[0],
                    "size": row[1], 
                    "compression_ratio": f"{row[2]:.2f}x"
                }
                for row in compression_stats
            ]
        }
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        import psutil
        return psutil.Process().memory_info().rss / 1024 / 1024
```

#### **Tarefas Específicas:**

- [ ] **F3.2.1** Implementar métricas de extração:
  - [ ] Success rate por endpoint
  - [ ] Records processed per second
  - [ ] Error rate e error categorization

- [ ] **F3.2.2** Implementar métricas de storage:
  - [ ] Storage efficiency por tier
  - [ ] Compression ratios
  - [ ] Growth trends

- [ ] **F3.2.3** Dashboard de monitoring:
  - [ ] Integrar com `src/baliza/ui/dashboard.py`
  - [ ] Real-time metrics
  - [ ] Alertas automáticos

---

## Critérios de Sucesso & Validação

### **Performance Targets**
- [ ] ✅ **-90% storage usage**: Eliminar duplicação de raw responses
- [ ] ✅ **+5x ingestion speed**: Direct-to-structured parsing  
- [ ] ✅ **+10x query performance**: Tabelas tipadas vs JSON
- [ ] ✅ **<30s** para extração mensal completa

### **Quality Gates**
- [ ] ✅ **100% Pydantic validation** success rate
- [ ] ✅ **Zero data loss** durante migration
- [ ] ✅ **E2E test coverage** para todos os pipelines
- [ ] ✅ **Type safety** garantida em todas as transformações

### **Operational Goals**  
- [ ] ✅ **Kedro orchestration** funcionando com Typer CLI
- [ ] ✅ **Automated tier management** movendo dados entre hot/cold/archive
- [ ] ✅ **Monitoring dashboards** com métricas em tempo real
- [ ] ✅ **CI/CD automation** com testes de regressão

---

## Next Actions (Prioritizadas)

1. **🔥 CRÍTICO - F1.1**: Implementar direct-to-structured parsing
   - Maior impacto: -90% storage, +5x speed
   - Arquivos: `pncp_writer.py`, `extractor.py`, `pncp_schemas.py`

2. **⚡ ALTO - F2.1**: Resolver CLI incompatibility  
   - Desbloqueador: Habilita Kedro orchestration
   - Arquivo: `src/baliza/cli.py`

3. **📊 MÉDIO - F3.1**: Implementar Hot-Cold storage
   - Performance: +5x faster hot queries
   - Arquivos: `sql/ddl/storage_tiers.sql`, `storage_manager.py`

**Tempo Estimado Total**: 6-8 semanas de desenvolvimento
**ROI Esperado**: 90% reduction storage + 10x performance improvement

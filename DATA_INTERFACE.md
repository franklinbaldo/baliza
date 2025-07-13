# BALIZA - Interface de Dados

## Consumindo Dados do BALIZA

O BALIZA mantém uma **PSA (Persistent Staging Area)** com dados limpos e estruturados que podem ser consumidos por projetos analytics externos.

### Estrutura de Dados

#### PSA Schema (DuckDB)
```sql
-- Dados brutos coletados
psa.contratos_raw

-- Dados limpos e estruturados  
staging.stg_contratos

-- KPIs básicos de monitoramento
analytics.dashboard_metrics
```

### Conectando ao BALIZA

#### Via DuckDB Federation
```python
import duckdb

# Conectar ao banco de dados do BALIZA
conn = duckdb.connect(':memory:')
conn.execute("ATTACH '/path/to/baliza/state/baliza.duckdb' AS baliza_source")

# Acessar dados estruturados
contratos = conn.execute("""
    SELECT * FROM baliza_source.staging.stg_contratos 
    WHERE data_assinatura >= '2024-01-01'
""").fetchall()
```

#### Via Parquet Files (Futuro)
```python
import duckdb

# Ler arquivos Parquet incrementais  
conn = duckdb.connect(':memory:')
df = conn.execute("""
    SELECT * FROM '/path/to/baliza/data/contratos_*.parquet'
    WHERE data_assinatura >= '2024-01-01'
""").df()
```

### Campos Disponíveis

#### Dados Básicos
- `contrato_id` - Identificador único do contrato
- `data_assinatura` - Data de assinatura
- `valor_global_brl` - Valor total em reais
- `objeto_contrato` - Descrição do objeto

#### Organizacional
- `orgao_cnpj` - CNPJ do órgão contratante
- `orgao_razao_social` - Nome do órgão
- `uf_sigla` - UF do órgão
- `municipio_nome` - Município

#### Fornecedor
- `fornecedor_ni` - Número de inscrição do fornecedor
- `fornecedor_nome` - Nome/Razão social
- `fornecedor_tipo_pessoa` - Física/Jurídica

#### Metadados
- `baliza_data_date` - Data de coleta
- `baliza_extracted_at` - Timestamp de extração
- `raw_data_json` - JSON original da API

### Casos de Uso

#### 1. Análise de Transparência
```sql
-- Órgãos com mais contratos
SELECT 
    orgao_razao_social,
    COUNT(*) as total_contratos,
    SUM(valor_global_brl) as valor_total
FROM baliza_source.staging.stg_contratos
GROUP BY 1
ORDER BY 2 DESC
```

#### 2. Análise Temporal
```sql
-- Evolução mensal de contratações
SELECT 
    DATE_TRUNC('month', data_assinatura) as mes,
    COUNT(*) as contratos,
    SUM(valor_global_brl) as valor_total
FROM baliza_source.staging.stg_contratos
GROUP BY 1
ORDER BY 1
```

#### 3. Análise Geográfica
```sql
-- Distribuição por UF
SELECT 
    uf_sigla,
    COUNT(*) as contratos,
    AVG(valor_global_brl) as valor_medio
FROM baliza_source.staging.stg_contratos
GROUP BY 1
ORDER BY 2 DESC
```

### Projetos Analytics Recomendados

#### baliza-analytics (Detecção de Anomalias)
- Algoritmos de scoring de suspeição
- Detecção de padrões anômalos
- Análise de risco em fornecedores

#### baliza-insights (Dashboards)
- Visualizações interativas
- KPIs de transparência
- Comparações regionais

#### baliza-compliance (Conformidade Legal)
- Verificação Lei 14.133/2021
- Análise de procedimentos
- Relatórios de auditoria

### Boas Práticas

1. **Read-only Access**: Sempre acesse os dados do BALIZA como somente leitura
2. **Filtros Temporais**: Use sempre filtros de data para performance
3. **Indexação**: Aproveite os índices em `data_assinatura`, `orgao_cnpj`, `uf_sigla`
4. **Campos Limpos**: Prefira dados de `staging.stg_contratos` sobre `psa.contratos_raw`

### Suporte

- O BALIZA mantém apenas dados estruturados e limpos
- Análises complexas devem ser implementadas em projetos separados
- A interface de dados é estável e versionada
- Mudanças na estrutura serão comunicadas com antecedência
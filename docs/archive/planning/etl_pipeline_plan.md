# Plano de Pipeline ETL - Baliza

## Visão Geral

O Baliza implementará um pipeline ETL completo para dados de licitações públicas do PNCP (Portal Nacional de Contratações Públicas), permitindo:

1. **Extract** (Extração) - Comando `baliza extract` (já implementado)
2. **Transform** (Transformação) - Comando `baliza transform` (novo)
3. **Load** (Carregamento) - Comando `baliza load` (novo)

## Comandos Simples

### `baliza transform`

**Comportamento padrão**: Executa todas as transformações necessárias nos dados brutos extraídos.

```bash
# Comando mais simples - faz tudo que precisa
baliza transform

# Equivalente a:
# - Processar dados brutos do DuckDB
# - Executar todas as transformações dbt
# - Gerar datasets analíticos prontos
# - Validar qualidade dos dados
```

**Flags opcionais** (para casos específicos):
```bash
baliza transform --models staging      # Apenas modelos staging
baliza transform --full-refresh        # Reconstruir tudo do zero
baliza transform --year 2023          # Processar apenas um ano
baliza transform --dry-run             # Mostrar o que seria executado
```

### `baliza load`

**Comportamento padrão**: Exporta dados transformados e faz upload para o Internet Archive.

```bash
# Comando mais simples - faz tudo que precisa
baliza load

# Equivalente a:
# - Exportar dados em múltiplos formatos (Parquet, CSV, JSON)
# - Gerar metadados e documentação
# - Compactar arquivos para upload
# - Fazer upload para Internet Archive
# - Atualizar versões e índices
```

**Flags opcionais** (para casos específicos):
```bash
baliza load --format parquet          # Apenas formato Parquet
baliza load --year 2023               # Apenas dados de 2023
baliza load --collection pncp-2023    # Coleção específica
baliza load --dry-run                 # Simular upload sem executar
baliza load --incremental             # Upload incremental
```

## Arquitetura do Pipeline

### 1. Dados Brutos (Extract)
- **Localização**: `data/baliza.duckdb` → tabela `psa.pncp_raw_responses`
- **Formato**: Respostas JSON brutas da API PNCP
- **Compressão**: ZSTD para eficiência de armazenamento

### 2. Transformação (Transform)
- **Ferramenta**: dbt com DuckDB
- **Localização**: Projeto `dbt_baliza/` (já existe)
- **Camadas**:
  - **Bronze**: Dados brutos parseados
  - **Silver**: Dados limpos e normalizados
  - **Gold**: Datasets analíticos prontos

### 3. Carregamento (Load)
- **Destino**: Internet Archive
- **Formatos**: Parquet (principal), CSV, JSON
- **Metadados**: Documentação completa, schemas, versões
- **Organização**: Coleções por ano e tipo de dados

## Implementação Proposta

### Estrutura de Arquivos
```
baliza/
├── src/baliza/
│   ├── __init__.py
│   ├── pncp_extractor.py      # Comando extract (existente)
│   ├── transform.py           # Comando transform (novo)
│   ├── load.py               # Comando load (novo)
│   └── cli.py                # CLI unificada (atualizar)
├── dbt_baliza/               # Projeto dbt (já existe)
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── dbt_project.yml
├── templates/                # Templates para metadados (novo)
│   ├── archive_metadata.json
│   └── collection_description.md
└── docs/                     # Documentação (existente)
```

### Dependências Necessárias

```toml
# Adicionar ao pyproject.toml
dependencies = [
    # ... dependências existentes
    "internetarchive>=3.0.0",    # Upload para Internet Archive
    "pyarrow>=10.0.0",           # Suporte a Parquet
    "jinja2>=3.0.0",             # Templates de metadados
    "pydantic>=2.0.0",           # Validação de dados
]

[project.optional-dependencies]
archive = [
    "internetarchive>=3.0.0",
    "pyarrow>=10.0.0",
]
```

## Fluxo de Trabalho Típico

### Extração Completa
```bash
# 1. Extrair dados brutos (pode demorar horas)
baliza extract

# 2. Transformar dados (alguns minutos)
baliza transform

# 3. Carregar para arquivo público (alguns minutos)
baliza load
```

### Atualizações Incrementais
```bash
# Extrair apenas novos dados
baliza extract --incremental

# Transformar apenas o que mudou
baliza transform

# Upload incremental
baliza load --incremental
```

## Configuração Padrão

### Transform
- **Modelos**: Todos os modelos dbt por padrão
- **Paralelismo**: Baseado no número de CPUs disponíveis
- **Validação**: Testes de qualidade automáticos
- **Formato de saída**: Tabelas no DuckDB otimizadas

### Load
- **Formatos**: Parquet (principal), CSV, JSON
- **Coleção**: `pncp-licitacoes-brasil`
- **Metadados**: Gerados automaticamente
- **Compressão**: GZIP para arquivos de texto, LZ4 para Parquet
- **Organização**: Por ano e tipo de dados

## Benefícios

### Simplicidade
- **Comando único**: `baliza transform` faz tudo que precisa
- **Configuração mínima**: Funciona "out of the box"
- **Sensible defaults**: Comportamento padrão otimizado

### Flexibilidade
- **Flags opcionais**: Para casos específicos
- **Configuração**: Arquivo de configuração para personalização
- **Modularidade**: Cada comando pode ser usado independentemente

### Transparência
- **Logs detalhados**: Progresso e status em tempo real
- **Dry-run**: Simular operações sem executar
- **Versionamento**: Controle de versão dos dados

## Dados Públicos Resultantes

### Internet Archive
- **URL**: `https://archive.org/details/pncp-licitacoes-brasil`
- **Formatos**: Parquet, CSV, JSON
- **Documentação**: Schemas, dicionários, metadados
- **Atualização**: Mensal ou conforme necessário

### Estrutura dos Dados
```
pncp-licitacoes-brasil/
├── 2021/
│   ├── contratos.parquet
│   ├── atas.parquet
│   └── metadata.json
├── 2022/
│   ├── contratos.parquet
│   ├── atas.parquet
│   └── metadata.json
├── schemas/
│   ├── contratos_schema.json
│   └── atas_schema.json
└── documentation/
    ├── README.md
    └── data_dictionary.md
```

## Cronograma de Implementação

### Fase 1 - Transform (1-2 semanas)
- [ ] Implementar `baliza transform`
- [ ] Integrar com dbt existente
- [ ] Adicionar validações de qualidade
- [ ] Testes e documentação

### Fase 2 - Load (1-2 semanas)
- [ ] Implementar `baliza load`
- [ ] Integração com Internet Archive
- [ ] Templates de metadados
- [ ] Testes e documentação

### Fase 3 - Integração (1 semana)
- [ ] CLI unificada
- [ ] Configuração centralizada
- [ ] Testes end-to-end
- [ ] Documentação final

## Impacto

### Dados Abertos
- **Acesso público**: Dados de licitações brasileiras acessíveis globalmente
- **Formatos padrão**: Compatível com ferramentas de análise modernas
- **Atualização regular**: Dados sempre atualizados

### Transparência
- **Histórico completo**: Desde 2021 até presente
- **Rastreabilidade**: Metadados de origem e transformação
- **Reprodutibilidade**: Pipeline documentado e versionado

### Pesquisa e Análise
- **Datasets prontos**: Dados limpos e normalizados
- **Múltiplos formatos**: Compatível com Python, R, SQL
- **Documentação completa**: Facilitando uso por pesquisadores

Este plano cria um pipeline ETL robusto, simples de usar e que democratiza o acesso aos dados de licitações públicas brasileiras.
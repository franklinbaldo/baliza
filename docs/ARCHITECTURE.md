# Arquitetura do Ecossistema Baliza

## Visão Geral

O ecossistema Baliza é composto por **dois repositórios independentes** com responsabilidades bem definidas:

```
┌─────────────────────────────────────────────────────────────┐
│                    ECOSSISTEMA BALIZA                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────────┐      ┌──────────────────────┐    │
│  │   baliza (CLI)       │      │   baliza-site        │    │
│  │   Este repositório   │─────▶│   Novo repositório   │    │
│  └──────────────────────┘      └──────────────────────┘    │
│           │                              │                 │
│           │ Extração                     │ Visualização    │
│           │ Transformação                │ Consulta        │
│           │ Exportação                   │ Download        │
│           │                              │                 │
│           ▼                              ▼                 │
│     [Parquet Files]  ──────────▶  [Static Site]            │
│     [DuckDB Files]                                         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 1. Repositório: `baliza` (Este)

### Responsabilidades

**Pipeline CLI de Extração de Dados do PNCP**

- ✅ Extração incremental de dados da API pública do PNCP
- ✅ Armazenamento em DuckDB (camada bronze)
- ✅ Exportação para Parquet particionado (ano/mês)
- ✅ Rastreamento de cobertura e detecção de gaps
- ✅ Verificação de integridade dos dados
- ✅ Backfill de dados históricos
- ✅ Upload opcional para Internet Archive

### Tecnologias

| Componente | Tecnologia |
|-----------|-----------|
| Linguagem | Python 3.11+ |
| CLI Framework | Typer |
| Data Pipeline | dlt (data load tool) |
| Database | DuckDB |
| Export Format | Parquet |
| Package Manager | uv |
| Testing | pytest |
| Linting | ruff |
| Type Checking | mypy |

### Interface de Saída

O Baliza CLI produz:

1. **Arquivo DuckDB**: `baliza.duckdb`
   - Schema: `baliza_raw`
   - Tabela principal: `contratos`
   - Tabelas de estado: `baliza_state.cobertura`, `baliza_state.janelas`

2. **Arquivos Parquet**: `data/contratos/ano=YYYY/mes=MM/*.parquet`
   - Particionamento por ano e mês
   - Schema preservado do DuckDB
   - Chave primária: `numeroControlePNCP`

3. **Metadados de Exportação**: JSON com:
   - Contagem de registros
   - Lista de partições
   - Range de datas
   - Timestamp da exportação

### Comandos Principais

```bash
# Extração incremental
baliza extract --lookback-days 3

# Backfill histórico
baliza backfill 2024-01 2024-12

# Exportação para Parquet
baliza export --table contratos --out data/contratos

# Verificação de cobertura
baliza verify
```

### Usuários Típicos

- **Engenheiros de dados**: configuração e manutenção do pipeline
- **Pesquisadores**: backfill de dados históricos
- **Administradores**: monitoramento e verificação de cobertura
- **CI/CD**: execução automatizada diária/mensal

### Deployment

- **Execução local**: via `uv run baliza`
- **GitHub Actions**: workflows diários e mensais
- **Futuramente**: containers Docker, orquestradores (Airflow, Dagster)

---

## 2. Repositório: `baliza-site` (Novo)

### Responsabilidades

**Site Estático de Visualização e Download de Dados**

- 🎯 Apresentar dados extraídos pelo Baliza CLI
- 🎯 Interface de busca e consulta
- 🎯 Download de datasets em Parquet
- 🎯 Visualizações e estatísticas
- 🎯 Documentação para usuários finais
- 🎯 API de consulta (opcional)

### Tecnologias Sugeridas

#### Opção 1: Site Estático (Recomendado)

| Componente | Tecnologia Sugerida |
|-----------|-------------------|
| Framework | Next.js (Static Export) ou Astro |
| UI | React + Tailwind CSS |
| Gráficos | Recharts ou Chart.js |
| Tabelas | TanStack Table |
| Query Engine | DuckDB-WASM (no browser) |
| Hosting | GitHub Pages, Vercel, Netlify |
| Build | GitHub Actions |

**Vantagens:**
- ✅ Zero custo de hosting
- ✅ Escala infinita (CDN)
- ✅ Consultas diretas nos arquivos Parquet via DuckDB-WASM
- ✅ Totalmente serverless
- ✅ Deployment automático via GitHub Actions

#### Opção 2: Site Dinâmico (Se necessário backend)

| Componente | Tecnologia Sugerida |
|-----------|-------------------|
| Backend | FastAPI (Python) ou Node.js |
| Frontend | React ou Vue.js |
| Database | PostgreSQL + Parquet via DuckDB |
| API | REST ou GraphQL |
| Hosting | Render, Railway, Fly.io |

### Interface de Entrada

O site consome os outputs do Baliza CLI:

1. **Arquivos Parquet**: leitura direta via DuckDB-WASM ou backend
2. **Metadados**: informações sobre partições disponíveis
3. **Manifesto de Cobertura**: status de completude dos dados

### Funcionalidades Principais

```
┌─────────────────────────────────────────────────┐
│            BALIZA SITE                          │
├─────────────────────────────────────────────────┤
│                                                 │
│  📊 Dashboard                                   │
│     • Estatísticas gerais                       │
│     • Gráficos de tendências                    │
│     • Mapa de cobertura temporal                │
│                                                 │
│  🔍 Busca e Consulta                            │
│     • Filtros por órgão, data, valor            │
│     • Busca por CNPJ, CPF                       │
│     • Filtros avançados (SQL-like)              │
│                                                 │
│  📥 Downloads                                   │
│     • Download de partições (mês/ano)           │
│     • Download de resultados filtrados          │
│     • Formato Parquet ou CSV                    │
│                                                 │
│  📖 Documentação                                │
│     • Dicionário de dados                       │
│     • Tutoriais de análise                      │
│     • Exemplos de consultas                     │
│                                                 │
│  📈 Análises Prontas                            │
│     • Maiores contratos por período             │
│     • Órgãos com mais contratações              │
│     • Análises por modalidade                   │
│                                                 │
└─────────────────────────────────────────────────┘
```

### Usuários Típicos

- **Jornalistas**: busca rápida de contratos específicos
- **Pesquisadores**: download de datasets para análise
- **Cidadãos**: consulta de transparência
- **Analistas**: visualizações prontas

### Deployment

- **Build**: GitHub Actions lê Parquet do repositório baliza
- **Hosting**: GitHub Pages (site estático)
- **Update**: Trigger automático quando baliza exporta novos dados

---

## 3. Integração entre Repositórios

### Fluxo de Dados

```
┌──────────────────────────────────────────────────────────────┐
│                     FLUXO DE DADOS                           │
└──────────────────────────────────────────────────────────────┘

1. [PNCP API]
      ↓
2. [baliza extract] → baliza.duckdb
      ↓
3. [baliza export] → data/contratos/*.parquet
      ↓
4. [GitHub Actions: baliza] → commit + push Parquet
      ↓
5. [GitHub Actions: baliza-site] → build site com novos dados
      ↓
6. [GitHub Pages] → site atualizado
```

### Estratégias de Integração

#### Opção A: Submodule Git
```bash
# No repositório baliza-site
git submodule add https://github.com/franklinbaldo/baliza data/baliza
```

**Vantagens:**
- ✅ Versionamento sincronizado
- ✅ Rastreamento de commits

**Desvantagens:**
- ⚠️ Complexidade para usuários

#### Opção B: GitHub Artifacts
```yaml
# No baliza: upload artifacts
- uses: actions/upload-artifact@v4
  with:
    name: parquet-exports
    path: data/contratos/

# No baliza-site: download artifacts
- uses: dawidd6/action-download-artifact@v2
  with:
    github_token: ${{secrets.GH_TOKEN}}
    workflow: daily-export.yml
    repo: franklinbaldo/baliza
```

**Vantagens:**
- ✅ Simples
- ✅ Armazenamento automático

**Desvantagens:**
- ⚠️ Limite de retenção (90 dias)
- ⚠️ Limite de tamanho

#### Opção C: GitHub Releases (Recomendado)
```yaml
# No baliza: criar release com Parquet
- uses: softprops/action-gh-release@v1
  with:
    files: |
      data/contratos/**/*.parquet
      baliza.duckdb
    tag_name: monthly-${{ env.YEAR }}-${{ env.MONTH }}

# No baliza-site: download latest release
- name: Download latest data
  run: |
    gh release download --repo franklinbaldo/baliza \
      --pattern "*.parquet" \
      --dir public/data
```

**Vantagens:**
- ✅ Permanente
- ✅ Versionado
- ✅ Público e acessível
- ✅ Sem limite de retenção

#### Opção D: Storage Externo (Cloud)
- S3 / R2 / Backblaze B2
- Google Cloud Storage
- Azure Blob Storage

**Vantagens:**
- ✅ Escalável
- ✅ CDN integrado

**Desvantagens:**
- ⚠️ Custo
- ⚠️ Complexidade

---

## 4. Separação de Responsabilidades

### O que NÃO deve estar no baliza (CLI)

❌ Código de frontend (React, Vue, etc.)
❌ CSS, HTML, JavaScript de interface
❌ Componentes de visualização
❌ Rotas de API web
❌ Server-side rendering
❌ Lógica de apresentação

### O que NÃO deve estar no baliza-site

❌ Lógica de extração do PNCP
❌ Pipeline dlt
❌ Configuração de DuckDB local
❌ Scripts de backfill
❌ Tracking de cobertura (apenas leitura)

### Responsabilidades Compartilhadas

✅ **Documentação**:
- `baliza`: documentação técnica do pipeline
- `baliza-site`: documentação para usuários finais

✅ **Schemas de dados**:
- `baliza`: define e exporta
- `baliza-site`: consome e valida

✅ **Testes**:
- `baliza`: testa extração e exportação
- `baliza-site`: testa leitura e apresentação

---

## 5. Próximos Passos

### No repositório `baliza` (atual)

1. ✅ Manter foco exclusivo no CLI
2. ✅ Melhorar documentação técnica
3. ✅ Implementar StateManager e GapDetector
4. ✅ Adicionar observabilidade
5. ✅ Configurar releases mensais com Parquet

### Para o novo repositório `baliza-site`

1. 🎯 Criar repositório `baliza-site`
2. 🎯 Escolher stack tecnológico
3. 🎯 Implementar protótipo de leitura de Parquet
4. 🎯 Criar interface básica de busca
5. 🎯 Configurar deployment automático
6. 🎯 Implementar dashboard com estatísticas

---

## 6. Exemplo de Estrutura do baliza-site

```
baliza-site/
├── README.md
├── package.json
├── next.config.js (ou astro.config.mjs)
├── public/
│   └── data/               # Parquet files baixados
│       └── contratos/
│           └── ano=2024/
│               └── mes=10/
│                   └── data.parquet
├── src/
│   ├── pages/
│   │   ├── index.tsx       # Dashboard
│   │   ├── search.tsx      # Busca
│   │   ├── download.tsx    # Downloads
│   │   └── docs.tsx        # Documentação
│   ├── components/
│   │   ├── DataTable.tsx
│   │   ├── Charts.tsx
│   │   └── Filters.tsx
│   ├── lib/
│   │   ├── duckdb.ts       # DuckDB-WASM wrapper
│   │   └── queries.ts      # Consultas prontas
│   └── styles/
├── scripts/
│   └── download-data.js    # Download Parquet do baliza
├── .github/
│   └── workflows/
│       └── deploy.yml      # Build + deploy
└── docs/
    └── user-guide.md
```

---

## Conclusão

Esta arquitetura de **dois repositórios separados** segue os princípios de:

- **Separação de Responsabilidades** (SoC)
- **Single Responsibility Principle** (SRP)
- **Loose Coupling** (acoplamento fraco via arquivos)
- **High Cohesion** (cada repo tem uma função clara)

**Benefícios:**
- ✅ Manutenção mais fácil
- ✅ Desenvolvimento paralelo
- ✅ Deploy independente
- ✅ Escalabilidade
- ✅ Clareza de propósito

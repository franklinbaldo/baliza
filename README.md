# Baliza CLI

[![Extraction](https://github.com/franklinbaldo/baliza/actions/workflows/continuous-extract.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/continuous-extract.yml)
[![Backfill](https://github.com/franklinbaldo/baliza/actions/workflows/historical-backfill.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/historical-backfill.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Dashboard](https://img.shields.io/badge/Dashboard-Live-green)](https://franklinbaldo.github.io/baliza/)

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) é uma **ferramenta
de linha de comando** de código aberto que captura dados de contratos do Portal
Nacional de Contratações Públicas (PNCP) e os armazena em um banco **DuckDB**
pronto para análise. O projeto nasceu para preservar o histórico das compras
públicas brasileiras e oferecer uma base consistente para jornalistas,
pesquisadores e órgãos de controle.

> **⚠️ Este repositório contém apenas o CLI de extração de dados.**
> Para visualização, dashboards e interface web, veja o projeto `baliza-site`
> (em breve). Documentação completa da arquitetura em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## 🎯 Project Goals / Non-Goals

### Goals
- **Reliable Data Extraction:** Ensure data is fetched resilently from PNCP.
- **Data Preservation:** Store raw data in DuckDB and export to Parquet for archival.
- **Accessibility:** Provide clean, partitioned data for researchers and journalists.
- **Transparency:** Audit coverage and identify gaps in the public data.

### Non-Goals
- **No Web UI:** Visualization belongs in `baliza-site`.
- **No Real-time:** Focus on batch daily/monthly updates.
- **No Analysis:** The CLI extracts; users analyze with their own tools.

## 📊 Current Status (February 2026)

- ✅ **Core Pipeline:** Robust `httpx`-based extraction with page-level checkpointing.
- ✅ **Storage:** Incremental merge into DuckDB and export to daily/monthly Parquet.
- ✅ **Verification:** Basic gap detection via `baliza verify`.
- 🚧 **In Progress:** Expanding the CLI with `state` management commands and dedicated `backfill` logic.
- 🧪 **Testing:** BDD test suite being stabilized and expanded to cover resilience scenarios.

## Visão geral

- **Extração direta com HTTPX:** O Baliza utiliza `httpx` para fazer chamadas
  diretas e resilientes ao endpoint `GET /v1/contratos` do PNCP.
- **CLI enxuta:** Comandos simples para extração, exportação e verificação.
- **Armazenamento em DuckDB:** Os dados brutos são armazenados em um banco de
  dados local `baliza.duckdb` para fácil acesso e análise.
- **Entrega analítica:** Geração de arquivos Parquet particionados.

## Instalação

### Opção 1: Execução direta com uvx (Recomendado)

```bash
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help
```

### Opção 2: Instalação local para desenvolvimento

```bash
git clone https://github.com/franklinbaldo/baliza.git
cd baliza
uv sync
uv run baliza extract --start 2024-01-01 --end 2024-01-05
```

## Comandos disponíveis

| Comando | Descrição |
|---------|-----------|
| `baliza extract` | Extrai dados para um intervalo de datas. |
| `baliza verify` | Audita a cobertura e detecta lacunas. |
| `baliza export` | Exporta tabela DuckDB para Parquet. |
| `baliza export-daily` | Gera pacote Parquet autocontido para um dia específico. |
| `baliza status` | Exibe resumo geral do banco de dados local. |
| `baliza buffer-stats` | Exibe estatísticas detalhadas do buffer de extração. |

> **Próximos comandos:** `baliza state show`, `baliza state gaps`, `baliza state history` e `baliza backfill` estão em desenvolvimento.

## Contribuindo

Veja [`docs/ROADMAP.md`](docs/ROADMAP.md) para prioridades e abra uma issue para discussão.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

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

## Visão geral

- **Extração direta com HTTPX:** O Baliza utiliza `httpx` para fazer chamadas
  diretas e resilientes ao endpoint `GET /v1/contratos` do PNCP, com paginação
  customizável e janelas de data (`dataInicial`/`dataFinal`).
- **Resumibilidade com Checkpoints:** O Baliza salva o progresso após cada página
  extraída, permitindo retomar extrações interrompidas sem perda de dados.
- **Armazenamento em DuckDB:** Os dados brutos são armazenados em um banco de
  dados local `baliza.duckdb` para fácil acesso e análise.
- **Exportação Analítica e Diária:** `baliza export` gera arquivos Parquet.
  `baliza export-daily` cria pacotes diários autocontidos (contratos, órgãos, unidades).

## Project Goals / Non-Goals

### Goals
- **Reliable Data Extraction:** Efficiently extract public procurement data from the PNCP.
- **Data Preservation:** Create a long-term archive in DuckDB and Parquet formats.
- **Accessibility:** Make data accessible for journalists and researchers.
- **Daily Export Packages:** Provide relational Parquet packages for archival services.
- **Robust Resumability:** Handle failures gracefully with a checkpoint system.

### Non-Goals
- **No Web UI:** Dashboards belong to the `baliza-site` project.
- **No Real-time Streaming:** Designed for batch extraction and backfilling.

## Current Status

- ✅ **Core Extraction:** Stable `extract` command with resumability.
- ✅ **Data Verification:** `verify` command for gap detection.
- ✅ **Daily Export:** Fully implemented `export-daily` for structured packages.
- ⏳ **CLI Subcommands:** Transitioning to a unified `state` command group (Planned).
- ⏳ **Tier System:** Classification system for features (Planned).

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
| `baliza extract --start YYYY-MM-DD --end YYYY-MM-DD` | Extrai dados do PNCP para o intervalo informado. |
| `baliza verify --start YYYY-MM-DD --end YYYY-MM-DD` | Audita a cobertura e detecta lacunas de dados. |
| `baliza export --table <nome>` | Exporta uma tabela do DuckDB para Parquet. |
| `baliza export-daily --date YYYY-MM-DD` | Cria um pacote diário autocontido para arquivamento. |
| `baliza status` | Exibe o resumo geral da extração e do buffer local. |
| `baliza buffer-stats` | Mostra estatísticas detalhadas do buffer por data. |

## Contribuindo

1. Abra uma issue descrevendo o problema ou melhoria desejada.
2. Crie um fork e uma branch baseada em `main`.
3. Rode os testes relevantes (`uv run pytest`) antes de abrir o PR.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

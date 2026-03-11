# Baliza CLI

[![Extraction](https://github.com/franklinbaldo/baliza/actions/workflows/continuous-extract.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/continuous-extract.yml)
[![Backfill](https://github.com/franklinbaldo/baliza/actions/workflows/historical-backfill.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/historical-backfill.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Dashboard](https://img.shields.io/badge/Dashboard-Live-green)](https://franklinbaldo.github.io/baliza/)

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) é uma **ferramenta
de linha de comando** de código aberto que captura dados de contratos do Portal
Nacional de Contratações Públicas (PNCP) e os armazena em um banco **DuckDB**
pronto para análise.

> **⚠️ Este repositório contém apenas o CLI de extração de dados.**
> Para visualização, dashboards e interface web, veja o projeto `baliza-site`.
> Documentação estratégica em [`docs/MASTERPLAN.md`](docs/MASTERPLAN.md).

## 🎯 Project Goals

- **Reliability:** Bulletproof extraction that survives network failures and API instability.
- **Preservation:** Creating a permanent, versioned record of Brazilian procurement history.
- **Accessibility:** Exporting data in open, high-performance formats (DuckDB, Parquet).
- **Transparency:** Clear reporting on data coverage and gaps.

### Non-Goals
- Not a general-purpose data analysis tool (use the exported Parquet files for that).
- Not a real-time monitoring tool (optimized for daily/batch updates).
- Not a frontend/dashboard provider (see `baliza-site`).

## 📊 Current Status

| Feature | Status | Tier |
|---------|--------|------|
| **Core Extraction** | ✅ Done | 🔴 Tier 0 |
| **Gap Detection** | ✅ Done | 🟠 Tier 1 |
| **Parquet Export** | ✅ Done | 🔴 Tier 0 |
| **Resumability** | ✅ Done | 🟠 Tier 1 |
| **State CLI** | ⏳ In Progress | 🟠 Tier 1 |
| **Backfill CLI** | 📝 Planned | 🟠 Tier 1 |

## Instalação

### Opção 1: Execução direta com uvx (Recomendado)

```bash
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help
```

### Opção 2: Instalação local

```bash
git clone https://github.com/franklinbaldo/baliza.git
cd baliza
uv sync
uv run baliza --help
```

## Comandos principais

| Comando | Descrição |
|---------|-----------|
| `baliza extract` | Extrai dados do PNCP (requer `--start` e `--end`). Suporta checkpointing. |
| `baliza verify` | Verifica cobertura e detecta lacunas (gaps) no período informado. |
| `baliza export` | Exporta tabela para Parquet. |
| `baliza export-daily` | Exporta pacote diário particionado. |
| `baliza status` | Exibe resumo do status da extração (Será movido para `baliza state show`). |

## 🚀 Performance Benchmarks

Recent benchmark findings for the Baliza extraction pipeline reveal the following speeds based on the number of workers:
- **1 worker**: ~12s
- **4 workers**: 3.5s (optimal speed)
- **16 workers**: 34s (regression due to PNCP API rate limits/timeouts)

**Recommendation:** Use 4-8 workers for best performance. Higher concurrency (e.g., 16 workers) leads to slower results because it triggers API rate limits and connection timeouts from the PNCP server, causing retries and backoffs.

## Contribuindo

Veja [CONTRIBUTING.md](CONTRIBUTING.md) (em breve) ou abra uma issue.

## Licença

MIT

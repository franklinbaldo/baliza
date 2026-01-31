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
  diretas e resilientes ao endpoint `GET /v1/contratos` do PNCP.
- **CLI Estruturada:** Comandos organizados por funcionalidade e criticidade (Tiers).
- **Armazenamento em DuckDB:** Os dados brutos são armazenados em um banco de
  dados local `baliza.duckdb` para fácil acesso e análise.
- **Entrega analítica imediata:** os dados são gravados no arquivo
  `baliza.duckdb` (dataset `baliza_raw`) com *merge* incremental baseado na
  chave oficial `numeroControlePNCP`.
- **Resumibilidade:** Suporte a checkpoints por página para retomar extrações interrompidas.
- **Manifesto de cobertura:** Rastreamento de janelas extraídas e detecção de lacunas.

## Instalação

### Opção 1: Execução direta com uvx (Recomendado)

```bash
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help
```

### Opção 2: Instalação local para desenvolvimento

```bash
git clone https://github.com/franklinbaldo/baliza.git
cd baliza
uv sync --all-extras
uv run baliza --help
```

## Início rápido

```bash
# Extrair dados para um intervalo de datas
uv run baliza extract --start 2024-01-01 --end 2024-01-05

# Verificar o estado da extração e lacunas
uv run baliza state show
uv run baliza state gaps --start 2024-01-01 --end 2024-01-10

# Exportar dados para Parquet
uv run baliza export --table contratos --output data/contratos
```

## Comandos disponíveis

### Extração e Backfill

| Comando | Descrição | Tier |
|---------|-----------|------|
| `baliza extract` | Extrai dados para um intervalo de datas específico. | 🔴 0 |
| `baliza backfill <YYYY-MM> <YYYY-MM>` | Executa a extração mês a mês para o intervalo informado. | 🟠 1 |

### Estado e Observabilidade (`baliza state`)

| Subcomando | Descrição | Tier |
|------------|-----------|------|
| `baliza state show` | Exibe o status geral da extração e banco de dados. | 🟠 1 |
| `baliza state gaps` | Detecta e lista lacunas na cobertura de dados. | 🟠 1 |
| `baliza state history` | Exibe o histórico das execuções de extração. | 🟠 1 |

### Outros Comandos

| Comando | Descrição | Tier |
|---------|-----------|------|
| `baliza export` | Exporta uma tabela do DuckDB para Parquet. | 🔴 0 |
| `baliza export-daily` | Gera pacotes diários autocontidos para o Internet Archive. | 🔴 0 |
| `baliza tiers` | Exibe a hierarquia de funcionalidades e seus respectivos Tiers. | 🟡 2 |
| `baliza buffer-stats` | Mostra estatísticas detalhadas sobre o buffer local. | 🟡 2 |

Use `uv run baliza --help` para ver todos os parâmetros suportados.

## Testes

O Baliza utiliza BDD (Behavior Driven Development) com `pytest-bdd`.

```bash
# Rodar todos os testes
uv run pytest

# Rodar apenas testes críticos (Tier 0)
uv run pytest -m tier0

# Rodar apenas testes de comportamento (BDD)
uv run pytest tests/step_defs/
```

## Arquitetura do Ecossistema

O projeto Baliza é dividido em **dois repositórios independentes**:

| Repositório | Responsabilidade | Status |
|------------|------------------|--------|
| **baliza** (este) | CLI de extração, transformação e exportação de dados | ✅ Ativo |
| **baliza-site** | Interface web, visualização e consultas | 🔜 Em breve |

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

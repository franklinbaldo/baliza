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
- **CLI enxuta:** O comando `baliza extract` executa a extração direta para o DuckDB.
- **Armazenamento em DuckDB:** Os dados brutos são armazenados em um banco de
  dados local `baliza.duckdb`. `baliza export` gera arquivos Parquet.
- **Resumibilidade:** Suporte a checkpoints por página, permitindo retomar
  extrações interrompidas.
- **Manifesto de cobertura:** O comando `baliza verify` audita a cobertura de dados
  e detecta lacunas.

## Instalação

### Usando uv (Recomendado)

```bash
# Clonar repositório
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Instalar dependências
uv sync

# Executar
uv run baliza extract --start 2024-01-01 --end 2024-01-05
```

## Comandos disponíveis

### Extração e Exportação

| Comando | Descrição | Status |
|---------|-----------|--------|
| `baliza extract` | Extrai dados do PNCP para o DuckDB em um intervalo de datas. | ✅ Ativo |
| `baliza verify` | Audita a cobertura e detecta janelas incompletas ou ausentes. | ✅ Ativo |
| `baliza export` | Exporta tabelas do DuckDB para Parquet. | ✅ Ativo |
| `baliza export-daily` | Gera pacotes diários autocontidos em Parquet. | ✅ Ativo |
| `baliza backfill` | Processamento mensal sequencial. | ⏳ Planejado |

### Observabilidade

| Comando | Descrição | Status |
|---------|-----------|--------|
| `baliza status` | Exibe resumo geral da extração e do banco. | ✅ Ativo |
| `baliza buffer-stats` | Exibe estatísticas do buffer de dados. | ✅ Ativo |
| `baliza state show` | Resumo detalhado de janelas (completas/incompletas). | ⏳ Em breve |
| `baliza state gaps` | Lista lacunas de cobertura. | ⏳ Em breve |
| `baliza state history` | Histórico de execuções. | ⏳ Em breve |

## Configuração

O Baliza utiliza parâmetros de linha de comando para configurar a extração. O banco de dados padrão é `baliza.duckdb` e o dataset padrão é `baliza_raw`.

```bash
# Exemplo com parâmetros customizados
uv run baliza extract --start 2024-01-01 --end 2024-01-31 --duckdb meu_banco.duckdb --resource contratos
```

## Contribuindo

Veja [`docs/ROADMAP.md`](docs/ROADMAP.md) para as prioridades de desenvolvimento.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

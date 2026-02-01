# Baliza CLI

[![Extraction](https://github.com/franklinbaldo/baliza/actions/workflows/continuous-extract.yml/badge.svg)](https://github.com/franklinbaldo/baliza/actions/workflows/continuous-extract.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) é uma **ferramenta
de linha de comando** de código aberto que captura dados de contratos do Portal
Nacional de Contratações Públicas (PNCP) e os armazena em um banco **DuckDB**
pronto para análise.

## Visão geral

- **Extração direta com HTTPX:** O Baliza utiliza `httpx` para fazer chamadas
  diretas e resilientes ao endpoint `GET /v1/contratos` do PNCP.
- **Armazenamento em DuckDB:** Os dados brutos são armazenados localmente
  para fácil acesso e análise.
- **Exportação Flexível:** Gera arquivos Parquet para consumo em ferramentas de BI e análise de dados.
- **Auditoria de Cobertura:** O comando `baliza verify` ajuda a identificar lacunas nos dados coletados.

## Instalação

### Instalação local (Recomendado para desenvolvimento)

```bash
git clone https://github.com/franklinbaldo/baliza.git
cd baliza
uv sync --all-extras
```

## Início rápido

```bash
# Extrair dados de um período
uv run baliza extract --start 2024-01-01 --end 2024-01-05

# Verificar se há lacunas na extração
uv run baliza verify --start 2024-01-01 --end 2024-01-10

# Exportar para Parquet
uv run baliza export --table contratos --output data/exports

# Exportar pacote diário consolidado
uv run baliza export-daily --date 2024-01-01

# Ver status do banco de dados
uv run baliza status
```

## Comandos Disponíveis

| Comando | Descrição |
|---------|-----------|
| `extract` | Extrai dados do PNCP para um intervalo de datas específico. |
| `verify` | Audita a cobertura e detecta janelas ausentes no banco local. |
| `export` | Exporta uma tabela do DuckDB para um arquivo Parquet. |
| `export-daily` | Cria um pacote diário com tabelas de contratos, órgãos e unidades. |
| `status` | Exibe um resumo da quantidade de dados e períodos cobertos. |
| `buffer-stats` | Mostra estatísticas detalhadas do buffer de dados. |

## Próximos Passos (Backlog)

- [ ] **Extração Incremental:** Suporte a `--lookback-days` para atualização automática.
- [ ] **Resumibilidade:** Retomada automática de extrações interrompidas.
- [ ] **Backfill:** Comando especializado para grandes janelas históricas.
- [ ] **Outros Endpoints:** Suporte para licitações e atas de registro de preços.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

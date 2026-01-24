# Baliza CLI

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) é uma **ferramenta
de linha de comando** de código aberto que captura dados de contratos do Portal
Nacional de Contratações Públicas (PNCP) e os armazena em um banco **DuckDB**
pronto para análise. O projeto nasceu para preservar o histórico das compras
públicas brasileiras e oferecer uma base consistente para jornalistas,
pesquisadores e órgãos de controle.

> **⚠️ Este repositório contém apenas o CLI de extração de dados.**
> A documentação completa da arquitetura está em `docs/ARCHITECTURE.md`.

## Visão Geral

- **CLI Simples e Direta:** A ferramenta usa `httpx` para fazer chamadas diretas à API do PNCP e `duckdb` para armazenamento local. Não há mais dependência da biblioteca `dlt`.
- **Comandos Claros:** A CLI oferece três comandos principais: `extract` para baixar dados por período, `verify` para checar a cobertura, e `export` para salvar os dados em formato Parquet.
- **Armazenamento Local:** Os dados são salvos em um arquivo `baliza.duckdb` no diretório em que o comando é executado.

## Instalação e Uso

### Requisitos
- Python 3.11 ou superior
- [uv](https://github.com/astral-sh/uv) instalado (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

### Instalação Local para Desenvolvimento

Clone o repositório e instale as dependências:

```bash
# Clonar repositório
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Instalar dependências com uv
uv sync --all-extras

# Executar os comandos
uv run baliza --help
```

## Comandos Disponíveis

### `extract`

Extrai dados de um recurso da API do PNCP para um intervalo de datas específico.

**Uso:**
```bash
uv run baliza extract --start AAAA-MM-DD --end AAAA-MM-DD [OPÇÕES]
```

**Argumentos Obrigatórios:**
- `--start`: Data de início no formato `YYYY-MM-DD`.
- `--end`: Data de fim no formato `YYYY-MM-DD`.

**Opções:**
- `--resource <nome>`: O recurso a ser extraído (padrão: `contratos`).
- `--duckdb <caminho>`: Caminho para o arquivo DuckDB (padrão: `baliza.duckdb`).

**Exemplo:**
```bash
# Extrair dados de contratos de 1 a 5 de janeiro de 2024
uv run baliza extract --start 2024-01-01 --end 2024-01-05
```

### `verify`

Verifica a cobertura dos dados em um intervalo de datas, procurando por lacunas.

**Uso:**
```bash
uv run baliza verify --start AAAA-MM-DD --end AAAA-MM-DD [OPÇÕES]
```

**Exemplo:**
```bash
uv run baliza verify --start 2024-01-01 --end 2024-01-31
```

### `export`

Exporta uma tabela do DuckDB para o formato Parquet.

**Uso:**
```bash
uv run baliza export --table <nome_tabela> --output <dir_saida> [OPÇÕES]
```

**Argumentos Obrigatórios:**
- `--table`: Nome da tabela a ser exportada (ex: `contratos`).
- `--output`: Diretório onde o arquivo Parquet será salvo.

**Exemplo:**
```bash
uv run baliza export --table contratos --output data/
```

## Inspecionando os Dados

Você pode usar o CLI do DuckDB para inspecionar o banco de dados:

```bash
duckdb baliza.duckdb
```

```sql
-- Exemplo de consulta
USE baliza_raw;
SELECT COUNT(*) FROM contratos;
```

## Estrutura do Repositório

```
├── src/baliza/
│   ├── cli_simple.py       # Interface de linha de comando (Typer)
│   ├── extractor.py        # Lógica de extração da API do PNCP
│   └── ...
├── docs/                   # Documentação de arquitetura e decisões
├── tests/                  # Testes automatizados
└── pyproject.toml          # Metadados e dependências do projeto
```

## Contribuindo

1. Abra uma issue descrevendo o problema ou melhoria.
2. Crie um fork e uma branch a partir de `main`.
3. Rode os testes com `uv run pytest`.
4. Envie um Pull Request com uma descrição clara das suas mudanças.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

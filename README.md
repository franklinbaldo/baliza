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
> (em breve).

## Visão geral

- **Extração direta com HTTPX:** O Baliza utiliza `httpx` para fazer chamadas
  diretas e resilientes ao endpoint `GET /v1/contratos` do PNCP, com paginação
  e tratamento de erros.
- **CLI com Typer:** A interface de linha de comando é construída com `Typer`,
  oferecendo comandos claros e ajuda integrada.
- **Armazenamento em DuckDB:** Os dados brutos são armazenados em um banco de
  dados local (`baliza.duckdb` por padrão) para fácil acesso e análise.
- **Exportação para Parquet:** O comando `export` permite a extração de dados
  do DuckDB para o formato Parquet.
- **Verificação de Cobertura:** O comando `verify` ajuda a identificar lacunas
  na extração de dados.

## Instalação

### Opção 1: Execução direta com uvx (Recomendado)

Execute o Baliza sem precisar clonar o repositório:

```bash
# Executar diretamente do GitHub
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help
```

### Opção 2: Instalação local para desenvolvimento

Clone o repositório e desenvolva localmente:

```bash
# Clonar repositório
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Instalar dependências
uv sync

# Executar
uv run baliza --help
```

### Opção 3: Docker (Produção e CI/CD)

Execute o Baliza em um container isolado:

```bash
# Baixar imagem do GitHub Container Registry
docker pull ghcr.io/franklinbaldo/baliza:latest

# Verificar ajuda
docker run --rm ghcr.io/franklinbaldo/baliza:latest --help

# Extrair dados (com volume montado)
docker run --rm -v $(pwd)/data:/data ghcr.io/franklinbaldo/baliza:latest extract --start 2024-01-01 --end 2024-01-31 --duckdb /data/baliza.duckdb

# Exportar para Parquet
docker run --rm -v $(pwd)/data:/data ghcr.io/franklinbaldo/baliza:latest export --table contratos --output /data/contratos --duckdb /data/baliza.duckdb
```

**Vantagens:**
- ✅ Ambiente completamente isolado
- ✅ Sem necessidade de instalar Python ou uv
- ✅ Ideal para produção e CI/CD
- ✅ Reprodutível em qualquer sistema com Docker

> **Nota:** O arquivo `baliza.duckdb` será criado no diretório montado (`-v $(pwd)/data:/data`).
> Certifique-se de montar um volume para persistir o estado entre execuções.

## Início rápido

```bash
# Extrair dados de um período específico
uv run baliza extract --start 2024-01-01 --end 2024-01-31

# Verificar a cobertura dos dados no mesmo período
uv run baliza verify --start 2024-01-01 --end 2024-01-31

# Exportar a tabela de contratos para Parquet
uv run baliza export --table contratos --output data/contratos

# Exportar um pacote diário de dados
uv run baliza export-daily --date 2024-01-15 --output data/daily

# Ver estatísticas do buffer de extração
uv run baliza buffer-stats

# Ver o status geral da base de dados
uv run baliza status
```

O comando `baliza extract` cria (ou atualiza) o arquivo `baliza.duckdb` no
diretório atual.

## Comandos disponíveis

| Comando | Descrição |
|---|---|
| `baliza extract` | Extrai dados da API do PNCP para um intervalo de datas. |
| `baliza verify` | Verifica a cobertura de dados e detecta lacunas. |
| `baliza export` | Exporta uma tabela do DuckDB para arquivos Parquet. |
| `baliza export-daily` | Exporta um pacote diário de dados em Parquet com metadados. |
| `baliza buffer-stats` | Mostra estatísticas sobre o buffer de extração. |
| `baliza status` | Mostra um resumo do status da extração. |

Use `uv run baliza --help` para ver todos os parâmetros suportados.

## Estrutura deste repositório

```
├── src/baliza/
│   ├── cli_simple.py       # Interface de linha de comando (Typer)
│   ├── extractor.py        # Lógica de extração com httpx e DuckDB
│   ├── daily_exporter.py   # Lógica para exportação diária de dados
│   └── utils.py            # Funções auxiliares
├── docs/                   # Guias de arquitetura e planos de evolução
├── tests/                  # Testes automatizados
│   ├── features/           # Cenários BDD (Gherkin)
│   └── step_defs/          # Implementação dos cenários BDD
└── pyproject.toml          # Metadados e dependências do projeto
```

## Contribuindo

1. Abra uma issue descrevendo o problema ou melhoria desejada.
2. Crie um fork e uma branch baseada em `main`.
3. Rode os testes relevantes (`uv run pytest`) antes de abrir o PR.
4. Descreva claramente o impacto das mudanças e atualize a documentação.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

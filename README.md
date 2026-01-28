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

## Status Atual (Alpha)

**Este projeto está em estágio inicial de desenvolvimento.** As funcionalidades
centrais de extração e exportação estão operacionais, mas recursos avançados
como pipelines totalmente resumíveis e detecção automática de lacunas ainda
estão em desenvolvimento ativo.

O branch `main` reflete a versão estável atual. Para um esboço detalhado de
nossos objetivos, prioridades e funcionalidades planejadas, por favor, veja o
documento de estratégia viva do projeto: [`docs/MASTERPLAN.md`](docs/MASTERPLAN.md).

## Visão geral

- **Extração Direta com HTTPX:** O Baliza utiliza `httpx` para fazer chamadas
  diretas e resilientes ao endpoint `GET /v1/contratos` do PNCP.
- **CLI Simples:** O comando `baliza extract` executa a extração para um
  intervalo de datas especificado.
- **Armazenamento em DuckDB:** Os dados brutos são armazenados em um banco de
  dados local `baliza.duckdb` para fácil acesso e análise.
- **Exportação para Parquet:** Os comandos `export` e `export-daily` geram
  arquivos Parquet a partir do DuckDB.
- **Documentação de Arquitetura:** Os arquivos em `docs/` registram as decisões
  e os próximos passos para a evolução do pipeline.

> 📌 **Escopo atual:** o pipeline cobre o endpoint de **contratos**. A inclusão
> de demais recursos do PNCP está detalhada na
> [`docs/endpoint_extraction_strategy.md`](docs/endpoint_extraction_strategy.md).

## Instalação

### Opção 1: Execução direta com uvx (Recomendado)

Execute o Baliza sem precisar clonar o repositório:

```bash
# Executar diretamente do GitHub
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Exemplos de uso
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract --start 2024-01-01 --end 2024-01-02
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza export --table contratos --output data/contratos
```

**Vantagens:**
- ✅ Não precisa clonar o repositório
- ✅ Sempre usa a versão mais recente do `main`
- ✅ Ambiente isolado automaticamente
- ✅ Ideal para uso em produção e CI/CD

### Opção 2: Instalação local para desenvolvimento

Clone o repositório e desenvolva localmente:

```bash
# Clonar repositório
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Instalar dependências
uv sync

# Executar
uv run baliza extract --start 2024-01-01 --end 2024-01-02
uv run baliza export --table contratos --output data/contratos
```

### Opção 3: Docker (Produção e CI/CD)

Execute o Baliza em um container isolado:

```bash
# Baixar imagem do GitHub Container Registry
docker pull ghcr.io/franklinbaldo/baliza:latest

# Verificar versão
docker run --rm ghcr.io/franklinbaldo/baliza:latest --version

# Extrair dados (com volume montado)
docker run --rm -v $(pwd)/data:/data ghcr.io/franklinbaldo/baliza:latest extract --start 2024-01-01 --end 2024-01-02

# Exportar para Parquet
docker run --rm -v $(pwd)/data:/data ghcr.io/franklinbaldo/baliza:latest export --table contratos --output /data/contratos
```

**Vantagens:**
- ✅ Ambiente completamente isolado
- ✅ Sem necessidade de instalar Python ou uv
- ✅ Ideal para produção e CI/CD
- ✅ Reprodutível em qualquer sistema com Docker

> **Nota:** O arquivo `baliza.duckdb` será criado no diretório montado (`-v $(pwd)/data:/data`).
> Certifique-se de montar um volume para persistir o estado entre execuções.

### Requisitos

**Para uvx ou instalação local:**
- Python 3.11 ou superior
- [uv](https://github.com/astral-sh/uv) instalado (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Acesso à internet para consultar a API pública do PNCP

**Para Docker:**
- Docker instalado ([Guia de instalação](https://docs.docker.com/get-docker/))
- ~500MB de espaço em disco para imagem
- Acesso à internet para download da imagem e consulta à API

## Início rápido

### Usando uvx (sem clonar)

```bash
# Alias para simplificar (adicione ao seu .bashrc ou .zshrc)
alias baliza='uvx --from "git+https://github.com/franklinbaldo/baliza" baliza'

# Extrair um intervalo de datas
baliza extract --start 2024-01-01 --end 2024-01-02

# Exportar para Parquet
baliza export --table contratos --output data/contratos
```

### Usando instalação local

```bash
# Dentro do diretório do projeto
uv run baliza extract --start 2024-01-01 --end 2024-01-02
uv run baliza export --table contratos --output data/contratos
```

O comando `baliza extract` cria (ou atualiza) o arquivo `baliza.duckdb` no
diretório atual. Ele busca dados para o intervalo de datas (`--start`/`--end`)
especificado. Em seguida, `baliza export` lê a tabela do DuckDB e escreve os
dados como um único arquivo Parquet no diretório de saída.

## Inspecionando os dados

Abra o DuckDB gerado diretamente pelo shell:

```bash
uv run python -m duckdb --batch <<'SQL'
.open baliza.duckdb
USE baliza_raw;
SELECT COUNT(*) AS total_contratos,
       MAX(dataatualizacao) AS ultima_atualizacao
FROM contratos;
SQL
```

Também é possível utilizar pandas ou polars:

```python
import duckdb

con = duckdb.connect("baliza.duckdb")
con.execute("USE baliza_raw")
contratos = con.execute("SELECT * FROM contratos").df()
print(contratos.head())
```

## Comandos Disponíveis

| Comando | Descrição |
|---------|-----------|
| `baliza extract --start <data> --end <data>` | Extrai dados do PNCP para o intervalo de datas especificado. |
| `baliza export --table <tabela> --output <dir>` | Exporta uma tabela do DuckDB para um único arquivo Parquet. |
| `baliza export-daily --date <data>` | Exporta um pacote diário de dados em formato Parquet. |
| `baliza verify --start <data> --end <data>` | Verifica a cobertura dos dados em um intervalo de datas. |
| `baliza status` | Exibe um resumo do estado da extração. |

Use `uv run baliza --help` para ver todos os parâmetros suportados.

## Arquitetura do Ecossistema

O projeto Baliza é dividido em **dois repositórios independentes**:

| Repositório | Responsabilidade | Status |
|------------|------------------|--------|
| **baliza** (este) | CLI de extração, transformação e exportação de dados | ✅ Ativo |
| **baliza-site** | Interface web, visualização e consultas | 🔜 Em breve |

Veja [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) para detalhes completos.

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

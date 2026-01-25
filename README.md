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

- **Extração direta com httpx:** O Baliza utiliza `httpx` para fazer chamadas diretas e paginadas ao endpoint `GET /v1/contratos` do PNCP, processando janelas de datas especificadas.
- **CLI simples e direta:** O comando `baliza extract` busca dados para um intervalo de datas explícito, e `baliza export` gera arquivos Parquet a partir do banco de dados local.
- **Armazenamento em DuckDB:** Os dados brutos são armazenados localmente em um arquivo DuckDB (`baliza.duckdb`), permitindo fácil acesso e análise com SQL.
- **Exportação para Parquet:** O comando `baliza export` converte tabelas do DuckDB para o formato Parquet, ideal para arquivamento e análise em larga escala.
- **Verificação de cobertura:** O comando `baliza verify` inspeciona o banco de dados local para encontrar lacunas (dias sem dados) em um determinado intervalo.

> 📌 **Escopo atual:** O pipeline cobre o endpoint de **contratos**. A inclusão
> de demais recursos do PNCP está detalhada na
> [`docs/endpoint_extraction_strategy.md`](docs/endpoint_extraction_strategy.md).

## Instalação

### Opção 1: Execução direta com uvx (Recomendado)

Execute o Baliza sem precisar clonar o repositório:

```bash
# Executar diretamente do GitHub
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Exemplo de uso
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
uv sync --all-extras

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

# Extrair dados para um intervalo (com volume montado)
docker run --rm -v $(pwd)/data:/app/data ghcr.io/franklinbaldo/baliza:latest extract --start 2024-01-01 --end 2024-01-02 --duckdb /app/data/baliza.duckdb

# Exportar para Parquet
docker run --rm -v $(pwd)/data:/app/data ghcr.io/franklinbaldo/baliza:latest export --table contratos --output /app/data/contratos --duckdb /app/data/baliza.duckdb
```

**Vantagens:**
- ✅ Ambiente completamente isolado
- ✅ Sem necessidade de instalar Python ou uv
- ✅ Ideal para produção e CI/CD
- ✅ Reprodutível em qualquer sistema com Docker

> **Nota:** O arquivo `baliza.duckdb` será criado no diretório montado.
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

# Extrair dados de um período específico
baliza extract --start 2024-01-01 --end 2024-01-02

# Exportar para Parquet
baliza export --table contratos --output data/contratos

# Verificar cobertura
baliza verify --start 2024-01-01 --end 2024-01-31
```

### Usando instalação local

```bash
# Dentro do diretório do projeto
uv run baliza extract --start 2024-01-01 --end 2024-01-02
uv run baliza export --table contratos --output data/contratos
uv run baliza verify --start 2024-01-01 --end 2024-01-31
```

O comando `baliza extract` cria (ou atualiza) o arquivo `baliza.duckdb` no
diretório atual, buscando os dados do intervalo de datas especificado. Em seguida,
`baliza export` lê a tabela do DuckDB e escreve os dados como um único arquivo Parquet no diretório informado (`data/contratos`, no exemplo).

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

## Configuração

A lógica de extração é implementada diretamente no código Python (`src/baliza/extractor.py`). Parâmetros como o tamanho da página (`500`) e a URL base da API (`https://pncp.gov.br/api/consulta`) são gerenciados internamente.

Para extrações, você deve fornecer as datas de início e fim e o caminho do banco de dados através das opções da linha de comando.

```bash
# Exemplo: Usando um arquivo de banco de dados customizado
uv run baliza extract --start 2024-01-01 --end 2024-01-02 --duckdb /path/to/my_data.duckdb
```

## Comandos disponíveis

| Comando | Descrição |
|---------|-----------|
| `baliza extract` | Extrai dados de um recurso para um intervalo de datas explícito. |
| `baliza export` | Exporta uma tabela do DuckDB para um arquivo Parquet. |
| `baliza export-daily`| Exporta um pacote diário autocontido em formato Parquet. |
| `baliza verify` | Verifica a cobertura de dados em um intervalo e detecta lacunas. |
| `baliza status` | Mostra um resumo do estado da extração. |
| `baliza buffer-stats`| Exibe estatísticas do buffer interno de extração. |

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
│   ├── extractor.py        # Lógica de extração de dados da API (httpx)
│   ├── daily_exporter.py   # Lógica de exportação diária para Parquet
│   └── utils.py            # Funções auxiliares
├── docs/                   # Guias de arquitetura e planos de evolução
├── tests/                  # Testes automatizados
└── pyproject.toml          # Metadados e dependências do projeto
```

## Contribuindo

1. Abra uma issue descrevendo o problema ou melhoria desejada.
2. Crie um fork e uma branch baseada em `main`.
3. Rode os testes relevantes (`uv run pytest`) antes de abrir o PR.
4. Descreva claramente o impacto das mudanças e atualize a documentação.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

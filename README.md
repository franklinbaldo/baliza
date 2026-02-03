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
- **CLI enxuta:** O comando `baliza extract` executa a extração de dados por
  intervalo de datas e `baliza status` exibe o estado atual da base.
- **Armazenamento em DuckDB:** Os dados brutos são armazenados em um banco de
  dados local `baliza.duckdb` para fácil acesso e análise. `baliza export`
  gera arquivos Parquet particionados por ano/mês a partir do DuckDB.
- **Entrega analítica imediata:** os dados são gravados no arquivo
  `baliza.duckdb` (dataset `baliza_raw`) com *merge* incremental baseado na
  chave oficial `numeroControlePNCP` (string completa `CNPJ-2-sequencial/ano`).
- **Manifesto de cobertura:** cada página coletada gera metadados com
  `totalPaginas` reportado, hashes de `numeroControlePNCP` e status das janelas.
  O comando `baliza verify` audita o manifesto chamando apenas a primeira página
  de cada janela e marcando lacunas ou crescimento tardio informado pela API.
- **Documentação de arquitetura:** os arquivos em `docs/` registram decisões e
  próximos passos para evolução do pipeline.

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
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza export --table contratos --out data/contratos
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
uv run baliza extract
uv run baliza export --table contratos --out data/contratos
```

### Opção 3: Docker (Produção e CI/CD)

Execute o Baliza em um container isolado:

```bash
# Baixar imagem do GitHub Container Registry
docker pull ghcr.io/franklinbaldo/baliza:latest

# Verificar versão
docker run --rm ghcr.io/franklinbaldo/baliza:latest --version

# Extrair dados (com volume montado)
docker run --rm -v $(pwd)/data:/data ghcr.io/franklinbaldo/baliza:latest extract

# Exportar para Parquet
docker run --rm -v $(pwd)/data:/data ghcr.io/franklinbaldo/baliza:latest export --table contratos --out /data/contratos

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

# Extrair dados dos últimos 3 dias
baliza extract

# Exportar para Parquet
baliza export --table contratos --out data/contratos

# Verificar cobertura
baliza verify --start 2024-01-01 --end 2024-01-31

```

### Usando instalação local

```bash
# Dentro do diretório do projeto
uv run baliza extract --start 2024-01-01 --end 2024-01-07
uv run baliza export --table contratos --output data/contratos
```

O comando `baliza extract` cria (ou atualiza) o arquivo `baliza.duckdb` no
diretório atual. Ele envia requisições paginadas com `tamanhoPagina=500` até que
`totalPaginas` seja percorrido. Em seguida, `baliza export` lê a tabela do DuckDB
e escreve os dados como Parquet no diretório informado.

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

A configuração declarativa do pipeline fica em `src/baliza/config/pncp.yml`.
Nela é possível ajustar:

- Parâmetros padrão de paginação (`tamanhoPagina=500`, `pagina=1`).
- Datas inicial/final utilizadas pelo incremental (`initial_value`,
  `lookback_days` via CLI) sempre convertidas para `AAAAMMDD`.
- Mapeamento da resposta padronizada (`data`, `totalPaginas`, etc.),
  preservando `numeroControlePNCP` como chave primária textual.

A API pública do PNCP fica em `https://pncp.gov.br/api/consulta` e retorna um
envelope com `data`, `totalRegistros`, `totalPaginas`, `numeroPagina`,
`paginasRestantes` e `empty`. A configuração do Baliza consome esses campos,
tratando respostas `204 No Content` como janelas vazias (sem erro) e sempre
respeitando `tamanhoPagina ≤ 500`.

Para usar uma configuração customizada, forneça o caminho via `--config`:

```bash
uv run baliza extract --config configs/pncp-custom.yml
```

## Comandos disponíveis

### Comandos Disponíveis

| Comando | Descrição |
|---------|-----------|
| `baliza extract` | Extrai dados do PNCP para um intervalo de datas. Suporta resumibilidade via checkpoints. |
| `baliza verify` | Verifica a cobertura de dados e detecta lacunas em um intervalo. |
| `baliza status` | Exibe o status geral da extração e dados armazenados. |
| `baliza export` | Exporta tabelas do DuckDB para arquivos Parquet. |
| `baliza export-daily` | Gera pacotes diários de dados (contratos, órgãos, unidades). |
| `baliza buffer-stats` | Exibe estatísticas do buffer de extração. |

Opções úteis:

- `--duckdb /caminho/arquivo.duckdb` — define o arquivo DuckDB de destino.
- `--dataset nome` — define o *schema* dentro do DuckDB (padrão: `baliza_raw`).
- `--resource nome` — define o recurso a ser extraído (padrão: `contratos`).

Use `uv run baliza --help` para ver todos os parâmetros suportados.

### Extração Resumível (Resumable Extraction) ✨

O Baliza possui **extração resumível**, permitindo que o processo continue de onde parou em caso de interrupção:

1. **Checkpoints:** O progresso é salvo após cada página extraída em `baliza_state.extraction_checkpoint`.
2. **Retomada Automática:** Se uma extração for interrompida e reiniciada com as mesmas datas, o Baliza detecta o checkpoint e retoma da última página pendente.

### Auditoria e Cobertura

- **`baliza verify`**: Audita a cobertura de dados em um intervalo, comparando as janelas registradas em `baliza_state.coverage` e identificando lacunas.
- **`baliza status`**: Fornece uma visão consolidada da base de dados, incluindo total de registros e intervalo temporal coberto.

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

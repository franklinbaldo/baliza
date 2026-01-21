# Baliza CLI

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

- **Extração direta com `httpx`:** O Baliza utiliza o cliente HTTP `httpx` para
  chamar diretamente o endpoint `GET /v1/contratos` do PNCP, paginando os
  resultados e processando janelas de tempo diárias.
- **Armazenamento em DuckDB:** Os dados brutos são salvos em uma tabela local
  do DuckDB (padrão: `baliza.duckdb`), permitindo acesso e consulta imediatos.
  A inserção é feita com `INSERT OR IGNORE` para evitar duplicatas, usando
  `numeroControlePNCP` como chave.
- **Exportação para Parquet:** O comando `baliza export` converte os dados do
  DuckDB para o formato Parquet, particionado por ano e mês, ideal para
  análise em larga escala.
- **Estado e Resiliência:** O CLI mantém o estado da extração em tabelas
  internas no mesmo arquivo DuckDB, permitindo que o processo seja retomado
  automaticamente em caso de falha (`baliza extract`) e que a cobertura dos
  dados seja verificada (`baliza verify`).

> 📌 **Escopo atual:** o pipeline cobre apenas o endpoint de **contratos**.

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

# Backfill
docker run --rm -v $(pwd)/data:/data ghcr.io/franklinbaldo/baliza:latest backfill 2024-01 2024-03
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

# Backfill mensal
baliza backfill 2024-01 2024-03

# Verificar cobertura
baliza verify
```

### Usando instalação local

```bash
# Dentro do diretório do projeto
uv run baliza extract
uv run baliza export --table contratos --out data/contratos
uv run baliza backfill 2024-01 2024-03
```

O comando `baliza extract` cria (ou atualiza) o arquivo `baliza.duckdb` no
diretório atual. Por padrão, a execução retrocede alguns dias (lookback) e abre
janelas diárias `dataInicial`/`dataFinal`, enviando requisições paginadas com
`tamanhoPagina=500` até que `totalPaginas` seja percorrido. Em seguida,
`baliza export` lê a tabela do DuckDB e escreve os dados como Parquet
particionado (ano/mês) no diretório informado (`data/contratos`, no exemplo).

## Inspecionando os dados

Abra o DuckDB gerado diretamente pelo shell:

```bash
# Conectar ao DuckDB e inspecionar o schema
uv run duckdb baliza.duckdb -c ".schemas"

# Contar registros na tabela de contratos
uv run duckdb baliza.duckdb -c "SELECT COUNT(*) FROM baliza_data.contratos;"
```

Também é possível utilizar `pandas` ou `polars` para carregar os dados
diretamente do DuckDB ou dos arquivos Parquet exportados.

## Comandos disponíveis

### Comandos

| Comando | Descrição |
|---------|-----------|
| `baliza extract` | Executa a extração de dados de forma incremental e resumível. Por padrão, busca os últimos dias e preenche quaisquer lacunas anteriores. |
| `baliza export` | Exporta a tabela `contratos` do DuckDB para Parquet particionado por ano/mês. |
| `baliza verify` | Audita a cobertura de dados no DuckDB, comparando com os metadados da API para encontrar janelas incompletas. |

### Opções Principais

- `--start <AAAA-MM-DD>`: Data de início para a extração.
- `--end <AAAA-MM-DD>`: Data de fim para a extração.
- `--duckdb <caminho.db>`: Caminho para o arquivo DuckDB.
- `--dataset <nome>`: Nome do schema/dataset dentro do DuckDB (padrão: `baliza_data`).
- `--out <diretorio>`: Diretório de saída para os arquivos Parquet (usado pelo `export`).

Use `uv run baliza --help` para ver todas as opções.

### Extração Resumível

O comando `baliza extract` foi projetado para ser robusto:

- **Resumível:** Se a extração for interrompida, a próxima execução continuará de onde parou, priorizando as janelas de dados que falharam.
- **Detecção de Lacunas:** O comando analisa o estado no DuckDB para encontrar períodos que não foram capturados e os preenche.
- **Incremental:** Por padrão, o comando extrai dados de um período recente (`--lookback-days`) para manter o banco de dados atualizado.

Este comportamento garante que o trabalho não seja refeito desnecessariamente e torna o processo resiliente a falhas de rede ou outras interrupções.

## Estrutura do Repositório

```
├── src/baliza/
│   ├── cli_simple.py       # Interface de linha de comando (Typer)
│   └── extractor.py        # Lógica principal de extração e estado
├── tests/
│   ├── features/           # Cenários de BDD (Gherkin)
│   └── step_defs/          # Implementação dos testes BDD
└── pyproject.toml          # Dependências e configuração do projeto
```

## Contribuindo

1. Abra uma issue descrevendo o problema ou melhoria desejada.
2. Crie um fork e uma branch baseada em `main`.
3. Rode os testes relevantes (`uv run pytest`) antes de abrir o PR.
4. Descreva claramente o impacto das mudanças e atualize a documentação.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

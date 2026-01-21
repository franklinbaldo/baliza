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

- **Pipeline Resumível e Robusto:** O Baliza utiliza uma arquitetura customizada
  com `httpx` e DuckDB para garantir extrações de dados resilientes. A lógica
  principal fica na classe `PNCPExtractor` em `src/baliza/extractor.py`.
- **Estado no DuckDB:** Todas as operações são rastreadas em um schema
  `baliza_state` dentro do arquivo DuckDB, permitindo que o pipeline pare e
  continue de onde parou, detectando e preenchendo lacunas de dados
  automaticamente.
- **CLI Intuitiva:** Comandos como `baliza extract` executam o pipeline
  incremental, `baliza verify` audita a integridade dos dados e `baliza export`
  gera arquivos Parquet.
- **Fluxo de Dados Direto:** Os dados são extraídos da API do PNCP e salvos
  diretamente no DuckDB, utilizando a chave `numeroControlePNCP` para evitar
  duplicatas.
- **Arquitetura Clara:** O projeto segue uma arquitetura bem definida,
  documentada no `docs/MASTERPLAN.md`, que serve como a fonte da verdade para
  metas, backlog e decisões técnicas.

> 📌 **Escopo atual:** O pipeline está focado no endpoint de **contratos**. A
> estratégia para incluir outros recursos do PNCP está definida no MASTERPLAN.

## Instalação

### Opção 1: Execução direta com `uvx` (Recomendado)

Execute o Baliza sem precisar clonar o repositório:

```bash
# Executar diretamente do GitHub
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Exemplos de uso
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza export --table contratos --out data/contratos
```

**Vantagens:**
- ✅ Não precisa clonar o repositório.
- ✅ Sempre usa a versão mais recente do `main`.
- ✅ Ambiente isolado automaticamente.
- ✅ Ideal para uso em produção e CI/CD.

### Opção 2: Instalação local para desenvolvimento

Clone o repositório e desenvolva localmente:

```bash
# Clonar repositório
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Instalar dependências (incluindo as de desenvolvimento)
uv sync --all-extras

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
docker run --rm -v $(pwd)/baliza.duckdb:/app/baliza.duckdb ghcr.io/franklinbaldo/baliza:latest extract

# Exportar para Parquet
docker run --rm -v $(pwd)/baliza.duckdb:/app/baliza.duckdb -v $(pwd)/data:/data ghcr.io/franklinbaldo/baliza:latest export --table contratos --out /data/contratos

# Backfill
docker run --rm -v $(pwd)/baliza.duckdb:/app/baliza.duckdb ghcr.io/franklinbaldo/baliza:latest backfill 2024-01 2024-03
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

### Usando `uvx` (sem clonar)

```bash
# Alias para simplificar (adicione ao seu .bashrc ou .zshrc)
alias baliza='uvx --from "git+https://github.com/franklinbaldo/baliza" baliza'

# Extrair dados dos últimos 3 dias (lookback padrão)
baliza extract

# Exportar para Parquet
baliza export --table contratos --out data/contratos

# Preencher um período histórico específico
baliza backfill 2024-01 2024-03

# Verificar a integridade dos dados
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
diretório atual. Ele analisa o estado existente, identifica janelas de tempo
faltantes ou incompletas e busca os dados necessários na API do PNCP. Em
seguida, `baliza export` lê a tabela do DuckDB e escreve os dados como Parquet
particionado por ano e mês no diretório de destino.

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

A configuração do Baliza é feita principalmente através de parâmetros de linha de
comando. Não há mais arquivos de configuração YAML.

-   **`--duckdb`**: Especifica o caminho para o arquivo DuckDB (ex:
    `--duckdb /path/to/baliza.duckdb`).
-   **`--lookback-days`**: Define por quantos dias o extrator deve retroceder a
    partir da última data extraída com sucesso.

A URL base da API do PNCP (`https://pncp.gov.br/api/consulta/v1`) e outros
parâmetros como o tamanho da página (`tamanhoPagina=500`) são definidos
diretamente no `PNCPExtractor`.

## Comandos disponíveis

### Comandos de Extração

| Comando | Descrição |
|---------|-----------|
| `baliza extract` | Executa o pipeline incremental com **detecção automática de lacunas** e **resumibilidade**. Identifica janelas incompletas, suspeitas ou ausentes e extrai apenas o necessário. |
| `baliza backfill <AAAA-MM> <AAAA-MM>` | Processa, mês a mês, o intervalo informado sem reaproveitar estado. |
| `baliza export --table <tabela>` | Exporta a tabela DuckDB para Parquet particionado por ano/mês. |
| `baliza verify --resource <recurso>` | Audita a cobertura e detecta janelas incompletas ou suspeitas. |

### Comandos de Estado (novo)

| Comando | Descrição |
|---------|-----------|
| `baliza state show --resource contratos` | Exibe resumo do estado: janelas completas, incompletas, suspeitas. |
| `baliza state gaps --resource contratos --start 2024-01-01` | Lista todas as lacunas de cobertura no período. |
| `baliza state history --resource contratos` | Exibe histórico das últimas execuções (sucessos e falhas). |

Opções úteis:

- `--duckdb /caminho/arquivo.duckdb` — define o arquivo DuckDB de destino.
- `--dataset nome` — define o *schema* dentro do DuckDB (padrão: `baliza_raw`).
- `--lookback-days N` — retrocede `N` dias em relação ao último cursor salvo ao
  construir a janela incremental.
- `baliza export --start-date AAAA-MM-DD --end-date AAAA-MM-DD` — delimita o
  intervalo exportado e cria `data/<recurso>/ano=YYYY/mes=MM/*.parquet`.

Use `uv run baliza --help` para ver todos os parâmetros suportados.

### Extração Resumível (Resumable Extraction) ✨

O Baliza agora possui **extração completamente resumível**, tornando o pipeline
robusto e pronto para produção:

**Como funciona:**
1. **Detecção inteligente de lacunas:** antes de cada extração, o Baliza analisa
   o estado atual e identifica quais janelas temporais precisam ser processadas:
   - Janelas **incompletas** de execuções anteriores que falharam (prioridade máxima)
   - Janelas **suspeitas** com anomalias de dados
   - Janelas **ausentes** que nunca foram extraídas
   - Janelas **recentes** dentro do período de *lookback* (re-extração de atualizações)

2. **Rastreamento de execuções:** cada run é registrado em `baliza_state.extraction_runs`
   com ID único, status (running/completed/failed), janelas processadas e métricas.

3. **Retomada automática:** se uma extração falhar (erro de rede, timeout, crash),
   basta executar `baliza extract` novamente e o processo continua de onde parou,
   priorizando janelas incompletas.

**Exemplo de uso:**

```bash
# Primeira execução - processa últimos 30 dias
$ baliza extract
Analyzing coverage from 2024-10-05 to 2024-11-04 (lookback: 3 days)...
Processing 30 window(s):
  • 30 missing
[1/30] Processing 2024-10-05 to 2024-10-06 (missing)...
  ✓ Completed
...
[15/30] Processing 2024-10-20 to 2024-10-21 (missing)...
✗ Extraction failed: Connection timeout

# Retoma automaticamente da janela 15
$ baliza extract
Found 1 incomplete window(s) from previous run. Resuming...
Merged 16 windows into 2 to reduce API calls.
Processing 2 window(s):
  • 1 incomplete
  • 15 missing
[1/2] Processing 2024-10-20 to 2024-10-21 (incomplete)...
  ✓ Completed
[2/2] Processing 2024-10-21 to 2024-11-05 (missing)...
  ✓ Completed
✓ Extraction completed successfully!
```

**Benefícios:**
- ✅ **Sem desperdício:** não refaz trabalho já concluído
- ✅ **Resiliência:** recupera automaticamente de falhas
- ✅ **Observabilidade:** histórico completo de execuções com `baliza state history`
- ✅ **Otimização:** mescla janelas adjacentes para reduzir chamadas à API

### Política incremental

- **Lookback configurável:** por padrão, `baliza extract` retrocede 3 dias em
  relação à última execução bem-sucedida, mas isso é totalmente configurável
  via `--lookback-days`.
- **Detecção de lacunas:** o Baliza compara as janelas processadas com as esperadas,
  identificando automaticamente períodos ausentes ou incompletos.
- **Backfill mensal:** o comando `baliza backfill <AAAA-MM> <AAAA-MM>` reexecuta
  meses inteiros em sequência, ideal para consolidar históricos.
- **Manifesto de cobertura:** além de `write_disposition=merge`, o Baliza registra
  `totalPaginas`, contagem de itens e hashes por página em `baliza_state.cobertura`
  para auditar janelas e identificar páginas ausentes.

### Exportação analítica

- **Bronze no DuckDB:** os dados brutos permanecem em `baliza.duckdb` dentro do
  dataset `baliza_raw`.
- **Parquet particionado:** `baliza export` gera `data/<recurso>/ano=YYYY/mes=MM/*.parquet`
  a partir de uma coluna de data do domínio (no caso de contratos, a data de
  publicação no PNCP; na ausência, usa-se a melhor proxy disponível e ela é
  documentada na CLI).
- **Consumo incremental:** os arquivos Parquet seguem a mesma chave primária
  utilizada no DuckDB, preservando a máscara oficial do `numeroControlePNCP`.

## Detecção de Gaps

A detecção de lacunas é um recurso central do Baliza, gerenciado pela tabela
`baliza_state.coverage` no DuckDB. Esta tabela registra o status de cada janela
temporal (`complete`, `failed`).

Quando `baliza extract` é executado, ele:
1.  Consulta a tabela `coverage` para encontrar a data mais recente processada.
2.  Retrocede um número configurável de dias (`--lookback-days`) para re-verificar
    dados recentes que possam ter sido atualizados.
3.  Identifica todas as janelas que falharam em execuções anteriores.
4.  Constrói uma lista de janelas a serem processadas, priorizando as falhas e
    preenchendo o período até a data atual.

O comando `baliza verify` complementa este processo, auditando a cobertura e
identificando períodos que ainda não foram processados.

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
│   ├── __init__.py
│   ├── cli_simple.py       # Interface de linha de comando (Typer)
│   ├── extractor.py        # Lógica principal de extração (PNCPExtractor)
│   └── utils.py            # Funções auxiliares
├── docs/                   # Documentação de arquitetura e decisões
│   └── MASTERPLAN.md       # Metas, backlog e arquitetura do projeto
├── tests/                  # Testes automatizados
│   └── test_extractor.py   # Testes para o PNCPExtractor
└── pyproject.toml          # Metadados e dependências do projeto
```

## Contribuindo

1. Abra uma issue descrevendo o problema ou melhoria desejada.
2. Crie um fork e uma branch baseada em `main`.
3. Rode os testes relevantes (`uv run pytest`) antes de abrir o PR.
4. Descreva claramente o impacto das mudanças e atualize a documentação.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

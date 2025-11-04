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

- **Pipeline declarativo com [dlt](https://dlthub.com/):** a configuração YAML
  em `src/baliza/config/pncp.yml` descreve como chamar o endpoint público
  `GET /v1/contratos` do PNCP, paginando com `tamanhoPagina=500` e janelas de
  `dataInicial`/`dataFinal` no formato `AAAAMMDD`.
- **CLI enxuta:** o comando `baliza extract` executa o pipeline incremental e
  `baliza backfill` permite processar janelas mensais de forma determinística.
- **Fluxo bronze → parquet:** `baliza extract` mantém o histórico bruto no
  DuckDB (`baliza.duckdb`) enquanto `baliza export` gera arquivos Parquet
  particionados por ano/mês em `data/<recurso>/ano=YYYY/mes=MM/*.parquet`.
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

### Requisitos

- Python 3.11 ou superior
- [uv](https://github.com/astral-sh/uv) instalado (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Acesso à internet para consultar a API pública do PNCP

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

O plano descrito em [`docs/extraction_resumability_plan.md`](docs/extraction_resumability_plan.md)
foi implementado a partir da criação de um manifesto de cobertura em
`baliza_state.cobertura` e `baliza_state.janelas`. Cada página registrada guarda
`pagina`, `total_paginas_observado`, hash dos `numeroControlePNCP` e momento da
captura. O comando `baliza verify` chama apenas a página 1 de cada janela para
comparar `totalPaginas` informado pela API com o manifesto, marcando janelas
`ok`, `incompleto`, `nao_processado` ou `suspeito` (quando o hash diverge). O
relatório em JSON lista lacunas abertas, páginas pendentes e quaisquer
sequências suspeitas (`--sequencia` ativa a auditoria de
`sequencialCompra`/`sequencialContrato`).

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
│   ├── cli.py              # Interface de linha de comando
│   ├── pipelines/pncp.py   # Execução do pipeline dlt
│   ├── config/pncp.yml     # Configuração declarativa do endpoint
│   ├── state/              # Rastreamento de cobertura
│   └── utils/              # Funções auxiliares (datas, hashing, export)
├── docs/                   # Guias de arquitetura e planos de evolução
│   ├── ARCHITECTURE.md     # Separação entre CLI e site
│   └── ROADMAP.md          # Roadmap do CLI
├── tests/                  # Testes automatizados
│   ├── unit/               # Testes unitários
│   └── e2e/                # Testes end-to-end
└── pyproject.toml          # Metadados e dependências do projeto
```

## Contribuindo

1. Abra uma issue descrevendo o problema ou melhoria desejada.
2. Crie um fork e uma branch baseada em `main`.
3. Rode os testes relevantes (`uv run pytest`) antes de abrir o PR.
4. Descreva claramente o impacto das mudanças e atualize a documentação.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

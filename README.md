# Baliza

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) é um extrator de
código aberto que captura dados de contratos do Portal Nacional de Contratações
Públicas (PNCP) e os armazena em um banco **DuckDB** pronto para análise. O
projeto nasceu para preservar o histórico das compras públicas brasileiras e
oferecer uma base consistente para jornalistas, pesquisadores e órgãos de
controle.

## Visão geral

- **Pipeline declarativo com [dlt](https://dlthub.com/):** a configuração YAML
  em `src/baliza/config/pncp.yml` descreve como chamar o endpoint
  `GET /v1/contratos` do PNCP, aplicando paginação, incremental por
  `dataAtualizacao` e regras de limpeza.
- **CLI enxuta:** o comando `baliza extract` executa o pipeline incremental e
  `baliza backfill` permite processar janelas mensais de forma determinística.
- **Fluxo bronze → parquet:** `baliza extract` mantém o histórico bruto no
  DuckDB (`baliza.duckdb`) enquanto `baliza export` gera arquivos Parquet
  particionados por ano/mês em `data/<recurso>/ano=YYYY/mes=MM/*.parquet`.
- **Entrega analítica imediata:** os dados são gravados no arquivo
  `baliza.duckdb` (dataset `baliza_raw`) com *merge* incremental baseado em
  chave primária (`numeroControlePNCP`).
- **Documentação de arquitetura:** os arquivos em `docs/` registram decisões e
  próximos passos para evolução do pipeline.

> 📌 **Escopo atual:** o pipeline cobre o endpoint de **contratos**. A inclusão
> de demais recursos do PNCP está detalhada na
> [`docs/endpoint_extraction_strategy.md`](docs/endpoint_extraction_strategy.md).

## Requisitos

- Python 3.11 ou superior
- [uv](https://github.com/astral-sh/uv) para gerenciamento de ambiente
- Acesso à internet para consultar a API pública do PNCP

## Início rápido

Clone o repositório, instale as dependências e execute o pipeline incremental:

```bash
uv sync
uv run baliza extract
uv run baliza export --table contratos --out data/contratos
```

O comando `baliza extract` cria (ou atualiza) o arquivo `baliza.duckdb` no
diretório atual. Por padrão, a execução repete os últimos três dias para
garantir que registros atualizados sejam recapturados com segurança. Em seguida,
`baliza export` lê a tabela do DuckDB e escreve os dados como Parquet
particionado (ano/mês) no diretório informado (`data/contratos`, no exemplo).

### Exemplo: executar um *backfill* mensal

```bash
uv run baliza backfill 2024-01 2024-03
```

O comando acima processa, mês a mês, o intervalo de janeiro a março de 2024
usando o mesmo arquivo `baliza.duckdb` como destino.

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

- Parâmetros padrão de paginação (`tamanhoPagina`, `pagina`).
- Datas inicial/final utilizadas pelo incremental (`initial_value`,
  `lookback_days` via CLI).
- Mapeamento de campos retornados (`data_selector`, chave primária etc.).

Para usar uma configuração customizada, forneça o caminho via `--config`:

```bash
uv run baliza extract --config configs/pncp-custom.yml
```

## Comandos disponíveis

| Comando | Descrição |
|---------|-----------|
| `baliza extract` | Executa o pipeline incremental usando o *lookback* informado (padrão: 3 dias). |
| `baliza backfill <AAAA-MM> <AAAA-MM>` | Processa, mês a mês, o intervalo informado sem reaproveitar estado. |
| `baliza export --table <tabela>` | Exporta a tabela DuckDB para Parquet particionado por ano/mês. |

Opções úteis:

- `--duckdb /caminho/arquivo.duckdb` — define o arquivo DuckDB de destino.
- `--dataset nome` — define o *schema* dentro do DuckDB (padrão: `baliza_raw`).
- `--lookback-days N` — retrocede `N` dias em relação ao último cursor salvo ao
  construir a janela incremental.
- `baliza export --start-date AAAA-MM-DD --end-date AAAA-MM-DD` — delimita o
  intervalo exportado e cria `data/<recurso>/ano=YYYY/mes=MM/*.parquet`.

Use `uv run baliza --help` para ver todos os parâmetros suportados.

## Estrutura do repositório

```
├── src/baliza/
│   ├── cli.py              # Interface de linha de comando
│   ├── pipelines/pncp.py   # Execução do pipeline dlt
│   ├── config/pncp.yml     # Configuração declarativa do endpoint
│   └── utils/              # Funções auxiliares (datas, hashing, etc.)
├── docs/                   # Guias de arquitetura e planos de evolução
├── tests/                  # Testes automatizados (em construção)
└── pyproject.toml          # Metadados e dependências do projeto
```

## Contribuindo

1. Abra uma issue descrevendo o problema ou melhoria desejada.
2. Crie um fork e uma branch baseada em `main`.
3. Rode os testes relevantes (`uv run pytest`) antes de abrir o PR.
4. Descreva claramente o impacto das mudanças e atualize a documentação.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

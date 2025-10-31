# Baliza CLI

**Baliza** (Backup Aberto de Licitações Zelando pelo Acesso) é uma **ferramenta
de linha de comando** de código aberto que captura dados de contratos do Portal
Nacional de Contratações Públicas (PNCP) e os armazena em um banco **DuckDB**
pronto para análise. O projeto nasceu para preservar o histórico das compras
públicas brasileiras e oferecer uma base consistente para jornalistas,
pesquisadores e órgãos de controle.

!!! warning "Escopo do Repositório"
    Este repositório contém apenas o CLI de extração de dados.
    Para visualização, dashboards e interface web, veja o projeto `baliza-site`
    (em breve). Documentação completa da arquitetura em [ARCHITECTURE.md](ARCHITECTURE.md).

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

!!! info "Escopo atual"
    O pipeline cobre o endpoint de **contratos**. A inclusão
    de demais recursos do PNCP está detalhada na
    [endpoint_extraction_strategy.md](technical/endpoint_extraction_strategy.md).

## Features

- ✅ Pipeline declarativo com dlt
- ✅ Extração incremental com lookback configurável
- ✅ Backfill mensal determinístico
- ✅ Manifesto de cobertura e detecção de gaps
- ✅ Exportação para Parquet particionado
- ✅ Merge incremental baseado em `numeroControlePNCP`
- ✅ Auditoria de janelas e páginas

## Quick Links

- [Installation Guide](getting-started/installation.md) - Get started with Baliza
- [Quick Start](getting-started/quickstart.md) - Run your first extraction
- [Architecture](ARCHITECTURE.md) - Understand the system design
- [API Reference](api/cli.md) - Detailed API documentation
- [Roadmap](ROADMAP.md) - See what's coming next

## Project Structure

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

## Contributing

1. Abra uma issue descrevendo o problema ou melhoria desejada.
2. Crie um fork e uma branch baseada em `main`.
3. Rode os testes relevantes (`uv run pytest`) antes de abrir o PR.
4. Descreva claramente o impacto das mudanças e atualize a documentação.

## License

Baliza is distributed under the MIT license.

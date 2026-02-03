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
  diretas e resilientes ao endpoint `GET /v1/contratos` do PNCP.
- **CLI enxuta:** O comando `baliza extract` executa a extração por período.
- **Armazenamento em DuckDB:** Os dados brutos são armazenados em um banco de
  dados local `baliza.duckdb` para fácil acesso e análise.
- **Exportação Parquet:** `baliza export` e `baliza export-daily` geram arquivos
  otimizados para consumo externo.
- **Resiliência:** Suporte a checkpoints para retomar extrações interrompidas.

## Instalação

### Opção 1: Execução direta com uvx (Recomendado)

Execute o Baliza sem precisar clonar o repositório:

```bash
# Executar diretamente do GitHub
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Exemplos de uso
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract --start 2023-01-01 --end 2023-01-02
```

### Opção 2: Instalação local

```bash
# Clonar repositório
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# Instalar dependências
uv sync

# Executar
uv run baliza extract --start 2023-01-01 --end 2023-01-02
```

## Comandos disponíveis

| Comando | Descrição |
|---------|-----------|
| `baliza extract` | Extrai dados do PNCP para o DuckDB (requer `--start` e `--end`). Suporta retoma automática via checkpoint. |
| `baliza verify` | Verifica cobertura de dados e detecta lacunas (gaps) no período informado. |
| `baliza export` | Exporta uma tabela inteira do DuckDB para arquivo Parquet. |
| `baliza export-daily` | Exporta pacote diário (contratos + orgaos + metadados) particionado por data. |
| `baliza buffer-stats` | Exibe estatísticas do banco de dados local (linhas, datas, status). |
| `baliza status` | Exibe resumo geral do status da extração e buffer. |

### Exemplos

```bash
# Extrair dados de um período
baliza extract --start 2023-10-01 --end 2023-10-05

# Verificar se há lacunas
baliza verify --start 2023-10-01 --end 2023-10-05

# Exportar pacote diário
baliza export-daily --date 2023-10-01
```

## Dashboard

O projeto inclui um dashboard estático para monitoramento do status da extração,
gerado via GitHub Actions.

Acesse: [docs/dashboard/index.html](docs/dashboard/index.html) (ou via GitHub Pages).

## Estrutura deste repositório

```
├── src/baliza/
│   ├── cli_simple.py       # Interface de linha de comando (Typer)
│   ├── extractor.py        # Lógica de extração com httpx e DuckDB
│   ├── daily_exporter.py   # Lógica para exportação diária de dados
│   └── utils.py            # Funções auxiliares (validação, segurança)
├── docs/                   # Documentação
├── tests/                  # Testes automatizados
└── pyproject.toml          # Metadados e dependências
```

## Contribuindo

1. Abra uma issue descrevendo o problema ou melhoria desejada.
2. Crie um fork e uma branch baseada em `main`.
3. Rode os testes relevantes (`uv run pytest`) antes de abrir o PR.
4. Descreva claramente o impacto das mudanças e atualize a documentação.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

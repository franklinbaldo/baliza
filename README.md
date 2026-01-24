# Baliza CLI

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

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

- **Extrator simples com `httpx`:** O núcleo do projeto é um extrator que utiliza `httpx` para fazer chamadas diretas à API do PNCP.
- **Armazenamento em DuckDB:** Os dados brutos são salvos em um banco de dados DuckDB (`baliza.duckdb`), permitindo fácil acesso e análise local.
- **CLI direta:** A ferramenta oferece comandos simples para extrair dados, verificar a cobertura, exportar para Parquet e verificar o status.
- **Sem dependências complexas:** A arquitetura foi simplificada para remover a dependência da biblioteca `dlt`, focando em um fluxo mais direto e transparente.

## Instalação

### Opção 1: Execução direta com uvx (Recomendado)

Execute o Baliza sem precisar clonar o repositório:

```bash
# Executar diretamente do GitHub
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza --help

# Exemplo de uso
uvx --from "git+https://github.com/franklinbaldo/baliza" baliza extract --start 2024-01-01 --end 2024-01-02
```

**Vantagens:**
- ✅ Não precisa clonar o repositório
- ✅ Sempre usa a versão mais recente do `main`
- ✅ Ambiente isolado automaticamente

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
```

## Início rápido

O comando `baliza extract` é o ponto de partida. Ele exige um intervalo de datas explícito para a extração.

```bash
# Extrair dados de um dia específico
uv run baliza extract --start 2024-07-20 --end 2024-07-21

# Exportar a tabela 'contratos' para um arquivo Parquet
uv run baliza export --table contratos --output data/

# Verificar a cobertura de dados em um intervalo
uv run baliza verify --start 2024-07-01 --end 2024-07-21

# Ver o status geral do banco de dados
uv run baliza status
```

O comando `extract` cria (ou atualiza) o arquivo `baliza.duckdb` no diretório atual.

## Inspecionando os dados

Abra o DuckDB gerado diretamente pelo shell:

```bash
uv run python -m duckdb --batch <<'SQL'
.open baliza.duckdb
USE baliza_raw;
SELECT COUNT(*) AS total_contratos,
       MAX(dataAtualizacao) AS ultima_atualizacao
FROM contratos;
SQL
```

## Comandos disponíveis

| Comando | Descrição |
|---------|-----------|
| `baliza extract` | Extrai dados da API do PNCP para um intervalo de datas **obrigatório**. |
| `baliza verify` | Verifica a integridade dos dados e aponta lacunas no período especificado. |
| `baliza export` | Exporta uma tabela do DuckDB para um único arquivo Parquet. |
| `baliza export-daily` | Exporta um pacote diário autocontido em formato Parquet. |
| `baliza status` | Exibe um resumo geral do estado do banco de dados local. |
| `baliza buffer-stats` | Mostra estatísticas do buffer de extração. |

Use `uv run baliza --help` para ver todos os parâmetros suportados.

## Estrutura do repositório

```
├── src/baliza/
│   ├── cli_simple.py       # Interface de linha de comando (Typer)
│   ├── extractor.py        # Lógica de extração com httpx e DuckDB
│   ├── daily_exporter.py   # Lógica para exportação diária
│   └── utils.py            # Funções auxiliares
├── docs/                   # Documentação do projeto
├── tests/                  # Testes automatizados
│   ├── features/           # Cenários BDD (.feature)
│   └── step_defs/          # Implementação dos passos BDD
└── pyproject.toml          # Metadados e dependências do projeto
```

## Contribuindo

1. Abra uma issue descrevendo o problema ou melhoria desejada.
2. Crie um fork e uma branch baseada em `main`.
3. Rode os testes (`uv run pytest`) antes de abrir o PR.
4. Descreva claramente o impacto das mudanças e atualize a documentação, se aplicável.

## Licença

Baliza é distribuído sob a licença [MIT](LICENSE).

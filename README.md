<div align="center">
  <img src="https://raw.githubusercontent.com/franklinbaldo/baliza/main/assets/logo.png" alt="Logo do BALIZA: Um farol de dados sobre um mar de informações, com o nome BALIZA abaixo" width="400">
  <br>
  <h1>BALIZA</h1>
  <h3>Backup Aberto de Licitações Zelando pelo Acesso</h3>
  <p><strong>Guardando a memória das compras públicas no Brasil.</strong></p>
  <p>
    <a href="https://github.com/franklinbaldo/baliza/actions/workflows/etl_pipeline.yml"><img src="https://img.shields.io/github/actions/workflow/status/franklinbaldo/baliza/etl_pipeline.yml?branch=main&label=Build%20Di%C3%A1rio&style=for-the-badge" alt="Status do Build"></a>
    <a href="https://github.com/franklinbaldo/baliza/blob/main/LICENSE"><img src="https://img.shields.io/github/license/franklinbaldo/baliza?style=for-the-badge" alt="Licença"></a>
    <a href="https://pypi.org/project/baliza/"><img src="https://img.shields.io/pypi/v/baliza?style=for-the-badge" alt="Versão no PyPI"></a>
    <a href="https://franklinbaldo.github.io/baliza/"><img src="https://img.shields.io/badge/docs-material-blue?style=for-the-badge" alt="Documentação"></a>
  </p>
</div>

> **BALIZA** é uma ferramenta de código aberto que extrai, armazena e estrutura dados do Portal Nacional de Contratações Públicas (PNCP), criando um backup histórico, confiável e otimizado para análises e auditoria da maior plataforma de compras públicas do país.

---

## 🚀 Para Análise de Dados (Comece Aqui)

Seu objetivo é **analisar os dados** de contratações públicas, sem a necessidade de executar o processo de extração. Com o BALIZA, você pode fazer isso em segundos, diretamente no seu navegador ou ambiente de análise preferido.

O banco de dados completo e atualizado diariamente está hospedado no [**Internet Archive**](https://archive.org/details/baliza-pncp) em formato DuckDB, e pode ser consultado remotamente.

**Exemplo de Análise Rápida com Python:**
Não é preciso baixar nada! Apenas instale as bibliotecas e execute o código.

```python
# Instale as bibliotecas necessárias
# !pip install duckdb pandas

import duckdb

# Conecte-se remotamente ao banco de dados no Internet Archive
# NOTA: O nome do arquivo pode ser atualizado. Verifique o link acima para o nome mais recente.
DB_URL = "https://archive.org/download/baliza-pncp/baliza.duckdb"

# Para uma análise mais rápida, use os arquivos Parquet exportados
# Exemplo: Analisar contratos de janeiro de 2024
# PARQUET_URL = "https://archive.org/download/baliza-pncp-data/2024/01/contratos_2024_01.parquet"

con = duckdb.connect(read_only=True)
con.execute("INSTALL httpfs; LOAD httpfs;")

# Exemplo: Top 10 órgãos por quantidade de contratos na camada Silver
# (Camadas Gold podem estar disponíveis dependendo da execução do build)
top_orgaos_query = """
    SELECT
        o.razao_social,
        COUNT(f.contract_key) AS total_contratos
    FROM read_parquet('https://archive.org/download/baliza-pncp/silver_fact_contratos.parquet') AS f
    JOIN read_parquet('https://archive.org/download/baliza-pncp/silver_dim_organizacoes.parquet') AS o
        ON f.org_key = o.org_key
    GROUP BY o.razao_social
    ORDER BY total_contratos DESC
    LIMIT 10;
"""

top_orgaos = con.sql(top_orgaos_query).to_df()
print(top_orgaos)
```

- ✅ **Zero Setup:** Comece a analisar em menos de um minuto.
- ✅ **Sempre Atualizado:** Acesse os dados mais recentes coletados pelo workflow diário.
- ✅ **Integração Total:** Funciona perfeitamente com Pandas, Polars, Jupyter Notebooks e outras ferramentas do ecossistema PyData.

## 🎯 O Problema: A Memória Volátil da Transparência

O Portal Nacional de Contratações Públicas (PNCP) é um avanço, mas sua API **não garante um histórico permanente dos dados**. Informações podem ser alteradas ou desaparecer, comprometendo análises de longo prazo, auditorias e o controle social.

## ✨ A Solução: Um Backup para o Controle Social

O BALIZA atua como uma **âncora de dados para o PNCP**. Ele sistematicamente coleta, armazena e estrutura os dados, garantindo que a memória das contratações públicas brasileiras seja preservada e acessível a todos.

-   🛡️ **Resiliência:** Cria um backup imune a mudanças na API ou indisponibilidades do portal.
-   🕰️ **Séries Históricas:** Constrói um acervo completo e cronológico, com deduplicação inteligente de conteúdo para economizar espaço.
-   🔍 **Dados Estruturados para Análise:** Transforma respostas JSON em tabelas limpas e prontas para SQL usando uma arquitetura *Medallion*.
-   🌍 **Aberto por Natureza:** Utiliza formatos abertos (DuckDB, Parquet), garantindo que os dados sejam seus, para sempre.

## 🔧 Para Desenvolvedores e Coletores de Dados

Seu objetivo é **executar o processo de extração** para criar ou atualizar o banco de dados localmente.

**Pré-requisitos:**
- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (um instalador de pacotes Python extremamente rápido)

**Instalação e Execução:**
```bash
# 1. Clone o repositório
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# 2. Instale as dependências com uv
uv sync --all-extras

# 3. Execute a extração para um período específico (ex: 01/01/2024 a 02/01/2024)
# Este é o principal comando do workflow.
uv run baliza pipeline --start-date 2024-01-01 --end-date 2024-01-02
```

**Principais Comandos:**
| Comando | Descrição |
|---|---|
| `baliza pipeline --start-date YYYY-MM-DD --end-date YYYY-MM-DD` | **Workflow principal.** Extrai e carrega dados de um período específico. |
| `baliza seed` | Carrega os dados de domínio (seeds) para o banco de dados. |
| `baliza load` | Carrega os dados para o Internet Archive. |
| `baliza mcp` | Inicia o servidor de análise com IA (Model Context Protocol). |
| `baliza status` | Mostra um painel detalhado sobre a saúde e o estado do sistema. |
| `baliza explore` | Inicia um explorador de dados interativo no terminal. |

## ⚙️ Como Funciona

O BALIZA opera com uma arquitetura de extração moderna e simplificada, garantindo que o processo seja robusto, eficiente e fácil de manter.

```mermaid
flowchart TD
    A[API do PNCP] -->|1. Requisições| B{BALIZA com dlt};
    subgraph BALIZA [Pipeline com dlt]
        direction LR
        B1(Fonte dlt) --> B2(Recurso dlt);
    end
    B -->|2. Carregamento Direto| C[DuckDB Local];
    C -->|3. Análise & Publicação| E(Análise Local, Internet Archive, Servidor IA);
```
_**Legenda:** O BALIZA orquestra a coleta da API do PNCP com um pipeline `dlt`, que gerencia a extração e o carregamento dos dados diretamente para o DuckDB, prontos para análise e publicação._

## 🤖 Análise com Inteligência Artificial (Servidor MCP)

O BALIZA inclui um servidor compatível com o **Model Context Protocol (MCP)** da Anthropic. Isso permite que modelos de linguagem, como o Claude, se conectem diretamente aos seus dados de licitações para realizar análises complexas, consultas e visualizações de forma segura.

**Como Funciona:**
Você inicia um servidor local, e um LLM compatível com MCP pode se conectar a ele para usar as "ferramentas" que o servidor oferece, como a capacidade de executar consultas SQL no seu banco de dados.

**Exemplo de Uso:**
```bash
# 1. Certifique-se de que os dados foram transformados (camadas silver/gold)
uv run baliza transform

# 2. Inicie o servidor MCP. Ele ficará aguardando conexões de um LLM.
uv run baliza mcp
```

O servidor expõe as seguintes capacidades ao LLM:
- **`baliza/available_datasets`**: Lista os conjuntos de dados disponíveis.
- **`baliza/dataset_schema`**: Descreve as colunas e tipos de dados de um dataset.
- **`baliza/execute_sql_query`**: Executa uma consulta SQL de leitura (`SELECT`) nos dados.

- 🧠 **Análise Profunda:** Permite que o LLM explore os dados de forma autônoma para responder a perguntas complexas.
- 🔒 **Segurança em Primeiro Lugar:** O servidor só permite consultas de leitura (`SELECT`), impedindo qualquer modificação nos dados.
- ⚙️ **Padrão Aberto:** Baseado no Model Context Protocol, garantindo interoperabilidade.

Para saber mais sobre a arquitetura, leia nosso [**Guia Teórico do MCP**](./docs/mcp_guide.md).

## 🏗️ Arquitetura e Decisões Técnicas

O BALIZA é construído sobre uma base de tecnologias modernas e decisões de arquitetura bem documentadas (ADRs).

| Camada | Tecnologias | Propósito | ADRs Relevantes |
|---|---|---|---|
| **Coleta e Transformação** | Python, dlt | Extração e transformação de dados em um único pipeline. | [ADR-014](docs/adr/014-dlt-adoption.md) |
| **Armazenamento**| DuckDB | Banco de dados analítico local, rápido e sem servidor, com arquitetura de tabelas divididas para deduplicação. | [ADR-001](docs/adr/001-adopt-duckdb.md), [ADR-008](docs/adr/008-split-psa-table-architecture.md) |
| **Interface** | Typer, Rich | CLI amigável, informativa e com ótima usabilidade. | [ADR-005](docs/adr/005-modern-python-toolchain.md) |
| **Dependências**| uv | Gerenciamento de pacotes e ambientes virtuais de alta performance. | [ADR-005](docs/adr/005-modern-python-toolchain.md) |
| **Publicação** | Internet Archive | Hospedagem pública e permanente dos dados. | [ADR-006](docs/adr/006-internet-archive.md) |
| **Análise IA** | MCP Server (fastmcp) | Análise de dados com LLMs de forma segura. | [ADR-007](docs/adr/007-mcp-server.md) |

📖 **[Veja todos os ADRs](docs/adr/README.md)** para entender as decisões que moldaram o projeto.

## 🙌 Como Contribuir

**Sua ajuda é fundamental para fortalecer o controle social no Brasil!**

1.  **Reporte um Bug:** Encontrou um problema? [Abra uma issue](https://github.com/franklinbaldo/baliza/issues).
2.  **Sugira uma Melhoria:** Tem uma ideia? Adoraríamos ouvi-la nas issues.
3.  **Desenvolva:** Faça um fork, crie uma branch e envie um Pull Request.
4.  **Dissemine:** Use os dados, crie análises, publique reportagens e compartilhe o projeto!

## 📜 Licença

Este projeto é licenciado sob a **Licença MIT**. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.
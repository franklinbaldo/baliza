# BALIZA

<div align="center">
  <img src="https://raw.githubusercontent.com/franklinbaldo/assets/main/logos/baliza/logo-completa-fundo-escuro.png" alt="Logo do BALIZA: Um farol de dados sobre um mar de informações, com o nome BALIZA abaixo" width="400">
  <br>
  <h3>Backup Aberto de Licitações Zelando pelo Acesso</h3>
  <p><strong>Guardando a memória das compras públicas no Brasil.</strong></p>
  <p>
    <a href="https://github.com/franklinbaldo/baliza/blob/main/LICENSE"><img src="https://img.shields.io/github/license/franklinbaldo/baliza?style=for-the-badge" alt="Licença"></a>
    <a href="https://github.com/franklinbaldo/baliza/actions/workflows/baliza_daily_run.yml"><img src="https://img.shields.io/github/actions/workflow/status/franklinbaldo/baliza/baliza_daily_run.yml?branch=main&label=Build%20Di%C3%A1rio&style=for-the-badge" alt="Status do Build"></a>
    <a href="https://pypi.org/project/baliza/"><img src="https://img.shields.io/pypi/v/baliza?style=for-the-badge" alt="Versão no PyPI"></a>
  </p>
</div>

> **BALIZA** é uma ferramenta de código aberto que extrai, armazena e estrutura dados do Portal Nacional de Contratações Públicas (PNCP), criando um backup histórico confiável para análises e auditoria da maior plataforma de compras públicas do país.

---

## 🎯 O Problema: A Memória Volátil da Transparência

O Portal Nacional de Contratações Públicas (PNCP) é um avanço para a transparência, mas possui uma limitação crítica: **os dados são voláteis**.

-   ❌ **Sem Histórico Permanente:** A API do PNCP não garante o acesso a dados antigos e não possui um sistema de versionamento. Dados podem ser alterados ou simplesmente desaparecer.
-   ❌ **Análise Histórica Comprometida:** Sem uma série temporal confiável, é impossível realizar análises de longo prazo, identificar tendências ou investigar padrões de contratações passadas.
-   ❌ **Auditoria Reativa, não Proativa:** A detecção de fraudes e o controle social dependem de dados estáveis. A volatilidade impede uma auditoria séria e sistemática.

## ✨ A Solução: Um Backup para o Controle Social

O BALIZA atua como uma **âncora de dados para o PNCP**. Ele sistematicamente coleta e armazena os dados, garantindo que a memória das contratações públicas brasileiras seja preservada e acessível a todos.

-   🛡️ **Resiliência:** Cria um backup local (ou federado) imune a mudanças na API ou indisponibilidades do portal.
-   🕰️ **Séries Históricas:** Constrói um acervo completo e cronológico, permitindo análises que hoje são inviáveis.
-   🔍 **Dados Estruturados para Análise:** Transforma respostas JSON complexas em tabelas limpas e prontas para serem consultadas com SQL.
-   🌍 **Aberto por Natureza:** Utiliza formatos e ferramentas abertas (DuckDB, Parquet), garantindo que os dados sejam seus, para sempre.

## ⚙️ Como Funciona

O BALIZA opera com uma arquitetura de extração em fases, garantindo que o processo seja robusto e possa ser retomado em caso de falhas.

```mermaid
flowchart TD
    A[API do PNCP] -->|1. Requisições| B(BALIZA);
    subgraph B [BALIZA: Processo de Extração]
        direction LR
        B1(Planejamento) --> B2(Descoberta) --> B3(Execução) --> B4(Reconciliação);
    end
    B -->|2. Armazenamento| C{DuckDB Local};
    C -->|3. Transformação (dbt)| D[Tabelas Limpas e Analíticas];
    D -->|4. Análise| E(Jornalistas, Pesquisadores, Cidadãos);
```
_**Legenda:** O BALIZA orquestra a coleta da API do PNCP, armazena os dados brutos em um banco DuckDB e, com dbt, os transforma em insumos para análise._

1.  **Planejamento:** Identifica os períodos (mês/ano) que precisam ser baixados.
2.  **Descoberta:** Consulta a API para saber o volume de dados (total de páginas) de cada período.
3.  **Execução:** Baixa todas as páginas de forma assíncrona e paralela para máxima eficiência.
4.  **Reconciliação:** Verifica se todos os dados foram baixados corretamente e marca as tarefas como concluídas.
5.  **Transformação:** Após a coleta, modelos **dbt** podem ser executados para limpar, estruturar e enriquecer os dados, criando tabelas prontas para análise.

## 🚀 Como Usar

Existem duas maneiras principais de interagir com o BALIZA, dependendo do seu objetivo.

<!-- Início dos Tabs -->
<details>
<summary><strong>📊 Para Analistas de Dados e Jornalistas</strong></summary>

Seu objetivo é analisar os dados já coletados. Você pode acessar diretamente o banco de dados gerado pelo projeto.

**Pré-requisitos:**
- Python e DuckDB (`pip install duckdb pandas`)

**Exemplo de Análise Rápida:**
```python
import duckdb

# Conecte-se ao banco de dados (baixe-o de uma execução do projeto)
con = duckdb.connect('data/baliza.duckdb')

# Exemplo: Contar o número de compras por UF
resultado = con.sql("""
    SELECT
        json_extract_string(data, '$.municipio.uf.sigla') AS uf,
        COUNT(1) AS total_compras
    FROM psa.pncp_raw_responses
    WHERE
        endpoint = 'compras' AND uf IS NOT NULL
    GROUP BY uf
    ORDER BY total_compras DESC;
""").to_df()

print(resultado)
```
- ✅ **SQL direto nos dados:** Use a sintaxe SQL que você já conhece.
- ✅ **Integração total:** Funciona perfeitamente com Pandas, Jupyter Notebooks, e outras ferramentas do ecossistema PyData.
- ✅ **Dados brutos e transformados:** Acesse tanto a resposta original da API quanto as tabelas já limpas.

</details>

<details>
<summary><strong>🔧 Para Desenvolvedores e Coletores de Dados</strong></summary>

Seu objetivo é executar o processo de extração para criar ou atualizar o banco de dados.

**Pré-requisitos:**
- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (um instalador de pacotes Python extremamente rápido)

**Instalação e Execução:**
```bash
# 1. Clone o repositório
git clone https://github.com/franklinbaldo/baliza.git
cd baliza

# 2. Instale as dependências com uv
uv sync

# 3. Execute a extração (isso pode levar horas!)
# Por padrão, extrai de 2021 até o mês atual
uv run baliza extract
```

**Principais Comandos:**
| Comando | Descrição |
|---|---|
| `uv run baliza extract` | Inicia a extração de dados do PNCP. |
| `uv run baliza extract --concurrency 4` | Limita o número de requisições paralelas. |
| `uv run baliza extract --force` | Força a re-extração de dados já existentes. |
| `uv run baliza stats` | Mostra estatísticas sobre os dados já baixados. |

**Federação com Internet Archive:**
Para garantir a longevidade dos dados, o BALIZA pode fazer upload dos arquivos para o Internet Archive. Configure as variáveis de ambiente `IA_KEY` e `IA_SECRET` (como segredos no seu repositório GitHub) para habilitar esta funcionalidade.
```bash
# Este comando faz o upload dos dados para o IA
uv run python src/baliza/ia_federation.py federate
```
</details>
<!-- Fim dos Tabs -->


## 🏗️ Arquitetura e Tecnologias

| Camada | Tecnologias | Propósito |
|---|---|---|
| **Coleta** | Python, asyncio, httpx, tenacity | Extração eficiente, assíncrona e resiliente. |
| **Armazenamento** | DuckDB | Banco de dados analítico local, rápido e sem servidor. |
| **Transformação** | dbt (Data Build Tool) | Transforma dados brutos em modelos de dados limpos e confiáveis. |
| **Interface** | Typer, Rich | CLI amigável, informativa e com ótima usabilidade. |
| **Dependências**| uv (da Astral) | Gerenciamento de pacotes e ambientes virtuais de alta performance. |

## 🗺️ Roadmap do Projeto

-   [✅] **Fase 1: Fundação** - Extração resiliente para múltiplos endpoints, armazenamento em DuckDB, CLI funcional.
-   [⏳] **Fase 2: Expansão e Análise** - Implementação de modelos `dbt` para análise, melhoria das estatísticas, documentação aprofundada.
-   [🗺️] **Fase 3: Ecossistema e Acessibilidade** - Exportação para formatos abertos (Parquet), criação de dashboards de exemplo, sistema de plugins para novas fontes.
-   [💡] **Futuro:** Painel de monitoramento, notificações sobre falhas, tutoriais em vídeo.

## 🙌 Como Contribuir

**Sua ajuda é fundamental para fortalecer o controle social no Brasil!**

1.  **Reporte um Bug:** Encontrou um problema? [Abra uma issue](https://github.com/franklinbaldo/baliza/issues) descrevendo-o com o máximo de detalhes.
2.  **Sugira uma Melhoria:** Tem uma ideia para uma nova funcionalidade ou melhoria? Adoraríamos ouvi-la nas issues.
3.  **Desenvolva:** Faça um fork do projeto, crie uma branch e envie um Pull Request com suas contribuições.
4.  **Dissemine:** Use os dados, crie análises, publique reportagens e compartilhe o projeto!

## 📜 Licença

Este projeto é licenciado sob a **Licença MIT**. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

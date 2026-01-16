# 🛣️ Roadmap do Baliza CLI

Este roadmap descreve a evolução planejada do **Baliza CLI** — a ferramenta de
linha de comando para extração de dados do PNCP. O objetivo é manter o foco
exclusivo na extração confiável, transformação e exportação de dados.

**⚠️ IMPORTANTE:** Este repositório contém **apenas o CLI**. Funcionalidades de
visualização, interface web, dashboards e consultas interativas fazem parte do
projeto `baliza-site` (repositório separado). Veja `docs/ARCHITECTURE.md` para
entender a separação de responsabilidades.

## Estado atual (Q1 2025)

- ✅ **Cobertura:** apenas o endpoint público `GET /v1/contratos` está habilitado
  e é executado através da configuração declarativa em `src/baliza/config/pncp.yml`.
- ✅ **Execução:** os comandos `baliza extract` e `baliza backfill` operam sobre
  um pipeline `dlt` que abre janelas `dataInicial`/`dataFinal` (formato
  `AAAAMMDD`), pagina com `tamanhoPagina=500` e grava resultados em DuckDB via
  `write_disposition=merge`.
- ✅ **Estado e Resumibilidade:** A extração é totalmente resumível. O `StateManager`
  e o `GapDetector` rastreiam janelas de extração, permitindo que o `baliza extract`
  retome automaticamente de falhas e processe apenas lacunas de dados.
- ⚠️ **Limitações conhecidas:**
  - não há monitoramento estruturado nem relatórios de execução;
  - a suíte de testes cobre somente o fluxo principal da CLI.

## Prioridades imediatas

1. **Observabilidade e resiliência**
   - Adicionar logs estruturados com contagem de páginas, tempo total e totais
     de linhas processadas.
   - Documentar opções de *retry* e *timeout* do `dlt` para lidar com instabilidade
     do PNCP.
3. **Qualidade**
   - Ampliar a suíte de testes com casos unitários para utilitários críticos e
     cenários de erro da CLI.
   - Configurar um fluxo básico de CI executando `pytest` e verificações de lint.

## Backlog (visão futura - CLI apenas)

As iniciativas abaixo permanecem como inspiração para quando o núcleo do projeto
estiver estável. Elas **não estão em desenvolvimento ativo**:

### Escopo do CLI (este repositório)
- ✅ Suporte aos demais 11 endpoints públicos do PNCP
- ✅ Melhorias na exportação Parquet (compressão, schemas)
- ✅ Publicação automatizada de releases com dados
- ✅ Ferramentas de acompanhamento (ex.: `baliza status`)
- ✅ Documentação técnica gerada com MkDocs
- ✅ Distribuição via container Docker
- ✅ Integrações com ferramentas de BI (conectores)
- ✅ Suporte a outros formatos de exportação (CSV, JSON)
- ✅ Validação de dados com schemas Pydantic
- ✅ Métricas e telemetria de execução

### Fora do escopo do CLI (vai para `baliza-site`)
- ❌ Interface web de visualização
- ❌ Dashboards interativos
- ❌ Busca e filtros web
- ❌ Gráficos e charts
- ❌ API REST/GraphQL de consulta
- ❌ Sistema de autenticação
- ❌ Frontend em React/Vue/etc

Contribuições são bem-vindas, especialmente nas prioridades imediatas. Abra uma
issue antes de iniciar itens do backlog para alinharmos o escopo.

Para contribuir com funcionalidades de visualização, aguarde a criação do
repositório `baliza-site`.

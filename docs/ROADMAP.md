# 🛣️ Roadmap do Baliza CLI

Este roadmap descreve a evolução planejada do **Baliza CLI** — a ferramenta de
linha de comando para extração de dados do PNCP. O objetivo é manter o foco
exclusivo na extração confiável, transformação e exportação de dados.

**⚠️ IMPORTANTE:** Este repositório contém **apenas o CLI**. Funcionalidades de
visualização, interface web, dashboards e consultas interativas fazem parte do
projeto `baliza-site` (repositório separado). Veja `docs/ARCHITECTURE.md` para
entender a separação de responsabilidades.

## Estado atual (Fevereiro 2026)

- ✅ **Arquitetura:** Migração completa para `httpx` + `DuckDB`. Referências ao legacy `dlt` foram removidas.
- ✅ **Extração:** Comando `baliza extract` com suporte a checkpointing por página e resumibilidade básica.
- ✅ **Exportação:** Comando `baliza export` para Parquet e `baliza export-daily` para pacotes diários autocontidos.
- ✅ **Verificação:** Comando `baliza verify` para detecção de lacunas de cobertura.
- ⚠️ **Limitações conhecidas:**
  - O grupo de comandos `baliza state` ainda não está totalmente exposto na CLI.
  - O comando `baliza backfill` ainda não está implementado.
  - Alguns testes BDD (Resilience, E2E) estão desabilitados ou incompletos.

## Prioridades imediatas (Sprint Atual)

1. **Expansão da CLI e Observabilidade**
   - Implementar o grupo de comandos `baliza state` (`show`, `gaps`, `history`) para melhor monitoramento.
   - Implementar o comando `baliza backfill` para processamento histórico determinístico.
2. **Estabilização de Testes**
   - Corrigir os problemas de timeout nos testes E2E substituindo `pytest-httpx` por `monkeypatch`.
   - Implementar as definições de passos para `resilience.feature`.
3. **Qualidade de Dados**
   - Melhorar a detecção de janelas "suspeitas" no comando `verify`.

## Backlog (visão futura - CLI apenas)

- ⏳ Suporte aos demais endpoints do PNCP (além de `contratos`).
- ⏳ Publicação automatizada de releases com dados (via GitHub Actions).
- ⏳ Documentação técnica gerada com MkDocs.
- ⏳ Distribuição via container Docker oficial.
- ⏳ Validação de dados com schemas Pydantic.

## Fora do escopo do CLI (vai para `baliza-site`)
- ❌ Interface web de visualização.
- ❌ Dashboards interativos.
- ❌ API REST/GraphQL de consulta.

Para contribuir com funcionalidades de visualização, veja o repositório `baliza-site`.

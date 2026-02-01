# 🛣️ Roadmap do Baliza CLI

Este roadmap descreve a evolução planejada do **Baliza CLI**.

## Estado atual (Q1 2026)

- ✅ **Arquitetura:** Migração completa para `httpx` + `DuckDB`.
- ✅ **Resumibilidade:** Checkpoints por página implementados em `PNCPExtractor`.
- ✅ **Exportação:** Suporte a Parquet e pacotes diários (`export-daily`).
- ✅ **Verificação:** Comando `baliza verify` funcional para detecção de lacunas.

## Prioridades imediatas

1. **Expansão da CLI (Epic 1)**
   - Implementar grupo de comandos `baliza state` (`show`, `gaps`, `history`).
   - Implementar comando `baliza backfill`.
   - Garantir que todos os testes BDD usem os comandos reais da CLI.

2. **Refinamento de Verificação**
   - Melhorar o relatório do `baliza verify` com indicadores de "janelas suspeitas".
   - Detecção automática de mudanças retroativas na API do PNCP.

3. **Consolidação de Testes**
   - Remover "quarentena" de testes E2E resolvendo problemas de timeout no ambiente de teste.
   - Aumentar cobertura de testes unitários para a lógica de extração.

## Backlog

### Escopo do CLI
- ✅ Suporte aos demais endpoints do PNCP (compras, licitacoes)
- ✅ Distribuição via container Docker
- ✅ Melhorias na exportação (compressão, particionamento customizado)
- ✅ Validação de dados com schemas Pydantic

### Fora do escopo do CLI (vai para `baliza-site`)
- ❌ Interface web e Dashboards
- ❌ API de consulta
- ❌ Sistema de autenticação

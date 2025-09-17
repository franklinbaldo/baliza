# DLT Migration Status

Atualizado em: 28 de julho de 2025

## Resumo executivo

A migração do Baliza para o ecossistema **dlt** está funcional para o cenário
principal (extração do endpoint `contratos` direto para DuckDB). A CLI `baliza`
utiliza o pipeline declarativo definido em `src/baliza/config/pncp.yml`, e o
incremental por `dataAtualizacao` está operando com *lookback* configurável.

Ainda existem componentes legados no repositório e funcionalidades planejadas
(deduplicação de requisições, múltiplos endpoints) que não foram concluídas.
Este documento consolida o status atual e os próximos passos para finalizar a
migração.

## Entregas concluídas ✅

- Pipeline `run_pncp` baseado em `dlt.pipeline` com destino DuckDB local.
- Configuração declarativa YAML descrevendo paginador, parâmetros padrão e regra
  incremental.
- CLI `baliza extract` e `baliza backfill` usando exclusivamente o pipeline dlt.
- Hashing e utilitários de data migrados para `baliza.utils` e reutilizados na
  configuração.

## Itens pendentes 🚧

1. **Cobertura de endpoints adicionais**
   - Expandir `config/pncp.yml` para suportar os demais recursos do PNCP.
   - Validar chaves primárias e cursores antes de habilitar cada recurso.

2. **Limpeza de legado**
   - Remover referências a Prefect e módulos descontinuados (ver
     `docs/cleanup-plan.md`).
   - Consolidar utilitários duplicados entre `extraction/` e `pipelines/`.

3. **Resiliência e estado avançado**
   - Implementar estratégia de resumibilidade descrita em
     `docs/extraction_resumability_plan.md` (estado centralizado e *gap
     detection* real).
   - Avaliar caching ou *rate limiting* quando adicionarmos endpoints mais
     sensíveis.

4. **Testes automatizados**
   - Restaurar testes end-to-end para validar a execução do pipeline com mocks
     controlados da API.
   - Adicionar testes unitários para funções críticas (_apply_incremental_...).

## Decisões técnicas registradas

- **Destino:** DuckDB local via `dlt.destinations.duckdb`, garantindo portabilidade
  e facilidade de análise posterior.
- **Incremental:** Uso de `cursor_path = dataAtualizacao` com `lookback` aplicado
  em tempo de execução para evitar perdas por atrasos de publicação.
- **Configuração:** Estratégia "configuration over code" — o comportamento do
  pipeline deve residir em YAML para facilitar auditoria e ajustes por analistas.

## Riscos e mitigação

| Risco | Impacto | Mitigação |
|-------|---------|-----------|
| Falta de testes automatizados | Regressões silenciosas | Priorizar recriação dos testes E2E antes de expandir endpoints |
| Estado incremental limitado ao DuckDB local | Dificuldade de executar em múltiplos ambientes | Parametrizar diretório de trabalho do pipeline e documentar como exportar os dados |
| Documentação desatualizada | Dificulta onboarding | Revisão contínua sempre que novos recursos forem ativados |

## Próxima revisão

Revisar este documento após a habilitação do segundo endpoint no pipeline ou, no
máximo, em 30 dias.

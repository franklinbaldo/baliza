# Estratégia de extração de endpoints do PNCP

Este guia descreve como o Baliza planeja ampliar a cobertura da API do PNCP a
partir da base já funcional para o endpoint de contratos. O objetivo é seguir um
processo incremental, garantindo qualidade e rastreabilidade a cada expansão.

## Situação atual

- **Endpoint ativo:** `GET /v1/contratos`
  - Paginador `page_number` com 500 itens por página.
  - Incremental baseado em `dataAtualizacao` com *lookback* configurável.
  - Escrita via `write_disposition = merge` em DuckDB.
- **Estado do código:** o pipeline declarativo em `config/pncp.yml` possui apenas
  este recurso habilitado.

## Princípios para novos endpoints

1. **Configuração antes de código** — sempre que possível, usar apenas ajustes no
   YAML declarativo.
2. **Uma chave primária por recurso** — definir `primary_key` claro para garantir
   merges consistentes.
3. **Cursor validado** — confirmar se o endpoint possui campo de atualização
   confiável antes de habilitar incremental.
4. **Testes obrigatórios** — cada novo endpoint deve incluir teste de integração
   com respostas mockadas.

## Roadmap proposto

| Fase | Endpoints | Objetivo |
|------|-----------|----------|
| 1 | `contratacoes_publicacao`, `contratos_atualizacao` | Cobrir eventos de publicação e atualizações de contratos |
| 2 | `atas`, `atas_atualizacao` | Expandir para atas de registro de preços |
| 3 | Demais endpoints públicos (pca, instrumentos de cobrança, etc.) | Completar espelho da API pública |

Cada fase deve seguir os passos:

1. **Mapeamento:** documentar parâmetros obrigatórios/opcionais e limites de
   paginação no YAML.
2. **Validação local:** executar `uv run baliza extract --config ...` com mocks ou
   janela de datas pequena para validar estrutura.
3. **Documentação:** atualizar este arquivo e o README com os novos dados
   produzidos.
4. **Monitoramento:** registrar métricas básicas (quantidade de linhas, datas
   mínimas/máximas) após a primeira execução.

## Considerações sobre desempenho

- A API do PNCP não possui limites explícitos de requisição, mas o aumento de
  endpoints pode multiplicar o número total de chamadas; revise a seção de
  deduplicação de requisições em `docs/request-deduplication-strategy.md`.
- Avaliar a necessidade de executar backfills em *batchs* menores para evitar
  timeouts.

## Próximos passos imediatos

1. Adicionar `contratacoes_publicacao` ao YAML com `page_size = 50` e cursor
   `dataPublicacao`.
2. Validar se `contratos_atualizacao` compartilha a mesma chave primária de
   `contratos` para permitir `merge` seguro.
3. Atualizar testes para garantir que múltiplos recursos sejam criados na mesma
   execução do pipeline.

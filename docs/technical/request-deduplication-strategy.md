# Estratégia de deduplicação de requisições

## Problema

Após a migração para dlt, passamos a contar com **deduplicação no nível de
dados** (via `write_disposition = merge`), mas ainda não possuímos
**deduplicação de requisições**. Quando o usuário executa o mesmo comando duas
vezes, o pipeline repete todas as chamadas HTTP, desperdiçando banda e tempo.

## Comportamento atual (sem deduplicação de requisições)

```bash
# Primeira execução: extração incremental cobrindo os últimos 7 dias
baliza extract --lookback-days 7
# -> Faz todas as requisições necessárias para o intervalo "hoje-7" .. "hoje"

# Segunda execução: mesmo comando
baliza extract --lookback-days 7
# -> Repete exatamente as mesmas requisições HTTP
# -> Registros duplicados são ignorados pelo DuckDB, mas o custo das requisições permanece
```

**Consequência:** o armazenamento continua limpo, porém o tempo total de execução
cresce linearmente com a quantidade de reruns.

## Achados da pesquisa

- O dlt não oferece cache ou rastreamento de URLs para fontes REST.
- O incremental reduz o volume gravado, mas não evita refazer páginas já
  consultadas.
- A API do PNCP não documenta limites de requisição, mas latências acumuladas
  podem tornar grandes janelas impraticáveis.

## Caminhos possíveis

### Opção 1 — Aceitar o comportamento atual (recomendado para o MVP)

- Manter o *lookback* curto (3 dias) para limitar redundância.
- Documentar a limitação no README.
- Monitorar a duração das execuções enquanto apenas o endpoint `contratos` está
  habilitado.

### Opção 2 — Cache em infraestrutura

Implementar cache HTTP fora do Baliza:

- **Reverse proxy (Nginx/HAProxy)** com `proxy_cache_key` baseado na URL.
- **Redis** armazenando respostas por poucas horas.

Vantagens: nenhuma alteração no código Python; útil quando múltiplos serviços
consumirem a mesma API.

### Opção 3 — Rastreamento de URLs no aplicativo

Persistir hashes das URLs requisitadas em uma tabela DuckDB ou arquivo de
estado. Antes de cada requisição, verificar se o hash foi processado
recentemente.

- Permite granularidade fina (por endpoint/página).
- Exige política de expiração para evitar crescimento indefinido.

### Opção 4 — Reaproveitar mecanismo legado

Migrar o antigo registro `raw.audit_log` que guardava requisições concluídas.
Exige portar o schema e as validações para o fluxo atual.

## Caminho recomendado

1. Manter o comportamento atual enquanto somente `contratos` estiver ativo.
2. Implementar o **StateManager/GapDetector** descrito em
   `docs/extraction_resumability_plan.md` antes de habilitar novos endpoints.
3. Reavaliar a necessidade de cache infraestrutural após medir o custo de
   backfills completos com múltiplos recursos.

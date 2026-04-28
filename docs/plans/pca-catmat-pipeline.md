# Plano: Pipeline PCA → CATMAT

**Status:** v7 — data_particao obrigatório, pca_itens normalizado, alavancas de stack  
**Escopo:** pipeline Python + Parquet novo + camada web  
**Estimativa:** 2–3 semanas (happy path após probe favorável: ~10 dias)

> ⚠️ **Pré-requisito:** este plano depende de `docs/plans/multi-table-pipeline.md` (PR #509).
> Os PRs A–D daquele plano devem ser mergeados antes dos PRs 3–4 deste.
> Sem o refactoring do `TableContract`, a implementação do PCA reproduz os cinco
> pontos de acoplamento hard-coded identificados na auto-análise de stack.

---

## Contexto e motivação

O Baliza tem como objetivo fazer o backup de todos os endpoints públicos do PNCP.
O PCA (Plano de Contratações Anuais) é um endpoint público documentado no OpenAPI
do PNCP — portanto está dentro do escopo de coleta independentemente do valor de produto.

O valor de produto adicional: o PCA expõe `codigoItem` (código CATMAT/CATSER) por item
planejado, transformando o `catmat.json` estático de "lookup de código" em "demanda
planejada consultável por código".

Os endpoints que usamos hoje (`/v1/contratacoes/publicacao`, `/v1/contratos`) retornam
apenas `objetoCompra` em texto livre — sem código de catálogo por item.

> **Aviso de produto obrigatório na UI:** PCA é demanda planejada, não contrato firmado.
> O Baliza deve deixar explícito: "Esses dados indicam intenção/planejamento informado
> ao PNCP, não contratação realizada."

---

## Schema real do endpoint PCA

Baseado no OpenAPI do repo (`docs/openapi/api-pncp-consulta.json`) e em
`src/baliza/models.py` — não em suposição:

### `PlanoContratacaoComItensDoUsuarioDTO` (pai, por PCA)

```
idPcaPncp               string  ← chave do PCA
anoPca                  integer
orgaoEntidadeCnpj       string
orgaoEntidadeRazaoSocial string
codigoUnidade           string
nomeUnidade             string
dataPublicacaoPNCP      string
dataAtualizacaoGlobalPCA string
itens[]                 → ver abaixo
```

**Nota importante:** não há `codigoIbge`, `ufSigla` nem `municipioNome` no DTO.
Dados geográficos exigem join externo com a tabela `unidades` — mas a chave da tabela
`unidades` é composta: `(codigo_unidade, cnpj_orgao)` (ver `UNIDADES_SCHEMA` e
`GROUP BY codigo_unidade, cnpj_orgao` em `daily_exporter.py:315-325`). Um join só por
`codigo_unidade` pode associar o `codigo_ibge` errado quando órgãos distintos
compartilham o mesmo código de unidade. O join correto é sempre pelos dois campos.

### `PlanoContratacaoItemDTO` (item)

```
numeroItem              integer  ← parte da chave primária (ver dedup)
codigoItem              string   ← CATMAT/CATSER — SEMPRE tratar como string
pdmCodigo               string
pdmDescricao            string
nomeClassificacaoCatalogo string
descricaoItem           string
quantidadeEstimada      number
valorUnitario           number
valorTotal              number
valorOrcamentoExercicio number
unidadeFornecimento     string
grupoContratacaoCodigo  string
grupoContratacaoNome    string
classificacaoSuperiorCodigo string
classificacaoSuperiorNome   string
classificacaoCatalogoId integer
categoriaItemPcaNome    string
dataInclusao            datetime
dataAtualizacao         datetime
dataDesejada            date
```

---

## Deduplicação — decisão necessária antes do Parquet

`/v1/pca/atualizacao` é um fluxo por data de atualização global. O mesmo PCA/item pode
reaparecer em coletas diferentes se for alterado. O plano precisa de uma regra explícita.

**Chave primária proposta:** `(idPcaPncp, numeroItem)`

**Estratégia de tabela:**

| Opção | Descrição | Prós | Contras |
|---|---|---|---|
| **current-state por ano PCA** | última versão de cada `(idPcaPncp, numeroItem)` por `anoPca` | simples de querir, menor volume | perde histórico de alterações |
| **event-log append-only** | toda ocorrência preservada | histórico completo | dedup na query; volume maior |

**Recomendação:** `current-state por ano PCA` para a primeira feature. Adicionar
campos de auditoria: `data_publicacao_pncp`, `data_atualizacao_global_pca`,
`data_inclusao`, `data_atualizacao`, `data_coleta`.

---

## PR 2 — Probe (obrigatório antes de qualquer implementação)

Uma página de um mês não valida a hipótese. A probe deve ser um script Python robusto
(`scripts/probe_pca.py`) que rode e produza um relatório Markdown/JSON commitado no PR,
cobrindo:

```
Janelas a cobrir:
  2025-01, 2025-06, 2025-12 (ou mês mais recente disponível)
  + uma janela com cnpj específico de órgão grande (ex: prefeitura SP)

Por janela, extrair:
  totalRegistros, totalPaginas
  total de itens explodidos
  % de itens com codigoItem preenchido
  % de match exato codigoItem → catmat.json (string, sem conversão)
  % de match normalizado (strip zeros à esquerda, etc.)
  exemplos de codigoItem sem match em catmat.json
  presença/nulidade dos campos críticos (idPcaPncp, numeroItem, anoPca)
  duplicatas por (idPcaPncp, numeroItem) dentro da janela
  tamanho médio por página
  estimativa extrapolada de Parquet por ano (row count × avg row size)
  verificar se codigoItem tem zeros à esquerda (ex: "07510" vs "7510")
```

**Regra:** `codigoItem` é sempre string — nunca converter para int, para não
destruir zeros à esquerda. Idem no `catmat.ts` frontend (`code: string`).

O resultado da probe define se o plano avança ou precisa ser reescrito.

**Árvore de decisão pós-probe:**

```
match rate ≥ 70%  → prosseguir com o plano como descrito
match rate 30–69% → prosseguir, mas UI mostra apenas itens com match confirmado;
                    campo "código não resolvido" visível para auditoria
match rate < 30%  → pivot: não usar codigoItem como link para catmat.json;
                    usar pdmDescricao/nomeClassificacaoCatalogo como texto de busca
                    (feature de demanda planejada ainda viável, sem resolução CATMAT)
cobertura < 20%   → reavaliação completa antes de qualquer implementação
```

---

## Arquitetura proposta (sujeita a resultado da probe)

### A. Coletor Python (`src/baliza/pca_extractor.py`)

Reutilizar padrões de retry/cache/validação do `extractor.py`, mas **não reutilizar
`fetch_page` diretamente** — o endpoint PCA usa `dataInicio`/`dataFim` (não
`dataInicial`/`dataFinal`) e o flattening é diferente (explode itens do pai para linhas).

Assim como `extractor.py` tem `_flatten_contrato()`, o `pca_extractor.py` precisa
de `_flatten_pca(pca: dict, item: dict) -> dict` que:
- converte todos os campos camelCase do DTO para snake_case
- explode o item aninhado no contexto do PCA pai
- garante `codigo_item` como `str` (nunca `int`)
- adiciona `data_coleta` (timestamp de coleta)

```python
async def fetch_pca_window(data_inicio: str, data_fim: str) -> list[PcaItemRow]:
    """Coleta e explode itens PCA de uma janela temporal via /pca/atualizacao."""
    # tamanhoPagina=500 (max do endpoint)
    # Para cada PlanoContratacaoComItensDoUsuarioDTO, explode itens em linhas planas
    # usando _flatten_pca(pca, item) → codigoItem sempre str, nunca int

def _flatten_pca(pca: dict, item: dict) -> dict:
    """Achata DTO camelCase para linha snake_case, codigoItem → str."""
    ...
```

Schema plano de saída (`PcaItemRow`) — campo a campo do OpenAPI, snake_case
(conversão camelCase → snake_case feita por `_flatten_pca`):
```
id_pca_pncp
ano_pca
cnpj_orgao
razao_social_orgao
codigo_unidade           ← join com unidades para obter codigo_ibge
nome_unidade
data_publicacao_pncp
data_atualizacao_global_pca
numero_item
codigo_item              ← string, nunca int
pdm_codigo
pdm_descricao
nome_classificacao_catalogo
descricao_item
quantidade_estimada
valor_unitario
valor_total
valor_orcamento_exercicio
unidade_fornecimento
unidade_requisitante
grupo_contratacao_codigo
grupo_contratacao_nome
classificacao_superior_codigo
classificacao_superior_nome
classificacao_catalogo_id
categoria_item_pca_nome
data_inclusao
data_atualizacao
data_desejada
data_coleta              ← timestamp da coleta (adicionado pelo extractor)
```

### B. Estratégia de Parquet (decisão pendente de benchmark)

**B1 — Parquet único por ano PCA** (proposta inicial):
```
data/pca/pca_2024.parquet
data/pca/pca_2025.parquet
```

**B2 — Tabela agregada pequena por código** (sugerida na revisão):
```
data/pca/pca_demanda_por_codigo_2025.parquet
  ano_pca, codigo_item, tipo_catalogo, descricao_catalogo,
  orgaos_count, unidades_count, quantidade_total, valor_total
```

**Decisão pendente:** benchmarkar no browser (`read_parquet(url_ia)` via DuckDB-WASM)
se há range requests / pushdown para filtro `WHERE codigo_item = ?`.
A documentação do DuckDB menciona pushdown Parquet com HTTP range requests, mas o
comportamento específico com duckdb-wasm + IA como servidor HTTP precisa ser medido
no nosso stack — não assuma.

Se não houver pushdown útil, B2 (tabela agregada pequena) sustenta o CatmatSearch
inline sem baixar arquivo grande; B1 fica para o caso de uso de detalhes por órgão.

**Alavanca de stack já disponível para B2 — `build-data.mjs`:**
`web/scripts/build-data.mjs` roda DuckDB Node.js em build time, lê arquivos `.qmd`
de `web/src/queries/` e escreve JSON estático em `public/data/`. Para a tabela
agregada B2, basta adicionar um `.qmd` com a query de agregação lendo o Parquet do IA
via `httpfs` — sem DuckDB-WASM no browser. O script já é chamado em `npm run build`
e `npm run dev`. Nota: o script atual tem um stub de manifesto hardcoded; será
necessário substituir pela leitura real do manifesto IA para ativar a produção.

### B.1 Exportador anual vs MonthlyExporter

O `MonthlyExporter` existente usa `data_particao` (DATE) como chave de partição e
`month_dir` como pasta de upload. O PCA exporta por `ano_pca` (int), não por mês —
portanto **não reutilizar `MonthlyExporter` diretamente**. Criar `PcaExporter` próprio
que:
- escreve `pca_{ano}.parquet` com chave de deduplicação `(id_pca_pncp, numero_item)`
- não tem sentinela `.fetched` por dia (a coleta é incremental por janela de datas)
- o campo `data_coleta` serve como auditoria, não como partition key
- define `PCA_SCHEMA_VERSION = "1.0.0"` (mesmo padrão de `SCHEMA_VERSION = "2.0.0"`
  em `daily_exporter.py`); bumpar quando colunas mudarem. **Atenção:** `builder.py`
  aplica `strptime(part, "%Y-%m")` — strings `"YYYY-12-31"` levantam `ValueError` e
  são silenciosamente ignoradas (linha 58–60). O auto-reprocessamento do builder
  **não cobre PCA**; `PcaExporter` precisa implementar sua própria checagem de
  `parquet_schema_version` ao decidir reexportar.
- sort order: `(codigo_item, cnpj_orgao)` + bloom filter em `codigo_item` para
  pushdown eficiente ao filtrar por código CATMAT no browser ou no build-time
- dedup com PK `(id_pca_pncp, numero_item)` — **atenção:** `BalizaEngine.upsert_rows()`
  (`engine.py:67`) aceita apenas `pk: str` (coluna única, linha 109:
  `t_existing[pk].isin(t_new[pk])`). Para usar a lógica existente, criar coluna
  sintética `pca_row_id = f"{id_pca_pncp}__{numero_item}"` no `_flatten_pca()` e
  passá-la como `pk`. Alternativa: estender `upsert_rows` para aceitar
  `pk: str | list[str]` — mas isso é mudança de contrato do helper existente.
- **Nulos na PK sintética:** `idPcaPncp` e `numeroItem` são nullable em `models.py`
  (linhas 242 e 28). Rows com qualquer parte nula colapsam para a mesma chave
  (ex: `"None__None"`) e `upsert_rows` pode sobrescrever/descartar dados. Requisito:
  `_flatten_pca()` deve **rejeitar** (quarentena, não silenciar) qualquer row onde
  `id_pca_pncp` seja `None`/vazio **ou** `numero_item` seja `None`/zero antes de
  construir `pca_row_id`.

### C. IA uploader

Adicionar tabela `pca_itens` ao manifest e upload via `ia_uploader.py`, mas **não é
plug-and-play**: `MANIFEST_FIELDNAMES` em `ia_uploader.py` é hardcoded para `contratos`.
A implementação requer:
- novo tipo de entrada no manifest com `table_name: "pca_itens"` — **crítico**:
  `ia-manifest.ts` valida `data_particao: z.string().min(1)` e ordena as linhas por
  `data_particao.localeCompare`. Para PCA (que não tem mês natural), usar
  `data_particao = f"{ano_pca}-12-31"` em cada linha de manifesto. Sem isso, o
  frontend nunca resolve arquivos PCA (`r.table_name === tableName` é exact match).
- `sort_key` e `bloom_filter_columns` específicos para PCA (sugestão: `codigo_item`)
- tratar múltiplos tipos de tabela em `_update_remote_manifest`

### D. Web layer

#### D.1 Schema TypeScript (`web/src/lib/archive/schema.ts`)

```typescript
export interface ArchivedPcaItem {
  id_pca_pncp: string;
  numero_item: number;
  ano_pca: number;
  cnpj_orgao: string;
  razao_social_orgao: string | null;
  codigo_unidade: string | null;
  nome_unidade: string | null;
  // sem codigo_ibge direto — join externo com unidades(codigo_unidade, cnpj_orgao)
  data_publicacao_pncp: string | null;
  data_atualizacao_global_pca: string | null;
  codigo_item: string;        // CATMAT/CATSER, sempre string
  pdm_codigo: string | null;
  pdm_descricao: string | null;
  nome_classificacao_catalogo: string | null;
  descricao_item: string | null;
  quantidade_estimada: number | null;
  valor_unitario: number | null;
  valor_total: number | null;
  valor_orcamento_exercicio: number | null;
  unidade_fornecimento: string | null;
  unidade_requisitante: string | null;
  grupo_contratacao_codigo: string | null;  // manter código E nome (não só nome)
  grupo_contratacao_nome: string | null;
  classificacao_superior_codigo: string | null;  // idem
  classificacao_superior_nome: string | null;
  classificacao_catalogo_id: number | null;
  categoria_item_pca_nome: string | null;
  data_inclusao: string | null;
  data_atualizacao: string | null;
  data_desejada: string | null;
  data_coleta: string;
}
```

**Nota sobre ARCHIVED_TABLES:** é uma lista fechada (closed allowlist) em
`web/src/lib/archive/schema.ts`. Adicionar `pca_itens` requer alteração em **dois**
arquivos: `schema.ts` (interface + entrada no array) e `parquetFallback.ts` (tabela
permitida no `switch` de validação). Não é só adicionar a interface.

**Nota sobre query keys:** `pcaItens` vai no objeto `QUERY_KEYS` em
`web/src/lib/queryKeys.ts`, não solto.

#### D.2 Página `/pca?codigo=7510` (Opção 2 — escopo inicial)

- Lista órgãos/unidades com aquele `codigo_item` no PCA do ano corrente
- Agrega: total de unidades, quantidade planejada, valor total estimado
- **Aviso obrigatório:** "Demanda planejada no PCA. Indica intenção, não contrato."
- BDD scenario `@green @pca-demand` em Journey 1 (fornecedor)

Integração com CatmatSearch (Opção 1) e ContractDetailView (Opção 3) ficam para depois
que o valor da Opção 2 for confirmado.

---

## Anos a coletar

Começar por **2025 e 2026** (anos com PCA atualizados e com data de atualizacao global
disponível a partir de 25/02/2025). Não assumir que o endpoint de atualizacao sirve bem
para backfill de 2023/2024 sem testar — validar na probe com janelas 2024-Q4/2025-Q1
para avaliar disponibilidade histórica.

---

## Estimativa de esforço

| Tarefa | Happy path | Estimativa realista |
|---|---|---|
| Probe script + relatório (PR 2) | 0.5d | 1d |
| `pca_extractor.py` + testes | 1d | 2d |
| `pca_exporter.py` + Parquet | 1d | 2d |
| IA uploader + manifest | 0.5d | 1d |
| Benchmark DuckDB-WASM | — | 0.5d |
| Schema TS + query helper | 0.5d | 1d |
| Página `/pca` + BDD | 2d | 3d |
| CI: refresh PCA mensal | 0.5d | 1d |
| **Total** | **~6d** | **~11.5d (2–3 semanas)** |

Se cobertura/matching da probe forem ruins: replanejar antes da web layer.

---

## Riscos

| Risco | Impacto | Mitigação |
|---|---|---|
| `codigoItem` não bate com `catmat.json` | Alto | Probe antes de tudo |
| Cobertura < 20% de `codigoItem` | Médio | Mostrar % cobertura na UI |
| Zeros à esquerda destruídos por parse int | Alto | Sempre string |
| DuckDB-WASM baixa arquivo inteiro | Médio | Benchmark; usar tabela agregada se necessário |
| `/pca/atualizacao` não cobre backfill 2023/2024 | Médio | Testar na probe |
| UI confunde PCA com contrato real | Alto (produto) | Aviso obrigatório na página |
| Volume > 200 MB/ano | Médio | Filtrar por ano, lazy-load, tabela agregada |

---

## Sequência de PRs

```
PR 1 (este) — plano revisado (este documento)
PR 2 — scripts/probe_pca.py + relatório de cobertura/matching
PR 3 — pca_extractor.py + pca_exporter.py + testes Python
         decisão de particionamento baseada em benchmark DuckDB-WASM
PR 4 — IA uploader + manifest + schema TS + query helper
PR 5 — /pca?codigo=... page + BDD @green @pca-demand
PR 6 — integração CatmatSearch (Opção 1) se valor confirmado pela Opção 2
```

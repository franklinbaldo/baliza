# Plano: Pipeline PCA → CATMAT

**Status:** v2 — incorpora revisão do PR #506  
**Escopo:** pipeline Python + Parquet novo + camada web  
**Estimativa:** 2–3 semanas (happy path após probe favorável: ~10 dias)

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
Dados geográficos exigem join externo com a tabela `unidades` (que tem `codigo_ibge`
via `codigo_unidade`). Qualquer UI de heatmap por UF depende desse join.

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

---

## Arquitetura proposta (sujeita a resultado da probe)

### A. Coletor Python (`src/baliza/pca_extractor.py`)

Reutilizar padrões de retry/cache/validação do `extractor.py`, mas **não reutilizar
`fetch_page` diretamente** — o endpoint PCA usa `dataInicio`/`dataFim` (não
`dataInicial`/`dataFinal`) e o flattening é diferente (explode itens do pai para linhas).

```python
async def fetch_pca_window(data_inicio: str, data_fim: str) -> list[PcaItemRow]:
    """Coleta e explode itens PCA de uma janela temporal via /pca/atualizacao."""
    # tamanhoPagina=500 (max do endpoint)
    # Para cada PlanoContratacaoComItensDoUsuarioDTO, explode itens em linhas planas
    # codigoItem → sempre str, nunca int
```

Schema plano de saída (`PcaItemRow`) — campo a campo do OpenAPI, snake_case:
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

### C. IA uploader

Adicionar tabela `pca` ao manifest e upload via `ia_uploader.py` (padrão existente).

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
  // sem codigo_ibge direto — join externo com unidades se necessário
  codigo_item: string;        // CATMAT/CATSER, sempre string
  pdm_codigo: string | null;
  pdm_descricao: string | null;
  nome_classificacao_catalogo: string | null;
  descricao_item: string | null;
  quantidade_estimada: number | null;
  valor_unitario: number | null;
  valor_total: number | null;
  unidade_fornecimento: string | null;
  grupo_contratacao_nome: string | null;
  classificacao_superior_nome: string | null;
  data_coleta: string;
}
```

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

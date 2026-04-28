# Plano: Pipeline PCA → CATMAT

**Status:** proposta para revisão  
**Escopo:** pipeline Python + Parquet novo + camada web  
**Estimativa:** 3–4 semanas (pipeline) + 1 semana (web)

---

## Contexto e motivação

O `catmat.json` já tem 13.280 entradas reais do gov.br (10.375 CATMAT + 2.905 CATSER).
A `CatmatSearch` já funciona como ferramenta de lookup em `/catmat`.

O problema: **nenhum contrato exibido no sistema tem o código CATMAT vinculado**.
Os endpoints do PNCP que usamos hoje (`/v1/contratacoes/publicacao`, `/v1/contratos`)
retornam apenas `objetoCompra` em texto livre — sem código de catálogo por item.

O único endpoint PNCP que expõe `codigoItem` (= código CATMAT/CATSER) é o
**Plano de Contratações Anuais (PCA)** — `/v1/pca/atualizacao`.

---

## Investigação: o que o PCA tem

### Endpoint disponível

```
GET /v1/pca/atualizacao
  dataInicio   (required) — ex: 2025-01-01
  dataFim      (required) — ex: 2025-01-31
  cnpj         (optional) — filtro por órgão
  codigoUnidade(optional)
  pagina       (required, min 1)
  tamanhoPagina(optional, 10–500)
```

Resposta: `PaginaRetornoPlanoContratacaoComItensDoUsuarioDTO`

```
data[]:
  orgaoEntidade.cnpj
  orgaoEntidade.razaoSocial
  unidadeOrgao.codigoUnidade
  unidadeOrgao.nomeUnidade
  unidadeOrgao.codigoIbge
  anoCompra  (ano do PCA)
  itens[]:
    codigoItem              ← CATMAT/CATSER code
    pdmCodigo               ← PDM (Padrão Descritivo de Material)
    pdmDescricao
    nomeClassificacaoCatalogo
    descricaoItem           ← descrição em texto livre
    quantidadeEstimada
    valorUnitario
    valorTotal
    valorOrcamentoExercicio
    unidadeFornecimento
    grupoContratacaoCodigo
    grupoContratacaoNome
    classificacaoSuperiorCodigo
    classificacaoSuperiorNome
```

### Incerteza a validar antes de implementar

Não temos acesso à rede no ambiente de dev. Precisamos confirmar empiricamente:

1. **`codigoItem` bate com `code` em `catmat.json`?**  
   A hipótese é que sim (ambos são o código CATMAT/CATSER oficial), mas o formato
   pode divergir (zero-padded? diferente número de dígitos?).

2. **Cobertura real**: qual % dos itens PCA têm `codigoItem` preenchido?  
   Muitos órgãos podem não informar o código.

3. **Volume**: quantos itens PCA existem por mês? Necessário para estimar o tamanho
   do Parquet e o custo de coleta.

**Ação imediata antes de qualquer implementação:**
```python
# scripts/probe_pca.py — rodar uma vez, jogar output aqui como comentário
GET /v1/pca/atualizacao?dataInicio=2025-01-01&dataFim=2025-01-31&pagina=1&tamanhoPagina=500
→ imprimir: totalRegistros, % codigoItem preenchido, amostra de codigoItem vs catmat.json
```

---

## Arquitetura proposta

### A. Coletor Python (`src/baliza/pca_extractor.py`)

Seguir o padrão de `extractor.py` (httpx + tenacity retry + structlog):

```python
async def fetch_pca_month(year: int, month: int) -> list[PcaItemRow]:
    """Coleta todos os itens PCA publicados/atualizados no mês via /pca/atualizacao."""
    data_inicio = f"{year}-{month:02d}-01"
    data_fim    = last_day_of_month(year, month)
    # página a página, tamanhoPagina=500
    # para cada PlanoContratacaoComItensDoUsuarioDTO, explode itens
    # retorna lista plana de PcaItemRow
```

Schema de saída plano (`PcaItemRow`):
```
ano_pca
cnpj_orgao
razao_social_orgao
codigo_unidade
nome_unidade
codigo_ibge
codigo_item       ← CATMAT/CATSER code
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
data_coleta       ← data de coleta (partition key)
```

### B. Exportador Parquet (`src/baliza/pca_exporter.py`)

Um Parquet por **ano PCA** (não por data de publicação):
```
data/pca/ano_pca=2024/pca_items.parquet
data/pca/ano_pca=2025/pca_items.parquet
```

Tamanho estimado: ~50–200 MB por ano (a confirmar na probe).

**Alternativa B2 — Parquet único por ano sem partição Hive:**
```
data/pca/pca_2025.parquet  (~50 MB gzipped)
```

Mais simples para o DuckDB-WASM carregar sem precisar descobrir partições.
Preferir B2 enquanto o volume for pequeno (< 100 MB).

### C. IA uploader

Adicionar tabela `pca` ao manifest e upload via `ia_uploader.py`.
Seguir padrão existente de `contratos`.

### D. Web layer

#### D.1 — Schema TypeScript (`web/src/lib/archive/schema.ts`)

```typescript
export interface ArchivedPcaItem {
  ano_pca: number;
  cnpj_orgao: string;
  razao_social_orgao: string | null;
  codigo_unidade: string | null;
  nome_unidade: string | null;
  codigo_ibge: string | null;
  codigo_item: string;          // CATMAT/CATSER code
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

// Adicionar 'pca_itens' ao ARCHIVED_TABLES
```

#### D.2 — Query key (`web/src/lib/queryKeys.ts`)

```typescript
pcaItens: (codigoItem: string) => ['pca-itens', codigoItem] as const,
```

#### D.3 — Integração no frontend

**Opção 1 — Inline no `CatmatSearch`:**  
Ao selecionar um resultado CATMAT, mostrar quantos órgãos planejam comprar aquele
item (agregado de `pca_itens WHERE codigo_item = $code`). Útil para fornecedor.

**Opção 2 — Página `/pca?codigo=7510`:**  
Nova página "Demanda planejada" que mostra todos os órgãos com esse item no PCA,
com valor estimado e quantidade. Heatmap por UF opcional.

**Opção 3 — Enriquecer `ContractDetailView`:**  
Se o contrato tem `numeroControlePncpCompra`, buscar o PCA associado e mostrar
quais itens foram planejados (com `codigoItem` resolvido para CATMAT).

Recomendação: **Opção 2 primeiro** (escopo isolado, BDD testável), depois integrar
nas demais views conforme valor comprovado.

---

## Particionamento e queries DuckDB-WASM

O DuckDB-WASM carrega o arquivo inteiro antes de executar queries — não tem
pushdown de predicados para arquivos remotos. Portanto:

- **Parquet único por ano** (B2) é melhor que partição Hive: 1 fetch vs N
- Comprimir com Zstd (DuckDB default): ~60–80% de compressão esperada
- Arquivos por ano PCA: estimativa pessimista 200 MB raw → ~50 MB Zstd
- Budget atual do bundle web: 320 KB JS — Parquet fica em IA, não no bundle

Para evitar recarregar o arquivo a cada query, usar o mesmo padrão de
`parquetFallback.ts`: fetch-once + cache em memória (via DuckDB WASM table).

---

## Estimativa de esforço

| Tarefa | Esforço | Dependências |
|---|---|---|
| **Probe script** (`scripts/probe_pca.py`) | 2h | — |
| **Validação codigoItem vs catmat.json** | 1h | probe |
| **`PcaItemRow` + `pca_extractor.py`** | 1d | probe |
| **`pca_exporter.py` + Parquet** | 1d | extractor |
| **IA uploader + manifest** | 0.5d | exporter |
| **Schema TS + `ArchivedPcaItem`** | 2h | — |
| **`queryPcaItems` helper** | 2h | schema |
| **`/pca` page + BDD scenario** | 2d | helper |
| **Integração `CatmatSearch`** | 1d | pca page |
| **CI: refresh PCA mensal** | 0.5d | extractor |

**Total estimado: ~10 dias de trabalho** (não incluindo retrabalhados dependentes
do resultado da probe).

---

## Riscos e trade-offs

| Risco | Impacto | Mitigação |
|---|---|---|
| `codigoItem` não bate com `catmat.json` | Alto — invalidaria toda a cadeia | Rodar probe antes de implementar |
| Cobertura baixa de `codigoItem` (< 20%) | Médio — feature pouco útil | Mostrar % de cobertura na UI |
| Volume > 200 MB/ano de Parquet | Médio — DuckDB-WASM lento | Filtrar por ano PCA, lazy-load |
| PCA endpoint instável / rate-limited | Baixo | Retry com backoff (já no padrão) |
| Join PCA → contrato não disponível | Alto — dificulta integração futura | Documentar limitação; Opção 2 não depende do join |

---

## Decisões necessárias antes de começar

1. **Rodar `probe_pca.py` primeiro?** (recomendado — valida a hipótese central)
2. **Escopo inicial do web layer**: Opção 1, 2 ou 3?
3. **Anos de PCA a coletar**: só 2025 ou histórico desde 2023?
4. **Parquet único por ano (B2) ou partição Hive (B)?**
5. **Prioridade relativa aos outros `@planned` de jornada** (`@alerts`, `@changelog`, `@api`)?

---

## Sequência de implementação sugerida

```
PR 1 (este) — plano (este documento)
PR 2 — scripts/probe_pca.py + resultado da probe como comentário no PR
PR 3 — pca_extractor.py + pca_exporter.py + testes unitários Python
PR 4 — IA uploader + manifest + schema TS + queryPcaItems
PR 5 — /pca page + BDD @green @catmat-demand
PR 6 — integração CatmatSearch (Opção 1) se valor confirmado
```

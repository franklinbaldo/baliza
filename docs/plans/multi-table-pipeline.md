# Plano: Ontologia de pipeline multi-recurso PNCP

**Status:** v3 — redesign conceitual: PNCPResource como abstração raiz  
**Escopo:** refatoração do pipeline Python + uploader + frontend para suportar múltiplos recursos PNCP sem misturar camadas  
**Estimativa:** 3–4 semanas (8 PRs incrementais)  
**Dependentes:** `docs/plans/pca-catmat-pipeline.md` e qualquer endpoint PNCP futuro (`/atas`, `/empenhos`, `/instrumentoscobranca`)

---

## Problema

O pipeline atual tem dois problemas relacionados:

**Problema 1 — acoplamento técnico:** cinco pontos hardcoded para `contratos` mensais:

| Ponto | Arquivo | Acoplamento |
|---|---|---|
| Schema-version reprocessing | `builder.py:58` | `strptime("%Y-%m")` — rejeita silenciosamente `data_particao` não-mensal |
| PK de dedup | `engine.py:72` | `pk: str` — apenas coluna única |
| Campos do manifest | `ia_uploader.py:81` | `MANIFEST_FIELDNAMES` hardcoded |
| Allowlist do frontend | `schema.ts` + `parquetFallback.ts` | duas edições manuais por tabela |
| Dados de build-time | `build-data.mjs` | stub de manifesto hardcoded |

**Problema 2 — ontologia errada:** `contratos` foi usado como modelo arquitetural porque hoje é o caso mais simples na camada canônica inicial. Mas isso não significa que o recurso `contratos` seja intrinsecamente 1:1:1: ele também pode gerar múltiplas tabelas derivadas e artefatos para consulta, frontend e agregações. Usar `contratos` como modelo criou semântica fraca para endpoints com estrutura pai/filho (PCA, ATAS, empenhos) que naturalmente produzem múltiplas tabelas e artefatos.

A solução não é só trocar nomes — é corrigir a ontologia do pipeline.

---

## Objetivo

Separar recurso PNCP, entidades de domínio, tabelas canônicas, tabelas derivadas, artefatos publicados e exposições frontend, permitindo que cada endpoint gere uma ou várias materializações sem contaminar a semântica das demais camadas.

---

## Hierarquia conceitual

```
PNCPResource          ← endpoint/recurso documentado no OpenAPI do PNCP
  ├── FetchSpec       ← como paginar e coletar
  ├── RawDatasetSpec  ← raw mirror (ZIP no IA)
  ├── EntitySpec[]    ← entidades lógicas do domínio extraídas do payload
  ├── CanonicalTableSpec[]  ← tabelas internas versionadas e deduplicáveis
  ├── DerivedTableSpec[]    ← índices, agregados e materializações analíticas
  ├── ArtifactSpec[]        ← arquivos publicados no IA ou consumidos pelo build
  └── FrontendExposureSpec[] ← como o frontend consome cada artefato
```

---

## Definição de cada camada

### `PNCPResource`

Representa um endpoint documentado no OpenAPI do PNCP. Exemplos:

```python
CONTRATOS = PNCPResource(resource_name="contratos", endpoint="/v1/contratos", ...)
PCA       = PNCPResource(resource_name="pca",       endpoint="/v1/pca/atualizacao", ...)
ATAS      = PNCPResource(resource_name="atas",      endpoint="/v1/atas", ...)
EMPENHOS  = PNCPResource(resource_name="empenhos",  endpoint="/v1/empenhos", ...)
```

Responsabilidades:
- descrever o endpoint e a estratégia de fetch/paginação (`FetchSpec`)
- declarar a estratégia de raw mirror (`RawDatasetSpec`)
- agrupar as demais camadas (`EntitySpec[]`, `CanonicalTableSpec[]`, `DerivedTableSpec[]`, `ArtifactSpec[]`, `FrontendExposureSpec[]`)

### `FetchSpec`

Descreve como o recurso é paginado e coletado. Separado do `PNCPResource` para que a lógica de retry/cache/validação do extractor possa ser compartilhada entre recursos sem acoplamento ao domínio.

```python
@dataclass
class FetchSpec:
    endpoint: str               # "/v1/contratos", "/v1/pca/atualizacao"
    pagination_param: str       # "pagina" (contratos) — difere entre endpoints
    page_size_param: str        # "tamanhoPagina"
    max_page_size: int          # 500 para PCA, 50 para publicação
    date_param_start: str       # "dataInicial" (contratos) vs "dataInicio" (PCA) — atenção
    date_param_end: str         # "dataFinal" vs "dataFim"
    response_data_key: str      # chave do array na resposta JSON ("data", "items", etc.)
```

**Nota:** `/v1/contratos` usa `dataInicial`/`dataFinal`; `/v1/pca/atualizacao` usa `dataInicio`/`dataFim`. Não assumir paridade de parâmetros entre endpoints.

### `RawDatasetSpec`

Descreve o raw mirror: como o dado bruto é espelhado antes de qualquer transformação.

```python
@dataclass
class RawDatasetSpec:
    ia_item_id: str             # item no IA onde o ZIP bruto vai
    filename_fn: Callable       # partição → nome do arquivo ZIP
    partition_strategy: str     # "monthly" (contratos) | "annual" (PCA) | "weekly"
    retention_policy: str       # "all" | "last_n=12"
```

A estratégia de partição do raw mirror **não precisa coincidir** com a partição dos artefatos publicados. Ex.: coletar PCA diariamente por janela de atualização e publicar Parquet anual consolidado.

### `EntitySpec`

Entidade lógica do domínio extraída do payload do recurso. Não é necessariamente uma tabela física — é a unidade conceitual antes da decisão de armazenamento.

Exemplos para `contratos`:
- `contrato`, `orgao`, `unidade_compra`, `fornecedor`

Exemplos para `pca`:
- `pca_plano`, `pca_item`, `catmat_match`

Exemplos para `atas`:
- `ata`, `ata_item`, `ata_fornecedor`

### `CanonicalTableSpec`

Tabela interna versionada, com PK explícita e regra de deduplicação. É a fonte de verdade para o dado bruto limpo.

```python
@dataclass
class CanonicalTableSpec:
    table_name: str                # "contratos_canonical", "pca_itens_canonical"
    schema_version: str            # "2.0.0" — bumpar quando colunas mudarem
    pk: str | list[str]            # PK simples ou composta
    flatten_fn: Callable           # row dict → linha flat snake_case
    dedup_strategy: str            # "current_state" | "append_only"
    source_entity: str             # EntitySpec.name que origina esta tabela
    sort_columns: list[str]
    bloom_filter_columns: list[str]
```

Exemplos:
```
contratos_canonical      pk: "numero_controle_pncp"
pca_planos_canonical     pk: "id_pca_pncp"
pca_itens_canonical      pk: ["id_pca_pncp", "numero_item"]
atas_canonical           pk: "numero_controle_pncp"
ata_itens_canonical      pk: ["numero_controle_pncp", "numero_item"]
```

**Nota sobre `schema_version`:** `CanonicalTableSpec` define sua própria versão. A lógica de reprocessamento por schema bump deve ser delegada ao `PNCPResource` — não assume-se que `builder.py` lida com isso automaticamente (PR D refatora o builder para despachar por contrato em vez de `strptime` hardcoded; PR E formaliza a versão no manifest via `ArtifactSpec`).

### `DerivedTableSpec`

Tabela derivada a partir de uma ou mais `CanonicalTableSpec`. Usada para consulta, performance, navegação ou análise. Não deve ser confundida com tabela canônica.

```python
@dataclass
class DerivedTableSpec:
    table_name: str
    source_tables: list[str]      # tabelas canônicas de origem
    query: str | Callable         # SQL ou callable que produz a tabela
    refresh_strategy: str         # "on_canonical_update" | "scheduled"
```

Exemplos:
```
contratos_by_municipio    ← agregado de contratos_canonical por ibge
contratos_search_index    ← índice full-text sobre objeto_contratacao
pca_itens_catmat_enriched ← pca_itens_canonical + join catmat.json
pca_catmat_summary        ← agregado por codigo_item e ano_pca
pca_itens_by_orgao        ← agregado por cnpj_orgao
```

### `ArtifactSpec`

Arquivo publicado no Internet Archive ou consumido pelo build/frontend. **Tabela interna não é igual a artefato publicado.** Um artefato pode ser produzido por query sobre uma ou várias tabelas canônicas ou derivadas.

```python
@dataclass
class ArtifactSpec:
    artifact_name: str             # "contratos_monthly_canonical"
    source_tables: list[str]       # tabelas de origem (canônicas ou derivadas)
    partition_fn: Callable         # row → data_particao string
    output_format: str             # "parquet" | "json"
    ia_item_id: str | None         # None = não publicado no IA (só local/build)
    filename_fn: Callable          # partição → nome do arquivo
    sort_columns: list[str]
    bloom_filter_columns: list[str]
```

Exemplos para `contratos`:
```
contratos_monthly_canonical   ← Parquet mensal, publicado no IA
contratos_yearly_uf           ← Parquet anual particionado por UF
contratos_by_orgao            ← JSON agregado, consumido pelo build
contratos_recent_sample       ← Parquet dos últimos 30 dias, dev-only
```

Exemplos para `pca`:
```
pca_itens_yearly              ← Parquet anual, publicado no IA
pca_catmat_summary            ← JSON agregado por codigo_item, consumido pelo build
pca_city_dashboard            ← JSON por ibge, consumido pelo build
```

O mesmo recurso PNCP pode publicar múltiplos Parquets otimizados para consultas diferentes. O plano não precisa implementar todos na primeira versão, mas a estrutura deve suportá-los sem nova refatoração conceitual.

### `FrontendExposureSpec`

Descreve como o frontend consome um artefato publicado. Não deve ser confundida com tabela canônica nem com recurso PNCP.

```python
@dataclass
class FrontendExposureSpec:
    archived_table_name: str      # nome em ARCHIVED_TABLES (schema.ts)
    artifact_name: str            # ArtifactSpec que origina este artefato
    loader: str                   # "parquetFallback" | "buildData" | "duckdbWasm"
    fallback_policy: str          # "error" | "empty" | "stale"
    file_type: str                # valor de file_type no manifest ("" ou "monthly_canonical", etc.)
```

**Nota sobre `file_type`:** `isCanonicalRow()` em `ia-manifest.ts` aceita apenas `!r.file_type` ou `r.file_type === 'monthly_canonical'`. Novos valores de `file_type` são silenciosamente ignorados. `FrontendExposureSpec` deve declarar o valor de `file_type` que a implementação deve usar, e PR F deve estender `isCanonicalRow` se necessário.

---

## `contratos` como caso simples — não como modelo ontológico

`contratos` é simples apenas na extração canônica:
- 1 `PNCPResource` → 1 `CanonicalTableSpec` principal

Mas `contratos` pode e provavelmente deve ser composto na publicação:

```
PNCPResource: contratos
  EntitySpec[]
    - contrato, orgao, unidade_compra, fornecedor

  CanonicalTableSpec[]
    - contratos_canonical

  DerivedTableSpec[]
    - contratos_by_municipio
    - contratos_by_orgao
    - contratos_search_index

  ArtifactSpec[]
    - contratos_monthly_canonical  ← o que existe hoje
    - contratos_yearly_uf          ← próxima evolução natural
    - contratos_by_orgao           ← JSON para build-time
    - contratos_by_municipio       ← JSON para build-time
```

`contratos` não é tomado como modelo simples absoluto — é o ponto de partida histórico, não a semântica ideal.

---

## PCA como caso composto

```
PNCPResource: pca
  EntitySpec[]
    - pca_plano, pca_item, catmat_match

  CanonicalTableSpec[]
    - pca_planos_canonical     pk: "id_pca_pncp"
    - pca_itens_canonical      pk: ["id_pca_pncp", "numero_item"]

  DerivedTableSpec[]
    - pca_itens_catmat_enriched   ← join com catmat.json (codigo_item → descricao)
    - pca_catmat_summary          ← agregado por codigo_item, ano_pca
    - pca_itens_by_orgao          ← agregado por cnpj_orgao, ano_pca

  ArtifactSpec[]
    - pca_itens_yearly            ← Parquet anual publicado no IA
    - pca_catmat_summary          ← JSON build-time (sem DuckDB-WASM no browser)
    - pca_city_dashboard          ← JSON por ibge para build-time

  FrontendExposureSpec[]
    - "pca_itens" via parquetFallback (lê pca_itens_yearly do IA)
    - "pca_catmat_summary" via buildData (JSON pré-gerado)
```

---

## Identidade de linha no manifest

O manifest é compartilhado entre múltiplos recursos e artefatos. A identidade de cada linha deve ser explícita para evitar colisões:

```
manifest row identity =
  (resource_name, artifact_name, data_particao, file_type, uf_sigla?)
```

- `resource_name`: "contratos", "pca", "atas" — identifica o recurso PNCP
- `artifact_name`: "contratos_monthly_canonical", "pca_itens_yearly" — identifica o artefato
- `data_particao`: "YYYY-MM" para mensais, "YYYY-12-31" para anuais — obrigatório, validado
- `file_type`: valor fixo por `ArtifactSpec`, não inventado no momento do upload
- `uf_sigla`: opcional, para artefatos particionados por UF

**Sobre `manifest_extra_fields` e CSV:** o manifest é um CSV compartilhado. Campos extras específicos por recurso podem tornar o CSV inconsistente (colunas ausentes em linhas de outros recursos) ou causar perda silenciosa de metadados. Preferir:
- campos globais estáveis em `BASE_MANIFEST_FIELDS`, ou
- coluna `metadata_json` para extras específicos de cada recurso.

Evitar adicionar colunas ad-hoc por recurso/tabela sem atualizar `BASE_MANIFEST_FIELDS` globalmente.

`BASE_MANIFEST_FIELDS` derivado de `MANIFEST_FIELDNAMES` atual (`ia_uploader.py:86–108`):

```python
BASE_MANIFEST_FIELDS = [
    "data_particao", "resource_name", "artifact_name",
    "table_name",    # mantido por compatibilidade com manifest existente
    "row_count", "quarantine_count",
    "ia_item_id", "raw_zip_url", "parquet_url", "quarantine_url",
    "uploaded_at", "file_type", "uf_sigla", "sort_key", "row_group_size",
    "bloom_filter_columns", "sha256", "file_size_bytes",
    "mirror_uploaded_at", "raw_zip_sha256", "raw_zip_size_bytes",
    "parquet_uploaded_at", "parquet_schema_version",
]
```

---

## Mudanças técnicas necessárias

### `engine.py` — PK composta

```python
def upsert_rows(
    self,
    data: list[dict],
    table_name: str,
    schema: str = "main",
    pk: str | list[str] = "numeroControlePNCP",
) -> int:
    # Se pk é lista, usar anti_join com predicados compostos:
    # t_result = t_existing.anti_join(t_new, predicates=[t_existing[k] == t_new[k] for k in pk])
    # NÃO usar coluna sintética no Parquet — manter schema limpo
```

Alternativa segura ao `isin` multi-coluna: `anti_join` com predicados compostos via Ibis. Testar isoladamente antes de integrar (PR A).

### `build-data.mjs` — manifest real

O manifest é um CSV leve (`manifest.csv`). Não use `INSTALL httpfs` para buscá-lo:
`INSTALL` baixa o binário da extensão do registry do DuckDB e falha em CI offline/air-gapped
antes do fallback poder rodar. Para o CSV do manifest, prefira `fetch()` Node.js:

```javascript
import fs from 'fs';
import path from 'path';
import os from 'os';

// manifest.csv: fetch via Node.js — sem httpfs, sem INSTALL
const IA_MANIFEST_CSV_URL = process.env.IA_MANIFEST_CSV_URL
  ?? 'https://archive.org/download/baliza-pncp-manifest/manifest.csv';

const LOCAL_FIXTURE = process.env.BALIZA_MANIFEST_FIXTURE; // set in CI/test

async function loadManifest(db) {
  const csvPath = LOCAL_FIXTURE ?? IA_MANIFEST_CSV_URL;
  let filePath = csvPath;
  if (csvPath.startsWith('http')) {
    const resp = await fetch(csvPath);
    if (!resp.ok) throw new Error(`manifest fetch failed: ${resp.status} ${resp.url}`);
    const csv = await resp.text();
    filePath = path.join(os.tmpdir(), 'baliza-manifest.csv');
    fs.writeFileSync(filePath, csv, 'utf-8');
  }
  // db.run é callback-based — promisificar para garantir que a tabela
  // exista antes de qualquer query .qmd ser executada
  await new Promise((resolve, reject) =>
    db.run(
      `CREATE TABLE manifest AS SELECT * FROM read_csv_auto('${filePath}', header=true)`,
      (err) => (err ? reject(err) : resolve()),
    ),
  );
}
```

`httpfs` continua necessário apenas para leitura de **Parquet remotos** (artefatos do IA) nas
queries `.qmd`. Nesse caso, pre-provisionar a extensão no ambiente de build (`duckdb -c "INSTALL httpfs"`)
em vez de fazer INSTALL dentro do script de cada query — assim o script usa só `LOAD httpfs`
(que não acessa a rede se a extensão já estiver instalada localmente).

```javascript
// .qmd queries que leem Parquet do IA:
// LOAD httpfs;  ← sem INSTALL — extensão deve estar pré-instalada no ambiente
// SELECT * FROM read_parquet('https://archive.org/...')
```

A separação entre "buscar manifest" (Node.js fetch) e "ler Parquet" (DuckDB httpfs pré-instalado)
torna o fallback de CI alcançável sem depender de conectividade de extensão.

### Responsabilidade de `data_particao`

A partição pertence a pelo menos três camadas diferentes — não misturá-las:

```
RawDatasetSpec.partition_strategy  → como o bruto é espelhado (mensal, anual, semanal)
CanonicalTableSpec                 → pode conter coluna data_particao como dado persistido,
                                     mas NÃO define a estratégia de particionamento
ArtifactSpec.partition_fn          → como o artefato publicado é particionado (mensal, anual, por UF)
```

`CanonicalTableSpec` **não define** `data_particao_fn`. A tabela canônica pode incluir a coluna como campo persistido (ex.: `contratos_canonical.data_particao = "2025-01"`), mas a estratégia de particionamento do raw mirror e dos artefatos publicados fica em `RawDatasetSpec` e `ArtifactSpec` respectivamente.

Exemplo de `contratos_canonical` — sem partição na spec da tabela:

```python
# _flatten_contrato já existe em src/baliza/extractor.py — PR C importa
# sem reimplementar; a coluna data_particao já está no output de _flatten_contrato.
contratos_canonical = CanonicalTableSpec(
    table_name="contratos_canonical",
    schema_version="2.0.0",
    pk="numero_controle_pncp",
    flatten_fn=_flatten_contrato,  # from src/baliza/extractor.py
    dedup_strategy="current_state",
    source_entity="contrato",
    sort_columns=["cnpj_orgao", "data_publicacao_pncp"],
    bloom_filter_columns=["cnpj_orgao"],
    # SEM partition_fn — particionamento é responsabilidade de ArtifactSpec
)
```

---

## Sequência de PRs

```
PR 0 — Testes de caracterização (obrigatório antes de qualquer refatoração)
  - Travar comportamento atual de contratos antes de mudar qualquer coisa
  - Testes: upsert simples, manifest.csv round-trip, build-data.mjs com fixture local
  - Testes: export/upload atual de contratos (golden output)
  - Objetivo: se um PR A–G quebrar algo, PR 0 detecta

PR A — PK composta em BalizaEngine
  - upsert_rows(pk: str | list[str])
  - usar anti_join/NOT EXISTS, não coluna sintética
  - testes: dedup com PK simples (não regride), dedup com PK composta

PR B — build-data.mjs lendo manifest real
  - substituir stub fake por read_csv_auto
  - fallback para fixture/local em CI sem acesso ao IA
  - smoke test: build não falha com manifesto vazio

PR C — introduzir PNCPResource mínimo
  - declarar CONTRATOS como PNCPResource com CanonicalTableSpec único
  - preservar comportamento atual (zero regressão)
  - não tentar generalizar uploader/builder ainda

PR D — introduzir CanonicalTableSpec e DerivedTableSpec
  - separar tabela canônica de tabela derivada explicitamente
  - refatorar builder.py para despachar por CanonicalTableSpec em vez de strptime hardcoded

PR E — introduzir ArtifactSpec
  - mover filename, IA item, parquet_url, file_type, sort_key, bloom_filter_columns para ArtifactSpec
  - formalizar identidade de linha do manifest
  - refatorar ia_uploader.py para usar ArtifactSpec

PR F — frontend registry / FrontendExposureSpec
  - reduzir drift entre schema.ts e parquetFallback.ts
  - extender isCanonicalRow() para aceitar novos file_type declarados por FrontendExposureSpec
  - se não for possível agora: ao menos teste de coerência Vitest

PR G — provar arquitetura com caso composto (PCA ou ATAS)
  - demonstrar 1 recurso → múltiplas entidades, tabelas canônicas e artefatos
  - PCA: pca_planos_canonical + pca_itens_canonical + pca_catmat_summary (JSON build-time)
  - critério: adicionar PCA não exige nenhuma edição cirúrgica nos PRs A–F
```

---

## O que NÃO muda

- Formato dos Parquet de `contratos` existentes (nenhuma migração de dados)
- Interface pública de `BalizaEngine` (assinatura de `upsert_rows` muda, mas é backward-compatible com `pk` default)
- Workflows de CI existentes

---

## Riscos

| Risco | Mitigação |
|---|---|
| Ibis `anti_join` composto com bug | Teste isolado em PR A antes de integrar |
| `build-data.mjs`: fetch do manifest.csv falha em CI sem acesso à rede | `BALIZA_MANIFEST_FIXTURE` aponta para fixture local em CI; `httpfs` reservado para Parquet remotos, não para o manifest |
| `builder.py` refactor quebra reprocessamento de contratos | PR 0 trava o comportamento atual; PR D deve passar esses testes |
| manifest CSV com colunas inconsistentes entre recursos | Definir `BASE_MANIFEST_FIELDS` global; não adicionar por recurso ad-hoc |
| `isCanonicalRow` silencia novos `file_type` | PR F obrigatoriamente estende a função antes do PR G |

---

## Critério de sucesso

O plano estará implementado corretamente quando:

1. `PNCPResource` é o endpoint/recurso de origem — não a tabela.
2. `EntitySpec` é a entidade lógica do domínio.
3. `CanonicalTableSpec` é a tabela versionada e deduplicável.
4. `DerivedTableSpec` é índice/agregado/materialização.
5. `ArtifactSpec` é o arquivo publicado no IA ou consumido pelo build.
6. `FrontendExposureSpec` é a exposição no app.
7. `contratos` funciona como antes — e pode evoluir para múltiplos artefatos sem refatoração.
8. PCA e ATAS são modelados como recursos compostos sem gambiarras.
9. A refatoração foi incremental e cada PR passou todos os testes do PR 0.
10. Este PR foi apenas documentação/plano — o código começa no PR 0.

# Plano: Pipeline multi-recurso PNCP

**Status:** consolidado em 2026-05-06 — substitui (e arquiva) os planos anteriores
`multi-table-pipeline.md`, `pca-catmat-pipeline.md` e `resource-atlas-roadmap.md`.

> Estratégia (a "porquê") vive em [`VISION.md`](../../VISION.md).
> Este documento descreve o **estado atual do pipeline** e o **roadmap
> executável** dos próximos recursos. Sempre que ele divergir do código,
> o código é a fonte de verdade — atualize este arquivo no mesmo PR.

---

## 1. Estado atual (o que já está em `main`)

A refatoração multi-recurso definida em `multi-table-pipeline.md` foi
executada em PRs incrementais entre #538 e #557. O pipeline hoje é:

```
PNCPResource (resources/__init__.py:RESOURCES)
  ├── FetchSpec        ← endpoint, paginação, payload key
  ├── RawDatasetSpec   ← raw mirror para baliza-pncp-raw
  ├── EntitySpec[]     ← entidades lógicas
  ├── CanonicalTableSpec[] ← tabelas com PK + sort + bloom + ORDER BY override
  └── data_start, entity_model, partition_by_uf
```

Recursos registrados (active):

| Resource    | endpoint              | entity_model        | partition_by_uf | data_start  |
|-------------|-----------------------|---------------------|-----------------|-------------|
| `contratos` | `/v1/contratos`       | `RecuperarContratoDTO` | True         | 2021-09-06  |
| `atas`      | `/v1/atas`            | `RecuperarAtaDTO`   | False           | 2021-09-06  |

Recursos declarados como `PLANNED_RESOURCES` (forma fixada, ainda fora do
pipeline ativo): `publicacoes`, `pca`. Promoção é uma linha em
`RESOURCES` + os TODOs nos seus módulos.

### CLIs e workflows

- `baliza sync|mirror|build|consolidate|verify|doctor|orphans` aceitam
  `--resource` (default: `contratos`); `_validate_resource` confere o
  registro antes de qualquer I/O.
- `pncp-sync.yml`, `pncp-backfill.yml`, `pncp-doctor.yml` ganharam o
  input `resource` (`workflow_dispatch`, default contratos). O cron
  fica em `contratos` até promoção explícita.
- Hygienic ops adicionadas fora dos planos antigos: `baliza doctor`,
  `baliza orphans` (com `--apply` destrutivo gateado), produção real
  de `web/public/data/sync_stats.json`, dedupe de issues
  `ci-failure`.

### Layering ainda não formalizado

Os PRs do plano antigo PR E (`ArtifactSpec`) e PR F
(`FrontendExposureSpec`) **não** foram tipados como dataclasses. O
comportamento equivalente foi distribuído em código:

- **Artefato canônico mensal** está embutido em
  `IAUploader.upload_parquet` + `_update_remote_manifest`
  (deriva nome de `canonical_tables[0].table_name`).
- **Artefato monthly_uf** está embutido em
  `IAConsolidator._build_per_uf_shards`, gateado pelo
  `partition_by_uf`.
- **Artefato annual_canonical** sai de
  `IAConsolidator.consolidate_year` com `_consolidated_file_name`.
- **Frontend exposures** são geradas por
  `scripts/build_frontend_config.py` em
  `web/src/lib/generated/frontend_exposures.ts`. A função
  `isCanonicalRow()` em `web/src/lib/ia-manifest.ts` ainda aceita
  apenas `'' | 'monthly_canonical'`. Adicionar um novo `file_type` exige
  edição manual do TS — ver §3 abaixo.

---

## 2. Camadas conceituais (referência canônica)

A linguagem abaixo é o vocabulário do projeto. Cada termo tem (ou terá)
uma dataclass em `src/baliza/resources/specs.py`. Os ✓ são tipos que já
existem; os ☐ são o débito a fechar.

| Camada                     | Status | Onde |
|----------------------------|--------|------|
| `PNCPResource`             | ✓ | `specs.py` |
| `FetchSpec`                | ✓ | idem |
| `RawDatasetSpec`           | ✓ | idem |
| `EntitySpec`               | ✓ | idem |
| `CanonicalTableSpec`       | ✓ | idem (com `partition_by_uf`, `order_by_sql`) |
| `DerivedTableSpec`         | ☐ | papel preenchido implicitamente pelo consolidador |
| `ArtifactSpec`             | ☐ | embutido em uploader/consolidator (ver §1) |
| `FrontendExposureSpec`     | ☐ | gerado em TS, sem tipo Python |

Definições em prosa (mantidas do `multi-table-pipeline.md`):

- **`PNCPResource`**: um endpoint documentado no OpenAPI do PNCP.
- **`EntitySpec`**: entidade lógica do payload (contrato, orgao,
  fornecedor, ata, ata_item, pca_plano, pca_item…). Não é
  necessariamente uma tabela física.
- **`CanonicalTableSpec`**: tabela versionada e deduplicável; fonte de
  verdade para o dado bruto limpo.
- **`DerivedTableSpec`**: agregação ou índice sobre canônicas.
- **`ArtifactSpec`**: arquivo publicado no IA ou consumido pelo build
  (Parquet mensal, Parquet anual, shard por UF, JSON dim cumulativo).
- **`FrontendExposureSpec`**: como o frontend lê um artefato (loader +
  fallback + `file_type` aceito por `isCanonicalRow`).

---

## 3. Débito (drift) a fechar antes de promover novos recursos

Em ordem de bloqueio:

### Drift-0 — Unificar registry de recursos (matar `pncp_resources.py`) ✓

**Estado atual (resolvido):** existiam dois registros paralelos:
`src/baliza/pncp_resources.py` (com `CONTRATOS` apenas) alimentava
`scripts/build_frontend_config.py` enquanto `src/baliza/resources/`
(com `CONTRATOS` + `ATAS`) alimentava o pipeline. Resultado: o site
não listava atas mesmo com o pipeline ingerindo.

**Resolução:**

- `FrontendExposureSpec` movido para `src/baliza/resources/specs.py`
  e populado em `contratos.py` / `atas.py`.
- `scripts/build_frontend_config.py` agora itera
  `RESOURCES.values()` e emite uma entrada por
  `frontend_exposures`.
- `src/baliza/pncp_resources.py` removido. Único registro = um único
  diretório.
- Pinado em `web/features/pncp-resource-atlas.feature` (cenário
  `@drift-0`) e em `tests/unit/test_build_frontend_config.py`.

### Drift-A — `BalizaEngine.upsert_rows` aceita PK composta ✓

**Estado (resolvido):** `engine.py:upsert_rows` agora aceita
`pk: str | list[str]`. PK string mantém o caminho rápido com
`isin`; PK lista usa `anti_join` em todas as colunas da chave.
Pinado em
`tests/characterization/test_pipeline_contratos.py::test_engine_upsert_composite_pk`
(prova explicitamente que dedup composto não derruba linhas que
colidiriam num anti-join de coluna única).

### Drift-B — `ArtifactSpec` formalizado ✓

**Estado (resolvido):** o tipo `ArtifactSpec` está em `specs.py` e
`PNCPResource.artifacts` está populada para contratos
(monthly_canonical + monthly_uf + annual_canonical) e atas
(monthly_canonical + annual_canonical — sem UF). Os runtime publish
paths consomem `src/baliza/artifacts.py`:

- `get_artifact(resource, file_type)` / `require_artifact(...)`
  para acesso direto.
- `resolve_export_params(resource, file_type)` que faz
  `artifact.<field> > CanonicalTableSpec.<field> > derivado` para
  `sort_columns`, `bloom_filter_columns` e `order_by_sql`.

`MonthlyExporter.export_month` consulta `resolve_export_params`,
`IAUploader._update_remote_manifest` lê `file_type` do
`ArtifactSpec` em vez de literal, e `IAConsolidator._resource_has_uf_shards`
gateia em `get_artifact(resource, "monthly_uf")` (mantendo
`partition_by_uf` como fallback enquanto os dois campos coexistem;
o teste de cruzamento já garante que não divergem).

Pinado em `tests/unit/test_artifacts_lookup.py` (6 testes:
matched/missing/required + chain de fallback + override por
artifact). Para contratos/atas o resultado é byte-identical
(toda a suite character/integration passa sem mudança).

### Drift-C — `FrontendExposureSpec` + `isCanonicalRow` extensível ✓

**Estado (resolvido):** `FrontendExposureSpec` ganhou um campo
`canonical_file_types: tuple[str, ...]` (default `("",
"monthly_canonical")`). O gerador une todos os `canonical_file_types`
declarados nas exposições e emite `CANONICAL_FILE_TYPES` no
`frontend_exposures.ts`. `isCanonicalRow()` consulta esse Set
(em vez do literal `'' | 'monthly_canonical'` hardcoded), portanto
introduzir um novo `file_type` (ex.: `annual_canonical` quando PCA
chegar) passa a ser uma linha no resource Python — sem editar o TS.

### Drift-D — Estratégia de partição não-mensal ✓

**Estado (resolvido):** `src/baliza/partitioning.py` carrega o
contrato completo:

- `iter_partitions(resource, start, end)` — itera períodos.
- `partition_label(resource, anchor)` — rótulo `data_particao`.
- `parse_partition_label(resource, label)` — inverso, devolve
  `None` se a forma não bate com `partition_strategy`.
- `partition_for(resource, anchor)` — `PartitionPeriod` que contém
  `anchor`, conveniente para os workers per-partition.
- `current_partition_start` / `previous_partition_start` /
  `clamp_to_data_start` — substituem o math monthly hardcoded em
  mirror/builder.

`_pending_mirror_months` (`mirror.py`) e `_pending_build_months`
(`builder.py`) consomem essas APIs em vez de
`strftime("%Y-%m")` / `relativedelta`. `mirror_month` e
`build_month` derivam o rótulo via `partition_for`, então o mesmo
worker serve mensal e anual sem branch.

Para contratos/atas o resultado é byte-identical (pinado pelos
testes characterization existentes). Para anuais (PCA quando
promovido), a iteração walks calendário-por-calendário sem
edição adicional — pinado em
`tests/unit/test_drift_d_wiring.py` com `_pending_mirror_months`
e `_pending_build_months` rodando contra um fixture PCA-em-RESOURCES.

---

## 4. Roadmap dos próximos recursos

A ordem é por alavancagem nas sete jornadas em `VISION.md`. Cada item
abre com seu pré-requisito de drift.

### Fase 1 — Promover `publicacoes`

Pré-req: nenhum (PK simples, partição mensal). É a primeira promoção
após o débito mais barato (Drift-A).

Trabalho:

- Definir fan-out de `codigoModalidadeContratacao` (obrigatório no
  `/v1/contratacoes/publicacao`). Recomendação: estender `FetchSpec`
  com `required_params: dict[str, list[int|str]]`; o extractor
  itera o produto cartesiano por janela.
- Implementar `_flatten_publicacao` em `transforms.py` contra
  `RecuperarCompraPublicacaoDTO`.
- Confirmar unicidade de `numeroControlePNCP` na janela; se houver
  colisão, mudar para `dedup_strategy="append_only"` com
  `data_atualizacao_global DESC`.
- Confirmar `data_start`; setar no resource.
- Mover de `PLANNED_RESOURCES` para `RESOURCES`.

Critério de saída: teste de caracterização cobrindo um mês mirrored e
o flatten round-trip; sem regressão em contratos/atas; cron pode
rodar `baliza sync --resource publicacoes` manual.

### Fase 2 — Promover `pca`

Pré-req: Drift-A (PK composta) + Drift-D (partição anual).

Trabalho:

- Decidir endpoint canônico (`/v1/pca/atualizacao` vs `/v1/pca/usuario`).
- Implementar `_flatten_pca_plano` + `_flatten_pca_item` (item-level
  enriquecido com campos do plano pai).
- Verificar PK composta `(id_pca_pncp, numero_item)`.
- Aviso explícito na UI: "PCA é demanda planejada, não contratação
  realizada" (vinha do `pca-catmat-pipeline.md`, mantido).

Critério de saída: ano de PCA carregado de ponta a ponta + página
`/pca?codigo=...` consumindo `pca_itens_canonical` (BDD `@pca-demand`).

### Fase 3 — Tabelas item-level derivadas

Pré-req: Drift-B (`ArtifactSpec`).

Goal: `itens_contratos`, depois `itens_atas`, `itens_publicacoes`,
explorando preço por CATMAT/CATSER. Itens não são endpoints próprios
do PNCP — são projeção long-form dos payloads pai.

Trabalho:

- Confirmar se PNCP expõe rows de item nos endpoints de listagem ou
  apenas na rota de detalhe (define se é fan-out por chave).
- Definir schema de price intelligence (CATMAT, unidade, preço unit,
  qtd, órgão, fornecedor, data).
- Hangar essas como `derived_tables` no `PNCPResource` pai
  (contratos/atas) — não criar resource "itens".

### Fase 4 — Consultas cross-resource

Pré-req: Fases 1–3.

Surfaces a serem habilitadas:

- "Esta agência contratou o que seu PCA prometeu?" — PCA × contratos
  via `cnpj_orgao` + janela.
- "Oportunidades abertas com evidência histórica de preço" —
  publicacoes × itens.
- "Atas vigentes cuja compra subjacente já virou contrato" —
  atas × contratos via `numero_controle_pncp_compra`.

Trabalho: DAG de chaves de join em `transforms.py`; sidebar "Atlas" no
explorer agrupando tabelas por camada (intent → open → reusable →
final → comparable).

---

## 5. Não-objetivos (mantidos)

- Real-time tender tracking (continua fora do escopo, conforme `VISION.md`).
- Alertas per-user (webhook/email) — exigem servidor; ficam fora.
- Um recurso PNCPResource separado por modalidade.

---

## 6. Como evoluir este plano

- Fase concluída → marcar como `(✓ shipped no PR #N)` e mover detalhe
  pro changelog do PR; manter aqui só o estado atual.
- Drift fechado → mover da §3 para a §1 e atualizar a tabela de camadas.
- Novo recurso planejado → entrada nova em §4 + módulo em
  `resources/` em `PLANNED_RESOURCES`. Não criar `.md` separado;
  consolidar aqui.
- BDD continua sendo a "executable specification"
  (`web/features/journeys/`). Este `.md` resume; lá é o que roda.

---

## Apêndice A — Histórico de PRs relevantes

Para arqueologia rápida quando alguém perguntar "quando isso virou
multi-resource":

| PR    | O que entregou                                                                 |
|-------|--------------------------------------------------------------------------------|
| #541  | sync_stats.json gerado da live IA manifest + dedupe de ci-failure issues       |
| #543  | Tier-0 ops: `baliza doctor`, `baliza orphans`, weekly cron, scheduled backfill |
| #544  | Registry `RESOURCES` + helpers `page_filename` / `first_page_filename`         |
| #545  | `mirror_cmd` / `mirror_month` aceitam `--resource`                             |
| #546  | `extractor.ingest_range` lê `entity_model` do registry; `build_cmd` plumbed    |
| #547  | Resource ATAS registrado (Tier-2)                                              |
| #548  | `IAUploader.upload_raw_zip` + `_upsert_manifest_row` resource-aware            |
| #549  | `IAUploader.upload_parquet` + `MonthlyExporter.export_month` resource-aware    |
| #553  | `sync_cmd` + workflows aceitam `--resource`; per-resource breakdown stats      |
| #554  | Atlas: `PUBLICACOES` + `PCA` em `PLANNED_RESOURCES`                            |
| #555  | Consolidator per-resource + skip UF shards para resources sem `partition_by_uf`|
| #556  | `baliza orphans --apply` destrutivo com guard rails                            |
| #557  | Hotfix: TypeError-proof na freshness gate non-UF (tz + max-None)               |

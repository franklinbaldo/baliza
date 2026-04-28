# Plano: Pipeline multi-tabela genérico

**Status:** v1  
**Escopo:** refatoração do pipeline Python + uploader + frontend para suportar múltiplas tabelas PNCP sem edições cirúrgicas  
**Estimativa:** 1–2 semanas  
**Dependentes:** `docs/plans/pca-catmat-pipeline.md` e qualquer endpoint PNCP futuro (`/atas`, `/empenhos`, `/instrumentoscobranca`, etc.)

---

## Problema

O pipeline atual é hard-acoplado a `contratos` mensais em pelo menos 5 pontos:

| Ponto | Arquivo | Acoplamento |
|---|---|---|
| Schema-version reprocessing | `builder.py:58` | `strptime("%Y-%m")` — rejeita silenciosamente qualquer `data_particao` não-mensal |
| PK de dedup | `engine.py:72` | `pk: str` — apenas coluna única; PKs compostas exigem workaround |
| Campos do manifest | `ia_uploader.py:81` | `MANIFEST_FIELDNAMES` hardcoded para contratos |
| Allowlist do frontend | `schema.ts` + `parquetFallback.ts` | duas edições manuais por nova tabela |
| Dados de build-time | `build-data.mjs` | stub de manifesto hardcoded (`SELECT '2024-04-01'...`) — nunca conectado ao IA real |

Cada novo endpoint PNCP repete o mesmo conjunto de workarounds.

---

## Solução: `TableContract`

Uma declaração única por tabela que elimina todas as edições cirúrgicas.

### `src/baliza/table_contract.py` (novo)

```python
from pydantic import BaseModel
from typing import Callable, Any

class TableContract(BaseModel, arbitrary_types_allowed=True):
    table_name: str                        # "contratos", "pca_itens", "atas", …
    schema_version: str                    # "2.0.0", "1.0.0", …
    pk: str | list[str]                    # "numeroControlePNCP" ou ["id_pca_pncp", "numero_item"]
    data_particao_fn: Callable[[dict], str] # row → "YYYY-MM" ou "YYYY-12-31"
    sort_columns: list[str]
    bloom_filter_columns: list[str]
    manifest_extra_fields: dict[str, Any] = {}  # campos adicionais além dos padrão

    def parse_data_particao(self, row: dict) -> str:
        """Extrai e valida data_particao; delega para data_particao_fn.
        Lança ValueError se o resultado for vazio — propagado pelo builder."""
        result = self.data_particao_fn(row)
        if not result:
            raise ValueError(f"data_particao_fn returned empty for table {self.table_name}")
        return result

def _contratos_particao(row: dict) -> str:
    v = row.get("data_particao")
    if not v:
        raise ValueError("Missing data_particao in contratos row")
    return v

# Contratos já existentes declarados aqui
CONTRATOS = TableContract(
    table_name="contratos",
    schema_version="2.0.0",
    pk="numeroControlePNCP",
    data_particao_fn=_contratos_particao,  # já é "YYYY-MM"; lança se ausente
    sort_columns=["cnpj_orgao", "data_publicacao_pncp"],
    bloom_filter_columns=["cnpj_orgao"],
)
```

### Registro global

```python
# src/baliza/table_contract.py
REGISTRY: dict[str, TableContract] = {
    c.table_name: c for c in [CONTRATOS]
    # PCA, atas, empenhos serão adicionados pelos PRs respectivos
}
```

---

## Mudanças necessárias por componente

### 1. `engine.py` — PK composta

```python
def upsert_rows(
    self,
    data: list[dict],
    table_name: str,
    schema: str = "main",
    pk: str | list[str] = "numeroControlePNCP",
) -> int:
    # Se pk é lista, criar join condition via multi-column filter
    # t_existing.filter(~ibis.and_(*[t_existing[k].isin(t_new[k]) for k in pk]))
    # NÃO usar coluna sintética no Parquet — manter schema limpo
```

**Nota:** o filtro `isin` por múltiplas colunas requer composição de predicados — testar com Ibis antes de integrar. Alternativa segura: `anti_join(t_new, predicates=[t_existing[k] == t_new[k] for k in pk])`.

### 2. `builder.py` — dispatch por contrato

Substituir:
```python
# antes: hardcoded
month_date = datetime.strptime(part, "%Y-%m").date()
```

Por:
```python
# depois: delegado ao contrato
contract = REGISTRY.get(row.get("table_name", "contratos"))
if contract is None:
    continue
try:
    period = contract.parse_data_particao(row)  # método em TableContract; lança ValueError se inválido
except ValueError:
    continue
```

`parse_data_particao(row)` está definido em `TableContract` (ver seção acima) — delega para `data_particao_fn` e levanta `ValueError` se o resultado for vazio, o que o `builder.py` captura e pula a row.

### 3. `ia_uploader.py` — campos por contrato

```python
# antes: MANIFEST_FIELDNAMES hardcoded para contratos
MANIFEST_FIELDNAMES = ["data_particao", "table_name", "parquet_url", ...]

# campos base extraídos de MANIFEST_FIELDNAMES atual (ia_uploader.py:86-108)
BASE_MANIFEST_FIELDS = [
    "data_particao", "table_name", "row_count", "quarantine_count",
    "ia_item_id", "raw_zip_url", "parquet_url", "quarantine_url",
    "uploaded_at", "file_type", "uf_sigla", "sort_key", "row_group_size",
    "bloom_filter_columns", "sha256", "file_size_bytes",
    "mirror_uploaded_at", "raw_zip_sha256", "raw_zip_size_bytes",
    "parquet_uploaded_at", "parquet_schema_version",
]

# depois: campos base + extras do contrato
def manifest_fields_for(contract: TableContract) -> list[str]:
    return BASE_MANIFEST_FIELDS + list(contract.manifest_extra_fields.keys())
```

Validação Pydantic antes de gravar: linha do manifest que violar `data_particao: str (min 1)` é rejeitada com erro explícito, não silenciada.

### 4. `build-data.mjs` — leitura real do manifesto IA

```javascript
// antes: stub hardcoded
db.run(`CREATE TABLE IF NOT EXISTS manifest AS SELECT '2024-04-01' as date...`);

// depois: leitura real via httpfs — manifesto é CSV, não JSON
// URL: https://archive.org/download/baliza-pncp-manifest/manifest.csv
db.run(`
  INSTALL httpfs; LOAD httpfs;
  CREATE TABLE manifest AS
  SELECT * FROM read_csv_auto('${IA_MANIFEST_CSV_URL}', header=true);
`);
```

Pré-requisito para qualquer `.qmd` produzir dados corretos em build. Sem isso, a estratégia B2 do plano PCA (agregação em build-time) não funciona.

### 5. Frontend — `ARCHIVED_TABLES` type-safe

**Hoje:** duas edições manuais por tabela nova (`schema.ts` + `parquetFallback.ts`).

**Proposta:** manter as duas edições, mas adicionar um teste de integração que verifica coerência — lista de tabelas em `schema.ts` deve bater com a lista em `parquetFallback.ts`. Isso não elimina as edições, mas torna o drift detectável imediatamente.

Geração automática (TypeScript a partir do `REGISTRY` Python) é possível mas fora do escopo desta fase.

---

## Sequência de PRs

```
PR A — engine.py: upsert_rows(pk: str | list[str]) + testes
  - Anti-join multi-coluna; nenhum schema de Parquet muda
  - Testes: dedup com PK simples (não regride), dedup com PK composta

PR B — table_contract.py: CONTRATOS declarado + builder.py dispatch + ia_uploader validação
  - Contratos existentes continuam funcionando (CONTRATOS declarado com os valores atuais)
  - builder.py: strptime substituído por contract.parse_data_particao
  - ia_uploader: validação Pydantic de data_particao antes de gravar manifest

PR C — build-data.mjs: leitura real do manifesto IA
  - Substituir stub por httpfs no build script
  - Teste de smoke: build não falha com manifesto vazio

PR D — frontend: teste de coerência ARCHIVED_TABLES
  - Vitest que verifica schema.ts e parquetFallback.ts têm as mesmas tabelas
```

---

## O que NÃO muda

- Interface pública de `BalizaEngine` (apenas assinatura de `upsert_rows` muda, backward-compatible se `pk` default permanece)
- Formato dos Parquet de contratos existentes (nenhuma migração de dados)
- Workflow de CI (os 4 PRs são adições/refatorações internas)

---

## Riscos

| Risco | Mitigação |
|---|---|
| Ibis `anti_join` com múltiplas colunas tem bug | Testar isoladamente antes de PR A |
| `build-data.mjs` com httpfs falha em CI sem credencial IA | Tornar leitura condicional; fallback para manifesto local em `test` |
| `builder.py` refactor quebra reprocessamento de contratos | PR B deve passar todos os testes de sync existentes antes de merge |

---

## Critério de conclusão

Após PR D:
- `pca_catmat_pipeline.md` pode ser implementado sem nenhuma das 5 edições cirúrgicas
- Adicionar `/atas` requer apenas: declarar `ATAS = TableContract(...)`, escrever `_flatten_atas()`, e adicionar a tabela ao frontend em dois arquivos (com teste que detecta drift)

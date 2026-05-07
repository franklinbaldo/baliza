# PNCP OpenAPI spec

Source-of-truth copy of the [PNCP API](https://pncp.gov.br/api/consulta) Swagger
spec. `src/baliza/models.py` is generated from this file via
`datamodel-code-generator`. **Edit the spec, not the generated module** — that
way the fix survives the next regen.

## Regenerate

```bash
uv run --with datamodel-code-generator datamodel-codegen \
  --input docs/openapi/api-pncp-consulta.json \
  --input-file-type openapi \
  --output src/baliza/models.py \
  --target-python-version 3.12 \
  --use-standard-collections \
  --use-union-operator \
  --use-schema-description \
  --output-model-type pydantic_v2.BaseModel
```

## Local patches to upstream

The PNCP-published spec doesn't always match the live API. Document each
divergence so a clean regen preserves the fix.

### `situacaoCompraId` is an integer, not a string

Spec said `{"type": "string", "enum": ["1","2","3","4"]}`. Verified by
[`scripts/probe_publicacoes.py`](../../scripts/probe_publicacoes.py): the live
`/v1/contratacoes/publicacao` endpoint always returns the integer form (`1`,
not `"1"`). Patched to `{"type": "integer", "enum": [1,2,3,4]}` so
`RecuperarCompraPublicacaoDTO` validates against real payloads.

### `RecuperarAtaDTO` rename

The spec calls the schema `AtaRegistroPrecoPeriodoDTO`; the codebase uses
`RecuperarAtaDTO`. After a regen, rename the class in `models.py` (or fix the
import in `src/baliza/resources/atas.py`).

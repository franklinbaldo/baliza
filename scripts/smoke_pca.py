#!/usr/bin/env -S uv run --quiet
"""Smoke test for PCA promotion (#569).

Exercises end-to-end against the live PNCP API:

1. Date-range fetch on ``/v1/pca/atualizacao`` (no fan-out).
2. Schema validation via ``PlanoContratacaoComItensDoUsuarioDTO``.
3. ``_flatten_pca_item`` produces one snake_case row per nested item.
4. Composite-PK upsert through ``BalizaEngine.upsert_rows`` exercises
   Drift-A's ``anti_join`` path on ``[id_pca_pncp, numero_item]``.
5. Strict PK uniqueness check (Codex P2 on #574 — only the declared
   composite key, no extra dimensions) confirms no duplicates inside
   one fetched window.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from baliza.engine import BalizaEngine  # noqa: E402
from baliza.extractor import PNCPExtractor  # noqa: E402
from baliza.resources import PCA  # noqa: E402
from baliza.transforms import _flatten_pca_item  # noqa: E402

EXPECTED_KEYS: tuple[str, ...] = (
    "id_pca_pncp",
    "numero_item",
    "ano_pca",
    "cnpj_orgao",
    "valor_total",
    "descricao_item",
    "data_publicacao_pncp",
)


def main() -> int:  # noqa: PLR0911
    end = datetime.combine(datetime.utcnow().date() - timedelta(days=2), datetime.min.time())
    start = end - timedelta(days=1)
    print(f"# Smoke `pca` promotion ({start.date()} → {end.date()})\n")

    with PNCPExtractor(engine=None) as ex:
        try:
            data = ex.fetch_page(PCA.name, start, end, page=1)
        except Exception as exc:  # noqa: BLE001
            print(f"- fetch failed: `{exc}`")
            return 1

    parents = data.get("data") or []
    print(f"- page 1: {len(parents)} parent rows, "
          f"{sum(len(p.get('itens') or []) for p in parents)} nested items")

    if not parents:
        print("\n**No rows fetched — smoke can't exercise downstream paths.**")
        return 1

    # Validate via Pydantic.
    valid_parents = 0
    for entry in parents:
        try:
            PCA.entity_model.model_validate(entry)
        except Exception as exc:  # noqa: BLE001
            print(f"\n**Pydantic validation failed**: `{exc}`")
            return 1
        valid_parents += 1

    print(f"- {valid_parents} parents validated.")

    # Flatten — emits one row per item, parent fields denormalized.
    flat_rows: list[dict] = []
    for entry in parents:
        flat_rows.extend(_flatten_pca_item(entry))

    if not flat_rows:
        print("\n**Flatten produced no rows** — every parent had empty `itens`.")
        return 1

    print(f"- flattened: {len(flat_rows)} item rows.")

    # Strict composite-PK uniqueness on the declared key only.
    pks = {(r["id_pca_pncp"], r["numero_item"]) for r in flat_rows}
    if len(pks) != len(flat_rows):
        dupes = len(flat_rows) - len(pks)
        print(f"\n**Composite PK collisions within window**: {dupes} dupes.")
        return 1
    print(f"- composite PK ({{id_pca_pncp, numero_item}}) unique: {len(pks)} distinct.")

    missing = {k for k in EXPECTED_KEYS if k not in flat_rows[0]}
    if missing:
        print(f"\n**Missing keys** in flatten output: {missing}")
        return 1

    # Round-trip through composite-PK upsert (Drift-A anti_join path).
    with BalizaEngine() as engine:
        engine.upsert_rows(
            flat_rows,
            "pca_smoke",
            pk=["id_pca_pncp", "numero_item"],
        )
        roundtrip = engine.con.table("pca_smoke").execute()
    if len(roundtrip) != len(flat_rows):
        print(
            f"\n**Upsert roundtrip mismatch**: ingested {len(flat_rows)}, "
            f"read back {len(roundtrip)}."
        )
        return 1

    print("\n## Schema sample (one row)\n")
    sample = flat_rows[0]
    print("| key | value |")
    print("|:----|:------|")
    for k in EXPECTED_KEYS:
        v = repr(sample.get(k))[:80]
        print(f"| `{k}` | {v} |")

    print("\n✅ All smoke checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

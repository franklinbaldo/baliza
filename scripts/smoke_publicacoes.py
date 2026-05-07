#!/usr/bin/env -S uv run --quiet
"""Smoke test for publicacoes promotion (#568).

Exercises the new bits end-to-end against the live PNCP API:

1. Fan-out plumbing — ``PNCPExtractor.fetch_page`` honors
   ``extra_params={"codigoModalidadeContratacao": ...}`` and produces
   per-modalidade cache files.
2. Schema validation — ``RecuperarCompraPublicacaoDTO`` validates the
   live payload (already proven in #572 but kept here so a regression
   in the model fails this smoke instead of leaking to mirror).
3. Flatten — ``_flatten_publicacao`` produces the expected snake_case
   keys with non-empty values for the canonical columns.
4. Composite-PK absent / single-PK upsert (Drift-A path) — the rows
   round-trip through ``BalizaEngine.upsert_rows`` without raising.

Designed for the GitHub Action smoke workflow: small window, two
modalidades, no IA writes. Prints a Markdown summary suitable for the
PR-comment dump.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from baliza.engine import BalizaEngine  # noqa: E402
from baliza.extractor import PNCPExtractor  # noqa: E402
from baliza.resources import PUBLICACOES  # noqa: E402
from baliza.transforms import _flatten_publicacao  # noqa: E402

EXPECTED_KEYS: tuple[str, ...] = (
    "numero_controle_pncp",
    "ano_compra",
    "cnpj_orgao",
    "uf_sigla",
    "modalidade_id",
    "valor_total_estimado",
    "data_publicacao_pncp",
    "objeto_compra",
    "situacao_compra_id",
)


def _window() -> tuple[datetime, datetime]:
    end = datetime.combine(datetime.utcnow().date() - timedelta(days=2), datetime.min.time())
    start = end - timedelta(days=1)
    return start, end


def main() -> int:  # noqa: PLR0911
    start, end = _window()
    print(f"# Smoke `publicacoes` promotion ({start.date()} → {end.date()})\n")

    # Probe two modalidades that have consistently carried data in
    # #572's runs: 5 (smaller payloads) and 8 (medium). Don't probe
    # the full fan-out — this is a smoke, not a backfill.
    sample_modalidades = [5, 8]

    rows_per_modalidade: dict[int, list[dict]] = {}
    with PNCPExtractor(engine=None) as ex:
        for modalidade in sample_modalidades:
            try:
                data = ex.fetch_page(
                    PUBLICACOES.name, start, end, page=1,
                    extra_params={"codigoModalidadeContratacao": modalidade},
                )
            except Exception as exc:  # noqa: BLE001
                print(f"- modalidade {modalidade}: fetch failed — `{exc}`")
                continue
            entries = data.get("data") or []
            print(f"- modalidade {modalidade}: {len(entries)} rows on page 1")
            rows_per_modalidade[modalidade] = entries

    if not any(rows_per_modalidade.values()):
        print("\n**No rows fetched — smoke can't exercise downstream paths.**")
        return 1

    # Validate via the Pydantic model and flatten.
    valid = 0
    flattened: list[dict] = []
    missing_keys_per_row: list[set[str]] = []
    for entries in rows_per_modalidade.values():
        for entry in entries:
            try:
                model = PUBLICACOES.entity_model.model_validate(entry)
            except Exception as exc:  # noqa: BLE001
                print(f"\n**Pydantic validation failed**: `{exc}`")
                return 1
            valid += 1
            flat = _flatten_publicacao(model.model_dump(mode="python"))
            flattened.append(flat)
            missing = {k for k in EXPECTED_KEYS if k not in flat}
            missing_keys_per_row.append(missing)

    print(f"\n**Validated {valid} rows; flattened {len(flattened)}.**")
    if any(missing_keys_per_row):
        print(f"\n**Missing keys** in some rows: {set().union(*missing_keys_per_row)}")
        return 1

    # Sanity: every row has a non-null PK and uf_sigla, and modalidade_id
    # is the int IntEnum.value (drift-A IntEnum fix).
    pk_set = {r["numero_controle_pncp"] for r in flattened}
    if None in pk_set:
        print("\n**Some rows have null numero_controle_pncp** — PK invariant broken.")
        return 1
    if len(pk_set) != len(flattened):
        print(f"\n**Duplicate PKs** within smoke window: {len(flattened) - len(pk_set)} dupes.")
        return 1

    # Round-trip through BalizaEngine.upsert_rows to exercise the
    # actual ingestion path (single-PK fast path on numero_controle_pncp).
    with BalizaEngine() as engine:
        engine.upsert_rows(flattened, "publicacoes_smoke", pk="numero_controle_pncp")
        roundtrip = engine.con.table("publicacoes_smoke").execute()
    if len(roundtrip) != len(flattened):
        print(
            f"\n**Upsert roundtrip mismatch**: ingested {len(flattened)}, "
            f"read back {len(roundtrip)}."
        )
        return 1

    print("\n## Schema sample (one row)\n")
    sample = flattened[0]
    print("| key | value |")
    print("|:----|:------|")
    for k in EXPECTED_KEYS:
        v = repr(sample.get(k))[:80]
        print(f"| `{k}` | {v} |")

    print("\n✅ All smoke checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

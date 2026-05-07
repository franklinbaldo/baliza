#!/usr/bin/env -S uv run --quiet
"""Probe ``/v1/pca/*`` to pin facts for issue #569.

Promoting PCA needs three decisions verified against the live API:

1. **Endpoint choice.** Three candidates in the spec:
   - ``/v1/pca/atualizacao`` — date-range (dataInicio, dataFim).
     Best fit for Baliza's partition model.
   - ``/v1/pca/`` — needs (anoPca, codigoClassificacaoSuperior). Would
     require fan-out across classificacaoSuperior values.
   - ``/v1/pca/usuario`` — per-user; not a global mirror endpoint.

   The probe hits ``/atualizacao`` (preferred) plus a sample
   ``/?anoPca`` request so we can compare row counts and decide.

2. **Schema validation.** ``PlanoContratacaoComItensDoUsuarioDTO`` is
   the published model; verify it accepts live payloads. PCA items
   live nested in ``itens``, so the smoke later will need to flatten
   one row per item.

3. **PK uniqueness.** Provisional PK is composite
   ``(id_pca_pncp, numero_item)``. Confirm by counting duplicates in a
   sample window across all returned items.

Output is a Markdown table for the PR-comment dump, like
``probe_publicacoes.py``.
"""
from __future__ import annotations

import os
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import httpx
from pydantic import ValidationError

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from baliza.models import PlanoContratacaoComItensDoUsuarioDTO  # noqa: E402

BASE = "https://pncp.gov.br/api/consulta/v1"
ENDPOINT_ATUALIZACAO = f"{BASE}/pca/atualizacao"
ENDPOINT_BY_YEAR = f"{BASE}/pca/"


def _window() -> tuple[str, str]:
    end = date.today() - timedelta(days=2)
    start = end - timedelta(days=2)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def _fetch(url: str, params: dict, *, timeout: float = 90.0) -> tuple[int, dict | None, str]:
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            resp = client.get(url, params=params)
    except httpx.RequestError as exc:
        return 0, None, f"request error: {exc}"
    if resp.status_code != 200:
        return resp.status_code, None, resp.text[:200]
    try:
        return 200, resp.json(), ""
    except ValueError as exc:
        return 200, None, f"json decode: {exc}"


def _validate(entries):
    valid = 0
    invalid = 0
    errors: list[str] = []
    for entry in entries:
        try:
            PlanoContratacaoComItensDoUsuarioDTO.model_validate(entry)
            valid += 1
        except ValidationError as exc:
            invalid += 1
            if len(errors) < 3:
                first = exc.errors()[0]
                loc = ".".join(str(p) for p in first.get("loc", ()))
                msg = (first.get("msg") or "")[:80]
                raw = repr(first.get("input"))[:60]
                errors.append(f"{loc} [{first.get('type')}] {msg} (input={raw})")
    return valid, invalid, errors


def _probe_atualizacao() -> dict:
    start, end = _window()
    status, payload, err = _fetch(ENDPOINT_ATUALIZACAO, {
        "dataInicio": start, "dataFim": end, "pagina": 1, "tamanhoPagina": 500,
    })
    if payload is None:
        return {"endpoint": "atualizacao", "status": status, "rows": 0, "error": err}
    data = payload.get("data") or []
    valid, invalid, errors = _validate(data)
    # Items per record + composite-PK uniqueness check.
    item_count = 0
    pk_counter: Counter[tuple] = Counter()
    for entry in data:
        items = entry.get("itens") or []
        item_count += len(items)
        # PCA's id_pca_pncp lives on the parent record; numero_item on
        # each item. Provisional composite PK = (id_pca_pncp, numero_item).
        # The DTO has no id_pca_pncp field directly — let's see what
        # candidate fields exist on the parent.
        parent_id_candidates = (
            entry.get("idPcaPncp"),
            entry.get("orgaoEntidadeCnpj"),
            entry.get("anoPca"),
        )
        for it in items:
            pk_counter[(parent_id_candidates, it.get("numeroItem"))] += 1

    dupes = {k: v for k, v in pk_counter.items() if v > 1}
    return {
        "endpoint": "atualizacao",
        "status": status,
        "rows": len(data),
        "items": item_count,
        "valid": valid,
        "invalid": invalid,
        "errors": errors,
        "pk_distinct": len(pk_counter),
        "pk_dupes": len(dupes),
        "total_pages": payload.get("totalPaginas"),
        "total_records": payload.get("totalRegistros"),
        "sample_keys": list(data[0].keys())[:20] if data else [],
    }


def _probe_by_year() -> dict:
    """Lighter probe of /v1/pca/ — just see what HTTP we get for a
    couple of (anoPca, codigoClassificacaoSuperior) combos."""
    rows = []
    for ano, classif in ((2024, "1"), (2024, "10")):
        status, payload, err = _fetch(ENDPOINT_BY_YEAR, {
            "anoPca": ano,
            "codigoClassificacaoSuperior": classif,
            "pagina": 1, "tamanhoPagina": 500,
        })
        rows.append((ano, classif, status, len(payload.get("data") or []) if payload else 0, err))
    return {"endpoint": "by_year", "samples": rows}


def main() -> int:
    start, end = _window()
    print(f"# Probe `/v1/pca/*` ({start} → {end})\n")

    a = _probe_atualizacao()
    print("## /v1/pca/atualizacao\n")
    print(f"- HTTP {a['status']}, {a['rows']} parent rows on page 1.")
    if a.get("error"):
        print(f"- error: `{a['error']}`")
    else:
        print(f"- nested items: {a['items']}")
        print(f"- Pydantic: {a['valid']} valid, {a['invalid']} invalid")
        if a["errors"]:
            print("- first errors:")
            for e in a["errors"]:
                print(f"  - {e}")
        if a.get("total_records") is not None:
            print(f"- API totalRegistros={a['total_records']}, totalPaginas={a['total_pages']}")
        if a.get("sample_keys"):
            print(f"- sample top-level keys: `{a['sample_keys']}`")
        print(
            f"- composite-PK uniqueness across items: {a['pk_distinct']} distinct, "
            f"{a['pk_dupes']} duplicated"
        )

    b = _probe_by_year()
    print("\n## /v1/pca/ (by anoPca)\n")
    print("| anoPca | codigoClassificacaoSuperior | HTTP | rows | note |")
    print("|---:|:---|---:|---:|:---|")
    for ano, classif, status, rows_, err in b["samples"]:
        print(f"| {ano} | {classif} | {status} | {rows_} | {err} |")

    print("\n## Recommendation\n")
    if a.get("invalid", 1) == 0 and a.get("rows", 0) > 0:
        print(
            "✅ ``/v1/pca/atualizacao`` accepts our existing date-range "
            "FetchSpec shape and `PlanoContratacaoComItensDoUsuarioDTO` "
            "validates clean. Use it as the canonical PCA endpoint; no "
            "fan-out needed (date-range query)."
        )
    else:
        print(
            "⚠️ ``/v1/pca/atualizacao`` did not validate cleanly — "
            "see errors above. Consider ``/v1/pca/`` with an anoPca + "
            "codigoClassificacaoSuperior fan-out instead."
        )

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        Path(summary_path).touch()  # workflow runner appends via tee already

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

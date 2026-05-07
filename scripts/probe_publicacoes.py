#!/usr/bin/env -S uv run --quiet
"""Probe ``/v1/contratacoes/publicacao`` to pin facts for issue #568.

Promoting ``publicacoes`` requires verifying:

1. The ``codigoModalidadeContratacao`` fan-out — the endpoint returns
   400 without it; we want to confirm the valid range and which values
   actually carry data within a recent window.
2. ``RecuperarCompraPublicacaoDTO`` validates against live payloads.
3. Whether ``numeroControlePNCP`` is unique across the cartesian product
   of (modalidade, page) for a single window — gates the PK choice.

Designed to run inside a GitHub Action with no IA credentials. Hits
PNCP only; prints a Markdown summary to stdout that the workflow can
post as a step summary.
"""
from __future__ import annotations

import os
import sys
from collections import Counter
from collections.abc import Iterable
from datetime import date, timedelta
from pathlib import Path

import httpx
from pydantic import ValidationError

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from baliza.models import RecuperarCompraPublicacaoDTO  # noqa: E402

BASE = "https://pncp.gov.br/api/consulta/v1"
ENDPOINT = f"{BASE}/contratacoes/publicacao"
# PNCP modalidade IDs from the API manual (1..14). Probe all of them
# so we know which ones populate data in our fixed window.
MODALIDADES = list(range(1, 15))


def _window() -> tuple[str, str]:
    """A small recent window (last 3 days) so the probe finishes fast.

    PNCP expects YYYYMMDD, no separators.
    """
    end = date.today() - timedelta(days=2)
    start = end - timedelta(days=2)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def _fetch_page_one(
    modalidade: int, start: str, end: str, *, timeout: float = 90.0
) -> tuple[int, dict | None, str]:
    """Return (status_code, parsed_json_or_None, error_msg)."""
    params = {
        "dataInicial": start,
        "dataFinal": end,
        "codigoModalidadeContratacao": modalidade,
        "pagina": 1,
        "tamanhoPagina": 50,
    }
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            resp = client.get(ENDPOINT, params=params)
    except httpx.RequestError as exc:
        return 0, None, f"request error: {exc}"
    if resp.status_code != 200:
        return resp.status_code, None, resp.text[:200]
    try:
        return 200, resp.json(), ""
    except ValueError as exc:
        return 200, None, f"json decode: {exc}"


def _validate_entries(entries: Iterable[dict]) -> tuple[int, int, list[str]]:
    """Run RecuperarCompraPublicacaoDTO over the entries.

    Returns (valid_count, invalid_count, sample_errors). When a
    validation fails we capture pydantic's structured error list (loc,
    type, msg) for the first 3 — that's enough to identify the schema
    drift without spamming the comment.
    """
    valid = 0
    invalid = 0
    errors: list[str] = []
    for entry in entries:
        try:
            RecuperarCompraPublicacaoDTO.model_validate(entry)
            valid += 1
        except ValidationError as exc:
            invalid += 1
            if len(errors) < 3:
                # exc.errors() gives structured info per field;
                # render compactly so it fits in a markdown table cell.
                pieces = []
                for err in exc.errors()[:3]:
                    loc = ".".join(str(p) for p in err.get("loc", ()))
                    type_ = err.get("type", "?")
                    msg = (err.get("msg") or "")[:100]
                    raw = err.get("input")
                    raw_repr = (
                        repr(raw)[:60] if raw is not None else "<missing>"
                    )
                    pieces.append(f"{loc} [{type_}] {msg} (input={raw_repr})")
                errors.append(" / ".join(pieces))
    return valid, invalid, errors


def main() -> int:
    start, end = _window()
    print(f"# Probe `/v1/contratacoes/publicacao` ({start} → {end})\n")

    summary_rows: list[tuple[int, int, int, int, str]] = []
    pk_counter: Counter[str] = Counter()
    total_valid = 0
    total_invalid = 0
    fatal_errors: list[str] = []

    for modalidade in MODALIDADES:
        status, payload, err = _fetch_page_one(modalidade, start, end)
        if payload is None:
            summary_rows.append((modalidade, status, 0, 0, err or "no payload"))
            continue
        if not isinstance(payload, dict) or "data" not in payload:
            summary_rows.append(
                (modalidade, status, 0, 0, f"unexpected shape: keys={list(payload.keys())[:5]}")
            )
            continue
        data = payload.get("data") or []
        valid, invalid, errors = _validate_entries(data)
        total_valid += valid
        total_invalid += invalid
        for entry in data:
            pk = entry.get("numeroControlePNCP")
            if pk:
                pk_counter[pk] += 1
        note = errors[0] if errors else ""
        summary_rows.append((modalidade, status, len(data), invalid, note))

    print("## modalidade × page1\n")
    print("| codigoModalidadeContratacao | HTTP | rows | invalid | note |")
    print("|---:|---:|---:|---:|:---|")
    for row in summary_rows:
        print("| {} | {} | {} | {} | {} |".format(*row))

    print(f"\n**Totals:** {total_valid} valid, {total_invalid} invalid Pydantic-side.\n")

    if pk_counter:
        dupes = {k: c for k, c in pk_counter.items() if c > 1}
        unique = sum(1 for c in pk_counter.values() if c == 1)
        print(
            f"**PK uniqueness:** {len(pk_counter)} distinct ``numeroControlePNCP``, "
            f"{unique} appear exactly once, {len(dupes)} appear more than once "
            f"across the fan-out."
        )
        if dupes:
            print("\nFirst 5 duplicate PKs (across modalidades):\n")
            for pk, count in list(dupes.items())[:5]:
                print(f"- `{pk}` × {count}")

    if fatal_errors:
        print("\n## Fatal errors\n")
        for e in fatal_errors:
            print(f"- {e}")
        return 1

    # Write to GitHub step summary if available.
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        # We've already printed to stdout; replicate to the summary so
        # it shows up in the Actions UI without having to open the log.
        Path(summary_path).write_text(_render_md(summary_rows, total_valid, total_invalid, pk_counter, start, end))

    return 0


def _render_md(rows, valid, invalid, pk_counter, start, end) -> str:
    out = [f"# Probe `/v1/contratacoes/publicacao` ({start} → {end})", ""]
    out.append("| codigoModalidadeContratacao | HTTP | rows | invalid | note |")
    out.append("|---:|---:|---:|---:|:---|")
    for row in rows:
        out.append("| {} | {} | {} | {} | {} |".format(*row))
    out.append("")
    out.append(f"**Totals:** {valid} valid, {invalid} invalid Pydantic-side.")
    if pk_counter:
        dupes = {k: c for k, c in pk_counter.items() if c > 1}
        unique = sum(1 for c in pk_counter.values() if c == 1)
        out.append(
            f"**PK uniqueness:** {len(pk_counter)} distinct, "
            f"{unique} once, {len(dupes)} >1× across modalidades."
        )
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Record VCR cassettes for PNCP API endpoints.

Fetches real responses from pncp.gov.br and saves them as VCR-compatible
YAML cassettes so the integration tests can run offline (record_mode: none).

Run this script whenever cassettes need refreshing (e.g. after an API schema
change).  Requires live network access to pncp.gov.br.

Usage:
    uv run python scripts/record_pncp_cassettes.py
"""

from __future__ import annotations

import json
from pathlib import Path

import requests
import yaml

CASSETTE_DIR = Path(__file__).parent.parent / "tests" / "cassettes" / "pncp"
BASE = "https://pncp.gov.br/api/consulta/v1"

# Fixed window that always has data (PNCP covers from 2021 onwards).
DATE_INITIAL = "20250101"
DATE_FINAL = "20250103"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Baliza/1.0 (cassette-recorder; data-pipeline-tests)",
}


def fetch_and_save(name: str, url: str, params: dict) -> None:
    """Fetch a live PNCP response and write it as a VCR cassette YAML."""
    cassette_path = CASSETTE_DIR / f"{name}.yaml"
    full_url = requests.Request("GET", url, params=params).prepare().url or url
    print(f"  GET {full_url}")

    resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    total = data.get("totalRegistros", len(data) if isinstance(data, list) else "?")
    print(f"  -> {resp.status_code}  totalRegistros={total}")

    # Trim to 3 records so cassettes stay small and fast to read.
    if isinstance(data, dict) and "data" in data:
        data["data"] = data["data"][:3]

    body_str = json.dumps(data, ensure_ascii=False)

    cassette = {
        "interactions": [
            {
                "request": {
                    "body": None,
                    "headers": {k: [v] for k, v in HEADERS.items()},
                    "method": "GET",
                    "uri": full_url,
                },
                "response": {
                    "body": {"string": body_str},
                    "headers": {
                        "Content-Type": [
                            resp.headers.get("Content-Type", "application/json")
                        ],
                    },
                    "status": {"code": resp.status_code, "message": resp.reason},
                    "url": full_url,
                },
            }
        ],
        "version": 1,
    }

    cassette_path.write_text(
        yaml.dump(cassette, allow_unicode=True, default_flow_style=False, width=120),
        encoding="utf-8",
    )
    print(f"  Saved {cassette_path.relative_to(Path.cwd())}")


def main() -> None:
    CASSETTE_DIR.mkdir(parents=True, exist_ok=True)
    print("Recording PNCP cassettes…\n")

    endpoints = [
        # (name, path, extra_params)
        (
            "contratos_page1",
            f"{BASE}/contratos",
            {"dataInicial": DATE_INITIAL, "dataFinal": DATE_FINAL, "pagina": 1, "tamanhoPagina": 10},
        ),
        (
            "publicacao_pregao_page1",
            f"{BASE}/contratacoes/publicacao",
            {
                "dataInicial": DATE_INITIAL,
                "dataFinal": DATE_FINAL,
                "codigoModalidadeContratacao": 6,
                "pagina": 1,
                "tamanhoPagina": 10,
            },
        ),
        (
            "publicacao_dispensa_page1",
            f"{BASE}/contratacoes/publicacao",
            {
                "dataInicial": DATE_INITIAL,
                "dataFinal": DATE_FINAL,
                "codigoModalidadeContratacao": 8,
                "pagina": 1,
                "tamanhoPagina": 10,
            },
        ),
        (
            "publicacao_inexigibilidade_page1",
            f"{BASE}/contratacoes/publicacao",
            {
                "dataInicial": DATE_INITIAL,
                "dataFinal": DATE_FINAL,
                "codigoModalidadeContratacao": 9,
                "pagina": 1,
                "tamanhoPagina": 10,
            },
        ),
    ]

    failed: list[str] = []
    for name, url, params in endpoints:
        print(f"[{name}]")
        try:
            fetch_and_save(name, url, params)
        except Exception as exc:
            print(f"  ERROR: {exc}")
            failed.append(name)
        print()

    if failed:
        print(f"FAILED to record {len(failed)} cassette(s): {failed}")
        print("Do NOT commit — fix the errors above and re-run.")
        raise SystemExit(1)

    print("Done.  Commit the files in tests/cassettes/pncp/")


if __name__ == "__main__":
    main()

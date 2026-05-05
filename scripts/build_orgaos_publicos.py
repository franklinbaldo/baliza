#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "httpx>=0.27",
# ]
# ///
"""Build the public-sector CNPJ reference dataset from the Receita Federal dump.

Downloads only `Empresas{0..9}.zip` (header: razão social + natureza jurídica
code) and `Naturezas.zip` (lookup table) from the official Receita Federal
CNPJ data portal, streams the CSV rows, filters by `natureza_juridica` codes
matching public administration entities, and writes a compact JSON to
`web/public/data/cnpj-orgaos-publicos.json` for the /status coverage section.

Source (official, monthly refresh by Receita Federal):
  https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/
  Manifest at /<YYYY-MM>/.

Why only Empresas — Estabelecimentos.zip would add UF and município but is
~5x bigger and adds ~30 minutes to the run. The /status page doesn't need
per-UF segmentation in v1; we can layer that in later by joining with
Estabelecimentos when the time budget allows.

Filter rule — natureza jurídica codes (4-digit) are grouped:
  1xxx = Administração Pública direta + indireta + autarquias +
         fundações + empresas públicas + sociedades de economia mista
  2xxx = Entidades empresariais (private). Excluded.
  3xxx, 4xxx, ... = associações, OS, instituições religiosas, etc.

Including all 1xxx (codes 1015..1309) gives the universe of agencies that
should be publishing on PNCP under the transparency rules.

Output schema (array of compact records, ~3 fields × N entries):
  [
    {"raiz": "00394502", "nome": "MINISTERIO DA SAUDE", "natureza": "1023"},
    ...
  ]
~30k–50k entries expected. Raw ~3-5 MB; gzipped ~600KB-1MB.

Refresh workflow (CI-driven):
  .github/workflows/build-orgaos-publicos.yml — monthly cron + manual
  dispatch. Commits the regenerated JSON when content changes.

Manual run (e.g. testing locally):
  $ uv run scripts/build_orgaos_publicos.py
  $ git add web/public/data/cnpj-orgaos-publicos.json
  $ git commit -m "chore(data): refresh public-CNPJ reference"
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import sys
import zipfile
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("build-orgaos-publicos")

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = REPO_ROOT / "web" / "public" / "data" / "cnpj-orgaos-publicos.json"

# Receita Federal publishes a new monthly snapshot under YYYY-MM/. Their
# index page is the canonical place to discover the latest folder; we parse
# it instead of hardcoding a date so this script keeps working past the
# next dump rotation.
INDEX_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
EMPRESAS_FILE_RE = re.compile(r"Empresas\d+\.zip", re.IGNORECASE)

# Public-administration natureza_juridica band. Codes 1015..1309 cover:
# - Direct admin (federal/state/municipal)
# - Autarquias (federal/state/municipal)
# - Fundações públicas
# - Empresas públicas + sociedades de economia mista (federal/state/municipal)
# - Comissões, consórcios, fundos
# Source: TabelaNaturezasJuridicas.pdf (Receita Federal).
PUBLIC_NATUREZA_MIN = 1000
PUBLIC_NATUREZA_MAX = 1999


def find_latest_folder(client: httpx.Client) -> str:
    """Read the dados_abertos_cnpj index and return the most recent YYYY-MM/ folder."""
    log.info("discovering latest snapshot folder at %s", INDEX_URL)
    resp = client.get(INDEX_URL)
    resp.raise_for_status()
    folders = re.findall(r'href="(\d{4}-\d{2}/)"', resp.text)
    if not folders:
        raise RuntimeError(f"no YYYY-MM/ folders found at {INDEX_URL}")
    folders.sort(reverse=True)
    log.info("latest snapshot: %s", folders[0])
    return folders[0]


def list_empresas_files(client: httpx.Client, folder_url: str) -> list[str]:
    """Return the list of Empresas*.zip filenames under the latest folder."""
    log.info("listing Empresas*.zip files under %s", folder_url)
    resp = client.get(folder_url)
    resp.raise_for_status()
    files = sorted(set(EMPRESAS_FILE_RE.findall(resp.text)))
    if not files:
        raise RuntimeError(f"no Empresas*.zip found at {folder_url}")
    log.info("found %d Empresas*.zip files: %s", len(files), files)
    return files


def stream_empresas_csv(client: httpx.Client, url: str):
    """Yield CSV rows from a streaming Empresas*.zip download.

    The Receita CSVs are Latin-1 encoded with `;` separators and no header
    row. Row layout (per Receita's layout doc):
      0  CNPJ_BASICO          (8 digits, the "raiz")
      1  RAZAO_SOCIAL
      2  NATUREZA_JURIDICA    (4-digit code)
      3  QUALIFICACAO_RESPONSAVEL
      4  CAPITAL_SOCIAL
      5  PORTE
      6  ENTE_FEDERATIVO_RESPONSAVEL  (set for some 1xxx — e.g. 'UNIAO')
    """
    log.info("downloading + streaming %s", url)
    with client.stream("GET", url) as resp:
        resp.raise_for_status()
        # zipfile needs a seekable file-like; buffer the small zip into memory.
        # Empresas zips are ~70-100 MB compressed — fits comfortably in RAM
        # on a GH runner (7 GB) without touching disk.
        buf = io.BytesIO()
        for chunk in resp.iter_bytes(chunk_size=8 * 1024 * 1024):
            buf.write(chunk)
    buf.seek(0)
    with zipfile.ZipFile(buf) as zf:
        for inner_name in zf.namelist():
            with zf.open(inner_name) as raw:
                # Wrap binary stream in TextIOWrapper to give csv.reader a
                # proper iterator without materialising the whole CSV.
                text = io.TextIOWrapper(raw, encoding="latin-1", newline="")
                yield from csv.reader(text, delimiter=";", quotechar='"')


def normalise_record(row: list[str]) -> dict | None:
    """Return a compact dict for public-sector rows; None to skip."""
    if len(row) < 3:
        return None
    raiz = row[0].strip()
    if not raiz.isdigit() or len(raiz) != 8:
        return None
    natureza_raw = row[2].strip()
    if not natureza_raw.isdigit():
        return None
    natureza_int = int(natureza_raw)
    if not (PUBLIC_NATUREZA_MIN <= natureza_int <= PUBLIC_NATUREZA_MAX):
        return None
    nome = row[1].strip()
    return {"raiz": raiz, "nome": nome, "natureza": natureza_raw}


def main() -> int:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    timeout = httpx.Timeout(60.0, connect=10.0)
    with httpx.Client(timeout=timeout, follow_redirects=True, http2=True) as client:
        latest_folder = find_latest_folder(client)
        folder_url = INDEX_URL + latest_folder
        empresas_files = list_empresas_files(client, folder_url)

        public_records: dict[str, dict] = {}  # dedup by raiz
        total_rows = 0
        for fname in empresas_files:
            url = folder_url + fname
            for row in stream_empresas_csv(client, url):
                total_rows += 1
                rec = normalise_record(row)
                if rec is None:
                    continue
                # If a CNPJ raiz appears in multiple zip shards (it shouldn't,
                # they're partitioned by hash), keep the first; razão social
                # is identical across shards by construction.
                public_records.setdefault(rec["raiz"], rec)
            log.info(
                "after %s: %d rows scanned, %d public-sector raizes captured",
                fname,
                total_rows,
                len(public_records),
            )

    if len(public_records) < 1000:
        # Sanity: even a partial Receita dump should yield tens of thousands.
        # Below 1000 means we filtered something fundamental (encoding,
        # natureza band) wrong — refuse to overwrite the committed JSON.
        log.error(
            "sanity check failed: only %d public-sector entries — refusing to write",
            len(public_records),
        )
        return 1

    sorted_records = sorted(public_records.values(), key=lambda r: r["raiz"])
    OUT_PATH.write_text(json.dumps(sorted_records, ensure_ascii=False))
    size_kb = OUT_PATH.stat().st_size / 1024
    log.info(
        "wrote %d public-sector CNPJ raizes to %s (%.1f KB)",
        len(sorted_records),
        OUT_PATH.relative_to(REPO_ROOT),
        size_kb,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

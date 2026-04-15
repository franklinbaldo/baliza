from __future__ import annotations

import json
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import structlog
from pydantic import ValidationError
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

from .engine import BalizaEngine
from .models import RecuperarContratoDTO as Contrato

logger = structlog.get_logger()
console = Console()


class PNCPExtractor:
    """Extracts data from PNCP API and ingests into DuckDB via Ibis."""

    def __init__(
        self,
        engine: BalizaEngine,
        base_url: str = "https://pncp.gov.br/api/pncp/v1",
        use_curl: bool = False,
    ):
        self.engine = engine
        self.base_url = base_url
        self.use_curl = use_curl
        self._lock = threading.Lock()

        self.headers = {
            "accept": "*/*",
            "User-Agent": "Baliza/1.0 (Data extraction pipeline)",
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_page(self, resource: str, extraction_date: datetime, page: int = 1) -> dict[str, Any]:
        """Fetch a single page from the PNCP API with resumption support."""
        date_iso = extraction_date.date().isoformat()
        filename = Path(f"data/raw/{date_iso}/{resource}_p{page}.json")

        # RESUMABILITY: Check if valid file already exists
        if filename.exists():
            try:
                with open(filename) as f:
                    data = json.load(f)
                if "data" in data or "totalPaginas" in data:
                    logger.info("resuming_from_cache", file=str(filename))
                    return data
            except (json.JSONDecodeError, KeyError):
                logger.warning("corrupt_cache_found", file=str(filename))
                filename.unlink()

        date_str = extraction_date.strftime("%Y-%m-%d")
        url = f"{self.base_url}/{resource}"
        params = {
            "dataPublicacao": date_str,
            "pagina": page,
            "tamanhoPagina": 500,
        }

        if self.use_curl:
            try:
                result = subprocess.run(
                    [
                        "curl",
                        "-s",
                        "-H",
                        "accept: */*",
                        "-H",
                        f"User-Agent: {self.headers['User-Agent']}",
                        f"{url}?dataPublicacao={date_str}&pagina={page}&tamanhoPagina=500",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                data = json.loads(result.stdout)
                self._save_raw(resource, extraction_date, page, data)
                return data
            except Exception:
                raise

        resp = self.client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        self._save_raw(resource, extraction_date, page, data)
        return data

    def _save_raw(self, resource: str, extraction_date: datetime, page: int, data: dict[str, Any]):
        """Save raw JSON payload to disk with deterministic name."""
        date_iso = extraction_date.date().isoformat()
        raw_dir = Path("data/raw") / date_iso
        raw_dir.mkdir(parents=True, exist_ok=True)

        # Deterministic filename for resumability
        filename = raw_dir / f"{resource}_p{page}.json"
        with open(filename, "w") as f:
            json.dump(data, f, ensure_ascii=False)

    def probe_date(self, resource: str, extraction_date: datetime) -> dict[str, Any]:
        """Fetch page 1 to determine total pages and registry count."""
        data = self.fetch_page(resource, extraction_date, page=1)
        return {
            "total_pages": data.get("totalPaginas", 1),
            "total_registries": data.get("totalRegistros", 0),
        }

    def ingest_day(self, extraction_date: datetime) -> dict[str, int]:
        """Validate and ingest all raw JSON files for a specific day into the shared engine."""
        date_iso = extraction_date.date().isoformat()
        raw_dir = Path("data/raw") / date_iso

        stats = {"valid": 0, "quarantine": 0}

        if not raw_dir.exists():
            return stats

        for json_file in raw_dir.glob("*.json"):
            with open(json_file) as f:
                data = json.load(f)

            entries = data.get("data", [])
            valid_rows = []

            for entry in entries:
                try:
                    # Validate with Pydantic
                    validated = Contrato.model_validate(entry)
                    valid_rows.append(validated.model_dump())
                    stats["valid"] += 1
                except ValidationError as e:
                    stats["quarantine"] += 1
                    logger.warning("validation_failed", error=str(e), entry_id=entry.get("id"))
                    self.engine.quarantine_record("contratos", extraction_date, str(e), entry)

            # Ingest valid rows into Ibis (shared engine) via UPSERT
            if valid_rows:
                # Direct memory ingestion (Idempotent)
                self.engine.upsert_rows(valid_rows, "contratos", schema="main")

        return stats

    def export_quarantine(self, extraction_date: datetime, output_path: Path) -> bool:
        """Export session quarantine to CSV if not empty."""
        try:
            q_table = self.engine.get_table("quarantine", schema="baliza_state")
            # Filter for current date if possible, but in stateless per-day loop,
            # the quarantine table is fresh for this run.
            df = q_table.execute()
            if not df.empty:
                df.to_csv(output_path, index=False)
                return True
        except Exception as e:
            logger.error("quarantine_export_failed", date=extraction_date.isoformat(), error=str(e))
        return False

    def __enter__(self):
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        self.close()

    def close(self):
        self.client.close()

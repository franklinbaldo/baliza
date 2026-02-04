"""Coverage tracking utilities for PNCP extractions."""

from __future__ import annotations

import string
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import duckdb  # type: ignore[import-untyped]

try:  # pragma: no cover - exercised indirectly
    import xxhash
except ModuleNotFoundError:  # pragma: no cover - fallback path
    xxhash = None


@dataclass(slots=True)
class WindowPage:
    """Metadata captured for a single page extraction."""

    pagina: int
    total_paginas: int
    hash_ids: str | None
    n_registros: int
    fetched_at: datetime


def _to_naive_utc(value: Any | None) -> datetime | None:
    """Normalize various timestamp inputs into naive UTC datetimes."""

    if value is None:
        return None

    if isinstance(value, str):
        # Optimization: Python 3.11+ handles 'Z' and ISO formats natively efficiently.
        # Try parsing directly to avoid string copy (strip).
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            dt = datetime.fromisoformat(value.strip())
    elif isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=UTC)
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time(), tzinfo=UTC)
    else:  # pragma: no cover - defensive, unexpected types
        raise TypeError(f"Unsupported timestamp value: {value!r}")

    # Optimization: Avoid replace() copy if already naive
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


def _to_iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    # Optimization: fast path for naive UTC datetimes to avoid replace() overhead.
    # The codebase stores datetimes as naive UTC, so this is the hot path.
    if value.tzinfo is None:
        return value.isoformat() + "Z"
    return value.replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")


def _quote_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


class CoverageTracker:
    """Persisted tracker that captures extraction coverage manifests."""

    def __init__(self, duckdb_path: Path | str, *, dataset: str = "baliza_raw") -> None:
        self.database_path = Path(duckdb_path)
        self.dataset = dataset
        self.conn = duckdb.connect(str(self.database_path))
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Setup & lifecycle helpers
    # ------------------------------------------------------------------
    def _ensure_tables(self) -> None:
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS baliza_state.cobertura (
                recurso TEXT,
                janela_inicio TIMESTAMP,
                janela_fim TIMESTAMP,
                pagina INTEGER,
                total_paginas_observado INTEGER,
                n_registros_pagina INTEGER,
                hash_ids TEXT,
                fetched_at TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS baliza_state.janelas (
                recurso TEXT,
                periodo TEXT,
                status TEXT,
                motivo TEXT,
                atualizado_em TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cobertura_lookup
            ON baliza_state.cobertura (recurso, pagina, janela_inicio, janela_fim)
            """
        )

    def close(self) -> None:
        """Flush changes and close the DuckDB connection."""

        self.conn.commit()
        self.conn.close()

    # ------------------------------------------------------------------
    # Formatting helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _period_key(start: datetime | None, end: datetime | None) -> str:
        start_iso = _to_iso_utc(start) if start else ""
        end_iso = _to_iso_utc(end) if end else ""
        return f"{start_iso}|{end_iso}"

    def period_label(self, start: datetime | None, end: datetime | None) -> str:
        """Public helper to build the storage period label."""

        return self._period_key(start, end)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def record_page(
        self,
        recurso: str,
        janela_inicio: Any | None,
        janela_fim: Any | None,
        pagina: int,
        total_paginas: int,
        registros: Iterable[dict[str, Any]],
    ) -> None:
        """Persist metadata for a fetched page and detect anomalies."""

        janela_inicio_dt = _to_naive_utc(janela_inicio)
        janela_fim_dt = _to_naive_utc(janela_fim)
        fetched_at = datetime.now(UTC).replace(tzinfo=None)

        # Optimization: Avoid copying list if it is already a list (typical case in dlt pipelines)
        if isinstance(registros, list):
            registros_list = registros
        else:
            registros_list = list(registros)

        n_registros = len(registros_list)

        # Optimization: Unified pass to compute hash and detect anomalies
        hash_ids, anomalies = self._analyze_records(registros_list)

        self.conn.execute(
            """
            DELETE FROM baliza_state.cobertura
            WHERE recurso = ?
              AND pagina = ?
              AND janela_inicio IS NOT DISTINCT FROM ?
              AND janela_fim IS NOT DISTINCT FROM ?
            """,
            [recurso, pagina, janela_inicio_dt, janela_fim_dt],
        )
        self.conn.execute(
            """
            INSERT INTO baliza_state.cobertura (
                recurso,
                janela_inicio,
                janela_fim,
                pagina,
                total_paginas_observado,
                n_registros_pagina,
                hash_ids,
                fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                recurso,
                janela_inicio_dt,
                janela_fim_dt,
                pagina,
                total_paginas,
                n_registros,
                hash_ids,
                fetched_at,
            ],
        )

        if anomalies:
            motivo = "; ".join(
                f"{anom['field']} {anom['cnpj']} {anom['ano']} salto {anom['gap_start']}->{anom['gap_end']}"
                for anom in anomalies
            )
            self.mark_window_status(
                recurso,
                self._period_key(janela_inicio_dt, janela_fim_dt),
                "suspeito",
                motivo,
            )

    def mark_window_status(
        self, recurso: str, periodo: str, status: str, motivo: str | None = None
    ) -> None:
        """Persist (or update) the status for a given coverage window."""

        atualizado_em = datetime.now(UTC).replace(tzinfo=None)
        self.conn.execute(
            """
            DELETE FROM baliza_state.janelas
            WHERE recurso = ? AND periodo = ?
            """,
            [recurso, periodo],
        )
        self.conn.execute(
            """
            INSERT INTO baliza_state.janelas (recurso, periodo, status, motivo, atualizado_em)
            VALUES (?, ?, ?, ?, ?)
            """,
            [recurso, periodo, status, motivo, atualizado_em],
        )

    # ------------------------------------------------------------------
    # Hashing and anomaly detection
    # ------------------------------------------------------------------
    @staticmethod
    def _hash_payload(payload: bytes, algorithm: str) -> str:
        if algorithm == "xxh64":
            if xxhash is None:  # pragma: no cover - depends on optional dependency
                raise RuntimeError(
                    "Cannot compute xxh64 digests because the 'xxhash' package is not installed. "
                    "Install baliza[xxhash] to verify legacy coverage data."
                )
            return xxhash.xxh64_hexdigest(payload)
        if algorithm == "sha256":
            return sha256(payload).hexdigest()
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    @classmethod
    def _compute_hash_from_ids(
        cls, ids: list[str], algorithm: str | None = None
    ) -> tuple[str, str]:
        """Compute hash payload and digest from a list of IDs."""
        ids.sort()
        payload = "\n".join(ids).encode("utf-8")
        algo = algorithm or ("xxh64" if xxhash is not None else "sha256")
        digest = cls._hash_payload(payload, algo)
        return algo, digest

    @classmethod
    def hash_registros(
        cls,
        registros: Iterable[dict[str, Any]],
        *,
        algorithm: str | None = None,
        include_algorithm: bool = True,
    ) -> str | None:
        # Optimization: Use walrus operator to avoid double lookup in dict
        ids = [
            str(val)
            for item in registros
            if isinstance(item, dict) and (val := item.get("numeroControlePNCP"))
        ]
        if not ids:
            return None

        algo, digest = cls._compute_hash_from_ids(ids, algorithm)
        if include_algorithm:
            return f"{algo}:{digest}"
        return digest

    @staticmethod
    def parse_hash_value(value: str) -> tuple[str, str]:
        if not value:
            raise ValueError("Empty hash value")
        if ":" in value:
            algorithm, digest = value.split(":", 1)
            if not algorithm or not digest:
                raise ValueError(f"Invalid hash encoding: {value!r}")
            return algorithm, digest
        is_hex = all(ch in string.hexdigits for ch in value)
        if is_hex and len(value) == 16:
            return "xxh64", value
        if is_hex and len(value) == 64:
            return "sha256", value
        raise ValueError(f"Unrecognized hash format: {value!r}")

    def _analyze_records(
        self, registros: Iterable[dict[str, Any]]
    ) -> tuple[str | None, list[dict[str, Any]]]:
        """Single-pass analysis to compute hash and detect sequence anomalies."""
        ids: list[str] = []
        grouped: dict[tuple[str, str, str], list[int]] = {}

        for item in registros:
            if not isinstance(item, dict):
                continue

            # 1. Collect ID for hashing
            val = item.get("numeroControlePNCP")
            if val:
                ids.append(str(val))

            # 2. Collect sequence data for anomaly detection
            cnpj = self._extract_cnpj(item)
            ano = self._extract_ano(item)
            if not cnpj or not ano:
                continue

            for field in ("sequencialCompra", "sequencialContrato"):
                seq_value = item.get(field)
                if seq_value is None:
                    continue
                try:
                    seq_int = int(seq_value)
                except (TypeError, ValueError):  # pragma: no cover - guard
                    continue

                # Optimization: Use try/except which is faster than setdefault for hot loops
                # because it avoids creating the default empty list object on every hit.
                key = (field, cnpj, ano)
                try:
                    grouped[key].append(seq_int)
                except KeyError:
                    grouped[key] = [seq_int]

        # Finalize Hash
        hash_ids = None
        if ids:
            algo, digest = self._compute_hash_from_ids(ids)
            hash_ids = f"{algo}:{digest}"

        # Finalize Anomalies
        anomalies: list[dict[str, Any]] = []
        for (field, cnpj, ano), values in grouped.items():
            if len(values) < 2:
                continue
            seen = sorted(set(values))
            for previous, current in zip(seen, seen[1:]):
                if current - previous > 1:
                    anomalies.append(
                        {
                            "field": field,
                            "cnpj": cnpj,
                            "ano": ano,
                            "gap_start": previous,
                            "gap_end": current,
                            "missing": current - previous - 1,
                        }
                    )
                    break

        return hash_ids, anomalies

    def _detect_sequence_anomalies(
        self, registros: Iterable[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        # Optimization: Delegate to unified pass to avoid logic duplication
        _, anomalies = self._analyze_records(registros)
        return anomalies

    @staticmethod
    def _extract_cnpj(item: dict[str, Any]) -> str | None:
        if (cnpj := item.get("cnpj")):
            return str(cnpj)
        contratante = item.get("contratante")
        if isinstance(contratante, dict) and (cnpj := contratante.get("cnpj")):
            return str(cnpj)
        return None

    @staticmethod
    def _extract_ano(item: dict[str, Any]) -> str | None:
        for key in ("ano", "anoCompra", "anoContrato"):
            if (val := item.get(key)):
                return str(val)
        for key in ("dataPublicacaoPncp", "dataPublicacao"):
            val = item.get(key)
            if not val:
                continue
            text = str(val).strip()

            # Optimization: fast path for common ISO date formats "YYYY-MM-DD" and "YYYY"
            # This avoids datetime parsing overhead for naive dates while falling back to
            # full parsing for timestamps with time/timezone info to ensure correctness.
            if len(text) == 4 and text.isdigit():
                return text
            if len(text) == 10 and text[4] == "-" and text[7] == "-" and text[:4].isdigit():
                return text[:4]

            try:
                dt = _to_naive_utc(text)
            except Exception:  # pragma: no cover - defensive
                continue
            if dt:
                return str(dt.year)
        return None

    # ------------------------------------------------------------------
    # Coverage inspection helpers
    # ------------------------------------------------------------------
    def fetch_pages_by_window(
        self,
        recurso: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Return recorded pages grouped by window key."""

        query = (
            "SELECT janela_inicio, janela_fim, pagina, total_paginas_observado, "
            "n_registros_pagina, hash_ids, fetched_at "
            "FROM baliza_state.cobertura WHERE recurso = ?"
        )
        params: list[Any] = [recurso]
        if start is not None:
            query += " AND janela_inicio >= ?"
            params.append(start)
        if end is not None:
            query += " AND janela_fim <= ?"
            params.append(end)
        rows = self.conn.execute(query, params).fetchall()

        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            (
                janela_inicio_dt,
                janela_fim_dt,
                pagina,
                total_paginas,
                n_registros,
                hash_ids,
                fetched_at,
            ) = row
            key = self._period_key(janela_inicio_dt, janela_fim_dt)
            try:
                entry = grouped[key]
            except KeyError:
                entry = grouped[key] = {
                    "janela_inicio": janela_inicio_dt,
                    "janela_fim": janela_fim_dt,
                    "pages": {},
                    "recorded_pages": set(),
                    "max_total": 0,
                }
            entry["pages"][pagina] = WindowPage(
                pagina=pagina,
                total_paginas=total_paginas,
                hash_ids=hash_ids,
                n_registros=n_registros,
                fetched_at=fetched_at,
            )
            entry["recorded_pages"].add(pagina)
            entry["max_total"] = max(entry["max_total"], total_paginas)
        return grouped

    def fetch_window_statuses(self, recurso: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT periodo, status, motivo, atualizado_em FROM baliza_state.janelas WHERE recurso = ?",
            [recurso],
        ).fetchall()
        return [
            {
                "periodo": periodo,
                "status": status,
                "motivo": motivo,
                "atualizado_em": atualizado_em,
            }
            for periodo, status, motivo, atualizado_em in rows
        ]

    def derive_window_candidates(
        self,
        table_name: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        date_field: str = "dataPublicacaoPncp",
    ) -> list[tuple[datetime, datetime]]:
        """Inspect the raw dataset to infer available daily windows."""

        dataset_ident = _quote_identifier(self.dataset)
        table_ident = _quote_identifier(table_name)
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {dataset_ident}")
        qualified_table = f"{dataset_ident}.{table_ident}"
        try:
            self.conn.execute(f"DESCRIBE {qualified_table}")
        except duckdb.CatalogException:
            return []

        field_expr = _quote_identifier(date_field)
        query = (
            f"SELECT DISTINCT date_trunc('day', {field_expr}) AS dia "
            f"FROM {qualified_table} WHERE {field_expr} IS NOT NULL"
        )
        params: list[Any] = []
        if start is not None:
            query += f" AND {field_expr} >= ?"
            params.append(start)
        if end is not None:
            query += f" AND {field_expr} < ?"
            params.append(end)
        query += " ORDER BY dia"
        rows = self.conn.execute(query, params).fetchall()

        windows: list[tuple[datetime, datetime]] = []
        for (dia,) in rows:
            if dia is None:
                continue
            inicio = _to_naive_utc(dia)
            if inicio is None:
                continue
            windows.append((inicio, inicio + timedelta(days=1)))
        return windows

    def summarize_windows(
        self,
        recurso: str,
        table_name: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, Any]:
        """Create a manifest summary of coverage and missing windows."""

        coverage = self.fetch_pages_by_window(recurso, start=start, end=end)
        candidates = self.derive_window_candidates(table_name, start=start, end=end)
        statuses = {row["periodo"]: row for row in self.fetch_window_statuses(recurso)}

        summary_windows: list[dict[str, Any]] = []
        for key, entry in coverage.items():
            status = statuses.get(key, {}).get("status")
            motivo = statuses.get(key, {}).get("motivo")
            summary_windows.append(
                {
                    "periodo": key,
                    "janela_inicio": entry["janela_inicio"],
                    "janela_fim": entry["janela_fim"],
                    "status": status,
                    "motivo": motivo,
                    "max_total": entry["max_total"],
                    "paginas": sorted(entry["recorded_pages"]),
                }
            )

        coverage_keys = set(coverage.keys())
        missing_windows = [
            self._period_key(start_dt, end_dt)
            for start_dt, end_dt in candidates
            if self._period_key(start_dt, end_dt) not in coverage_keys
        ]

        suspeitas = [row for row in statuses.values() if row.get("status") == "suspeito"]

        return {
            "windows": summary_windows,
            "missing": missing_windows,
            "suspeitas": suspeitas,
        }

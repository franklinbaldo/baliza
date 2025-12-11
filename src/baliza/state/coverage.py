"""Coverage tracking utilities for PNCP extractions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
import string
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duckdb  # type: ignore[import-untyped]

try:  # pragma: no cover - exercised indirectly
    import xxhash
except ModuleNotFoundError:  # pragma: no cover - fallback path
    xxhash = None


@dataclass
class WindowPage:
    """Metadata captured for a single page extraction."""

    pagina: int
    total_paginas: int
    hash_ids: Optional[str]
    n_registros: int
    fetched_at: datetime


def _to_naive_utc(value: Optional[Any]) -> Optional[datetime]:
    """Normalize various timestamp inputs into naive UTC datetimes."""

    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    else:  # pragma: no cover - defensive, unexpected types
        raise TypeError(f"Unsupported timestamp value: {value!r}")

    if dt.tzinfo is None:
        return dt.replace(tzinfo=None)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _to_iso_utc(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


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

    def close(self) -> None:
        """Flush changes and close the DuckDB connection."""

        self.conn.commit()
        self.conn.close()

    # ------------------------------------------------------------------
    # Formatting helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _period_key(start: Optional[datetime], end: Optional[datetime]) -> str:
        start_iso = _to_iso_utc(start) if start else ""
        end_iso = _to_iso_utc(end) if end else ""
        return f"{start_iso}|{end_iso}"

    def period_label(self, start: Optional[datetime], end: Optional[datetime]) -> str:
        """Public helper to build the storage period label."""

        return self._period_key(start, end)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def record_page(
        self,
        recurso: str,
        janela_inicio: Optional[Any],
        janela_fim: Optional[Any],
        pagina: int,
        total_paginas: int,
        registros: Iterable[Dict[str, Any]],
    ) -> None:
        """Persist metadata for a fetched page and detect anomalies."""

        janela_inicio_dt = _to_naive_utc(janela_inicio)
        janela_fim_dt = _to_naive_utc(janela_fim)
        fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)

        # Optimization: Avoid copying list if it is already a list (typical case in dlt pipelines)
        if isinstance(registros, list):
            registros_list = registros
        else:
            registros_list = list(registros)

        n_registros = len(registros_list)
        hash_ids = self.hash_registros(registros_list)

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

        anomalies = self._detect_sequence_anomalies(registros_list)
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
        self, recurso: str, periodo: str, status: str, motivo: Optional[str] = None
    ) -> None:
        """Persist (or update) the status for a given coverage window."""

        atualizado_em = datetime.now(timezone.utc).replace(tzinfo=None)
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
    def hash_registros(
        cls,
        registros: Iterable[Dict[str, Any]],
        *,
        algorithm: Optional[str] = None,
        include_algorithm: bool = True,
    ) -> Optional[str]:
        # Optimization: Use walrus operator to avoid double lookup in dict
        ids = [
            str(val)
            for item in registros
            if isinstance(item, dict) and (val := item.get("numeroControlePNCP"))
        ]
        if not ids:
            return None
        ids.sort()
        payload = "\n".join(ids).encode("utf-8")
        algo = algorithm or ("xxh64" if xxhash is not None else "sha256")
        digest = cls._hash_payload(payload, algo)
        if include_algorithm:
            return f"{algo}:{digest}"
        return digest

    @staticmethod
    def parse_hash_value(value: str) -> Tuple[str, str]:
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

    def _detect_sequence_anomalies(
        self, registros: Iterable[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        grouped: Dict[Tuple[str, str, str], List[int]] = {}
        anomalies: List[Dict[str, Any]] = []
        for item in registros:
            if not isinstance(item, dict):
                continue
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
                grouped.setdefault((field, cnpj, ano), []).append(seq_int)

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
        return anomalies

    @staticmethod
    def _extract_cnpj(item: Dict[str, Any]) -> Optional[str]:
        cnpj = item.get("cnpj")
        if cnpj:
            return str(cnpj)
        contratante = item.get("contratante")
        if isinstance(contratante, dict) and contratante.get("cnpj"):
            return str(contratante["cnpj"])
        return None

    @staticmethod
    def _extract_ano(item: Dict[str, Any]) -> Optional[str]:
        for key in ("ano", "anoCompra", "anoContrato"):
            if item.get(key):
                return str(item[key])
        for key in ("dataPublicacaoPncp", "dataPublicacao"):
            if not item.get(key):
                continue
            text = str(item[key])
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
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Return recorded pages grouped by window key."""

        query = (
            "SELECT janela_inicio, janela_fim, pagina, total_paginas_observado, "
            "n_registros_pagina, hash_ids, fetched_at "
            "FROM baliza_state.cobertura WHERE recurso = ?"
        )
        params: List[Any] = [recurso]
        if start is not None:
            query += " AND janela_inicio >= ?"
            params.append(start)
        if end is not None:
            query += " AND janela_fim <= ?"
            params.append(end)
        rows = self.conn.execute(query, params).fetchall()

        grouped: Dict[str, Dict[str, Any]] = {}
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
            entry = grouped.setdefault(
                key,
                {
                    "janela_inicio": janela_inicio_dt,
                    "janela_fim": janela_fim_dt,
                    "pages": {},
                    "recorded_pages": set(),
                    "max_total": 0,
                },
            )
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

    def fetch_window_statuses(self, recurso: str) -> List[Dict[str, Any]]:
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
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        date_field: str = "dataPublicacaoPncp",
    ) -> List[Tuple[datetime, datetime]]:
        """Inspect the raw dataset to infer available daily windows."""

        dataset_ident = _quote_identifier(self.dataset)
        table_ident = _quote_identifier(table_name)
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {dataset_ident}")
        qualified_table = f"{dataset_ident}.{table_ident}"
        try:
            self.conn.execute(f"DESCRIBE {qualified_table}")
        except duckdb.CatalogException:
            return []

        field_expr = date_field
        query = (
            f"SELECT DISTINCT date_trunc('day', {field_expr}) AS dia "
            f"FROM {qualified_table} WHERE {field_expr} IS NOT NULL"
        )
        params: List[Any] = []
        if start is not None:
            query += f" AND {field_expr} >= ?"
            params.append(start)
        if end is not None:
            query += f" AND {field_expr} < ?"
            params.append(end)
        query += " ORDER BY dia"
        rows = self.conn.execute(query, params).fetchall()

        windows: List[Tuple[datetime, datetime]] = []
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
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Create a manifest summary of coverage and missing windows."""

        coverage = self.fetch_pages_by_window(recurso, start=start, end=end)
        candidates = self.derive_window_candidates(table_name, start=start, end=end)
        statuses = {row["periodo"]: row for row in self.fetch_window_statuses(recurso)}

        summary_windows: List[Dict[str, Any]] = []
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

        suspeitas = [
            row for row in statuses.values() if row.get("status") == "suspeito"
        ]

        return {
            "windows": summary_windows,
            "missing": missing_windows,
            "suspeitas": suspeitas,
        }

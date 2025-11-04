"""State management for extraction runs and pipeline resumability."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .coverage import CoverageTracker


@dataclass
class ExtractionRun:
    """Metadata for a single extraction run."""

    run_id: str
    resource: str
    started_at: datetime
    completed_at: datetime | None
    status: str  # 'running', 'completed', 'failed'
    windows_attempted: int = 0
    windows_completed: int = 0
    pages_fetched: int = 0
    rows_extracted: int = 0
    error_message: str | None = None
    config_hash: str | None = None

    def asdict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "run_id": self.run_id,
            "resource": self.resource,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "windows_attempted": self.windows_attempted,
            "windows_completed": self.windows_completed,
            "pages_fetched": self.pages_fetched,
            "rows_extracted": self.rows_extracted,
            "error_message": self.error_message,
            "config_hash": self.config_hash,
        }


class StateManager:
    """Manages extraction state and run history for resumability."""

    def __init__(self, duckdb_path: Path | str, *, dataset: str = "baliza_raw") -> None:
        """
        Initialize StateManager with a DuckDB connection.

        Args:
            duckdb_path: Path to the DuckDB database file
            dataset: Dataset (schema) name for storing raw data
        """
        self.duckdb_path = Path(duckdb_path)
        self.dataset = dataset
        self.tracker = CoverageTracker(duckdb_path, dataset=dataset)
        self._ensure_run_table()

    def _ensure_run_table(self) -> None:
        """Create extraction_runs table if it doesn't exist."""
        self.tracker.conn.execute("""
            CREATE TABLE IF NOT EXISTS baliza_state.extraction_runs (
                run_id TEXT PRIMARY KEY,
                resource TEXT NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                status TEXT NOT NULL,
                windows_attempted INTEGER DEFAULT 0,
                windows_completed INTEGER DEFAULT 0,
                pages_fetched INTEGER DEFAULT 0,
                rows_extracted INTEGER DEFAULT 0,
                error_message TEXT,
                config_hash TEXT
            )
        """)
        self.tracker.conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        self.tracker.close()

    # ------------------------------------------------------------------
    # Run lifecycle management
    # ------------------------------------------------------------------

    def start_run(self, resource: str, config_hash: str | None = None) -> str:
        """
        Start a new extraction run and return its unique ID.

        Args:
            resource: Name of the resource being extracted
            config_hash: Optional hash of the configuration for tracking changes

        Returns:
            Unique run ID in format: YYYYMMDD-HHMMSS-<uuid>
        """
        now = datetime.now(UTC).replace(tzinfo=None)
        run_id = f"{now:%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:8]}"

        self.tracker.conn.execute(
            """
            INSERT INTO baliza_state.extraction_runs
            (run_id, resource, started_at, status, config_hash)
            VALUES (?, ?, ?, 'running', ?)
            """,
            [run_id, resource, now, config_hash],
        )
        self.tracker.conn.commit()
        return run_id

    def update_run_progress(
        self,
        run_id: str,
        *,
        windows_attempted: int | None = None,
        windows_completed: int | None = None,
        pages_fetched: int | None = None,
        rows_extracted: int | None = None,
    ) -> None:
        """
        Update progress metrics for a running extraction.

        Args:
            run_id: The run identifier
            windows_attempted: Number of windows attempted
            windows_completed: Number of windows successfully completed
            pages_fetched: Total pages fetched
            rows_extracted: Total rows extracted
        """
        updates = []
        params: list[Any] = []

        if windows_attempted is not None:
            updates.append("windows_attempted = ?")
            params.append(windows_attempted)
        if windows_completed is not None:
            updates.append("windows_completed = ?")
            params.append(windows_completed)
        if pages_fetched is not None:
            updates.append("pages_fetched = ?")
            params.append(pages_fetched)
        if rows_extracted is not None:
            updates.append("rows_extracted = ?")
            params.append(rows_extracted)

        if not updates:
            return

        params.append(run_id)
        query = f"""
            UPDATE baliza_state.extraction_runs
            SET {", ".join(updates)}
            WHERE run_id = ?
        """
        self.tracker.conn.execute(query, params)
        self.tracker.conn.commit()

    def complete_run(self, run_id: str, stats: dict[str, Any] | None = None) -> None:
        """
        Mark a run as completed successfully with final statistics.

        Args:
            run_id: The run identifier
            stats: Optional dictionary with final statistics (windows_completed, pages_fetched, etc.)
        """
        now = datetime.now(UTC).replace(tzinfo=None)
        stats = stats or {}

        self.tracker.conn.execute(
            """
            UPDATE baliza_state.extraction_runs
            SET status = 'completed',
                completed_at = ?,
                windows_completed = ?,
                pages_fetched = ?,
                rows_extracted = ?
            WHERE run_id = ?
            """,
            [
                now,
                stats.get("windows_completed", 0),
                stats.get("pages_fetched", 0),
                stats.get("rows_extracted", 0),
                run_id,
            ],
        )
        self.tracker.conn.commit()

    def fail_run(self, run_id: str, error: str) -> None:
        """
        Mark a run as failed with an error message.

        Args:
            run_id: The run identifier
            error: Error message describing the failure
        """
        now = datetime.now(UTC).replace(tzinfo=None)
        self.tracker.conn.execute(
            """
            UPDATE baliza_state.extraction_runs
            SET status = 'failed',
                completed_at = ?,
                error_message = ?
            WHERE run_id = ?
            """,
            [now, error[:1000], run_id],  # Limit error message length
        )
        self.tracker.conn.commit()

    # ------------------------------------------------------------------
    # State inspection
    # ------------------------------------------------------------------

    def get_run(self, run_id: str) -> ExtractionRun | None:
        """
        Get details for a specific run.

        Args:
            run_id: The run identifier

        Returns:
            ExtractionRun object or None if not found
        """
        row = self.tracker.conn.execute(
            """
            SELECT run_id, resource, started_at, completed_at, status,
                   windows_attempted, windows_completed, pages_fetched,
                   rows_extracted, error_message, config_hash
            FROM baliza_state.extraction_runs
            WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()

        if not row:
            return None

        return ExtractionRun(
            run_id=row[0],
            resource=row[1],
            started_at=row[2],
            completed_at=row[3],
            status=row[4],
            windows_attempted=row[5],
            windows_completed=row[6],
            pages_fetched=row[7],
            rows_extracted=row[8],
            error_message=row[9],
            config_hash=row[10],
        )

    def get_last_successful_run(self, resource: str) -> ExtractionRun | None:
        """
        Get the most recent successful run for a resource.

        Args:
            resource: Name of the resource

        Returns:
            ExtractionRun object or None if no successful runs exist
        """
        row = self.tracker.conn.execute(
            """
            SELECT run_id, resource, started_at, completed_at, status,
                   windows_attempted, windows_completed, pages_fetched,
                   rows_extracted, error_message, config_hash
            FROM baliza_state.extraction_runs
            WHERE resource = ? AND status = 'completed'
            ORDER BY completed_at DESC
            LIMIT 1
            """,
            [resource],
        ).fetchone()

        if not row:
            return None

        return ExtractionRun(
            run_id=row[0],
            resource=row[1],
            started_at=row[2],
            completed_at=row[3],
            status=row[4],
            windows_attempted=row[5],
            windows_completed=row[6],
            pages_fetched=row[7],
            rows_extracted=row[8],
            error_message=row[9],
            config_hash=row[10],
        )

    def get_last_run(self, resource: str) -> ExtractionRun | None:
        """
        Get the most recent run (any status) for a resource.

        Args:
            resource: Name of the resource

        Returns:
            ExtractionRun object or None if no runs exist
        """
        row = self.tracker.conn.execute(
            """
            SELECT run_id, resource, started_at, completed_at, status,
                   windows_attempted, windows_completed, pages_fetched,
                   rows_extracted, error_message, config_hash
            FROM baliza_state.extraction_runs
            WHERE resource = ?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            [resource],
        ).fetchone()

        if not row:
            return None

        return ExtractionRun(
            run_id=row[0],
            resource=row[1],
            started_at=row[2],
            completed_at=row[3],
            status=row[4],
            windows_attempted=row[5],
            windows_completed=row[6],
            pages_fetched=row[7],
            rows_extracted=row[8],
            error_message=row[9],
            config_hash=row[10],
        )

    def get_run_history(self, resource: str, limit: int = 10) -> list[ExtractionRun]:
        """
        Get recent run history for a resource.

        Args:
            resource: Name of the resource
            limit: Maximum number of runs to return

        Returns:
            List of ExtractionRun objects, newest first
        """
        rows = self.tracker.conn.execute(
            """
            SELECT run_id, resource, started_at, completed_at, status,
                   windows_attempted, windows_completed, pages_fetched,
                   rows_extracted, error_message, config_hash
            FROM baliza_state.extraction_runs
            WHERE resource = ?
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [resource, limit],
        ).fetchall()

        return [
            ExtractionRun(
                run_id=row[0],
                resource=row[1],
                started_at=row[2],
                completed_at=row[3],
                status=row[4],
                windows_attempted=row[5],
                windows_completed=row[6],
                pages_fetched=row[7],
                rows_extracted=row[8],
                error_message=row[9],
                config_hash=row[10],
            )
            for row in rows
        ]

    def get_running_runs(self, resource: str | None = None) -> list[ExtractionRun]:
        """
        Get all runs currently in 'running' status.

        Args:
            resource: Optional resource name to filter by

        Returns:
            List of ExtractionRun objects with status='running'
        """
        query = """
            SELECT run_id, resource, started_at, completed_at, status,
                   windows_attempted, windows_completed, pages_fetched,
                   rows_extracted, error_message, config_hash
            FROM baliza_state.extraction_runs
            WHERE status = 'running'
        """
        params: list[Any] = []

        if resource:
            query += " AND resource = ?"
            params.append(resource)

        query += " ORDER BY started_at DESC"
        rows = self.tracker.conn.execute(query, params).fetchall()

        return [
            ExtractionRun(
                run_id=row[0],
                resource=row[1],
                started_at=row[2],
                completed_at=row[3],
                status=row[4],
                windows_attempted=row[5],
                windows_completed=row[6],
                pages_fetched=row[7],
                rows_extracted=row[8],
                error_message=row[9],
                config_hash=row[10],
            )
            for row in rows
        ]

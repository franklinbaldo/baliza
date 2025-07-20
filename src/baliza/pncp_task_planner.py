import calendar
import logging
from datetime import date, timedelta
from typing import Any

from .config import get_all_active_endpoints


class PNCPTaskPlanner:
    """Handles the planning of PNCP data extraction tasks."""

    def _format_date(self, date_obj: date) -> str:
        """Format date for PNCP API (YYYYMMDD)."""
        return date_obj.strftime("%Y%m%d")

    def _monthly_chunks(
        self, start_date: date, end_date: date
    ) -> list[tuple[date, date]]:
        """Generate monthly date chunks (start to end of each month)."""
        chunks = []
        current = start_date

        while current <= end_date:
            # Get the first day of the current month
            month_start = current.replace(day=1)

            # Get the last day of the current month
            _, last_day = calendar.monthrange(current.year, current.month)
            month_end = current.replace(day=last_day)

            # Adjust for actual start/end boundaries
            chunk_start = max(month_start, start_date)
            chunk_end = min(month_end, end_date)

            chunks.append((chunk_start, chunk_end))

            # Move to first day of next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)

        return chunks

    def __init__(self, settings, writer_conn=None):
        self.settings = settings
        self.writer_conn = writer_conn

    async def plan_tasks(
        self, start_date: date, end_date: date
    ) -> list[tuple[str, str, date, Any]]:
        """Populate the control table with all necessary tasks.

        Optimized for resumable extractions - only creates new tasks that don't already exist.
        """
        existing_tasks = set()
        if self.writer_conn:
            try:
                existing_task_ids = self.writer_conn.execute(
                    "SELECT task_id FROM psa.pncp_extraction_tasks"
                ).fetchall()
                existing_tasks = {task_id for (task_id,) in existing_task_ids}
                logging.info(f"Found {len(existing_tasks):,} existing tasks in database")
            except Exception as e:
                logging.warning(f"Could not check existing tasks: {e}")
                existing_tasks = set()

        date_chunks = self._monthly_chunks(start_date, end_date)
        tasks_to_create = []
        total_planned = 0
        skipped_existing = 0

        endpoints = get_all_active_endpoints()
        for endpoint_name, endpoint_config in endpoints.items():
            modalidades = endpoint_config.get(
                "iterate_modalidades", [None]
            )

            for modalidade in modalidades:
                if endpoint_config.get("requires_single_date", False):
                    if endpoint_config.get("requires_future_date", False):
                        future_days = endpoint_config.get("future_days_offset", 1825)
                        future_date = date.today() + timedelta(days=future_days)
                        task_suffix = f"_modalidade_{modalidade}" if modalidade is not None else ""
                        task_id = f"{endpoint_name}_{future_date.isoformat()}{task_suffix}"

                        total_planned += 1
                        if task_id in existing_tasks:
                            skipped_existing += 1
                        else:
                            tasks_to_create.append(
                                (task_id, endpoint_name, future_date, modalidade)
                            )
                    else:
                        task_suffix = f"_modalidade_{modalidade}" if modalidade is not None else ""
                        task_id = f"{endpoint_name}_{end_date.isoformat()}{task_suffix}"

                        total_planned += 1
                        if task_id in existing_tasks:
                            skipped_existing += 1
                        else:
                            tasks_to_create.append(
                                (task_id, endpoint_name, end_date, modalidade)
                            )
                else:
                    for chunk_start, _ in date_chunks:
                        task_suffix = f"_modalidade_{modalidade}" if modalidade is not None else ""
                        task_id = f"{endpoint_name}_{chunk_start.isoformat()}{task_suffix}"

                        total_planned += 1
                        if task_id in existing_tasks:
                            skipped_existing += 1
                        else:
                            tasks_to_create.append(
                                (task_id, endpoint_name, chunk_start, modalidade)
                            )

        logging.info("Resumable Planning Results:")
        logging.info(f"   Total tasks planned: {total_planned:,}")
        logging.info(f"   Already exists: {skipped_existing:,}")
        logging.info(f"   New tasks to create: {len(tasks_to_create):,}")

        return tasks_to_create

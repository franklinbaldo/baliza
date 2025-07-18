import calendar
from datetime import date, timedelta
from typing import Any, List, Tuple

from baliza.config import PNCP_ENDPOINTS


class PNCPTaskPlanner:
    """Handles the planning of PNCP data extraction tasks."""

    def __init__(self):
        pass

    def _format_date(self, date_obj: date) -> str:
        """Format date for PNCP API (YYYYMMDD)."""
        return date_obj.strftime("%Y%m%d")

    def _monthly_chunks(
        self, start_date: date, end_date: date
    ) -> List[Tuple[date, date]]:
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

    async def plan_tasks(self, start_date: date, end_date: date) -> List[Tuple[str, str, date, Any]]:
        """Populate the control table with all necessary tasks."""
        date_chunks = self._monthly_chunks(start_date, end_date)
        tasks_to_create = []

        for endpoint in PNCP_ENDPOINTS:
            modalidades = endpoint.get(
                "iterate_modalidades", [None]
            )  # None means no modalidade iteration

            for modalidade in modalidades:
                if endpoint.get("requires_single_date", False):
                    # For single-date endpoints, create only one task with the end_date
                    # Special handling for endpoints that need future dates
                    if endpoint.get("requires_future_date", False):
                        # Use a future date for endpoints that need current/future dates
                        future_days = endpoint.get("future_days_offset", 1825)
                        future_date = date.today() + timedelta(days=future_days)
                        task_suffix = (
                            f"_modalidade_{modalidade}"
                            if modalidade is not None
                            else ""
                        )
                        task_id = (
                            f"{endpoint['name']}_{future_date.isoformat()}{task_suffix}"
                        )
                        tasks_to_create.append(
                            (task_id, endpoint["name"], future_date, modalidade)
                        )
                    else:
                        task_suffix = (
                            f"_modalidade_{modalidade}"
                            if modalidade is not None
                            else ""
                        )
                        task_id = (
                            f"{endpoint['name']}_{end_date.isoformat()}{task_suffix}"
                        )
                        tasks_to_create.append(
                            (task_id, endpoint["name"], end_date, modalidade)
                        )
                else:
                    # For range endpoints, use monthly chunking
                    for chunk_start, _ in date_chunks:
                        task_suffix = (
                            f"_modalidade_{modalidade}"
                            if modalidade is not None
                            else ""
                        )
                        task_id = (
                            f"{endpoint['name']}_{chunk_start.isoformat()}{task_suffix}"
                        )
                        tasks_to_create.append(
                            (task_id, endpoint["name"], chunk_start, modalidade)
                        )
        return tasks_to_create

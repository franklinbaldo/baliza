from datetime import datetime, timedelta
from typing import Iterator, Tuple

def date_range_slicer(
    start_date: datetime,
    end_date: datetime,
    chunk_days: int = 7
) -> Iterator[Tuple[datetime, datetime]]:
    """
    Slices a date range into smaller chunks.

    Args:
        start_date: The start of the date range.
        end_date: The end of the date range.
        chunk_days: The size of each chunk in days.

    Yields:
        A tuple with the start and end of each chunk.
    """
    current_date = start_date
    while current_date < end_date:
        chunk_end_date = current_date + timedelta(days=chunk_days - 1)
        if chunk_end_date > end_date:
            chunk_end_date = end_date
        yield current_date, chunk_end_date
        current_date += timedelta(days=chunk_days)

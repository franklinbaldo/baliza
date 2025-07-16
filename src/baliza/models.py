from enum import Enum


class TaskStatus(str, Enum):
    """Enum for task statuses."""

    PENDING = "PENDING"
    DISCOVERING = "DISCOVERING"
    FETCHING = "FETCHING"
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"

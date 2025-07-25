import logging
import sys
from prefect import get_run_logger
from .config import settings


def get_logger(name: str):
    """
    Get a logger that is compatible with Prefect and local execution.
    """
    try:
        # Try to get the Prefect run logger
        logger = get_run_logger()
    except Exception:
        # Fallback to a standard Python logger
        logger = logging.getLogger(name)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(settings.log_level.upper())

    return logger

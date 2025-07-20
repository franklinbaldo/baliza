"""
Handles the 'transform' command by running dbt.
"""

import logging
import subprocess

logger = logging.getLogger(__name__)

def transform(dbt_command: list[str] = None):
    """
    Runs the dbt transformation process.
    """
    if dbt_command is None:
        dbt_command = ["dbt", "run", "--project-dir", "dbt_baliza"]

    try:
        process = subprocess.Popen(
            dbt_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )
        if process.stdout:
            for line in process.stdout:
                logger.info(line.strip())
        process.wait()
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, dbt_command)
    except FileNotFoundError:
        logger.error("dbt not found. Please ensure dbt is installed and in your PATH.")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt process failed with exit code {e.returncode}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise

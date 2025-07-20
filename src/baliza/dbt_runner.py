"""dbt runner for executing dbt commands."""

import subprocess
import logging
from pathlib import Path

from .plan_fingerprint import PlanFingerprint

logger = logging.getLogger(__name__)

class DbtRunner:
    """Handles execution of dbt commands."""

    def __init__(self, dbt_project_dir: str = None):
        self.dbt_project_dir = dbt_project_dir or str(Path(__file__).parent.parent.parent / "dbt_baliza")

    def create_task_plan(self, start_date: str, end_date: str, environment: str = "prod") -> str:
        """Create a new task plan using dbt models.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            environment: Environment name

        Returns:
            Plan fingerprint for the generated plan
        """
        fingerprint = PlanFingerprint()
        plan_fingerprint = fingerprint.generate_fingerprint(start_date, end_date, environment)

        dbt_vars = {
            'plan_start_date': start_date,
            'plan_end_date': end_date,
            'plan_fingerprint': plan_fingerprint,
            'plan_environment': environment,
            'config_version': '1.0'
        }

        vars_str = ','.join([f'"{k}": "{v}"' for k, v in dbt_vars.items()])

        try:
            seed_cmd = ["uv", "run", "dbt", "seed", "--project-dir", self.dbt_project_dir, "--profiles-dir", self.dbt_project_dir]
            subprocess.run(seed_cmd, cwd=self.dbt_project_dir)
            deps_cmd = ["uv", "run", "dbt", "deps", "--project-dir", self.dbt_project_dir, "--profiles-dir", self.dbt_project_dir]
            subprocess.run(deps_cmd, cwd=self.dbt_project_dir)

            cmd = [
                "uv", "run", "dbt", "run", "--select", "planning",
                "--vars", f"{{{vars_str}}}",
                "--project-dir", self.dbt_project_dir,
                "--profiles-dir", self.dbt_project_dir
            ]

            result = subprocess.run(
                cmd,
                cwd=self.dbt_project_dir,
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                logger.error(f"dbt run failed: {result.stderr}")
                return None

            logger.info(f"✅ Generated task plan with fingerprint {plan_fingerprint[:16]}...")
            logger.info(f"dbt output: {result.stdout}")

            return plan_fingerprint

        except subprocess.CalledProcessError as e:
            logger.error(f"dbt run failed: {e.stderr}")
            return None

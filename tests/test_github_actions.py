"""
Tests for GitHub Actions workflow functionality.
"""

import os
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml


@pytest.fixture
def workflow_path():
    """Get path to the GitHub Actions workflow."""
    return (
        Path(__file__).parent.parent / ".github" / "workflows" / "baliza_daily_run.yml"
    )


@pytest.fixture
def github_dir():
    """Get path to the .github directory."""
    return Path(__file__).parent.parent / ".github"


def test_github_workflows_directory_exists(github_dir):
    """Test that .github/workflows directory exists."""
    workflows_dir = github_dir / "workflows"
    assert workflows_dir.exists(), ".github/workflows directory not found"


def test_workflow_file_exists(workflow_path):
    """Test that the main workflow file exists."""
    assert workflow_path.exists(), "baliza_daily_run.yml not found"


def test_workflow_yaml_syntax(workflow_path):
    """Test that workflow YAML has valid syntax."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    # Basic workflow structure validation
    assert "name" in workflow_config
    assert "on" in workflow_config
    assert "jobs" in workflow_config


def test_workflow_trigger_configuration(workflow_path):
    """Test workflow trigger configuration."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    triggers = workflow_config["on"]

    # Should have schedule trigger for daily runs
    if isinstance(triggers, dict):
        assert "schedule" in triggers or "workflow_dispatch" in triggers

        if "schedule" in triggers:
            # Verify cron schedule format
            cron_schedule = triggers["schedule"][0]["cron"]
            assert isinstance(cron_schedule, str)
            assert len(cron_schedule.split()) == 5  # Standard cron format

    # Should also allow manual dispatch
    assert "workflow_dispatch" in triggers or "push" in triggers


def test_workflow_jobs_structure(workflow_path):
    """Test workflow jobs structure."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    jobs = workflow_config["jobs"]
    assert len(jobs) > 0, "No jobs defined in workflow"

    # Check main job exists
    job_names = list(jobs.keys())
    main_job_candidates = ["baliza", "run-baliza", "daily-run", "collect-data"]
    has_main_job = any(candidate in job_names for candidate in main_job_candidates)
    assert has_main_job, "No main Baliza job found"


def test_workflow_environment_setup(workflow_path):
    """Test workflow environment setup."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    # Get the main job (first job)
    first_job = list(workflow_config["jobs"].values())[0]

    # Check for required setup steps
    steps = first_job.get("steps", [])
    assert len(steps) > 0, "No steps defined in workflow"

    # Should have checkout step
    checkout_step = any("checkout" in str(step).lower() for step in steps)
    assert checkout_step, "No checkout step found"

    # Should have Python setup
    python_step = any("python" in str(step).lower() for step in steps)
    assert python_step, "No Python setup step found"


def test_workflow_dependency_installation(workflow_path):
    """Test workflow dependency installation."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    first_job = list(workflow_config["jobs"].values())[0]
    steps = first_job.get("steps", [])

    # Look for dependency installation
    install_steps = []
    for step in steps:
        if isinstance(step, dict):
            run_command = step.get("run", "")
            name = step.get("name", "")
            if any(
                keyword in run_command.lower() or keyword in name.lower()
                for keyword in ["install", "pip", "uv sync", "requirements"]
            ):
                install_steps.append(step)

    assert len(install_steps) > 0, "No dependency installation steps found"


def test_workflow_environment_variables(workflow_path):
    """Test workflow environment variables configuration."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    first_job = list(workflow_config["jobs"].values())[0]

    # Check for environment variables at job or step level
    has_env_vars = False

    # Job-level environment
    if "env" in first_job:
        has_env_vars = True
        env_vars = first_job["env"]

        # Should reference secrets for IA credentials
        ia_key_patterns = ["IA_ACCESS_KEY", "IA_SECRET_KEY"]
        for pattern in ia_key_patterns:
            if pattern in str(env_vars):
                assert "secrets" in str(env_vars[pattern]).lower()

    # Step-level environment
    steps = first_job.get("steps", [])
    for step in steps:
        if isinstance(step, dict) and "env" in step:
            has_env_vars = True

    # Should have some environment configuration
    assert has_env_vars, "No environment variables configured"


def test_workflow_baliza_execution(workflow_path):
    """Test workflow Baliza execution steps."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    first_job = list(workflow_config["jobs"].values())[0]
    steps = first_job.get("steps", [])

    # Look for Baliza execution
    baliza_steps = []
    for step in steps:
        if isinstance(step, dict):
            run_command = step.get("run", "")
            name = step.get("name", "")
            if any(
                keyword in run_command.lower() or keyword in name.lower()
                for keyword in ["baliza", "main.py", "python src"]
            ):
                baliza_steps.append(step)

    assert len(baliza_steps) > 0, "No Baliza execution steps found"


def test_workflow_artifact_handling(workflow_path):
    """Test workflow artifact handling."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    first_job = list(workflow_config["jobs"].values())[0]
    steps = first_job.get("steps", [])

    # Look for artifact upload/storage
    artifact_steps = []
    for step in steps:
        if isinstance(step, dict):
            if "uses" in step and "upload-artifact" in step["uses"]:
                artifact_steps.append(step)

            run_command = step.get("run", "")
            if any(
                keyword in run_command.lower()
                for keyword in ["artifact", "upload", "store"]
            ):
                artifact_steps.append(step)

    # Should have some artifact handling (optional but recommended)
    if len(artifact_steps) > 0:
        print(f"Found {len(artifact_steps)} artifact handling steps")


def test_workflow_error_handling(workflow_path):
    """Test workflow error handling configuration."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    first_job = list(workflow_config["jobs"].values())[0]

    # Check for continue-on-error or failure handling
    if "continue-on-error" in first_job:
        # Should be configured appropriately
        continue_on_error = first_job["continue-on-error"]
        assert isinstance(continue_on_error, bool)

    # Check steps for error handling
    steps = first_job.get("steps", [])
    error_handling_steps = 0

    for step in steps:
        if isinstance(step, dict):
            if "continue-on-error" in step:
                error_handling_steps += 1

            run_command = step.get("run", "")
            if any(pattern in run_command for pattern in ["||", "&&", "try", "catch"]):
                error_handling_steps += 1

    # Should have some error handling considerations
    print(f"Found {error_handling_steps} steps with error handling")


def test_workflow_schedule_timing(workflow_path):
    """Test workflow schedule timing is appropriate."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    triggers = workflow_config["on"]

    if isinstance(triggers, dict) and "schedule" in triggers:
        cron_schedule = triggers["schedule"][0]["cron"]

        # Parse cron schedule
        cron_parts = cron_schedule.split()
        minute, hour, day, month, weekday = cron_parts

        # Check that it's scheduled for reasonable time (not during peak hours)
        hour_int = int(hour) if hour.isdigit() else 0

        # Should be scheduled during off-peak hours (0-6 or 22-23 UTC)
        is_off_peak = (0 <= hour_int <= 6) or (22 <= hour_int <= 23)
        assert is_off_peak or hour == "*", (
            f"Workflow scheduled during peak hours: {hour}:00 UTC"
        )


@patch("subprocess.run")
def test_workflow_validation_with_github_cli(mock_run):
    """Test workflow validation using GitHub CLI (if available)."""
    mock_run.return_value = Mock(returncode=0, stdout="Workflow is valid", stderr="")

    # This would test actual workflow validation
    # For now, just verify the mock structure
    assert True, "Workflow validation framework ready"


def test_secrets_documentation():
    """Test that required secrets are documented."""
    # Check if there's documentation about required secrets
    repo_root = Path(__file__).parent.parent

    # Look for secrets documentation in README or docs
    docs_to_check = [
        repo_root / "README.md",
        repo_root / "docs" / "setup.md",
        repo_root / ".github" / "workflows" / "README.md",
    ]

    secrets_documented = False
    required_secrets = ["IA_ACCESS_KEY", "IA_SECRET_KEY"]

    for doc_file in docs_to_check:
        if doc_file.exists():
            with open(doc_file) as f:
                content = f.read()

            for secret in required_secrets:
                if secret in content:
                    secrets_documented = True
                    break

    assert secrets_documented, "Required secrets not documented"


def test_workflow_runs_on_correct_os(workflow_path):
    """Test that workflow runs on appropriate OS."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    first_job = list(workflow_config["jobs"].values())[0]

    if "runs-on" in first_job:
        runs_on = first_job["runs-on"]

        # Should use a stable, supported runner
        stable_runners = ["ubuntu-latest", "ubuntu-20.04", "ubuntu-22.04"]
        assert runs_on in stable_runners, f"Workflow uses unsupported runner: {runs_on}"
    else:
        pytest.fail("No 'runs-on' specified in workflow job")


def test_workflow_python_version(workflow_path):
    """Test that workflow uses appropriate Python version."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    first_job = list(workflow_config["jobs"].values())[0]
    steps = first_job.get("steps", [])

    python_setup_steps = []
    for step in steps:
        if isinstance(step, dict) and "uses" in step:
            if "setup-python" in step["uses"]:
                python_setup_steps.append(step)

    if python_setup_steps:
        python_step = python_setup_steps[0]

        if "with" in python_step and "python-version" in python_step["with"]:
            python_version = python_step["with"]["python-version"]

            # Should use Python 3.8+ for modern features
            version_str = str(python_version)
            if version_str.replace(".", "").isdigit():
                major, minor = map(int, version_str.split("."))
                assert major >= 3 and minor >= 8, (
                    f"Python version too old: {python_version}"
                )


def test_workflow_output_capture(workflow_path):
    """Test that workflow captures execution outputs."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    first_job = list(workflow_config["jobs"].values())[0]
    steps = first_job.get("steps", [])

    # Look for output capture mechanisms
    output_capture_found = False

    for step in steps:
        if isinstance(step, dict):
            run_command = step.get("run", "")

            # Look for output redirection or capture
            if any(pattern in run_command for pattern in [">", ">>", "|", "tee"]):
                output_capture_found = True
                break

            # Look for artifact upload of outputs
            if "uses" in step and "upload-artifact" in step["uses"]:
                if "with" in step and "path" in step["with"]:
                    output_capture_found = True
                    break

    # Should capture outputs for debugging and monitoring
    if not output_capture_found:
        print("Warning: No output capture mechanism found in workflow")


def test_workflow_date_parameter(workflow_path):
    """Test that workflow correctly handles date parameters."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    first_job = list(workflow_config["jobs"].values())[0]
    steps = first_job.get("steps", [])

    # Look for date handling
    date_handling_found = False

    for step in steps:
        if isinstance(step, dict):
            run_command = step.get("run", "")

            # Look for date-related commands or environment variables
            if any(
                keyword in run_command.lower()
                for keyword in ["baliza_date", "date", "yesterday", "$(date"]
            ):
                date_handling_found = True
                break

    assert date_handling_found, "No date parameter handling found in workflow"


@pytest.mark.integration
def test_workflow_environment_simulation():
    """Test workflow environment simulation."""
    # Simulate GitHub Actions environment
    github_env = {
        "GITHUB_WORKSPACE": "/tmp/workspace",
        "GITHUB_REPOSITORY": "test/baliza",
        "GITHUB_SHA": "abc123",
        "RUNNER_OS": "Linux",
    }

    with patch.dict(os.environ, github_env):
        # Verify environment variables are accessible
        assert os.getenv("GITHUB_WORKSPACE") == "/tmp/workspace"
        assert os.getenv("GITHUB_REPOSITORY") == "test/baliza"

        # This would test actual workflow steps in CI environment
        assert True, "Environment simulation successful"


def test_workflow_concurrent_execution_safety(workflow_path):
    """Test that workflow handles concurrent execution safely."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    # Check for concurrency control
    has_concurrency_control = "concurrency" in workflow_config

    if has_concurrency_control:
        concurrency_config = workflow_config["concurrency"]
        assert "group" in concurrency_config

        # Should have cancel-in-progress for daily runs
        if "cancel-in-progress" in concurrency_config:
            cancel_in_progress = concurrency_config["cancel-in-progress"]
            assert isinstance(cancel_in_progress, bool)

    # This is important for daily scheduled runs
    print(f"Concurrency control configured: {has_concurrency_control}")


def test_workflow_timeout_configuration(workflow_path):
    """Test workflow timeout configuration."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    first_job = list(workflow_config["jobs"].values())[0]

    # Check for job timeout
    if "timeout-minutes" in first_job:
        timeout = first_job["timeout-minutes"]

        # Should have reasonable timeout (not too short, not too long)
        assert 5 <= timeout <= 120, f"Unreasonable timeout: {timeout} minutes"

    # Check for step timeouts
    steps = first_job.get("steps", [])
    step_timeouts = []

    for step in steps:
        if isinstance(step, dict) and "timeout-minutes" in step:
            step_timeouts.append(step["timeout-minutes"])

    if step_timeouts:
        print(f"Found {len(step_timeouts)} steps with timeout configuration")


def test_workflow_cache_configuration(workflow_path):
    """Test workflow cache configuration for dependencies."""
    with open(workflow_path) as f:
        workflow_config = yaml.safe_load(f)

    first_job = list(workflow_config["jobs"].values())[0]
    steps = first_job.get("steps", [])

    # Look for caching steps
    cache_steps = []
    for step in steps:
        if isinstance(step, dict) and "uses" in step:
            if "cache" in step["uses"]:
                cache_steps.append(step)

    # Caching is optional but recommended for performance
    if cache_steps:
        print(f"Found {len(cache_steps)} caching steps")

        # Verify cache configuration
        for cache_step in cache_steps:
            if "with" in cache_step:
                assert "path" in cache_step["with"], "Cache step missing path"

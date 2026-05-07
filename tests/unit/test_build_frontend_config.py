"""Pins the @drift-0 contract: the TS generator surfaces every active resource.

Drift-0 unified the legacy ``baliza.pncp_resources`` module into the live
``baliza.resources.RESOURCES`` registry. The script
``scripts/build_frontend_config.py`` is the bridge between the Python
registry and the web app's archive UI; if it ever stops covering a
registered resource, atas (or any future resource) silently disappears
from the website. This test runs the script in a temporary CWD and
parses its output to assert one TS entry per ``frontend_exposures`` entry
across ``RESOURCES``.
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path

from baliza.resources import RESOURCES

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "build_frontend_config.py"


def _expected_aliases() -> list[str]:
    aliases: list[str] = []
    for resource in RESOURCES.values():
        for exposure in resource.frontend_exposures:
            aliases.append(exposure.table_alias)
    return aliases


def test_generated_ts_lists_one_entry_per_registered_exposure(tmp_path: Path) -> None:
    # Copy the script + the source tree into tmp_path so we don't clobber
    # the real generated TS while tests are running in parallel.
    work = tmp_path / "repo"
    work.mkdir()
    (work / "scripts").mkdir()
    shutil.copy(SCRIPT, work / "scripts" / "build_frontend_config.py")
    shutil.copytree(REPO_ROOT / "src", work / "src")

    proc = subprocess.run(
        [sys.executable, str(work / "scripts" / "build_frontend_config.py")],
        cwd=work,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr

    out_file = work / "web" / "src" / "lib" / "generated" / "frontend_exposures.ts"
    assert out_file.exists(), "generator did not write the TS file"
    content = out_file.read_text()

    aliases_in_ts = re.findall(r"table_alias:\s*'([^']+)'", content)
    expected = _expected_aliases()

    assert aliases_in_ts == expected, (
        f"generated TS aliases {aliases_in_ts} do not match registry "
        f"frontend_exposures {expected}"
    )

    # And one entry per (resource, exposure) — no extras, no drops.
    artifact_names = re.findall(r"artifact_name:\s*'([^']+)'", content)
    assert len(artifact_names) == len(expected)


def test_every_registered_resource_declares_a_frontend_exposure() -> None:
    """Every active resource must show up in the website.

    A resource without ``frontend_exposures`` would still ingest data but
    the archive page would never list it — exactly the bug Drift-0 fixed
    for atas.
    """
    missing = [
        name for name, r in RESOURCES.items() if not r.frontend_exposures
    ]
    assert not missing, (
        f"resources missing frontend_exposures: {missing}. "
        "Either declare one or move the resource to PLANNED_RESOURCES."
    )

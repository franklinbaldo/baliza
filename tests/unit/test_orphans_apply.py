"""Safety tests for ``baliza orphans --apply``.

Locks the destructive-path guarantees so a regression that drops one
of them can't accidentally delete a canonical Parquet:

  * ``--apply`` requires ``IA_ACCESS_KEY`` + ``IA_SECRET_KEY`` in env.
  * Partial scans (any IA metadata fetch failure) refuse to delete.
  * Deletion is capped by ``--max-deletions``.
  * Only files outside the allow-list ever reach ``ia.delete``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()


# Manifest fixture used by every test — one canonical contratos row
# for 2024-04 so the orphans command picks a single per-month item.
_FAKE_MANIFEST = [
    {
        "data_particao": "2024-04",
        "table_name": "contratos",
        "file_type": "monthly_canonical",
        "ia_item_id": "baliza-pncp-2024-04",
    }
]

_ITEM_ID = "baliza-pncp-2024-04"


def _ia_metadata_response(file_names: list[str]) -> MagicMock:
    """httpx response stub matching the .json() interface used by orphans."""
    resp = MagicMock()
    resp.json.return_value = {"files": [{"name": n} for n in file_names]}
    return resp


@pytest.fixture
def patch_orphans_io(monkeypatch):
    """Wire baliza.cli_simple's IA-touching surface so the test never goes online.

    Returns a sentinel dict the test can assert against (which IA delete
    calls actually fired, with what filenames).
    """
    delete_calls: list[dict] = []

    def fake_delete(identifier, *, files, access_key, secret_key, **kwargs):
        for f in files:
            delete_calls.append(
                {
                    "identifier": identifier,
                    "file": f,
                    "access_key": access_key,
                    "secret_key": secret_key,
                }
            )

    monkeypatch.setattr("baliza.cli_simple.read_manifest_from_ia", lambda: _FAKE_MANIFEST)
    monkeypatch.setattr("baliza.cli_simple.ia.delete", fake_delete)
    return {"delete_calls": delete_calls}


def _patch_metadata(monkeypatch, file_names: list[str], *, fail: bool = False) -> None:
    """Stub out the per-month httpx.Client metadata fetch."""

    class _Client:
        def __init__(self, *_a, **_k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def get(self, _url):
            if fail:
                raise RuntimeError("simulated IA outage")
            return _ia_metadata_response(file_names)

    monkeypatch.setattr("baliza.cli_simple.httpx.Client", lambda *_a, **_k: _Client())


def test_apply_requires_ia_credentials(monkeypatch, patch_orphans_io):
    """Without IA keys, --apply must fail before any I/O."""
    monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IA_SECRET_KEY", raising=False)
    monkeypatch.delenv("IAS3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IAS3_SECRET_KEY", raising=False)
    _patch_metadata(monkeypatch, ["whatever.parquet"])

    result = runner.invoke(app, ["orphans", "--apply", "--months", "1"])
    assert result.exit_code != 0
    assert "IA_ACCESS_KEY" in result.output
    assert patch_orphans_io["delete_calls"] == [], "must not delete without keys"


def test_apply_refuses_partial_scan(monkeypatch, patch_orphans_io):
    """A fetch failure means we don't know what's there → no deletion."""
    monkeypatch.setenv("IA_ACCESS_KEY", "k")
    monkeypatch.setenv("IA_SECRET_KEY", "s")
    _patch_metadata(monkeypatch, [], fail=True)

    result = runner.invoke(app, ["orphans", "--apply", "--months", "1"])
    assert result.exit_code != 0
    assert patch_orphans_io["delete_calls"] == []


def test_apply_refuses_when_above_cap(monkeypatch, patch_orphans_io):
    """Cap is hard: the dry-run-style listing still happens, but
    `ia.delete` is never called when the count exceeds --max-deletions."""
    monkeypatch.setenv("IA_ACCESS_KEY", "k")
    monkeypatch.setenv("IA_SECRET_KEY", "s")
    orphans_present = [f"legacy-{i}.parquet" for i in range(10)] + [
        # Allowed files — must be ignored by the deletion plan even when
        # the count is over the cap.
        "contratos-2024-04.parquet",
        "raw-2024-04.zip",
    ]
    _patch_metadata(monkeypatch, orphans_present)

    result = runner.invoke(
        app, ["orphans", "--apply", "--months", "1", "--max-deletions", "3"]
    )
    assert result.exit_code != 0
    assert patch_orphans_io["delete_calls"] == [], "cap must short-circuit before deletion"


def test_apply_deletes_only_orphans(monkeypatch, patch_orphans_io):
    """Happy path: only files outside the allow-list ever reach ia.delete."""
    monkeypatch.setenv("IA_ACCESS_KEY", "k")
    monkeypatch.setenv("IA_SECRET_KEY", "s")
    files = [
        # ALLOWED — must NOT be deleted.
        "contratos-2024-04.parquet",
        "raw-2024-04.zip",
        "quarentena-2024-04.csv",
        f"{_ITEM_ID}_archive.torrent",
        f"{_ITEM_ID}_files.xml",
        f"{_ITEM_ID}_meta.sqlite",
        f"{_ITEM_ID}_meta.xml",
        # ORPHANS — must be deleted.
        "fornecedores-2024-04-01.parquet",
        "metadata-2024-04-01.json",
    ]
    _patch_metadata(monkeypatch, files)

    result = runner.invoke(app, ["orphans", "--apply", "--months", "1"])
    deleted = {c["file"] for c in patch_orphans_io["delete_calls"]}
    expected = {"fornecedores-2024-04-01.parquet", "metadata-2024-04-01.json"}
    assert deleted == expected, (
        f"only orphan files should be deleted; got {deleted}, expected {expected}. "
        f"Output:\n{result.output}"
    )
    # Belt-and-suspenders: assert canonicals are NOT in the deletion set.
    canonical = {
        "contratos-2024-04.parquet",
        "raw-2024-04.zip",
        "quarentena-2024-04.csv",
    }
    assert deleted.isdisjoint(canonical), (
        "canonical files must never reach ia.delete"
    )


def test_dry_run_default_does_not_delete(monkeypatch, patch_orphans_io):
    """No --apply flag means listing-only; ia.delete must never fire."""
    _patch_metadata(monkeypatch, ["legacy.parquet"])

    result = runner.invoke(app, ["orphans", "--months", "1"])
    # Exits 1 because orphans are present (listing-only signals via exit code).
    assert result.exit_code != 0
    assert patch_orphans_io["delete_calls"] == []

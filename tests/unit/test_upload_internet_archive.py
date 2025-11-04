from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import upload_internet_archive


class DummyFile:
    def __init__(self, exists: bool, size: int | None):
        self.exists = exists
        self.size = size


class DummyItem:
    def __init__(self, file: DummyFile):
        self._file = file

    def get_file(self, name: str) -> DummyFile:
        return self._file


def test_upload_writes_summary_and_calls_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = tmp_path / "contratos-2024-01.tar.gz"
    package.write_bytes(b"data")

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "dataset_version": "2024-01",
                "row_count": 10,
                "min_data": "2024-01-01",
                "max_data": "2024-01-31",
            }
        ),
        encoding="utf-8",
    )

    summary_path = tmp_path / "summary.json"

    calls: dict[str, int] = {"upload": 0}

    def fake_upload(identifier: str, **kwargs):
        calls["upload"] += 1
        assert identifier == "baliza-contratos-2024-01"
        assert kwargs["files"] == [str(package)]
        return [SimpleNamespace(status_code=200)]

    def fake_get_item(identifier: str, **kwargs):
        assert identifier == "baliza-contratos-2024-01"
        return DummyItem(DummyFile(exists=False, size=None))

    summary = upload_internet_archive.upload_dataset(
        package,
        "baliza-contratos-2024-01",
        manifest=json.loads(manifest_path.read_text(encoding="utf-8")),
        metadata_output=summary_path,
        get_item=fake_get_item,
        upload_func=fake_upload,
        access_key="key",
        secret_key="secret",
    )

    assert calls["upload"] == 1
    assert summary["uploaded"] is True
    assert summary_path.exists()
    stored = json.loads(summary_path.read_text(encoding="utf-8"))
    assert stored["metadata"]["mediatype"] == "data"
    assert stored["manifest"]["dataset_version"] == "2024-01"


def test_skip_when_same_file_exists(tmp_path: Path) -> None:
    package = tmp_path / "contratos-2024-02.tar.gz"
    data = b"another"
    package.write_bytes(data)

    summary_path = tmp_path / "summary.json"

    dummy_file = DummyFile(exists=True, size=len(data))
    dummy_item = DummyItem(dummy_file)

    def fake_get_item(identifier: str, **kwargs):
        return dummy_item

    def fake_upload(*args, **kwargs):  # pragma: no cover - should not be called
        raise AssertionError("upload should not be called when file exists")

    summary = upload_internet_archive.upload_dataset(
        package,
        "baliza-contratos-2024-02",
        metadata_output=summary_path,
        get_item=fake_get_item,
        upload_func=fake_upload,
        access_key="key",
        secret_key="secret",
    )

    assert summary["skipped"] is True
    assert summary["reason"] == "already_present"
    stored = json.loads(summary_path.read_text(encoding="utf-8"))
    assert stored["skipped"] is True

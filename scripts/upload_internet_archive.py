#!/usr/bin/env python3
"""Upload packaged datasets to the Internet Archive using direct S3 API.

The script is designed to be *resumable*: if the same file was previously
published for the same identifier, the upload is skipped and a structured
summary is emitted. The summary can be stored as a GitHub Actions artifact so
that the state of the Internet Archive stays in sync with the workflow run.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import httpx

Summary = dict[str, Any]


def _load_manifest(manifest_path: Path) -> dict[str, Any]:
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Invalid JSON manifest at {manifest_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("Manifest JSON must be an object")
    return data


def _build_metadata(
    manifest: dict[str, Any] | None,
    extra_metadata: Iterable[str],
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "mediatype": "data",
        "collection": "opensource",
        "language": "por",
        "subject": [
            "dados abertos",
            "brasil",
            "pncp",
            "contratos",
        ],
        "creator": "Projeto Baliza",
    }

    if manifest is not None:
        dataset_version = manifest.get("dataset_version")
        row_count = manifest.get("row_count")
        min_data = manifest.get("min_data")
        max_data = manifest.get("max_data")
        checksum = manifest.get("sha256")

        if dataset_version:
            metadata.setdefault("title", f"Baliza contratos {dataset_version}")
            metadata.setdefault("date", dataset_version)
            metadata.setdefault("dataset_version", dataset_version)

        description_lines: list[str] = []
        if dataset_version:
            description_lines.append(f"Versão do dataset: {dataset_version}")
        if row_count is not None:
            description_lines.append(f"Total de linhas: {row_count}")
        if min_data and max_data:
            description_lines.append(f"Intervalo de dados: {min_data} → {max_data}")
        elif min_data:
            description_lines.append(f"Data mínima: {min_data}")
        elif max_data:
            description_lines.append(f"Data máxima: {max_data}")
        if checksum:
            description_lines.append(f"SHA256 do manifesto: {checksum}")

        if description_lines:
            description = "\n".join(description_lines)
            metadata.setdefault("description", description)

    for pair in extra_metadata:
        if "=" not in pair:
            raise ValueError(f"Invalid metadata entry '{pair}'. Use the format key=value.")
        key, value = pair.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError("Metadata key cannot be empty.")
        metadata[key] = value

    return metadata


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _coerce_size(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return None


def _write_summary(metadata_output: Path | None, summary: Summary) -> None:
    if not metadata_output:
        return
    metadata_output.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _get_ia_s3_auth(access_key: str, secret_key: str) -> dict[str, str]:
    """Generate S3 auth headers for IA (LOW auth)."""
    return {"Authorization": f"LOW {access_key}:{secret_key}"}


def _check_remote_exists(
    client: httpx.Client,
    identifier: str,
    filename: str,
    headers: dict[str, str],
) -> int | None:
    """Check if file exists on IA and return its size if it does."""
    url = f"https://s3.us.archive.org/{identifier}/{filename}"
    try:
        response = client.head(url, headers=headers, timeout=10.0)
        if response.status_code == 200:
            content_length = response.headers.get("Content-Length")
            return int(content_length) if content_length else None
        return None
    except Exception:
        return None


def upload_dataset(
    file_path: Path,
    identifier: str,
    *,
    manifest: Mapping[str, Any] | None = None,
    extra_metadata: Iterable[str] = (),
    metadata_output: Path | None = None,
    force: bool = False,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> Summary:
    if not file_path.exists():
        raise FileNotFoundError(f"Package not found: {file_path}")

    manifest_dict: dict[str, Any] | None = dict(manifest) if manifest else None
    metadata = _build_metadata(manifest_dict, extra_metadata)

    access_key = access_key or _require_env("IA_ACCESS_KEY")
    secret_key = secret_key or _require_env("IA_SECRET_KEY")

    local_size = file_path.stat().st_size
    filename = file_path.name

    summary: Summary = {
        "identifier": identifier,
        "file": filename,
        "size": local_size,
        "metadata": metadata,
        "manifest": manifest_dict,
        "uploaded": False,
        "skipped": False,
    }

    auth_headers = _get_ia_s3_auth(access_key, secret_key)

    with httpx.Client() as client:
        # Check if already exists
        if not force:
            remote_size = _check_remote_exists(client, identifier, filename, auth_headers)
            if remote_size is not None and remote_size == local_size:
                summary.update(
                    {
                        "skipped": True,
                        "reason": "already_present",
                        "remote_size": remote_size,
                    }
                )
                _write_summary(metadata_output, summary)
                return summary

        # Prepare upload
        url = f"https://s3.us.archive.org/{identifier}/{filename}"

        # Headers
        headers = auth_headers.copy()
        headers["x-archive-auto-make-bucket"] = "1"
        headers["x-archive-queue-derive"] = "0"
        for k, v in metadata.items():
            headers[f"x-archive-meta-{k}"] = str(v)

        # Upload
        try:
            with open(file_path, "rb") as f:
                response = client.put(url, content=f, headers=headers, timeout=300.0)
                response.raise_for_status()
        except httpx.HTTPStatusError as e:
            sys.stderr.write(f"Upload failed with status {e.response.status_code} {e.response.reason_phrase}\n")
            sys.stderr.write(f"Response: {e.response.text}\n")
            raise SystemExit(1) from e
        except Exception as e:
            sys.stderr.write(f"Upload failed: {e}\n")
            raise SystemExit(1) from e

    summary["uploaded"] = True
    _write_summary(metadata_output, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload a Baliza archive to the Internet Archive")
    parser.add_argument(
        "--file",
        dest="file_path",
        type=Path,
        required=True,
        help="Path to the tarball to upload",
    )
    parser.add_argument(
        "--identifier",
        required=True,
        help="Internet Archive identifier for the item",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Path to manifest.json to enrich metadata",
    )
    parser.add_argument(
        "--metadata",
        action="append",
        default=[],
        help="Extra metadata entries in key=value format",
    )
    parser.add_argument(
        "--metadata-output",
        type=Path,
        help="Optional path to write a JSON summary of the upload",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force upload even if the file already exists on the Internet Archive",
    )
    args = parser.parse_args()

    manifest = _load_manifest(args.manifest) if args.manifest else None

    summary = upload_dataset(
        args.file_path,
        args.identifier,
        manifest=manifest,
        extra_metadata=args.metadata,
        metadata_output=args.metadata_output,
        force=args.force,
    )

    sys.stdout.write(json.dumps(summary, ensure_ascii=False) + "\n")


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()

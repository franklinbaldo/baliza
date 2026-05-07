"""Unit tests for the annual-only resource skip in IAConsolidator.

Pins two invariants:
1. PCA (partition_strategy=ANNUAL) is skipped by consolidate_year — the
   annual canonical is produced by ``baliza build``, not the consolidator.
   consolidate_year returns False immediately with no IA upload attempt.
2. Contratos (partition_strategy=MONTHLY) is NOT skipped — its normal
   consolidation path remains unaffected.
3. _current_year_is_fresh returns True immediately for annual resources
   (belt-and-suspenders guard for direct callers).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from baliza.consolidator import (
    IAConsolidator,
    _current_year_is_fresh,
    _is_monthly_partition_resource,
)

# ---------------------------------------------------------------------------
# _is_monthly_partition_resource
# ---------------------------------------------------------------------------


def test_is_monthly_partition_resource_contratos():
    assert _is_monthly_partition_resource("contratos") is True


def test_is_monthly_partition_resource_atas():
    assert _is_monthly_partition_resource("atas") is True


def test_is_monthly_partition_resource_publicacoes():
    assert _is_monthly_partition_resource("publicacoes") is True


def test_is_monthly_partition_resource_pca_is_false():
    """PCA uses ANNUAL partitioning — must return False so consolidate_year skips it."""
    assert _is_monthly_partition_resource("pca") is False


# ---------------------------------------------------------------------------
# _current_year_is_fresh belt-and-suspenders guard
# ---------------------------------------------------------------------------


def test_current_year_is_fresh_returns_true_for_annual_resource():
    """Annual resources have no monthly_canonical rows; freshness is vacuously True."""
    manifest = [
        {
            "data_particao": "2024",
            "table_name": "pca",
            "file_type": "annual_canonical",
            "uploaded_at": "2024-12-01T10:00:00",
        }
    ]
    assert _current_year_is_fresh(manifest, 2024, resource="pca") is True


# ---------------------------------------------------------------------------
# consolidate_year skips PCA entirely
# ---------------------------------------------------------------------------


def test_consolidate_year_skips_pca_no_ia_upload():
    """consolidate_year must return False for PCA without touching IA.

    Uses a synthetic manifest with both PCA and contratos rows; asserts:
    - PCA returns False immediately (skip path)
    - No IA upload is attempted for PCA (internetarchive.upload not called)
    """
    consolidator = IAConsolidator()

    # Synthetic manifest: one annual_canonical row for PCA, one monthly for contratos
    manifest = [
        {
            "data_particao": "2024",
            "table_name": "pca",
            "file_type": "annual_canonical",
            "parquet_url": "https://archive.org/download/baliza-pncp-consolidated/pca-2024.parquet",
            "uploaded_at": "2024-12-01T10:00:00",
        },
        {
            "data_particao": "2024-01",
            "table_name": "contratos",
            "file_type": "monthly_canonical",
            "parquet_url": "https://archive.org/download/baliza-pncp-2024-01/contratos.parquet",
            "uploaded_at": "2024-02-01T10:00:00",
        },
    ]

    with patch("baliza.consolidator.ia.upload") as mock_upload:
        result = consolidator.consolidate_year(
            year=2024,
            ia_access_key="k",
            ia_secret_key="s",
            force=False,
            manifest=manifest,
            resource="pca",
        )

    assert result is False, "consolidate_year must return False for annual-partition resource"
    mock_upload.assert_not_called(), "No IA upload must occur for PCA"


def test_consolidate_year_contratos_is_not_skipped_by_annual_guard():
    """The annual guard must not affect contratos — it reaches the normal path.

    We don't want a full IA upload in this test, so we stop the run at the
    point where it would check the frozen-year gate and fail because we have
    no real IA access. We confirm it got past the annual-skip guard by
    checking that ia.get_item (used by _check_consolidated_exists_on_ia) IS
    called — which only happens after the guard is cleared.
    """
    consolidator = IAConsolidator()

    manifest = [
        {
            "data_particao": "2023-01",
            "table_name": "contratos",
            "file_type": "monthly_canonical",
            "parquet_url": "https://archive.org/download/baliza-pncp-2023-01/contratos.parquet",
            "uploaded_at": "2023-02-01T10:00:00",
        },
    ]

    # 2023 is a frozen year — the consolidator checks IA for the existing file
    # before deciding whether to rebuild. If the annual-skip guard incorrectly
    # fires for contratos, ia.get_item would never be called.
    with patch("baliza.consolidator.ia.get_item") as mock_get_item:
        # Simulate "frozen year, file already exists" so the run stops early
        # (returns False for "already done") without needing real IA creds.
        mock_item = MagicMock()
        mock_item.files = [{"name": "contratos-2023.parquet"}]
        mock_get_item.return_value = mock_item

        result = consolidator.consolidate_year(
            year=2023,
            ia_access_key="k",
            ia_secret_key="s",
            force=False,
            manifest=manifest,
            resource="contratos",
        )

    # The frozen-year short-circuit returns False ("already done") — that's fine;
    # the important assertion is that get_item was called, proving the annual-skip
    # guard did NOT intercept contratos.
    mock_get_item.assert_called_once()
    assert result is False  # frozen year + file present = no rebuild needed

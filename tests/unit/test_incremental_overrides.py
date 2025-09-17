from __future__ import annotations

from copy import deepcopy

import pytest

from baliza.pipelines import pncp


@pytest.fixture()
def base_config() -> dict:
    return {
        "resources": [
            {
                "name": "contratos",
                "endpoint": {
                    "path": "/v1/contratos",
                    "incremental": {
                        "cursor_path": "dataAtualizacao",
                        "lag": 3 * pncp.SECONDS_PER_DAY,
                        "end_value": "placeholder",
                    },
                },
            }
        ]
    }


def test_apply_incremental_overrides_does_not_mutate_original(
    base_config: dict,
) -> None:
    original = deepcopy(base_config)
    pncp._apply_incremental_overrides(base_config, lookback_days=1)
    assert base_config == original


def test_apply_incremental_overrides_updates_lag(base_config: dict) -> None:
    adjusted = pncp._apply_incremental_overrides(base_config, lookback_days=5)
    incremental = adjusted["resources"][0]["endpoint"]["incremental"]
    assert incremental["lag"] == 5 * pncp.SECONDS_PER_DAY


def test_apply_incremental_overrides_removes_lag_when_zero(base_config: dict) -> None:
    adjusted = pncp._apply_incremental_overrides(base_config, lookback_days=0)
    incremental = adjusted["resources"][0]["endpoint"]["incremental"]
    assert "lag" not in incremental


def test_apply_incremental_overrides_sets_range_bounds(base_config: dict) -> None:
    adjusted = pncp._apply_incremental_overrides(
        base_config,
        range_start="2024-01-01",
        range_end="2024-01-31",
    )
    incremental = adjusted["resources"][0]["endpoint"]["incremental"]
    assert incremental["initial_value"] == "2024-01-01"
    assert incremental["end_value"] == "2024-01-31"


def test_apply_incremental_overrides_clears_end_value_when_not_provided(
    base_config: dict,
) -> None:
    adjusted = pncp._apply_incremental_overrides(base_config, range_start="2024-01-01")
    incremental = adjusted["resources"][0]["endpoint"]["incremental"]
    assert incremental["initial_value"] == "2024-01-01"
    assert "end_value" not in incremental


def test_apply_incremental_overrides_preserves_end_value_without_overrides(
    base_config: dict,
) -> None:
    adjusted = pncp._apply_incremental_overrides(base_config)
    incremental = adjusted["resources"][0]["endpoint"]["incremental"]
    assert incremental["end_value"] == "placeholder"


def test_apply_incremental_overrides_rejects_negative_lookback(
    base_config: dict,
) -> None:
    with pytest.raises(ValueError):
        pncp._apply_incremental_overrides(base_config, lookback_days=-1)

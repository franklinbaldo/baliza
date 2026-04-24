from datetime import date

import pytest

from baliza.constants import (
    RESOURCE_CONTRATOS,
    clamp_to_known_data_start_month,
    known_data_start_month,
    month_predates_known_data,
)


def test_known_data_start_month_for_contratos():
    assert known_data_start_month(RESOURCE_CONTRATOS) == date(2021, 9, 1)


def test_clamp_to_known_data_start_month_clamps_pre_history_date():
    assert clamp_to_known_data_start_month(RESOURCE_CONTRATOS, date(2020, 1, 1)) == date(
        2021, 9, 1
    )


def test_clamp_to_known_data_start_month_normalizes_later_date_to_month_start():
    assert clamp_to_known_data_start_month(RESOURCE_CONTRATOS, date(2022, 3, 15)) == date(
        2022, 3, 1
    )


def test_month_predates_known_data():
    assert month_predates_known_data(RESOURCE_CONTRATOS, date(2021, 8, 1)) is True
    assert month_predates_known_data(RESOURCE_CONTRATOS, date(2021, 9, 1)) is False


def test_unknown_resource_raises_descriptive_error():
    with pytest.raises(ValueError, match="No known PNCP data start configured"):
        known_data_start_month("atas")


def test_clamp_to_known_data_start_month_unknown_resource_passes_through():
    assert clamp_to_known_data_start_month("atas", date(2020, 1, 15)) == date(2020, 1, 1)

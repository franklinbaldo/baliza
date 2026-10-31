from __future__ import annotations

import json
from datetime import datetime

import pytest

from baliza.coverage import IncompletePageError, modalidade_coverage
from baliza.publicacoes_capture import PaginationUnstableError, capture_modalidade

COMBO = {"codigoModalidadeContratacao": 6}
START = datetime(2026, 10, 1)
END = datetime(2026, 10, 31)


class LivePNCP:
    def __init__(self, root, totals, *, short_tail_attempts=0):
        self.root = root
        self.totals = iter(totals)
        self.total = 51
        self.short_tail_attempts = short_tail_attempts
        self.fetches = []
        self.probes = 0

    def path(self, page):
        return self.root / f"publicacoes_codigoModalidadeContratacao6_p{page}.json"

    def write(self, page, total, *, short=False):
        count = min(50, total - (page - 1) * 50)
        rows = [{"numeroControlePNCP": str(i)} for i in range((page - 1) * 50, (page - 1) * 50 + count)]
        if short:
            rows = []
        self.path(page).write_text(json.dumps({"data": rows, "totalPaginas": (total + 49) // 50,
                                               "totalRegistros": total}))

    def probe_range(self, *args, **kwargs):
        self.probes += 1
        self.total = next(self.totals, self.total)
        self.write(1, self.total)
        return {"total_pages": (self.total + 49) // 50, "total_registries": self.total}

    def fetch_page(self, resource, start, end, page, **kwargs):
        self.fetches.append(page)
        short = page == (self.total + 49) // 50 and self.short_tail_attempts > 0
        if short:
            self.short_tail_attempts -= 1
        self.write(page, self.total, short=short)


def capture(source, root):
    return capture_modalidade(source, root, COMBO, START, END, lambda _: None)


def test_growth_during_capture_refetches_only_tail(tmp_path):
    source = LivePNCP(tmp_path, [51, 52, 52, 52])
    row, _, _ = capture(source, tmp_path)
    assert row["records"] == 52
    assert source.fetches == [2, 2]
    assert source.probes == 4


def test_short_tail_retries_without_deleting_full_pages(tmp_path):
    source = LivePNCP(tmp_path, [101] * 4, short_tail_attempts=1)
    source.write(2, 101)
    full = source.path(2).read_bytes()
    row, _, _ = capture(source, tmp_path)
    assert row["records"] == 101
    assert source.fetches.count(2) == 1  # old cached tail refreshed once
    assert source.fetches.count(3) == 2
    assert source.path(2).read_bytes() == full


def test_permanent_short_tail_exhausts_budget_and_retains_cache(tmp_path):
    source = LivePNCP(tmp_path, [101] * 6, short_tail_attempts=10)
    with pytest.raises(PaginationUnstableError, match="expected 1, got 0"):
        capture(source, tmp_path)
    assert source.probes == 6
    assert source.fetches.count(2) == 1
    assert source.path(2).exists()
    assert source.path(3).exists()
    assert not (tmp_path / "_coverage.json").exists()


def test_shrinking_totals_do_not_leave_surplus_pages(tmp_path):
    source = LivePNCP(tmp_path, [51, 50, 50, 50])
    row, _, _ = capture(source, tmp_path)
    assert row["records"] == 50
    assert not source.path(2).exists()


def test_typed_page_error_contains_expected_and_observed_counts(tmp_path):
    source = LivePNCP(tmp_path, [])
    source.write(1, 51)
    source.write(2, 51, short=True)
    with pytest.raises(IncompletePageError) as caught:
        modalidade_coverage(tmp_path, COMBO, 2, 51)
    assert (caught.value.page, caught.value.expected, caught.value.actual) == (2, 1, 0)


def test_network_failure_preserves_prior_pages(tmp_path):
    source = LivePNCP(tmp_path, [101])

    def fail(*args, **kwargs):
        raise TimeoutError("PNCP timeout")

    source.fetch_page = fail
    with pytest.raises(TimeoutError):
        capture(source, tmp_path)
    assert source.path(1).exists()


def test_rollover_requeues_uncertified_cached_month(monkeypatch, tmp_path):
    from baliza.mirror import _pending_mirror_months

    monkeypatch.chdir(tmp_path)
    raw = tmp_path / "data/raw/2026-10"
    raw.mkdir(parents=True)
    (raw / "publicacoes_codigoModalidadeContratacao6_p1.json").write_text('{}')
    monkeypatch.setattr("baliza.mirror.read_manifest_from_ia", lambda: [{
        "table_name": "publicacoes", "data_particao": "2026-10", "raw_zip_url": "old-zip",
    }])
    monkeypatch.setattr("baliza.mirror.current_partition_start", lambda _: datetime(2026, 11, 1).date())
    monkeypatch.setattr("baliza.mirror.previous_partition_start", lambda _: START.date())
    assert START.date() in _pending_mirror_months(START.date(), None, resource="publicacoes")

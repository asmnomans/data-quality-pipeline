"""A filter drives unexpected_count to zero by DELETING the offending rows.

The efficacy gate can't tell that apart from a real repair, so which one you
get is pure model whim - one run imputed 5000 customer_ids, the next filtered
them, same code and same data. For columns whose rows are otherwise valid, the
row loss is itself the failure, and saying so feeds the self-repair loop.
"""
from dataclasses import dataclass, field

import dq_framework.pipeline.stages as stages


@dataclass
class _Failure:
    expectation_type: str = "expect_column_values_to_be_between"
    column: str = "order_timestamp"
    expectation_kwargs: dict = field(default_factory=dict)
    unexpected_count: int = 51


@dataclass
class _EngineFailure:
    unexpected_count: int


@dataclass
class _Result:
    failures: list


class _Engine:
    def dry_run_single(self, df, module, exp_def):
        return _Result([])  # fully resolved either way - only row loss differs


class _RCA:
    suggested_pyspark_fix = "cleaned_df = df"


def _patch(monkeypatch, kept_rows):
    @dataclass
    class Frame:
        n: int

        def count(self):
            return self.n

    @dataclass
    class R:
        passed: bool = True
        notes: str = "Passed sandboxed execution."
        cleaned_df: object = None

    monkeypatch.setattr(stages, "apply_fix", lambda code, df: R(cleaned_df=Frame(kept_rows)))


def test_rejects_a_filter_when_enabled(monkeypatch):
    _patch(monkeypatch, kept_rows=49949)
    result = stages._check_fix(
        _Engine(), None, _RCA(), None, _Failure(), repair_not_drop=True, row_count=50000
    )
    assert not result.passed
    assert "deleted 51 rows" in result.notes
    assert "median_value" in result.notes  # tells the repair loop what to do instead


def test_accepts_an_in_place_repair(monkeypatch):
    _patch(monkeypatch, kept_rows=50000)  # withColumn keeps every row
    result = stages._check_fix(
        _Engine(), None, _RCA(), None, _Failure(), repair_not_drop=True, row_count=50000
    )
    assert result.passed


def test_applies_to_every_column_not_just_the_repairable_ones(monkeypatch):
    """One boolean, no per-column carve-out: customer_email is protected too."""
    _patch(monkeypatch, kept_rows=49991)
    failure = _Failure(column="customer_email", unexpected_count=9)
    result = stages._check_fix(
        _Engine(), None, _RCA(), None, failure, repair_not_drop=True, row_count=50000
    )
    assert not result.passed
    assert "deleted 9 rows" in result.notes


def test_disabled_preserves_previous_behaviour(monkeypatch):
    _patch(monkeypatch, kept_rows=49949)
    result = stages._check_fix(_Engine(), None, _RCA(), None, _Failure(), repair_not_drop=False, row_count=50000)
    assert result.passed

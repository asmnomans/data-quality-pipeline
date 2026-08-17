"""A fix must actually reduce the failure it was generated for.

Safety, executability and schema-preservation all passed for
`filter(F.col('order_amount').isNotNull())` proposed against a *range*
failure - and it removed 0 of 6 offending rows, because none were null. It was
auto-promoted four times over and replayed on every later run. None of the
existing gates ask whether a fix worked; this one does.
"""
from dataclasses import dataclass, field

import dq_framework.pipeline.stages as stages


@dataclass
class _Failure:
    expectation_type: str = "expect_column_values_to_be_between"
    column: str = "order_amount"
    expectation_kwargs: dict = field(default_factory=lambda: {"column": "order_amount", "min_value": 10.01})
    unexpected_count: int = 6


@dataclass
class _EngineFailure:
    unexpected_count: int


@dataclass
class _Result:
    failures: list


class _Engine:
    """Reports how many rows still violate the expectation after the fix."""

    def __init__(self, remaining):
        self.remaining = remaining

    def dry_run_single(self, df, module, exp_def):
        return _Result([_EngineFailure(self.remaining)] if self.remaining else [])


class _RCA:
    suggested_pyspark_fix = "cleaned_df = df"


def _patch_apply_fix(monkeypatch, passed=True):
    @dataclass
    class R:
        passed: bool
        notes: str = "Passed sandboxed execution."
        cleaned_df: object = None

    monkeypatch.setattr(stages, "apply_fix", lambda code, df: R(passed))
    return R


def test_rejects_a_fix_that_removes_no_offending_rows(monkeypatch):
    _patch_apply_fix(monkeypatch)
    result = stages._check_fix(_Engine(remaining=6), None, _RCA(), None, _Failure())
    assert not result.passed
    assert "changed nothing" in result.notes
    assert "6 of 6" in result.notes  # the count is fed back so a repair can act on it


def test_accepts_a_fix_that_reduces_the_failure(monkeypatch):
    _patch_apply_fix(monkeypatch)
    result = stages._check_fix(_Engine(remaining=0), None, _RCA(), None, _Failure())
    assert result.passed


def test_partial_improvement_still_counts_as_progress(monkeypatch):
    _patch_apply_fix(monkeypatch)
    result = stages._check_fix(_Engine(remaining=2), None, _RCA(), None, _Failure())
    assert result.passed


def test_a_fix_that_makes_it_worse_is_rejected(monkeypatch):
    _patch_apply_fix(monkeypatch)
    result = stages._check_fix(_Engine(remaining=9), None, _RCA(), None, _Failure())
    assert not result.passed


def test_sandbox_rejection_short_circuits_before_measuring(monkeypatch):
    """No point asking whether a snippet worked when it never ran."""
    _patch_apply_fix(monkeypatch, passed=False)

    class Explode:
        def dry_run_single(self, *a):
            raise AssertionError("must not measure a fix that failed the sandbox")

    assert not stages._check_fix(Explode(), None, _RCA(), None, _Failure()).passed


def test_unmeasurable_failure_does_not_block_the_fix(monkeypatch):
    """No matching failure record - can't judge efficacy, so don't pretend to."""
    _patch_apply_fix(monkeypatch)
    assert stages._check_fix(_Engine(remaining=6), None, _RCA(), None, None).passed

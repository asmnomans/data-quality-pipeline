"""run_remediation_loop's stop-condition logic - the whole point of the
`loop_till_no_exception` feature (docs/ARCHITECTURE.md section 6.2): keep
looping until the data is fully clean, until two consecutive loops leave the
IDENTICAL set of primary keys still failing (no further progress possible),
or until `max_loops` is hit - whichever comes first. `loop_till_no_exception
= False` must always stop after exactly one loop, regardless of outcome.

extract_failures/run_rca/process_remediation all need a real Spark
DataFrame + GX engine + LLM provider in production, so they're monkeypatched
here to isolate pure loop control flow - the only thing under test is
whether run_remediation_loop reads `close_loop`'s EngineValidationResult
sequence and decides correctly when to stop.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dq_framework.core.config import ModuleConfig, SourceConfig
from dq_framework.core.models import Candidate, RCAReport, RCAResult
from dq_framework.pipeline import stages
from dq_framework.validation.base import EngineValidationResult


@pytest.fixture
def module(tmp_path):
    module_dir = tmp_path / "modules" / "orders"
    module_dir.mkdir(parents=True)
    return ModuleConfig(
        name="orders",
        source=SourceConfig(baseline_path="x", incoming_path="y"),
        primary_key="order_id",
        module_dir=module_dir,
    )


class _FakeRuleStore:
    def get_active_suite(self):
        return {"expectations": [], "suite_name": "orders_suite", "version": 1}


def _result(pk_reasons: dict) -> EngineValidationResult:
    return EngineValidationResult(
        success=not pk_reasons, total_rows=100, failures=[], failing_row_reasons=pk_reasons
    )


def _rca_result() -> RCAResult:
    return RCAResult(
        failure_category="Data Entry Error",
        root_cause_explanation="Test explanation.",
        suggested_pyspark_fix="cleaned_df = df",
        new_ge_expectation={"expectation_type": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "x"}},
    )


def _patch_loop_internals(monkeypatch, close_loop_sequence: list[EngineValidationResult]):
    """extract_failures/run_rca/process_remediation are no-ops; close_loop
    hands back the next canned result from `close_loop_sequence` on each
    call - that sequence alone drives every stop-condition decision."""
    calls = {"n": 0}

    def fake_extract_failures(*args, **kwargs):
        return object()  # opaque - fake_run_rca below never inspects it

    def fake_run_rca(*args, **kwargs):
        return RCAReport(run_id="r", module="orders", generated_at=datetime.now(timezone.utc), results=[])

    def fake_process_remediation(
        engine, rule_store, module, baseline_df, dirty_df, rca_report, run_id,
        fix_promotion_mode, rule_promotion_mode, iteration=1, **kwargs,
    ):
        candidate = Candidate(
            candidate_id=f"cand-loop{iteration}",
            run_id=run_id,
            module=module.name,
            artifact_type="fix",
            created_at=datetime.now(timezone.utc),
            rca_result=_rca_result(),
            sandbox_passed=True,
            iteration=iteration,
        )
        return dirty_df, [candidate]

    def fake_close_loop(engine, module, rule_store, df):
        idx = calls["n"]
        calls["n"] += 1
        return close_loop_sequence[idx]

    monkeypatch.setattr(stages, "extract_failures", fake_extract_failures)
    monkeypatch.setattr(stages, "run_rca", fake_run_rca)
    monkeypatch.setattr(stages, "process_remediation", fake_process_remediation)
    monkeypatch.setattr(stages, "close_loop", fake_close_loop)


def _run_loop(module, monkeypatch, initial, close_loop_sequence, loop_till_no_exception=True, max_loops=5):
    _patch_loop_internals(monkeypatch, close_loop_sequence)
    return stages.run_remediation_loop(
        engine=None,
        rule_store=_FakeRuleStore(),
        module=module,
        provider=None,
        baseline_df=None,
        dirty_df="dirty_df_placeholder",
        initial_result=initial,
        run_id="run-1",
        source_ref="data/incoming/orders/dirty_orders_50k.csv",
        fix_promotion_mode="auto",
        rule_promotion_mode="auto",
        loop_till_no_exception=loop_till_no_exception,
        max_loops=max_loops,
        max_sample_rows=8,
        mask_pii=True,
    )


def test_stops_clean_as_soon_as_a_loop_fully_resolves(module, monkeypatch):
    initial = _result({"ORD-1": ["a"]})
    outcome = _run_loop(module, monkeypatch, initial, close_loop_sequence=[_result({})])

    assert outcome.stop_reason == "clean"
    assert outcome.loops_run == 1
    assert outcome.final_result.success


def test_stops_on_no_progress_when_two_consecutive_loops_match(module, monkeypatch):
    initial = _result({"ORD-1": ["a"]})
    stuck = _result({"ORD-2": ["b"]})  # identical set both times - nothing is helping
    outcome = _run_loop(module, monkeypatch, initial, close_loop_sequence=[stuck, stuck])

    assert outcome.stop_reason == "no_progress"
    assert outcome.loops_run == 2


def test_progress_every_loop_keeps_going_until_max_loops(module, monkeypatch):
    initial = _result({"ORD-1": ["a"]})
    # a DIFFERENT single row fails each loop - never identical to the one before, never empty
    sequence = [_result({f"ORD-{i}": ["a"]}) for i in range(2, 8)]
    outcome = _run_loop(module, monkeypatch, initial, close_loop_sequence=sequence, max_loops=5)

    assert outcome.stop_reason == "max_loops_reached"
    assert outcome.loops_run == 5


def test_loop_till_no_exception_false_stops_after_one_loop_regardless(module, monkeypatch):
    initial = _result({"ORD-1": ["a"]})
    # same row still failing after loop 1 - with loop_till_no_exception=True this
    # would need a second loop before "no_progress" could even be detected;
    # with it False, it must stop right here anyway.
    still_failing = _result({"ORD-1": ["a"]})
    outcome = _run_loop(
        module, monkeypatch, initial, close_loop_sequence=[still_failing], loop_till_no_exception=False
    )

    assert outcome.stop_reason == "single_pass_mode"
    assert outcome.loops_run == 1


def test_loop_till_no_exception_false_still_reports_clean_if_the_first_loop_resolves_it(module, monkeypatch):
    initial = _result({"ORD-1": ["a"]})
    outcome = _run_loop(
        module, monkeypatch, initial, close_loop_sequence=[_result({})], loop_till_no_exception=False
    )

    # a clean result is more informative than a blanket "single_pass_mode"
    assert outcome.stop_reason == "clean"
    assert outcome.loops_run == 1


def test_candidates_are_tagged_with_their_loop_number(module, monkeypatch):
    initial = _result({"ORD-1": ["a"]})
    stuck = _result({"ORD-2": ["b"]})
    outcome = _run_loop(module, monkeypatch, initial, close_loop_sequence=[stuck, stuck])

    assert [c.iteration for c in outcome.candidates] == [1, 2]
    assert [c.candidate_id for c in outcome.candidates] == ["cand-loop1", "cand-loop2"]


def test_iterations_history_records_remaining_row_counts_per_loop(module, monkeypatch):
    initial = _result({"ORD-1": ["a"], "ORD-2": ["a"]})
    outcome = _run_loop(module, monkeypatch, initial, close_loop_sequence=[_result({"ORD-2": ["a"]}), _result({})])

    assert [i["loop"] for i in outcome.iterations] == [1, 2]
    assert [i["remaining_rows"] for i in outcome.iterations] == [1, 0]


def test_already_clean_initial_result_never_enters_the_loop(module, monkeypatch):
    initial = _result({})  # nothing failing to begin with
    outcome = _run_loop(module, monkeypatch, initial, close_loop_sequence=[])

    assert outcome.loops_run == 0
    assert outcome.stop_reason == "clean"
    assert outcome.candidates == []

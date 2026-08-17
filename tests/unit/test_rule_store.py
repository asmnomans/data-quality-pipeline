"""The whole point of rule_store.py: every promotion archives the prior
version (never overwrites in place) and appends an audit-log entry - and
rollback is just another logged promotion, not a special case.
"""
from datetime import datetime, timezone

import pytest

from dq_framework.core.config import ModuleConfig, SourceConfig
from dq_framework.core.models import RCAResult
from dq_framework.rules.rule_store import RuleStore


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


@pytest.fixture
def rca_result():
    return RCAResult(
        failure_category="Data Entry Error",
        root_cause_explanation="Test explanation.",
        suggested_pyspark_fix="cleaned_df = df",
        new_ge_expectation={"expectation_type": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "x"}},
    )


def test_active_suite_starts_at_version_zero(module):
    store = RuleStore(module)
    assert store.get_active_suite()["version"] == 0


def test_promote_archives_before_overwriting(module, rca_result):
    store = RuleStore(module)
    store.write_active_suite([{"expectation_type": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "order_id"}}])

    candidate = store.add_candidate("run-1", "rule", rca_result, sandbox_passed=True, sandbox_notes="ok")
    store.promote(candidate.candidate_id, actor="system:auto")

    active = store.get_active_suite()
    assert active["version"] == 2  # v1 (calibration) -> v2 (this promotion)
    assert len(active["expectations"]) == 2

    archive_files = list((module.module_dir / "expectations" / "archive").glob("*.json"))
    assert len(archive_files) == 1  # the pre-promotion (v1) snapshot, preserved


def test_promote_logs_to_promotion_log(module, rca_result):
    store = RuleStore(module)
    candidate = store.add_candidate("run-1", "fix", rca_result, sandbox_passed=True, sandbox_notes="ok")
    store.promote(candidate.candidate_id, actor="system:auto")

    log_path = module.module_dir / "expectations" / "promotion_log.jsonl"
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    assert '"action":"auto_promoted"' in lines[0].replace(" ", "")


def test_promote_is_idempotent_under_double_submission(module, rca_result):
    """A double-submitted approval (slow network + an impatient double-click
    is normal, not exotic) must not re-archive/re-apply a second time."""
    store = RuleStore(module)
    candidate = store.add_candidate("run-1", "fix", rca_result, sandbox_passed=True, sandbox_notes="ok")

    store.promote(candidate.candidate_id, actor="user:test")
    store.promote(candidate.candidate_id, actor="user:test")  # double-submit
    store.promote(candidate.candidate_id, actor="user:test")  # and again

    active = store.get_active_fixes()
    assert active["version"] == 1  # only ever incremented once
    assert len(active["fixes"]) == 1  # not duplicated three times

    log_path = module.module_dir / "expectations" / "promotion_log.jsonl"
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1  # only the first promotion was ever logged


def test_reject_never_touches_active_artifact(module, rca_result):
    store = RuleStore(module)
    before = store.get_active_suite()
    candidate = store.add_candidate("run-1", "rule", rca_result, sandbox_passed=False, sandbox_notes="failed dry-run")
    store.reject(candidate.candidate_id, actor="user:test", reason="failed dry-run")

    after = store.get_active_suite()
    assert before == after
    assert store.list_candidates("pending") == []
    assert len(store.list_candidates("rejected")) == 1


def test_rollback_restores_archived_version_and_is_itself_logged(module, rca_result):
    store = RuleStore(module)
    store.write_active_suite([{"expectation_type": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "order_id"}}])
    v1_active_snapshot = store.get_active_suite()

    candidate = store.add_candidate("run-1", "rule", rca_result, sandbox_passed=True, sandbox_notes="ok")
    store.promote(candidate.candidate_id, actor="system:auto")
    assert store.get_active_suite()["version"] == 2

    archive_ref = (module.module_dir / "expectations" / "archive").glob("*.json")
    archive_path = next(iter(archive_ref))

    store.rollback("rule", str(archive_path), actor="user:test")
    restored = store.get_active_suite()
    assert restored["expectations"] == v1_active_snapshot["expectations"]

    log_path = module.module_dir / "expectations" / "promotion_log.jsonl"
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2  # the promote + the rollback, both logged
    assert '"action":"rolled_back"' in lines[-1].replace(" ", "")

"""The point of a self-healing pipeline is that a guardrail rejection heals
inside the run. Before this, the sandbox produced an exact reason ("'(' was
never closed"), stored it on the candidate, and threw it away - so every later
loop re-derived the same rejected snippet and only a human editing the prompt
could move the needle.
"""
from dataclasses import dataclass

import dq_framework.pipeline.stages as stages


@dataclass
class _Check:
    passed: bool
    notes: str


class _RCA:
    def __init__(self, fix):
        self.suggested_pyspark_fix = fix
        self.new_ge_expectation = {}
        self.source_column = "order_timestamp"
        self.source_expectation_type = "ExpectColumnValuesToBeBetween"
        self.provider_used = None


def test_retries_with_the_rejection_reason_and_keeps_the_repaired_artifact(monkeypatch):
    seen_notes = []

    def fake_repair(provider, module, rca_result, artifact, error_note):
        seen_notes.append(error_note)
        return _RCA("cleaned_df = df.filter((F.col('t') >= 1) & (F.col('t') <= 2))")

    monkeypatch.setattr(stages, "repair_rca_result", fake_repair)

    def check(r):
        """Stand-in for the sandbox: balanced parens pass, unbalanced don't."""
        code = r.suggested_pyspark_fix
        if code.count("(") == code.count(")"):
            return _Check(True, "Passed sandboxed execution.")
        return _Check(False, "Syntax error: '(' was never closed")

    result, check_result = stages._attempt_with_repair(
        None, None, _RCA("cleaned_df = df.filter((F.col('t') >= 1 & F.col('t') <= 2)"),
        "fix", check, max_repair_attempts=2,
    )
    assert check_result.passed
    assert result.suggested_pyspark_fix.count("(") == result.suggested_pyspark_fix.count(")")
    assert seen_notes == ["Syntax error: '(' was never closed"]  # the reason was actually handed back


def test_gives_up_after_the_configured_attempts(monkeypatch):
    calls = []

    def fake_repair(provider, module, rca_result, artifact, error_note):
        calls.append(1)
        return _RCA("still broken")

    monkeypatch.setattr(stages, "repair_rca_result", fake_repair)
    _result, check_result = stages._attempt_with_repair(
        None, None, _RCA("broken"), "fix", lambda r: _Check(False, "nope"), max_repair_attempts=2
    )
    assert not check_result.passed
    assert len(calls) == 2  # bounded, not infinite


def test_zero_attempts_preserves_original_one_shot_behavior(monkeypatch):
    def explode(*a, **k):
        raise AssertionError("must not call the LLM when repairs are disabled")

    monkeypatch.setattr(stages, "repair_rca_result", explode)
    original = _RCA("broken")
    result, check_result = stages._attempt_with_repair(
        None, None, original, "fix", lambda r: _Check(False, "nope"), max_repair_attempts=0
    )
    assert result is original and not check_result.passed


def test_a_failed_repair_falls_back_to_the_original(monkeypatch):
    monkeypatch.setattr(stages, "repair_rca_result", lambda *a, **k: None)  # provider exhausted
    original = _RCA("broken")
    result, check_result = stages._attempt_with_repair(
        None, None, original, "fix", lambda r: _Check(False, "nope"), max_repair_attempts=2
    )
    assert result is original and not check_result.passed

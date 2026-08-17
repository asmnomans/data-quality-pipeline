"""Promoted fixes must be replayed by run-pipeline, not just by the approve path.

apply_active_fixes was only ever called from rerun_validation_after_promotion,
so the fix registry accumulated across runs but the main pipeline never read it:
every run re-derived the same repairs, re-paid for the same LLM calls, and
re-promoted near-duplicates (cleaning_fixes.json held isNotNull on order_amount
five times over).
"""
import inspect

import dq_framework.pipeline.runner as runner
from dq_framework.core.models import RunMetadata


def test_run_pipeline_replays_active_fixes_before_validating():
    src = inspect.getsource(runner.run)
    assert "apply_active_fixes" in src, "run() must consume the fix registry"
    # order matters: replay has to happen before the validation it should influence.
    # Match the assignment, not the bare name - the surrounding comment mentions
    # rerun_validation_after_promotion, which contains "run_validation".
    assert src.index("apply_active_fixes(") < src.index("engine_result = run_validation")


def test_replay_is_skipped_when_nothing_has_been_learned_yet():
    """A module's first run has an empty registry - it must not pay for a replay
    pass, and must still report the count."""
    src = inspect.getsource(runner.run)
    assert "if fixes_replayed:" in src


def test_run_metadata_records_how_much_was_reused():
    assert RunMetadata.model_fields["fixes_replayed"].default == 0

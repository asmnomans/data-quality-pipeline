"""The seven phases from docs/ARCHITECTURE.md section 3, as plain functions.

Kept separate from runner.py's orchestration/IO/locking so the CLI's
`approve` command can re-run just phase 7 (close_loop) cheaply - by
re-reading the source file and replaying currently-active fixes, not by
caching a DataFrame snapshot (see apply_active_fixes' docstring for why).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame

from dq_framework.core.config import ModuleConfig
from dq_framework.core.models import Candidate, FailureReport, RCAReport
from dq_framework.failure_extraction.extractor import build_failure_report
from dq_framework.llm.provider_factory import FallbackLLMProvider
from dq_framework.llm.rca_service import generate_rca_report, repair_rca_result
from dq_framework.remediation.applier import apply_fix
from dq_framework.remediation.sandbox import SandboxResult, run_snippet
from dq_framework.rules.rule_store import RuleStore
from dq_framework.rules.rule_validator import validate_new_rule
from dq_framework.validation.base import EngineValidationResult, ExpectationDef
from dq_framework.validation.gx_engine import GXEngine

logger = logging.getLogger(__name__)
from dq_framework.validation.profiler import calibrate_baseline_suite


def calibrate_or_load_baseline(
    engine: GXEngine, rule_store: RuleStore, module: ModuleConfig, baseline_df: DataFrame
) -> dict:
    """Phase 1. Only calibrates once per module (version 0 -> 1); subsequent
    runs reuse whatever's currently active, since that may already include
    promoted rules from prior runs."""
    active = rule_store.get_active_suite()
    if active["version"] == 0:
        expectations = calibrate_baseline_suite(baseline_df, module)
        active = rule_store.write_active_suite(expectations, suite_name=f"{module.name}_baseline_suite")
    return active


def run_validation(
    engine: GXEngine, module: ModuleConfig, df: DataFrame, suite: dict
) -> EngineValidationResult:
    """Phase 2."""
    return engine.validate(df, module, suite["expectations"], suite_name=suite["suite_name"])


def extract_failures(
    engine_result: EngineValidationResult,
    module: ModuleConfig,
    df: DataFrame,
    run_id: str,
    source_ref: str,
    suite: dict,
    max_sample_rows: int,
    mask_pii: bool,
) -> FailureReport:
    """Phase 3."""
    return build_failure_report(
        engine_result,
        module,
        df,
        run_id,
        source_ref,
        suite_name=suite["suite_name"],
        suite_version_ref=f"v{suite['version']}",
        max_sample_rows=max_sample_rows,
        mask_pii=mask_pii,
    )


def run_rca(provider: FallbackLLMProvider, module: ModuleConfig, failure_report: FailureReport) -> RCAReport:
    """Phase 4."""
    return generate_rca_report(provider, module, failure_report)


def process_remediation(
    engine: GXEngine,
    rule_store: RuleStore,
    module: ModuleConfig,
    baseline_df: DataFrame,
    dirty_df: DataFrame,
    rca_report: RCAReport,
    run_id: str,
    fix_promotion_mode: str,
    rule_promotion_mode: str,
    iteration: int = 1,
    *,
    failure_report: FailureReport | None = None,
    provider: FallbackLLMProvider | None = None,
    max_repair_attempts: int = 0,
    repair_not_drop: bool = False,
) -> tuple[DataFrame, list[Candidate]]:
    """Phases 5 & 6: guarded remediation + rule promotion.

    Every RCAResult produces exactly two candidates (one 'rule', one 'fix'),
    always recorded via rule_store.add_candidate regardless of promotion
    mode - auto vs manual only changes whether promote() is called
    immediately. Fixes are chained sequentially onto `current_df` so later
    failures are diagnosed/fixed against an already-partially-cleaned frame.

    A passing fix updates `current_df` regardless of fix_promotion_mode -
    in "manual" mode this is a *provisional* in-memory application only, so
    the remediation loop (run_remediation_loop, below) can still make real
    progress across multiple loops; nothing is actually persisted to the
    fix registry (rule_store.promote()) until a human approves it via the
    existing `approve` CLI/API command. `iteration` tags each candidate with
    which loop produced it, for audit trail purposes only.
    """
    current_df = dirty_df
    candidates: list[Candidate] = []
    # Lets the efficacy check below find the exact expectation + kwargs each
    # RCAResult was generated for, and the count it has to beat.
    failures_by_key = {
        (f.expectation_type, f.column): f for f in (failure_report.failures if failure_report else [])
    }
    profiles_by_column = {
        p.column: p for p in (failure_report.column_profiles if failure_report else [])
    }

    for rca_result in rca_report.results:
        rule_result, rule_check = _attempt_with_repair(
            provider,
            module,
            rca_result,
            "rule",
            lambda r: validate_new_rule(engine, module, baseline_df, r.new_ge_expectation),
            max_repair_attempts,
        )
        rule_candidate = rule_store.add_candidate(
            run_id, "rule", rule_result, rule_check.passed, rule_check.notes, iteration=iteration
        )
        candidates.append(rule_candidate)
        if rule_check.passed and rule_promotion_mode == "auto":
            rule_store.promote(rule_candidate.candidate_id, actor="system:auto")

        failure = failures_by_key.get((rca_result.source_expectation_type, rca_result.source_column))
        fix_rca, fix_result = _attempt_with_repair(
            provider,
            module,
            rca_result,
            "fix",
            lambda r: _check_fix(
                engine, module, r, current_df, failure, repair_not_drop, current_df.count()
            ),
            max_repair_attempts,
        )
        # Last resort: the LLM and its repair attempts are exhausted, and the
        # column is one we refuse to solve by deleting rows. Synthesize the fix
        # instead of losing the data - it still faces every gate below.
        if not fix_result.passed and failure is not None and repair_not_drop:
            template = build_template_fix(failure, profiles_by_column.get(failure.column))
            if template:
                candidate_rca = fix_rca.model_copy(deep=True)
                candidate_rca.suggested_pyspark_fix = template
                templated = _check_fix(
                    engine, module, candidate_rca, current_df, failure, repair_not_drop, current_df.count()
                )
                if templated.passed:
                    logger.info("Templated fix used for '%s' after LLM attempts failed", failure.column)
                    fix_rca, fix_result = candidate_rca, templated

        fix_candidate = rule_store.add_candidate(
            run_id, "fix", fix_rca, fix_result.passed, fix_result.notes, iteration=iteration
        )
        candidates.append(fix_candidate)
        if fix_result.passed:
            if fix_promotion_mode == "auto":
                rule_store.promote(fix_candidate.candidate_id, actor="system:auto")
            current_df = fix_result.cleaned_df

    return current_df, candidates


def _literal(value, dtype: str) -> str:
    """Render a profile value as PySpark source. Timestamps have to go through
    an explicit cast - a bare string literal compares as a string."""
    if dtype == "timestamp":
        return f"F.lit({str(value)!r}).cast('timestamp')"
    return repr(value)


def build_template_fix(failure, profile) -> str | None:
    """Deterministic median imputation for a range failure - no LLM involved.

    A range failure on a column whose other fields are valid has exactly one
    sensible repair: replace the out-of-bounds value with the column's own
    outlier-robust centre. Every input is already to hand (the expectation's
    own min/max, the profile's median), so there is nothing to reason about -
    and llama3.2:3b spent five runs failing to write this two-line snippet,
    cycling through Scala operators, unbalanced parentheses and nested F.when
    chains. Returns None when the shape doesn't fit, leaving the LLM path alone.
    """
    if profile is None or profile.median_value is None:
        return None
    kwargs = failure.expectation_kwargs or {}
    lo, hi = kwargs.get("min_value"), kwargs.get("max_value")
    if lo is None or hi is None:
        return None

    col, dtype = failure.column, profile.dtype
    return (
        f"cleaned_df = df.withColumn({col!r}, F.when("
        f"(F.col({col!r}) < {_literal(lo, dtype)}) | (F.col({col!r}) > {_literal(hi, dtype)}), "
        f"{_literal(profile.median_value, dtype)}"
        f").otherwise(F.col({col!r})))"
    )


def _check_fix(engine, module, rca_result, current_df, failure, repair_not_drop=False, row_count=None):
    """Sandbox the snippet, then prove it actually resolved the failure.

    Safety, executability and schema preservation were all checked before this;
    none of them ask whether the fix WORKED. A null-filter proposed for a range
    failure passes every one of them and removes zero offending rows - and then
    gets auto-promoted and replayed forever. Comparing the failed expectation's
    unexpected_count before and after is the only question that matters, and
    the answer becomes repair feedback the model can act on.
    """
    result = apply_fix(rca_result.suggested_pyspark_fix, current_df)
    if not result.passed or failure is None:
        return result

    # A filter drives unexpected_count to zero by deleting the rows, which the
    # efficacy check below can't distinguish from actually repairing them. For
    # repair_not_drop mode, losing rows is itself the failure.
    if repair_not_drop and row_count is not None:
        kept = result.cleaned_df.count()
        if kept < row_count:
            return SandboxResult(
                passed=False,
                notes=(
                    f"Fix deleted {row_count - kept} rows. No row may be dropped: the rest "
                    f"of those rows is valid, so replace the bad value instead of filtering. Use "
                    f"withColumn + F.when(...) and the column_profile median_value as the replacement."
                ),
            )

    exp_def = {"expectation_type": failure.expectation_type, "kwargs": failure.expectation_kwargs}
    try:
        after = engine.dry_run_single(result.cleaned_df, module, exp_def)
    except Exception as exc:  # can't measure it - don't block on our own tooling
        logger.warning("Efficacy check skipped for '%s': %s", failure.column, exc)
        return result

    remaining = after.failures[0].unexpected_count if after.failures else 0
    if remaining >= failure.unexpected_count:
        return SandboxResult(
            passed=False,
            notes=(
                f"Fix ran but changed nothing: {remaining} of {failure.unexpected_count} rows still "
                f"fail {failure.expectation_type} on '{failure.column}' with {failure.expectation_kwargs}. "
                "Target that expectation's own bounds - do not filter nulls for a range failure."
            ),
        )
    return result


def _attempt_with_repair(provider, module, rca_result, artifact, check, max_repair_attempts):
    """Run `check`; on rejection, hand the reason back to the LLM and retry.

    Returns the (possibly repaired) RCAResult alongside its final check result,
    so the candidate that gets recorded is the artifact that was actually
    validated - not the original rejected one. The last attempt's result is
    returned even if it also failed, which keeps the audit trail honest about
    what was tried.
    """
    result = check(rca_result)
    attempts = 0
    while not result.passed and attempts < max_repair_attempts:
        attempts += 1
        repaired = repair_rca_result(provider, module, rca_result, artifact, result.notes)
        if repaired is None:
            break
        rca_result = repaired
        result = check(rca_result)
        if result.passed:
            logger.info(
                "Self-repaired %s for '%s' on attempt %d", artifact, rca_result.source_column, attempts
            )
    return rca_result, result


def close_loop(engine: GXEngine, module: ModuleConfig, rule_store: RuleStore, df: DataFrame) -> EngineValidationResult:
    """Phase 7: re-validate against whatever is active *right now* - reflects
    any auto-promoted (or, on a later re-run, manually approved) changes."""
    suite = rule_store.get_active_suite()
    return engine.validate(df, module, suite["expectations"], suite_name=suite["suite_name"])


@dataclass
class LoopOutcome:
    final_df: DataFrame
    final_result: EngineValidationResult
    loops_run: int
    stop_reason: str
    candidates: list[Candidate]
    iterations: list[dict]
    rca_reports: list[RCAReport]


def run_remediation_loop(
    engine: GXEngine,
    rule_store: RuleStore,
    module: ModuleConfig,
    provider: FallbackLLMProvider,
    baseline_df: DataFrame,
    dirty_df: DataFrame,
    initial_result: EngineValidationResult,
    run_id: str,
    source_ref: str,
    fix_promotion_mode: str,
    rule_promotion_mode: str,
    loop_till_no_exception: bool,
    max_loops: int,
    max_sample_rows: int,
    mask_pii: bool,
    max_repair_attempts: int = 0,
    repair_not_drop: bool = False,
) -> LoopOutcome:
    """Repeats (diagnose -> RCA -> remediate -> re-validate) starting from
    `initial_result` (the pre-remediation validation the caller already ran),
    until one of three things happens:

    - the data is fully clean (`stop_reason="clean"`)
    - two consecutive loops leave the IDENTICAL set of primary keys still
      failing - no further progress is possible, whatever the LLM proposes
      next isn't helping (`stop_reason="no_progress"`)
    - `max_loops` is reached regardless of ongoing progress
      (`stop_reason="max_loops_reached"`)

    `loop_till_no_exception=False` always stops after exactly one loop
    (`stop_reason="single_pass_mode"`), matching the framework's original
    single-shot behavior - it still runs one full diagnose/fix/re-validate
    cycle, it just never repeats.

    Comparing primary-key SETS (not failure counts) matters here: in "auto"
    rule-promotion mode, a newly promoted rule can legitimately surface a
    brand-new failure on the next loop that didn't exist before - that's
    real progress/adaptation, not a stuck loop, and a naive count comparison
    could hide it.
    """
    current_df = dirty_df
    engine_result = initial_result
    previous_pk_set: set[str] | None = None
    all_candidates: list[Candidate] = []
    rca_reports: list[RCAReport] = []
    iterations: list[dict] = []
    loop_num = 0
    stop_reason = "clean"

    while not engine_result.success:
        loop_num += 1

        suite = rule_store.get_active_suite()
        failure_report = extract_failures(
            engine_result,
            module,
            current_df,
            run_id,
            source_ref,
            suite,
            max_sample_rows=max_sample_rows,
            mask_pii=mask_pii,
        )
        rca_report = run_rca(provider, module, failure_report)
        rca_reports.append(rca_report)
        current_df, round_candidates = process_remediation(
            engine,
            rule_store,
            module,
            baseline_df,
            current_df,
            rca_report,
            run_id,
            fix_promotion_mode,
            rule_promotion_mode,
            iteration=loop_num,
            failure_report=failure_report,
            provider=provider,
            max_repair_attempts=max_repair_attempts,
            repair_not_drop=repair_not_drop,
        )
        all_candidates.extend(round_candidates)

        engine_result = close_loop(engine, module, rule_store, current_df)
        pk_set = set(engine_result.failing_row_reasons)

        iterations.append(
            {
                "loop": loop_num,
                "remaining_failures": len(engine_result.failures),
                "remaining_rows": len(pk_set),
                "candidates": [c.candidate_id for c in round_candidates],
            }
        )

        if engine_result.success:
            stop_reason = "clean"
            break
        if previous_pk_set is not None and pk_set == previous_pk_set:
            stop_reason = "no_progress"
            break
        if loop_num >= max_loops:
            stop_reason = "max_loops_reached"
            break
        if not loop_till_no_exception:
            stop_reason = "single_pass_mode"
            break
        previous_pk_set = pk_set

    return LoopOutcome(
        final_df=current_df,
        final_result=engine_result,
        loops_run=loop_num,
        stop_reason=stop_reason,
        candidates=all_candidates,
        iterations=iterations,
        rca_reports=rca_reports,
    )


def apply_active_fixes(rule_store: RuleStore, df: DataFrame) -> DataFrame:
    """Replay every currently-promoted fix, in promotion order, onto a freshly
    re-read DataFrame.

    Deliberately NOT backed by a cached Parquet snapshot of the DataFrame:
    Spark's file writers go through Hadoop's output-commit protocol, which on
    Windows requires a real winutils.exe binary to actually execute (not just
    exist) - and downloading an unsigned third-party binary just to cache a
    snapshot isn't a trade worth making. Re-reading the source CSV (a plain
    read, no Hadoop write path involved) and replaying fixes is a few seconds
    of work, is portable across platforms, and - as a bonus - always reflects
    every fix promoted since the original run, not a frozen snapshot from it.
    """
    current_df = df
    for fix in rule_store.get_active_fixes()["fixes"]:
        result = run_snippet(fix["code"], current_df)
        if result.passed:
            current_df = result.cleaned_df
        else:
            # A fix that passed its sandbox check at promotion time should
            # always pass here too; if the underlying data shape changed
            # since then, skip it rather than abort the whole re-validation.
            logger.warning("Skipping active fix '%s' on replay: %s", fix["fix_id"], result.notes)
    return current_df

"""Guardrail for LLM-proposed Great Expectations rules: a candidate rule is
never promoted on trust. It must be (1) structurally valid - a real GX
expectation class with kwargs it accepts - and (2) dry-run against the
CLEAN baseline dataset, so a rule that would flag good data as bad is
rejected automatically, before a human or the auto-promotion path ever
sees it as a candidate.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass

from pyspark.sql import DataFrame

from dq_framework.core.config import ModuleConfig
from dq_framework.validation.base import ExpectationDef, resolve_expectation_class
from dq_framework.validation.gx_engine import GXEngine


@dataclass
class RuleCheckResult:
    passed: bool
    notes: str


def validate_new_rule(
    engine: GXEngine, module: ModuleConfig, baseline_df: DataFrame, expectation_def: ExpectationDef
) -> RuleCheckResult:
    structural = _check_structure(expectation_def)
    if not structural.passed:
        return structural

    try:
        result = engine.dry_run_single(baseline_df, module, expectation_def)
    except Exception as exc:
        return RuleCheckResult(passed=False, notes=f"Dry-run against baseline raised an error: {exc}")

    if not result.success:
        failure = result.failures[0] if result.failures else None
        detail = (
            f"{failure.unexpected_count}/{failure.element_count} baseline rows would be flagged"
            if failure
            else "baseline validation failed"
        )
        return RuleCheckResult(
            passed=False,
            notes=f"Rejected: rule would false-positive on the clean baseline ({detail}).",
        )

    return RuleCheckResult(passed=True, notes="Passed structural check and clean-baseline dry-run.")


def _coerce_stringified_containers(kwargs: dict) -> dict:
    """Turn "['A', 'B']" back into ['A', 'B'].

    The LLM answers in JSON mode, where every field is serialized as a string,
    and it routinely emits a list-shaped STRING for value_set rather than a JSON
    array - which GX rejects with "value is not a valid list". The text already
    unambiguously denotes the container, so parse it rather than lose the rule
    on an encoding artifact.

    literal_eval only, never eval: it evaluates literals and nothing else, so a
    malicious or malformed value raises instead of executing. Anything that
    doesn't parse is left exactly as-is for the normal kwargs check to reject.
    """
    coerced = dict(kwargs)
    for key, value in kwargs.items():
        if not isinstance(value, str) or not value.strip().startswith(("[", "{")):
            continue
        try:
            parsed = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            continue
        if isinstance(parsed, (list, dict, set, tuple)):
            coerced[key] = list(parsed) if isinstance(parsed, (set, tuple)) else parsed
    return coerced


def _check_structure(expectation_def: ExpectationDef) -> RuleCheckResult:
    exp_type = expectation_def.get("expectation_type")
    kwargs = expectation_def.get("kwargs", {})
    if not exp_type:
        return RuleCheckResult(passed=False, notes="Missing 'expectation_type'.")

    cls = resolve_expectation_class(exp_type)
    if cls is None:
        return RuleCheckResult(passed=False, notes=f"'{exp_type}' is not a known Great Expectations expectation.")

    # Normalize in place so the dry-run and the persisted candidate both carry
    # the PascalCase name the rest of the system expects.
    expectation_def["expectation_type"] = cls.__name__
    kwargs = _coerce_stringified_containers(kwargs)
    expectation_def["kwargs"] = kwargs

    try:
        cls(**kwargs)
    except Exception as exc:
        # Report the RESOLVED class name, not the model's spelling: a message
        # reading "Invalid kwargs for expect_column_values_to_be_in_set" next to
        # a pydantic error naming ExpectColumnValuesToBeInSet reads like the
        # name lookup failed, when in fact only the kwargs did.
        return RuleCheckResult(passed=False, notes=f"Invalid kwargs for {cls.__name__}: {exc}")

    return RuleCheckResult(passed=True, notes="ok")

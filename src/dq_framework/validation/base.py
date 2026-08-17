"""ValidationEngine abstraction over Great Expectations.

Everything above this layer (failure_extraction, rules, pipeline) talks in
terms of our own `ExpectationDef` dicts and a plain `EngineValidationResult`
- never a raw GX object. If GX's API changes again (as it did between 0.18
and 1.x), only gx_engine.py needs to change.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import great_expectations as gx
from pyspark.sql import DataFrame

from dq_framework.core.config import ModuleConfig

# Our own serialization of a Great Expectations expectation: the PascalCase
# class name (e.g. "ExpectColumnValuesToNotBeNull") + its constructor kwargs.
# This is the format stored in expectations/active/*.json and produced by
# both the profiler and the LLM's `new_ge_expectation` output.
ExpectationDef = dict[str, Any]


def resolve_expectation_class(exp_type: str):
    """PascalCase class name -> GX expectation class, or None if unknown.

    `gx.expectations` only exposes PascalCase (GX 1.x). The RCA prompt asks for
    PascalCase, but LLMs routinely answer with the GX 0.18 snake_case name
    (`expect_column_values_to_be_in_set`) because that's what dominates their
    training data - so accept either spelling here rather than rejecting a
    perfectly valid rule on a naming technicality. Every lookup (gx_engine,
    rule_validator) goes through this one function.
    """
    cls = getattr(gx.expectations, exp_type, None)
    if cls is None and "_" in exp_type:
        cls = getattr(gx.expectations, exp_type.title().replace("_", ""), None)
    return cls


@dataclass
class EngineFailure:
    expectation_type: str
    column: str | None
    kwargs: dict[str, Any]
    element_count: int
    unexpected_count: int
    unexpected_percent: float
    sample_failed_rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class EngineValidationResult:
    success: bool
    total_rows: int
    failures: list[EngineFailure] = field(default_factory=list)
    # primary-key value (as string) -> every "<expectation_type> on '<column>'"
    # reason it's still failing. Drives both the remediation loop's stop
    # condition (compare the key set across consecutive loops) and the
    # Quarantine export's Reason column - see gx_engine.py::_failing_primary_keys.
    failing_row_reasons: dict[str, list[str]] = field(default_factory=dict)


class ValidationEngine(Protocol):
    def validate(
        self, df: DataFrame, module: ModuleConfig, expectations: list[ExpectationDef], suite_name: str
    ) -> EngineValidationResult: ...

    def dry_run_single(
        self, df: DataFrame, module: ModuleConfig, expectation: ExpectationDef
    ) -> EngineValidationResult:
        """Validate ONE candidate expectation in isolation - used by rule_validator
        to check a new/proposed rule against the clean baseline before promotion."""
        ...

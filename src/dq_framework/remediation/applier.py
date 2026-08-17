"""Applies an LLM-suggested fix to real data - but only after trying it on a
small sample first. A snippet that passes the AST safety check can still be
wrong (e.g. it filters 100% of rows, or references a column that doesn't
exist) - catching that on a 500-row sample is cheap; catching it after
running on the full 50k-row dirty DataFrame is not.
"""
from __future__ import annotations

from pyspark.sql import DataFrame

from dq_framework.remediation.sandbox import SandboxResult, run_snippet

_SAMPLE_SIZE = 500


def apply_fix(code: str, df: DataFrame, sample_size: int = _SAMPLE_SIZE) -> SandboxResult:
    """Sample-check, then apply to the full DataFrame. Returns the FULL
    cleaned_df on success; the sample result's failure notes on rejection."""
    sample_result = run_snippet(code, df.limit(sample_size))
    if not sample_result.passed:
        return sample_result

    full_result = run_snippet(code, df)
    if not full_result.passed:
        return SandboxResult(
            passed=False,
            notes=f"Passed on a {sample_size}-row sample but failed on the full dataset: {full_result.notes}",
        )
    return full_result

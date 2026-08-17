"""Phase 3: turn a raw EngineValidationResult into the FailureReport artifact
the LLM will actually see - column-profiled, PII-masked, row-capped.

PII masking here is *structural*, not a blanket redaction: for an email
column, `jane.doe@example.com` becomes `xxxx.xxx@xxxxxxx.com` - punctuation
and length are preserved (so the LLM can still see "no @", "no TLD", "empty
domain" - literally the anomaly it's diagnosing) but no real identity leaks
into a prompt that may be sent to a third-party API.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from dq_framework.core.config import ModuleConfig
from dq_framework.core.models import ColumnProfile, FailureDetail, FailureReport
from dq_framework.validation.base import EngineValidationResult

_PII_COLUMN_HINTS = ("email", "phone", "ssn", "address", "name")
_NUMERIC_TYPES = {"double", "integer", "long"}


def _mask_structural(value: str) -> str:
    """Replace letters with 'x' and digits with 'N', keep everything else
    (@, ., -, spaces, ...) so format anomalies stay visible."""
    return re.sub(r"[A-Za-z]", "x", re.sub(r"\d", "N", value))


def _mask_pii_in_row(row: dict, pii_columns: set[str]) -> dict:
    masked = dict(row)
    for col_name in pii_columns:
        val = masked.get(col_name)
        if isinstance(val, str):
            masked[col_name] = _mask_structural(val)
    return masked


def _pii_columns(module: ModuleConfig) -> set[str]:
    return {
        entry.name
        for entry in (module.source.columns or [])
        if any(hint in entry.name.lower() for hint in _PII_COLUMN_HINTS)
    }


def compute_column_profiles(df: DataFrame, module: ModuleConfig) -> list[ColumnProfile]:
    """One combined Spark aggregation job for every column - not one job per
    column - so profiling stays cheap even as row/column counts grow."""
    total_rows = df.count()
    columns = module.source.columns or []
    agg_exprs = []
    for entry in columns:
        c = entry.name
        agg_exprs.append(F.count(F.when(F.col(c).isNull(), 1)).alias(f"{c}__nulls"))
        agg_exprs.append(F.countDistinct(F.col(c)).alias(f"{c}__distinct"))
        if entry.type in _NUMERIC_TYPES:
            agg_exprs.append(F.min(c).alias(f"{c}__min"))
            agg_exprs.append(F.max(c).alias(f"{c}__max"))
            agg_exprs.append(F.avg(c).alias(f"{c}__mean"))
            agg_exprs.append(F.percentile_approx(c, 0.5).alias(f"{c}__median"))
        elif entry.type == "timestamp":
            # Timestamps were profiled as all-nulls before, so an LLM asked to
            # repair a *range* failure on this column got no range to reason
            # from and fell back to a generic isNotNull() filter that fixed
            # nothing. avg has no float representation for a timestamp, so
            # median (via epoch seconds) carries the central value instead.
            agg_exprs.append(F.min(c).alias(f"{c}__min"))
            agg_exprs.append(F.max(c).alias(f"{c}__max"))
            agg_exprs.append(
                F.percentile_approx(F.col(c).cast("long"), 0.5).cast("timestamp").alias(f"{c}__median")
            )

    row = df.agg(*agg_exprs).collect()[0].asDict()

    profiles = []
    for entry in columns:
        c = entry.name
        null_count = row.get(f"{c}__nulls", 0) or 0
        profiles.append(
            ColumnProfile(
                column=c,
                dtype=entry.type,
                row_count=total_rows,
                null_count=null_count,
                null_ratio=(null_count / total_rows) if total_rows else 0.0,
                distinct_count=row.get(f"{c}__distinct"),
                min_value=row.get(f"{c}__min"),
                max_value=row.get(f"{c}__max"),
                mean_value=row.get(f"{c}__mean"),
                median_value=row.get(f"{c}__median"),
            )
        )
    return profiles


def build_failure_report(
    engine_result: EngineValidationResult,
    module: ModuleConfig,
    df: DataFrame,
    run_id: str,
    source_ref: str,
    suite_name: str,
    suite_version_ref: str,
    max_sample_rows: int = 8,
    mask_pii: bool = True,
) -> FailureReport:
    pii_columns = _pii_columns(module) if mask_pii else set()

    failures: list[FailureDetail] = []
    for f in engine_result.failures:
        sample_rows = [
            _mask_pii_in_row(row, pii_columns) for row in f.sample_failed_rows[:max_sample_rows]
        ]
        failures.append(
            FailureDetail(
                expectation_type=f.expectation_type,
                column=f.column,
                expectation_kwargs=f.kwargs,
                element_count=f.element_count,
                unexpected_count=f.unexpected_count,
                unexpected_percent=f.unexpected_percent,
                sample_failed_rows=sample_rows,
            )
        )

    return FailureReport(
        run_id=run_id,
        module=module.name,
        generated_at=datetime.now(timezone.utc),
        source_ref=source_ref,
        suite_name=suite_name,
        suite_version_ref=suite_version_ref,
        total_rows=engine_result.total_rows,
        success=engine_result.success,
        failures=failures,
        column_profiles=compute_column_profiles(df, module),
    )

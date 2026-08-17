"""Phase 1: infer a baseline ExpectationSuite from the clean/control dataset.

Every statistic here comes from native Spark aggregations (`.agg(F.min/max/
countDistinct)`, capped `.distinct().limit(N)`) - never a `.collect()` of the
full DataFrame, so this scales to a real (not 50k-row) baseline without
blowing up the driver.
"""
from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from dq_framework.core.config import ModuleConfig
from dq_framework.validation.base import ExpectationDef

_EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
_MAX_CATEGORICAL_DISTINCT = 20
_NUMERIC_TYPES = {"double", "integer", "long"}
_TIMESTAMP_BUFFER_DAYS = 3


def calibrate_baseline_suite(df: DataFrame, module: ModuleConfig) -> list[ExpectationDef]:
    """Profile the clean baseline DataFrame and return a list of ExpectationDefs.

    This is intentionally simple/generic (not orders-specific) so a new
    module dropped into modules/ gets a sane starting suite for free, based
    only on the column types and critical_columns it declares.
    """
    expectations: list[ExpectationDef] = []

    pk = module.primary_key
    expectations.append(_not_null(pk))
    expectations.append({"expectation_type": "ExpectColumnValuesToBeUnique", "kwargs": {"column": pk}})

    columns = module.source.columns or []
    for entry in columns:
        col_name = entry.name
        if col_name == pk:
            continue

        if col_name in module.critical_columns:
            expectations.append(_not_null(col_name))

        if entry.type in _NUMERIC_TYPES:
            expectations.append(_numeric_range(df, col_name))
        elif entry.type == "timestamp":
            expectations.append(_timestamp_range(df, col_name))
        elif entry.type == "string" and "email" in col_name.lower():
            expectations.append(
                {
                    "expectation_type": "ExpectColumnValuesToMatchRegex",
                    "kwargs": {"column": col_name, "regex": _EMAIL_REGEX},
                }
            )
        elif entry.type == "string":
            categorical = _maybe_categorical_set(df, col_name)
            if categorical is not None:
                expectations.append(
                    {
                        "expectation_type": "ExpectColumnValuesToBeInSet",
                        "kwargs": {"column": col_name, "value_set": categorical},
                    }
                )

    return expectations


def _not_null(column: str) -> ExpectationDef:
    return {"expectation_type": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": column}}


def _numeric_range(df: DataFrame, column: str) -> ExpectationDef:
    row = df.agg(F.min(column).alias("min_v"), F.max(column).alias("max_v")).collect()[0]
    return {
        "expectation_type": "ExpectColumnValuesToBeBetween",
        "kwargs": {"column": column, "min_value": row["min_v"], "max_value": row["max_v"]},
    }


def _timestamp_range(df: DataFrame, column: str) -> ExpectationDef:
    """Stored (and later JSON-persisted by rule_store) as ISO strings, not
    datetime objects - gx_engine converts back to datetime at instantiation
    time, keyed off the column's declared type, so the suite JSON on disk
    stays plain JSON regardless of expectation type."""
    row = df.agg(F.min(column).alias("min_v"), F.max(column).alias("max_v")).collect()[0]
    min_v, max_v = row["min_v"], row["max_v"]
    if min_v is not None and max_v is not None:
        from datetime import timedelta

        min_v = (min_v - timedelta(days=_TIMESTAMP_BUFFER_DAYS)).isoformat()
        max_v = (max_v + timedelta(days=_TIMESTAMP_BUFFER_DAYS)).isoformat()
    return {
        "expectation_type": "ExpectColumnValuesToBeBetween",
        "kwargs": {"column": column, "min_value": min_v, "max_value": max_v},
    }


def _maybe_categorical_set(df: DataFrame, column: str) -> list[str] | None:
    distinct_rows = df.select(column).distinct().limit(_MAX_CATEGORICAL_DISTINCT + 1).collect()
    values = [r[column] for r in distinct_rows if r[column] is not None]
    if len(values) > _MAX_CATEGORICAL_DISTINCT:
        return None  # too high-cardinality to be a categorical column
    return sorted(values)

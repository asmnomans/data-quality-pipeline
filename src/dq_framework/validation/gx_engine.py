"""Great Expectations 1.20.0 Fluent-API adapter for Spark DataFrames.

Confirmed working directly against pyspark==4.1.3 via a throwaway spike
before this file was written (see docs/ARCHITECTURE.md section 9):
context.data_sources.add_spark -> add_dataframe_asset ->
add_batch_definition_whole_dataframe -> get_batch(batch_parameters={...}) ->
batch.validate(suite). No CSV round-trip through GX itself.
"""
from __future__ import annotations

import threading
from pathlib import Path

import great_expectations as gx
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from dq_framework.core.config import ModuleConfig
from dq_framework.core.exceptions import ValidationEngineError
from dq_framework.validation.base import (
    EngineFailure,
    EngineValidationResult,
    ExpectationDef,
    resolve_expectation_class,
)

_RESULT_FORMAT = {"result_format": "COMPLETE"}
_MAX_SAMPLE_ROWS = 8  # hard ceiling; the real per-run cap comes from config.sampling

# GX's file-backed context is NOT safe for concurrent use: two threads can
# both find a datasource/asset/batch_definition missing and both try to
# create it (one wins, one raises DataContextError - or, worse, one reads a
# half-written config and silently gets back empty/zeroed metrics instead
# of an error). The backend runs multiple jobs concurrently by design, so
# this WILL happen, not just theoretically - reproduced directly before
# adding this lock. Every GX operation across the whole process is
# serialized through this one lock; Spark's own scheduler already queues
# actual computation, so this costs little beyond that.
_GX_LOCK = threading.Lock()


class GXEngine:
    """One GXEngine per process. Caches the file-backed context and, per
    module, the (datasource, asset, batch_definition) triple GX needs -
    Fluent API raises if you try to add the same-named datasource twice.
    """

    def __init__(self, ge_root: Path):
        self.ge_root = ge_root
        self._context = None
        self._batch_defs: dict[str, tuple] = {}

    @property
    def context(self):
        if self._context is None:
            self.ge_root.mkdir(parents=True, exist_ok=True)
            self._context = gx.get_context(mode="file", project_root_dir=str(self.ge_root))
        return self._context

    def _batch_definition_for(self, module: ModuleConfig):
        if module.name in self._batch_defs:
            return self._batch_defs[module.name]

        ds_name = f"{module.name}_spark_ds"
        asset_name = f"{module.name}_asset"
        try:
            data_source = self.context.data_sources.get(ds_name)
        except (KeyError, LookupError):
            data_source = self.context.data_sources.add_spark(name=ds_name)

        try:
            data_asset = data_source.get_asset(asset_name)
        except (KeyError, LookupError):
            data_asset = data_source.add_dataframe_asset(name=asset_name)

        batch_def_name = f"{module.name}_batch_def"
        try:
            batch_definition = data_asset.get_batch_definition(batch_def_name)
        except (KeyError, LookupError):
            batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_def_name)

        result = (data_source, data_asset, batch_definition)
        self._batch_defs[module.name] = result
        return result

    @staticmethod
    def _column_type(module: ModuleConfig, column: str | None) -> str | None:
        for entry in module.source.columns or []:
            if entry.name == column:
                return entry.type
        return None

    def _expectation_instance(self, expectation_def: ExpectationDef, module: ModuleConfig):
        exp_type = expectation_def["expectation_type"]
        cls = resolve_expectation_class(exp_type)
        if cls is None:
            raise ValidationEngineError(
                f"Unknown Great Expectations expectation type: '{exp_type}'"
            )

        kwargs = dict(expectation_def.get("kwargs", {}))
        # Suites are persisted as plain JSON (rule_store.py), so a timestamp
        # bound comes back as an ISO string, not a datetime - convert it back
        # here, keyed off the column's declared type, so JSON on disk never
        # needs to special-case any expectation type.
        if self._column_type(module, kwargs.get("column")) == "timestamp":
            from datetime import datetime as _dt

            for key in ("min_value", "max_value"):
                if isinstance(kwargs.get(key), str):
                    kwargs[key] = _dt.fromisoformat(kwargs[key])

        return cls(**kwargs)

    def _build_suite(self, suite_name: str, expectations: list[ExpectationDef], module: ModuleConfig):
        suite = gx.ExpectationSuite(name=suite_name)
        for exp_def in expectations:
            suite.add_expectation(self._expectation_instance(exp_def, module))
        return suite

    def _get_batch(self, module: ModuleConfig, df: DataFrame):
        _, _, batch_definition = self._batch_definition_for(module)
        return batch_definition.get_batch(batch_parameters={"dataframe": df})

    def validate(
        self,
        df: DataFrame,
        module: ModuleConfig,
        expectations: list[ExpectationDef],
        suite_name: str,
    ) -> EngineValidationResult:
        try:
            with _GX_LOCK:
                batch = self._get_batch(module, df)
                suite = self._build_suite(suite_name, expectations, module)
                result = batch.validate(suite, result_format=_RESULT_FORMAT)
        except Exception as exc:
            raise ValidationEngineError(f"GX validation failed for module '{module.name}': {exc}") from exc

        return self._to_engine_result(df, result, total_rows=df.count(), module=module)

    def dry_run_single(
        self, df: DataFrame, module: ModuleConfig, expectation: ExpectationDef
    ) -> EngineValidationResult:
        """Used by rule_validator to check ONE candidate expectation against a
        DataFrame (typically the clean baseline) before it's allowed to promote."""
        return self.validate(df, module, [expectation], suite_name=f"{module.name}_dry_run")

    def _to_engine_result(
        self, df: DataFrame, gx_result, total_rows: int, module: ModuleConfig
    ) -> EngineValidationResult:
        failures: list[EngineFailure] = []
        failing_row_reasons: dict[str, list[str]] = {}
        for r in gx_result.results:
            if r.success:
                continue
            exp_config = r.expectation_config
            column = exp_config.kwargs.get("column")
            result_dict = r.result or {}
            element_count = result_dict.get("element_count", total_rows) or total_rows
            unexpected_count = result_dict.get("unexpected_count", 0) or 0
            unexpected_percent = result_dict.get("unexpected_percent", 0.0) or 0.0

            sample_rows = self._sample_failed_rows(df, column=column, result_dict=result_dict)

            failures.append(
                EngineFailure(
                    # snake_case GX identifier (e.g. "expect_column_values_to_not_be_null"),
                    # used for display/LLM context only - our own suite storage uses the
                    # PascalCase class name instead (see _expectation_instance below).
                    expectation_type=str(exp_config.type),
                    column=column,
                    kwargs=dict(exp_config.kwargs),
                    element_count=element_count,
                    unexpected_count=unexpected_count,
                    unexpected_percent=unexpected_percent,
                    sample_failed_rows=sample_rows,
                )
            )

            reason = f"{exp_config.type} on '{column}'" if column else str(exp_config.type)
            for pk_value in self._failing_primary_keys(df, module.primary_key, column, result_dict):
                failing_row_reasons.setdefault(pk_value, []).append(reason)

        return EngineValidationResult(
            success=gx_result.success,
            total_rows=total_rows,
            failures=failures,
            failing_row_reasons=failing_row_reasons,
        )

    @staticmethod
    def _sample_failed_rows(df: DataFrame, column, result_dict) -> list[dict]:
        """Best-effort sample of actual failing rows, using only Spark-native
        filters + a `.limit()` before any `.collect()` - never collects the
        full DataFrame to the driver."""
        partial_values = result_dict.get("partial_unexpected_list") or []
        if not partial_values or column is None:
            return []
        try:
            sample_df = df.filter(col(column).isin(partial_values) | col(column).isNull()).limit(_MAX_SAMPLE_ROWS)
            return [row.asDict(recursive=True) for row in sample_df.collect()]
        except Exception:
            # Sampling is best-effort context for the LLM, never load-bearing.
            return [{column: v} for v in partial_values[:_MAX_SAMPLE_ROWS]]

    @staticmethod
    def _failing_primary_keys(df: DataFrame, primary_key: str, column: str | None, result_dict: dict) -> list[str]:
        """Every primary-key value whose row trips this one expectation - not
        a capped sample. Reuses the same Spark-native filter idea as
        `_sample_failed_rows` (native filter, no full-dataset `.collect()`),
        but against `unexpected_list` (the FULL failing-value list GX returns
        under `result_format: COMPLETE`, unlike the truncated
        `partial_unexpected_list`) and selecting only the narrow primary-key
        column - never the full row set - to the driver.

        Feeds both the remediation loop's stop condition (compare this set
        across consecutive loops) and the Quarantine export's Reason column.
        """
        if column is None:
            return []  # no column to filter on - can't attribute rows generically
        unexpected_values = result_dict.get("unexpected_list") or []
        try:
            condition = col(column).isNull()
            if unexpected_values:
                condition = condition | col(column).isin(unexpected_values)
            pk_rows = df.filter(condition).select(primary_key).distinct().collect()
            return [str(row[0]) for row in pk_rows]
        except Exception:
            # Row-level attribution is best-effort; a failure here must never
            # abort validation itself.
            return []

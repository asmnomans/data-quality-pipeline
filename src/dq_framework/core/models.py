"""Pydantic models shared across every layer of the pipeline.

These are the framework's own internal schema - deliberately decoupled from
whatever shape Great Expectations or a given LLM SDK happens to return, so
an upstream API change only touches the one adapter that translates into
these models, never the rest of the pipeline.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

ArtifactType = Literal["rule", "fix"]
PromotionAction = Literal["auto_promoted", "approved", "rejected", "rolled_back"]
PromotionMode = Literal["auto", "manual"]
RunStatus = Literal["running", "succeeded", "failed"]


class ColumnProfile(BaseModel):
    """Summary statistics for one column, computed with native Spark aggregations."""

    column: str
    dtype: str
    row_count: int
    null_count: int
    null_ratio: float
    distinct_count: int | None = None
    min_value: Any | None = None
    max_value: Any | None = None
    mean_value: float | None = None
    # Outlier-robust central value, usable as an imputation target. Typed Any
    # (not float) so a timestamp column can carry one - a sentinel date like
    # 2099-01-01 drags the mean out of range but leaves the median valid.
    median_value: Any | None = None


class FixRepair(BaseModel):
    """Just the corrected snippet. Asking a small model to restate a whole
    RCAResult for a one-field correction makes it echo the schema instead of
    an instance; a single-field target is reliably answerable."""

    suggested_pyspark_fix: str


class RuleRepair(BaseModel):
    """Just the corrected expectation - see FixRepair for why it's this narrow."""

    new_ge_expectation: dict[str, Any]


class FailureDetail(BaseModel):
    """One failed expectation from a GX ValidationResult, normalized."""

    expectation_type: str
    column: str | None = None
    expectation_kwargs: dict[str, Any] = Field(default_factory=dict)
    element_count: int
    unexpected_count: int
    unexpected_percent: float
    sample_failed_rows: list[dict[str, Any]] = Field(default_factory=list)


class FailureReport(BaseModel):
    """Phase-3 artifact: everything the LLM needs to diagnose a validation run.

    Written to artifacts/runs/<run_id>/failure_report.json.
    """

    run_id: str
    module: str
    generated_at: datetime
    source_ref: str
    suite_name: str
    suite_version_ref: str
    total_rows: int
    success: bool
    failures: list[FailureDetail] = Field(default_factory=list)
    column_profiles: list[ColumnProfile] = Field(default_factory=list)


class RCAResult(BaseModel):
    """Strict structured output the LLM must produce for one failed expectation."""

    failure_category: str = Field(
        description="e.g. Schema Drift, Upstream System Bug, Sensor Anomaly, Data Entry Error"
    )
    root_cause_explanation: str = Field(
        description="2-3 sentence executive summary of why the data failed"
    )
    suggested_pyspark_fix: str = Field(
        description="Syntactically valid PySpark snippet to filter, impute, or quarantine the affected rows"
    )
    new_ge_expectation: dict[str, Any] = Field(
        description="A new Great Expectations expectation configuration to catch this edge case earlier"
    )

    # populated by rca_service after the LLM call, not by the LLM itself
    source_expectation_type: str | None = None
    source_column: str | None = None
    provider_used: str | None = None


class RCAReport(BaseModel):
    """Phase-4 artifact: one RCAResult per failed expectation.

    Written to artifacts/runs/<run_id>/rca_report.json.
    """

    run_id: str
    module: str
    generated_at: datetime
    results: list[RCAResult] = Field(default_factory=list)


class Candidate(BaseModel):
    """A pending fix or rule awaiting promotion (auto or manual)."""

    candidate_id: str
    run_id: str
    module: str
    artifact_type: ArtifactType
    created_at: datetime
    rca_result: RCAResult
    sandbox_passed: bool
    sandbox_notes: str | None = None
    status: Literal["pending", "approved", "rejected", "auto_promoted"] = "pending"
    iteration: int = 1  # which remediation loop produced this candidate (see pipeline/stages.py::run_remediation_loop)


class PromotionLogEntry(BaseModel):
    """One line of modules/<name>/expectations/promotion_log.jsonl (append-only)."""

    timestamp: datetime
    run_id: str
    candidate_id: str
    artifact_type: ArtifactType
    action: PromotionAction
    before_ref: str | None
    after_ref: str
    actor: str  # "system:auto" | "user:<email>" | "system:rollback"


class RunMetadata(BaseModel):
    """Tracks one pipeline run end-to-end. Written alongside the run's other artifacts."""

    run_id: str
    module: str
    source_ref: str
    started_at: datetime
    finished_at: datetime | None = None
    status: RunStatus = "running"
    phase_reached: str = "bootstrap"
    total_rows: int | None = None
    failed_expectation_count: int | None = None
    candidates_generated: int = 0
    # How many already-promoted fixes were replayed before this run validated
    # anything - i.e. how much this module had already learned. 0 on a module's
    # first run, and the number the LLM therefore did NOT have to rediscover.
    fixes_replayed: int = 0
    iterations_run: int = 0  # number of RCA->remediate->re-validate loops actually executed
    stop_reason: str | None = None  # "clean" | "no_progress" | "max_loops_reached" | "single_pass_mode"
    quarantine_file: str | None = None  # path to the exported still-failing-rows CSV, if any
    error: str | None = None

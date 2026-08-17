"""Module 2: orchestrates one LLM call per failed expectation, aggregates
into an RCAReport. A single expectation's diagnosis failing (both providers
exhausted) is logged and skipped - it never aborts the whole report, since
the other failures' diagnoses are independently useful.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from dq_framework.core.config import ModuleConfig
from dq_framework.core.exceptions import LLMAllProvidersExhaustedError
from dq_framework.core.models import FailureReport, FixRepair, RCAReport, RCAResult, RuleRepair
from dq_framework.llm.prompt_templates.rca_prompt import build_prompts, build_repair_prompts
from dq_framework.llm.provider_factory import FallbackLLMProvider

logger = logging.getLogger(__name__)


def repair_rca_result(
    provider: FallbackLLMProvider,
    module: ModuleConfig,
    rca_result: RCAResult,
    artifact: str,
    error_note: str,
) -> RCAResult | None:
    """Ask the LLM to correct ONE guardrail-rejected artifact ('fix' or 'rule').

    Returns a fresh RCAResult, or None if the provider is exhausted - callers
    keep the original rejected candidate in that case, so a failed repair is
    never worse than not having tried.
    """
    system_prompt, user_prompt = build_repair_prompts(rca_result, artifact, error_note)
    response_model = FixRepair if artifact == "fix" else RuleRepair
    try:
        patch, provider_name = provider.generate_structured(system_prompt, user_prompt, response_model)
    except LLMAllProvidersExhaustedError as exc:
        logger.warning("Repair of %s for '%s' failed: %s", artifact, rca_result.source_column, exc)
        return None

    # Copy so a failed re-check never mutates the candidate we'd fall back to.
    repaired = rca_result.model_copy(deep=True)
    if artifact == "fix":
        repaired.suggested_pyspark_fix = patch.suggested_pyspark_fix
    else:
        repaired.new_ge_expectation = patch.new_ge_expectation
    repaired.provider_used = provider_name
    return repaired


def generate_rca_report(
    provider: FallbackLLMProvider, module: ModuleConfig, failure_report: FailureReport
) -> RCAReport:
    profiles_by_column = {p.column: p for p in failure_report.column_profiles}
    results: list[RCAResult] = []

    for failure in failure_report.failures:
        system_prompt, user_prompt = build_prompts(failure, module, profiles_by_column.get(failure.column))
        try:
            rca_result, provider_name = provider.generate_structured(system_prompt, user_prompt, RCAResult)
        except LLMAllProvidersExhaustedError as exc:
            logger.warning(
                "RCA skipped for %s on '%s': %s", failure.expectation_type, failure.column, exc
            )
            continue

        rca_result.source_expectation_type = failure.expectation_type
        rca_result.source_column = failure.column
        rca_result.provider_used = provider_name
        results.append(rca_result)

    return RCAReport(
        run_id=failure_report.run_id,
        module=module.name,
        generated_at=datetime.now(timezone.utc),
        results=results,
    )

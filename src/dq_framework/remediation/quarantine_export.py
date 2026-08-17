"""Writes the rows still failing when the remediation loop closes to a CSV
for manual follow-up - the ones neither an LLM-suggested fix nor further
looping could resolve. Written once per run (not once per loop - see
docs/ARCHITECTURE.md section 6.2), gated by `remediation.export_quarantined`.

Uses `.toPandas()` rather than Spark's native `.write.csv()` because Spark's
file-output path goes through Hadoop's commit protocol, which needs a real
winutils.exe on Windows (the same constraint documented in
pipeline/stages.py's `apply_active_fixes` docstring - it's why this project
never writes Spark output natively). This is a one-time, size-bounded
collect of just the quarantined subset (never the full dataset), which is
the pragmatic tradeoff for a required deliverable rather than an
"unnecessary .collect()" in a hot path.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from dq_framework.core.config import ModuleConfig
from dq_framework.validation.base import EngineValidationResult


def export_quarantine_csv(
    df: DataFrame,
    result: EngineValidationResult,
    module: ModuleConfig,
    run_id: str,
    source_ref: str,
    loop_num: int,
    artifacts_root: Path,
) -> Path | None:
    """Writes every row still failing when the loop closed, each tagged with
    a Reason column (which expectation(s) it still fails), no PII masking
    (this is an internal ops file, not LLM-facing). Returns None - and
    writes nothing - if the loop ended fully clean."""
    pk_reasons = result.failing_row_reasons
    if not pk_reasons:
        return None

    pk_col = module.primary_key
    quarantined = df.filter(col(pk_col).isin(list(pk_reasons))).toPandas()
    quarantined["Reason"] = quarantined[pk_col].astype(str).map(
        lambda pk: "; ".join(pk_reasons.get(pk, []))
    )

    out_dir = artifacts_root / "runs" / run_id / "fileOutput" / "QuarantineData"
    out_dir.mkdir(parents=True, exist_ok=True)
    file_stem = Path(source_ref).stem
    out_path = out_dir / f"{run_id}_{file_stem}_Quarantine_loop{loop_num}.csv"
    quarantined.to_csv(out_path, index=False)
    return out_path

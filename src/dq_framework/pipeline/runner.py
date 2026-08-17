"""Orchestrates the seven phases end-to-end for one module + writes every
artifact under artifacts/runs/<run_id>/. This is the single function every
client (CLI, scheduler, file watcher, FastAPI backend) calls to start a run -
see docs/ARCHITECTURE.md section 4/7.
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dq_framework.core.config import AppConfig, ModuleConfig, ModuleRegistry, load_settings
from dq_framework.core.exceptions import PipelineLockError
from dq_framework.core.models import RunMetadata
from dq_framework.ingestion.base import get_spark_session
from dq_framework.ingestion.spark_reader import get_data_source
from dq_framework.llm.provider_factory import build_llm_provider
from dq_framework.pipeline.stages import (
    apply_active_fixes,
    calibrate_or_load_baseline,
    close_loop,
    extract_failures,
    run_remediation_loop,
    run_validation,
)
from dq_framework.remediation.quarantine_export import export_quarantine_csv
from dq_framework.rules.rule_store import RuleStore
from dq_framework.validation.gx_engine import GXEngine

logger = logging.getLogger(__name__)

_MAX_LOCK_AGE_SECONDS = 3 * 60 * 60  # a real run realistically never takes this long


def _pid_is_alive(pid: int) -> bool:
    """Cross-platform liveness check - no extra dependency (e.g. psutil)."""
    if sys.platform == "win32":
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, just owned by someone else - still alive
    return True


class _ModuleLock:
    """Cross-process advisory lock (a file, not a Python threading.Lock) -
    the CLI, scheduler, watcher, and API all run in different processes, so
    an in-memory lock wouldn't see across them.

    Self-healing: a normal run releases this on its own (this is a `with`
    block - that cleanup code runs even on a handled exception). But if the
    *process itself* is killed before it can clean up (stop button, closed
    terminal, crash), nothing is left to release the file - it just sits
    there blocking every future run until a human notices and deletes it,
    which happened more than once during development. So every acquisition
    attempt checks whether the PID that holds the existing lock is even
    still alive (and falls back to a generous max-age check in case a PID
    got reused by an unrelated process) - a dead owner's lock is reclaimed
    automatically instead of requiring manual cleanup.
    """

    def __init__(self, artifacts_root: Path, module_name: str):
        self.path = artifacts_root / "runs" / ".locks" / f"{module_name}.lock"

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists() and self._is_stale():
            with contextlib.suppress(FileNotFoundError):
                self.path.unlink()
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
        except FileExistsError as exc:
            raise PipelineLockError(
                f"A run is already in flight for this module ({self.path}). Try again shortly."
            ) from exc
        return self

    def _is_stale(self) -> bool:
        try:
            age = time.time() - self.path.stat().st_mtime
            if age > _MAX_LOCK_AGE_SECONDS:
                return True
            pid = int(self.path.read_text().strip())
        except (OSError, ValueError):
            return True  # unreadable/corrupt/empty lock - safe to reclaim
        return not _pid_is_alive(pid)

    def __exit__(self, *exc_info):
        with contextlib.suppress(FileNotFoundError):
            self.path.unlink()


def _run_id(module_name: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{module_name}-{ts}-{uuid.uuid4().hex[:8]}"


def _find_default_incoming(module: ModuleConfig, project_root: Path) -> str:
    """When no explicit source_ref is given (CLI default run, scheduled
    trigger), pick the most recently modified file under the module's
    incoming_path - the file-watcher/manual-upload path always passes an
    explicit source_ref instead of relying on this."""
    incoming_dir = project_root / module.source.incoming_path
    candidates = sorted(incoming_dir.glob(f"*.{module.source.format}"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise FileNotFoundError(f"No .{module.source.format} files found under {incoming_dir}")
    return str(candidates[-1].relative_to(project_root))


def _write_json(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def run(module_name: str, source_ref: str | None = None, app_config: AppConfig | None = None) -> RunMetadata:
    app_config = app_config or load_settings()
    registry = ModuleRegistry(app_config.paths.modules_root)
    module = registry.get(module_name)
    project_root = app_config.paths.modules_root.parent

    with _ModuleLock(app_config.paths.artifacts_root, module_name):
        run_id = _run_id(module_name)
        run_dir = app_config.paths.artifacts_root / "runs" / run_id
        metadata = RunMetadata(
            run_id=run_id,
            module=module_name,
            source_ref=source_ref or "(default: latest incoming file)",
            started_at=datetime.now(timezone.utc),
        )

        try:
            spark = get_spark_session()
            source = get_data_source(module.source.format)
            engine = GXEngine(app_config.paths.great_expectations_root)
            rule_store = RuleStore(module)

            baseline_df = source.read_baseline(spark, module)
            suite = calibrate_or_load_baseline(engine, rule_store, module, baseline_df)
            metadata.phase_reached = "baseline_ready"

            actual_source_ref = source_ref or _find_default_incoming(module, project_root)
            metadata.source_ref = actual_source_ref
            dirty_df = source.read(spark, module, actual_source_ref)

            # Replay everything this module has already learned to fix before
            # diagnosing anything. Without this the fix registry was only ever
            # consumed by the approve path (rerun_validation_after_promotion),
            # so every run re-derived the same repairs from scratch, paid for
            # the same LLM calls, and re-promoted near-duplicates - the library
            # accumulated but was never used where it mattered.
            fixes_replayed = len(rule_store.get_active_fixes()["fixes"])
            if fixes_replayed:
                dirty_df = apply_active_fixes(rule_store, dirty_df)
                logger.info("Replayed %d already-promoted fix(es) before validation", fixes_replayed)
            metadata.fixes_replayed = fixes_replayed

            engine_result = run_validation(engine, module, dirty_df, suite)
            metadata.phase_reached = "validated"
            metadata.total_rows = engine_result.total_rows
            metadata.failed_expectation_count = len(engine_result.failures)

            failure_report = extract_failures(
                engine_result,
                module,
                dirty_df,
                run_id,
                actual_source_ref,
                suite,
                max_sample_rows=app_config.sampling.max_sample_rows_per_failure,
                mask_pii=app_config.sampling.mask_pii,
            )
            _write_json(run_dir / "failure_report.json", failure_report.model_dump_json(indent=2))
            metadata.phase_reached = "failures_extracted"

            if engine_result.success:
                metadata.status = "succeeded"
                metadata.phase_reached = "loop_closed_no_failures"
                metadata.iterations_run = 0
                metadata.stop_reason = "clean"
                return metadata

            provider = build_llm_provider(app_config)

            loop_till_no_exception = (
                module.loop_till_no_exception
                if module.loop_till_no_exception is not None
                else app_config.remediation.loop_till_no_exception
            )
            max_loops = module.max_loops or app_config.remediation.max_loops
            export_quarantined = (
                module.export_quarantined
                if module.export_quarantined is not None
                else app_config.remediation.export_quarantined
            )
            fix_mode = module.fix_promotion_mode or app_config.remediation.fix_promotion_mode
            rule_mode = module.rule_promotion_mode or app_config.remediation.rule_promotion_mode

            outcome = run_remediation_loop(
                engine,
                rule_store,
                module,
                provider,
                baseline_df,
                dirty_df,
                engine_result,
                run_id,
                actual_source_ref,
                fix_mode,
                rule_mode,
                loop_till_no_exception,
                max_loops,
                max_sample_rows=app_config.sampling.max_sample_rows_per_failure,
                mask_pii=app_config.sampling.mask_pii,
                max_repair_attempts=app_config.remediation.max_repair_attempts,
                repair_not_drop=(
                    module.repair_not_drop
                    if module.repair_not_drop is not None
                    else app_config.remediation.repair_not_drop
                ),
            )
            metadata.phase_reached = "remediation_processed"
            metadata.candidates_generated = len(outcome.candidates)
            metadata.iterations_run = outcome.loops_run
            metadata.stop_reason = outcome.stop_reason

            # rca_report.json always reflects the MOST RECENT loop's diagnosis
            # (the /api/runs/{id}/rca endpoint's contract - one report, not a
            # history); each loop's full candidate list is still in
            # post_remediation_result.json's `iterations` below.
            if outcome.rca_reports:
                _write_json(run_dir / "rca_report.json", outcome.rca_reports[-1].model_dump_json(indent=2))

            quarantine_path = None
            if export_quarantined:
                quarantine_path = export_quarantine_csv(
                    outcome.final_df,
                    outcome.final_result,
                    module,
                    run_id,
                    actual_source_ref,
                    outcome.loops_run,
                    app_config.paths.artifacts_root,
                )
                metadata.quarantine_file = str(quarantine_path) if quarantine_path else None

            _write_json(
                run_dir / "post_remediation_result.json",
                json.dumps(
                    {
                        "success": outcome.final_result.success,
                        "remaining_failures": len(outcome.final_result.failures),
                        "remaining_rows": len(outcome.final_result.failing_row_reasons),
                        "loops_run": outcome.loops_run,
                        "stop_reason": outcome.stop_reason,
                        "quarantine_file": str(quarantine_path) if quarantine_path else None,
                        "iterations": outcome.iterations,
                        "candidates": [c.candidate_id for c in outcome.candidates],
                    },
                    indent=2,
                ),
            )

            metadata.status = "succeeded"
            metadata.phase_reached = "loop_closed"
            return metadata

        except Exception as exc:
            metadata.status = "failed"
            metadata.error = str(exc)
            raise
        finally:
            metadata.finished_at = datetime.now(timezone.utc)
            _write_json(run_dir / "run_metadata.json", metadata.model_dump_json(indent=2))


def rerun_validation_after_promotion(module_name: str, run_id: str, app_config: AppConfig | None = None) -> dict:
    """Phase 7 only - this is what the CLI/API 'approve' action triggers.

    Re-reads the run's original source_ref (a plain CSV read, no Hadoop
    write path involved) and replays every currently-active fix, rather than
    restoring a cached DataFrame snapshot - see apply_active_fixes' docstring
    in stages.py for why. Still cheap: no LLM call, no re-validation of
    already-settled expectations beyond the current suite.
    """
    app_config = app_config or load_settings()
    registry = ModuleRegistry(app_config.paths.modules_root)
    module = registry.get(module_name)

    run_dir = app_config.paths.artifacts_root / "runs" / run_id
    run_metadata = json.loads((run_dir / "run_metadata.json").read_text(encoding="utf-8"))
    source_ref = run_metadata["source_ref"]

    spark = get_spark_session()
    source = get_data_source(module.source.format)
    df = source.read(spark, module, source_ref)

    engine = GXEngine(app_config.paths.great_expectations_root)
    rule_store = RuleStore(module)
    cleaned_df = apply_active_fixes(rule_store, df)
    result = close_loop(engine, module, rule_store, cleaned_df)

    comparison = {"run_id": run_id, "success": result.success, "remaining_failures": len(result.failures)}
    _write_json(run_dir / "post_approval_result.json", json.dumps(comparison, indent=2))
    return comparison

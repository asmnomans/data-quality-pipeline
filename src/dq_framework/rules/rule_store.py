"""Versioned storage for a module's active GE suite and accumulated PySpark
fixes, with mandatory archiving before every promotion and an append-only
audit log. This is the piece that makes "self-healing" safe to demo: no
promotion ever overwrites history, and every promotion (auto or manual) is
reversible via rollback().

Layout per module (see docs/ARCHITECTURE.md section 6.1):
    expectations/active/baseline_suite.json
    expectations/archive/<timestamp>__v<N>.json
    expectations/candidate/{pending,approved,rejected}/<candidate_id>.json
    expectations/promotion_log.jsonl
    remediation_code/active/cleaning_fixes.json
    remediation_code/archive/<timestamp>__v<N>.json
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from dq_framework.core.config import ModuleConfig
from dq_framework.core.exceptions import RuleStoreError
from dq_framework.core.models import Candidate, PromotionLogEntry, RCAResult

ArtifactKind = Literal["rule", "fix"]

_KIND_DIRS = {
    "rule": ("expectations", "baseline_suite.json"),
    "fix": ("remediation_code", "cleaning_fixes.json"),
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp_slug(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H-%M-%S")


class RuleStore:
    def __init__(self, module: ModuleConfig):
        if module.module_dir is None:
            raise RuleStoreError(f"Module '{module.name}' has no module_dir set.")
        self.module = module
        self.root = module.module_dir

    # -- paths ----------------------------------------------------------

    def _active_path(self, kind: ArtifactKind) -> Path:
        subdir, filename = _KIND_DIRS[kind]
        return self.root / subdir / "active" / filename

    def _archive_dir(self, kind: ArtifactKind) -> Path:
        subdir, _ = _KIND_DIRS[kind]
        return self.root / subdir / "archive"

    def _candidate_dir(self, status: str) -> Path:
        return self.root / "expectations" / "candidate" / status

    def _promotion_log_path(self) -> Path:
        return self.root / "expectations" / "promotion_log.jsonl"

    # -- active artifact read/init ---------------------------------------

    def get_active_suite(self) -> dict:
        return self._read_or_init("rule")

    def get_active_fixes(self) -> dict:
        return self._read_or_init("fix")

    def _read_or_init(self, kind: ArtifactKind) -> dict:
        path = self._active_path(kind)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            initial = (
                {"suite_name": f"{self.module.name}_baseline_suite", "version": 0, "expectations": []}
                if kind == "rule"
                else {"version": 0, "fixes": []}
            )
            path.write_text(json.dumps(initial, indent=2, default=str), encoding="utf-8")
            return initial
        return json.loads(path.read_text(encoding="utf-8"))

    def write_active_suite(self, expectations: list[dict], suite_name: str | None = None) -> dict:
        """Used once, by the baseline-calibration phase, to seed v0 -> v1
        with no promotion ceremony (there's nothing to archive yet)."""
        current = self.get_active_suite()
        payload = {
            "suite_name": suite_name or current["suite_name"],
            "version": current["version"] + 1,
            "expectations": expectations,
        }
        self._active_path("rule").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return payload

    # -- candidates -------------------------------------------------------

    def add_candidate(
        self,
        run_id: str,
        artifact_type: ArtifactKind,
        rca_result: RCAResult,
        sandbox_passed: bool,
        sandbox_notes: str | None,
        iteration: int = 1,
    ) -> Candidate:
        candidate = Candidate(
            candidate_id=str(uuid.uuid4()),
            run_id=run_id,
            module=self.module.name,
            artifact_type=artifact_type,
            created_at=_utc_now(),
            rca_result=rca_result,
            sandbox_passed=sandbox_passed,
            sandbox_notes=sandbox_notes,
            status="pending",
            iteration=iteration,
        )
        self._write_candidate(candidate, "pending")
        return candidate

    def list_candidates(self, status: str = "pending") -> list[Candidate]:
        directory = self._candidate_dir(status)
        if not directory.exists():
            return []
        out = []
        for f in sorted(directory.glob("*.json")):
            out.append(Candidate.model_validate_json(f.read_text(encoding="utf-8")))
        return out

    def get_candidate(self, candidate_id: str) -> Candidate:
        for status in ("pending", "approved", "rejected"):
            path = self._candidate_dir(status) / f"{candidate_id}.json"
            if path.exists():
                return Candidate.model_validate_json(path.read_text(encoding="utf-8"))
        raise RuleStoreError(f"No candidate '{candidate_id}' found for module '{self.module.name}'.")

    def _write_candidate(self, candidate: Candidate, status: str) -> None:
        directory = self._candidate_dir(status)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{candidate.candidate_id}.json").write_text(
            candidate.model_dump_json(indent=2), encoding="utf-8"
        )

    def _move_candidate(self, candidate: Candidate, from_status: str, to_status: str) -> None:
        old_path = self._candidate_dir(from_status) / f"{candidate.candidate_id}.json"
        self._write_candidate(candidate, to_status)
        if old_path.exists():
            old_path.unlink()

    # -- promotion (the guarded, archived, logged path) -------------------

    def promote(self, candidate_id: str, actor: str) -> Candidate:
        """Archive the current active artifact, apply the candidate as the
        new active artifact, move it to approved/, log the event. Used by
        BOTH auto-promotion (actor="system:auto") and manual approval
        (actor="user:<email>") - identical code path either way.

        Idempotent: a double-submitted approval (a slow network + an
        impatient double-click is a completely ordinary way for this to
        happen, not an edge case) returns the already-promoted candidate
        as-is instead of re-archiving and re-applying the fix/rule a second
        time - promotion is a one-way transition, not a repeatable action.
        """
        candidate = self.get_candidate(candidate_id)
        if candidate.status in ("approved", "auto_promoted"):
            return candidate
        kind: ArtifactKind = candidate.artifact_type

        before_ref = self._archive_current(kind)
        after_ref = self._apply_candidate_as_active(candidate, kind)

        candidate.status = "auto_promoted" if actor.startswith("system:") else "approved"
        self._move_candidate(candidate, "pending", candidate.status)

        self._append_log(
            PromotionLogEntry(
                timestamp=_utc_now(),
                run_id=candidate.run_id,
                candidate_id=candidate.candidate_id,
                artifact_type=kind,
                action="auto_promoted" if actor.startswith("system:") else "approved",
                before_ref=before_ref,
                after_ref=after_ref,
                actor=actor,
            )
        )
        return candidate

    def reject(self, candidate_id: str, actor: str, reason: str | None = None) -> Candidate:
        candidate = self.get_candidate(candidate_id)
        if candidate.status == "rejected":
            return candidate  # idempotent, same reasoning as promote() above
        candidate.status = "rejected"
        self._move_candidate(candidate, "pending", "rejected")
        self._append_log(
            PromotionLogEntry(
                timestamp=_utc_now(),
                run_id=candidate.run_id,
                candidate_id=candidate.candidate_id,
                artifact_type=candidate.artifact_type,
                action="rejected",
                before_ref=None,
                after_ref=reason or "no reason given",
                actor=actor,
            )
        )
        return candidate

    def rollback(self, kind: ArtifactKind, archive_ref: str, actor: str) -> dict:
        """Restore an archived version as active. Archives the version it
        replaces first, so a rollback is itself just another logged
        promotion event - never a special, unaudited case."""
        archive_path = Path(archive_ref)
        if not archive_path.is_absolute():
            archive_path = self.root.parents[1] / archive_ref
        if not archive_path.exists():
            raise RuleStoreError(f"Archive ref does not exist: {archive_path}")

        before_ref = self._archive_current(kind)
        restored = json.loads(archive_path.read_text(encoding="utf-8"))
        current = self._read_or_init(kind)
        restored["version"] = current["version"] + 1
        self._active_path(kind).write_text(json.dumps(restored, indent=2, default=str), encoding="utf-8")

        self._append_log(
            PromotionLogEntry(
                timestamp=_utc_now(),
                run_id="manual-rollback",
                candidate_id="n/a",
                artifact_type=kind,
                action="rolled_back",
                before_ref=before_ref,
                after_ref=str(self._active_path(kind)),
                actor=actor,
            )
        )
        return restored

    # -- internals ----------------------------------------------------------

    def _archive_current(self, kind: ArtifactKind) -> str:
        active_path = self._active_path(kind)
        current = self._read_or_init(kind)
        archive_dir = self._archive_dir(kind)
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_dir / f"{_timestamp_slug(_utc_now())}__v{current['version']}.json"
        archive_path.write_text(active_path.read_text(encoding="utf-8"), encoding="utf-8")
        return str(archive_path)

    def _apply_candidate_as_active(self, candidate: Candidate, kind: ArtifactKind) -> str:
        current = self._read_or_init(kind)
        if kind == "rule":
            current["expectations"].append(candidate.rca_result.new_ge_expectation)
        else:
            current["fixes"].append(
                {
                    "fix_id": candidate.candidate_id,
                    "description": candidate.rca_result.root_cause_explanation,
                    "code": candidate.rca_result.suggested_pyspark_fix,
                    "added_at": _utc_now().isoformat(),
                }
            )
        current["version"] += 1
        active_path = self._active_path(kind)
        active_path.write_text(json.dumps(current, indent=2, default=str), encoding="utf-8")
        return str(active_path)

    def _append_log(self, entry: PromotionLogEntry) -> None:
        path = self._promotion_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(entry.model_dump_json() + "\n")

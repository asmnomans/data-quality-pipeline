"""Candidate review queue: list pending fixes/rules, approve/reject them.
approve() calls RuleStore.promote() - the exact same call the CLI's
`dq approve` makes - then triggers the cheap phase-7-only re-validation, so
the UI can show a before/after without re-running the whole pipeline.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from dq_framework.core.config import AppConfig, ModuleRegistry
from dq_framework.pipeline.runner import rerun_validation_after_promotion
from dq_framework.rules.rule_store import RuleStore

from backend.app.deps import get_app_config, get_module_registry
from backend.app.schemas import ActorRequest, RejectRequest

router = APIRouter(prefix="/api", tags=["candidates"])


@router.get("/candidates")
def list_candidates(
    module: str,
    status: str = "pending",
    registry: ModuleRegistry = Depends(get_module_registry),
):
    store = RuleStore(registry.get(module))
    return [c.model_dump(mode="json") for c in store.list_candidates(status)]


@router.post("/candidates/{candidate_id}/approve")
def approve_candidate(
    candidate_id: str,
    module: str,
    body: ActorRequest | None = None,
    registry: ModuleRegistry = Depends(get_module_registry),
    app_config: AppConfig = Depends(get_app_config),
):
    store = RuleStore(registry.get(module))
    candidate = store.promote(candidate_id, actor=(body.actor if body else "user:api"))
    comparison = rerun_validation_after_promotion(module, candidate.run_id, app_config)
    return {"candidate": candidate.model_dump(mode="json"), "comparison": comparison}


@router.post("/candidates/{candidate_id}/reject")
def reject_candidate(
    candidate_id: str,
    module: str,
    body: RejectRequest | None = None,
    registry: ModuleRegistry = Depends(get_module_registry),
):
    store = RuleStore(registry.get(module))
    candidate = store.reject(
        candidate_id, actor=(body.actor if body else "user:api"), reason=(body.reason if body else None)
    )
    return candidate.model_dump(mode="json")

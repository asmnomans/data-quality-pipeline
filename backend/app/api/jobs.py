from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from backend.app.deps import get_job_runner
from backend.app.jobs.inprocess_runner import InProcessJobRunner

router = APIRouter(prefix="/api", tags=["jobs"])


@router.get("/jobs/{job_id}")
def get_job(job_id: str, job_runner: InProcessJobRunner = Depends(get_job_runner)):
    try:
        return job_runner.status(job_id)
    except KeyError:
        raise HTTPException(404, f"No job '{job_id}'") from None

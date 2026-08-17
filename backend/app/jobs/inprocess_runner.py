"""POC JobRunner: a bounded ThreadPoolExecutor + an in-memory status dict.

Deliberately NOT a hardcoded assumption - api/deps.py hands this out through
the JobRunner protocol, so moving to Celery/RQ later means writing one new
class, not touching any route in api/.
"""
from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from pydantic import BaseModel


class InProcessJobRunner:
    def __init__(self, max_workers: int = 2):
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def submit(self, fn, *args, **kwargs) -> str:
        job_id = str(uuid.uuid4())
        with self._lock:
            self._jobs[job_id] = {"status": "running", "result": None, "error": None}
        self._executor.submit(self._run, job_id, fn, *args, **kwargs)
        return job_id

    def _run(self, job_id: str, fn, *args, **kwargs) -> None:
        try:
            result = fn(*args, **kwargs)
            if isinstance(result, BaseModel):
                result = result.model_dump(mode="json")
            with self._lock:
                self._jobs[job_id] = {"status": "done", "result": result, "error": None}
        except Exception as exc:
            with self._lock:
                self._jobs[job_id] = {"status": "failed", "result": None, "error": str(exc)}

    def status(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        return {"job_id": job_id, **job}

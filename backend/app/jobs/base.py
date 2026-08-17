"""JobRunner abstraction: swap InProcessJobRunner for a Celery/RQ-backed
implementation later without any API route changing - they only ever call
.submit()/.status()."""
from __future__ import annotations

from typing import Any, Protocol


class JobRunner(Protocol):
    def submit(self, fn, *args, **kwargs) -> str: ...
    def status(self, job_id: str) -> dict[str, Any]: ...

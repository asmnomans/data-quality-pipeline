"""Thin request bodies for the API. Response payloads are, deliberately, the
core library's own pydantic models (FailureReport, RCAReport, Candidate,
RunMetadata) dumped straight through - the API adds no shape of its own on
the way out, only on the way in.
"""
from __future__ import annotations

from pydantic import BaseModel


class RunRequest(BaseModel):
    source_ref: str | None = None


class ActorRequest(BaseModel):
    actor: str = "user:api"


class RejectRequest(BaseModel):
    actor: str = "user:api"
    reason: str | None = None

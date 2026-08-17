"""FastAPI app factory. Every route wraps the core library (src/dq_framework)
- the CLI wraps the exact same functions (see cli.py) - so this process
holds no business logic of its own, only HTTP plumbing + a background job
runner (see docs/ARCHITECTURE.md section 7).
"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from dq_framework.core.exceptions import DQFrameworkError

from backend.app.api import candidates, jobs, runs
from backend.app.deps import get_app_config


def create_app() -> FastAPI:
    app_config = get_app_config()
    app = FastAPI(title="DQ Framework API", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=app_config.api.cors_origins or ["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(runs.router)
    app.include_router(candidates.router)
    app.include_router(jobs.router)

    @app.exception_handler(DQFrameworkError)
    def handle_dq_error(request: Request, exc: DQFrameworkError):
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


app = create_app()

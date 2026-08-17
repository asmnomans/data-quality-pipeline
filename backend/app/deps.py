"""FastAPI dependency providers. Every route gets its config/registry/job
runner through these - never imports dq_framework.core.config directly -
so tests can override them without monkeypatching module globals.
"""
from __future__ import annotations

from functools import lru_cache

from dq_framework.core.config import AppConfig, ModuleRegistry, load_settings

from backend.app.jobs.inprocess_runner import InProcessJobRunner

_job_runner = InProcessJobRunner(max_workers=2)


@lru_cache
def get_app_config() -> AppConfig:
    return load_settings()


@lru_cache
def get_module_registry() -> ModuleRegistry:
    return ModuleRegistry(get_app_config().paths.modules_root)


def get_job_runner() -> InProcessJobRunner:
    return _job_runner

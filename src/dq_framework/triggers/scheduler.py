"""Periodic trigger: APScheduler running in-process, one cron job per module
that declares `trigger.modes: [scheduled, ...]` + a `trigger.schedule` cron
string in its module.yaml. Swappable later for Airflow/Databricks Jobs
calling the CLI on a schedule instead - nothing downstream would change.
"""
from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from dq_framework.core.config import AppConfig, ModuleRegistry
from dq_framework.pipeline.runner import run

logger = logging.getLogger(__name__)


class SchedulerTrigger:
    def __init__(self, registry: ModuleRegistry, app_config: AppConfig):
        self.registry = registry
        self.app_config = app_config
        self.scheduler = BackgroundScheduler()

    def start(self) -> None:
        for module in self.registry.list():
            if "scheduled" in module.trigger.modes and module.trigger.schedule:
                self.scheduler.add_job(
                    self._run_module,
                    CronTrigger.from_crontab(module.trigger.schedule),
                    args=[module.name],
                    id=f"scheduled-{module.name}",
                    replace_existing=True,
                    max_instances=1,  # a slow run should skip the next tick, not stack up
                )
                logger.info("Scheduled '%s' on cron '%s'", module.name, module.trigger.schedule)
        self.scheduler.start()

    def _run_module(self, module_name: str) -> None:
        try:
            run(module_name, app_config=self.app_config)
        except Exception:
            logger.exception("Scheduled run failed for module '%s'", module_name)

    def stop(self) -> None:
        self.scheduler.shutdown(wait=False)

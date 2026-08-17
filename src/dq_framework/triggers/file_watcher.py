"""Event-driven trigger: a filesystem watcher on each module's
`trigger.watch_path`, firing a run the moment a matching file lands. In
production this adapter gets swapped for an S3 event notification or a
queue consumer that calls the same `pipeline.runner.run` - the interface
(start/stop, react-to-new-file) stays identical.

Idempotency by content hash: the same file re-appearing (a re-save, a
watcher re-trigger on a metadata-only touch) is detected and skipped rather
than silently reprocessed - a duplicate run would mean a duplicate LLM call
and, if fix_promotion_mode is 'auto', a duplicate promoted candidate.
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from dq_framework.core.config import AppConfig, ModuleConfig, ModuleRegistry
from dq_framework.pipeline.runner import run

logger = logging.getLogger(__name__)


def _content_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


class _ProcessedHashStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, list[str]] = json.loads(path.read_text()) if path.exists() else {}

    def seen(self, module_name: str, digest: str) -> bool:
        return digest in self._data.get(module_name, [])

    def mark(self, module_name: str, digest: str) -> None:
        self._data.setdefault(module_name, []).append(digest)
        self.path.write_text(json.dumps(self._data, indent=2), encoding="utf-8")


class _ModuleFileHandler(FileSystemEventHandler):
    def __init__(self, module: ModuleConfig, app_config: AppConfig, hash_store: _ProcessedHashStore, project_root: Path):
        self.module = module
        self.app_config = app_config
        self.hash_store = hash_store
        self.project_root = project_root

    def on_created(self, event):
        if not event.is_directory:
            self._maybe_trigger(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._maybe_trigger(event.src_path)

    def _maybe_trigger(self, raw_path: str) -> None:
        path = Path(raw_path)
        if path.suffix.lstrip(".") != self.module.source.format:
            return

        try:
            digest = _content_hash(path)
        except OSError:
            return  # file is still being written; a later on_modified will retry

        if self.hash_store.seen(self.module.name, digest):
            logger.info("Skipping duplicate event-driven trigger (identical content already processed): %s", path)
            return
        self.hash_store.mark(self.module.name, digest)

        source_ref = str(path.relative_to(self.project_root))
        # Off the watchdog thread: a Spark job must not block file-event delivery.
        threading.Thread(target=self._run, args=(source_ref,), daemon=True).start()

    def _run(self, source_ref: str) -> None:
        try:
            run(self.module.name, source_ref=source_ref, app_config=self.app_config)
        except Exception:
            logger.exception("Event-driven run failed for '%s' (%s)", self.module.name, source_ref)


class FileWatcherTrigger:
    def __init__(self, registry: ModuleRegistry, app_config: AppConfig):
        self.registry = registry
        self.app_config = app_config
        self.observer = Observer()
        self.project_root = app_config.paths.modules_root.parent
        self.hash_store = _ProcessedHashStore(
            app_config.paths.artifacts_root / "runs" / ".processed_hashes.json"
        )

    def start(self) -> None:
        for module in self.registry.list():
            if "event_driven" not in module.trigger.modes or not module.trigger.watch_path:
                continue
            watch_dir = self.project_root / module.trigger.watch_path
            watch_dir.mkdir(parents=True, exist_ok=True)
            handler = _ModuleFileHandler(module, self.app_config, self.hash_store, self.project_root)
            self.observer.schedule(handler, str(watch_dir), recursive=False)
            logger.info("Watching '%s' for module '%s'", watch_dir, module.name)
        self.observer.start()

    def stop(self) -> None:
        self.observer.stop()
        self.observer.join(timeout=5)

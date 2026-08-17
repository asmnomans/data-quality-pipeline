"""Every trigger - manual (API upload+run), scheduled, event-driven - ends
by calling the identical `pipeline.runner.run(module, source_ref)`. Adding a
new trigger source (a Kafka consumer, an S3 event) means writing one new
class with start()/stop(), never touching the pipeline itself.
"""
from __future__ import annotations

from typing import Protocol


class Trigger(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...

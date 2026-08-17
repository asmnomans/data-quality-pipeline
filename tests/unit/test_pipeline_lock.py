"""_ModuleLock must self-heal: a lock whose owning process is dead (killed
before it could release the lock itself - a stop button, a closed terminal,
a crash) must be reclaimed automatically on the next attempt, not require a
human to notice and delete a file. This is exactly the failure mode hit
repeatedly during development before this fix.
"""
import os
import subprocess
import sys
import time

import pytest

from dq_framework.core.exceptions import PipelineLockError
from dq_framework.pipeline.runner import _ModuleLock


@pytest.fixture
def artifacts_root(tmp_path):
    return tmp_path


def _dead_pid() -> int:
    """A PID that WAS valid but is now guaranteed dead."""
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def test_lock_blocks_a_second_acquisition_while_holder_is_alive(artifacts_root):
    lock = _ModuleLock(artifacts_root, "orders")
    with lock:
        with pytest.raises(PipelineLockError):
            with _ModuleLock(artifacts_root, "orders"):
                pass  # our own process is alive, so this must NOT be reclaimed


def test_lock_releases_normally_on_exit(artifacts_root):
    lock = _ModuleLock(artifacts_root, "orders")
    with lock:
        assert lock.path.exists()
    assert not lock.path.exists()

    # and a fresh acquisition afterward must succeed with no error
    with _ModuleLock(artifacts_root, "orders"):
        pass


def test_lock_is_reclaimed_when_owning_process_is_dead(artifacts_root):
    lock_path = artifacts_root / "runs" / ".locks" / "orders.lock"
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text(str(_dead_pid()))

    with _ModuleLock(artifacts_root, "orders"):
        pass  # must not raise - the dead owner's lock is reclaimed


def test_lock_is_reclaimed_when_file_is_corrupt(artifacts_root):
    lock_path = artifacts_root / "runs" / ".locks" / "orders.lock"
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text("not-a-pid")

    with _ModuleLock(artifacts_root, "orders"):
        pass  # must not raise


def test_lock_is_reclaimed_when_stale_by_age(artifacts_root, monkeypatch):
    from dq_framework.pipeline import runner as runner_module

    monkeypatch.setattr(runner_module, "_MAX_LOCK_AGE_SECONDS", 0)

    lock_path = artifacts_root / "runs" / ".locks" / "orders.lock"
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text(str(os.getpid()))  # a genuinely live PID (this test process)
    old_time = time.time() - 10
    os.utime(lock_path, (old_time, old_time))

    with _ModuleLock(artifacts_root, "orders"):
        pass  # even though the PID is alive, age alone should trigger reclaim

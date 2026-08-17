#!/usr/bin/env python
"""Single entry point for the assignment's required 'python run_pipeline.py'
deliverable. Thin wrapper over the `dq` CLI (src/dq_framework/cli.py) - all
behavior lives in the library, not here.

Examples:
    python run_pipeline.py run-pipeline --module orders
    python run_pipeline.py candidates --module orders
    python run_pipeline.py approve <candidate_id> --module orders
"""
from dq_framework.cli import app

if __name__ == "__main__":
    app()

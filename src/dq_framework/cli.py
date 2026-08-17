"""`dq` CLI - one of several clients over the core library (see
docs/ARCHITECTURE.md section 7). The FastAPI backend calls the exact same
functions these commands call; neither one contains business logic itself.
"""
from __future__ import annotations

import json

import typer
from rich.console import Console
from rich.table import Table

from dq_framework.core.config import ModuleRegistry, load_settings
from dq_framework.core.exceptions import DQFrameworkError
from dq_framework.pipeline.runner import rerun_validation_after_promotion, run
from dq_framework.rules.rule_store import RuleStore

app = typer.Typer(help="Automated Data Quality & Self-Healing Pipeline CLI.")
console = Console()


@app.command()
def modules():
    """List every onboarded module (one row per modules/<name>/module.yaml)."""
    app_config = load_settings()
    registry = ModuleRegistry(app_config.paths.modules_root)
    table = Table("name", "description", "primary_key", "trigger modes")
    for m in registry.list():
        table.add_row(m.name, m.description, m.primary_key, ", ".join(m.trigger.modes))
    console.print(table)


@app.command()
def run_pipeline(
    module: str = typer.Option(..., "--module", "-m", help="Module name, e.g. 'orders'"),
    source_ref: str | None = typer.Option(
        None, "--source-ref", "-s", help="Relative path to a specific input file. Defaults to the latest file under the module's incoming_path."
    ),
):
    """Run the full pipeline (ingest -> validate -> RCA -> remediate -> promote -> close loop) for one module."""
    try:
        metadata = run(module, source_ref=source_ref)
    except DQFrameworkError as exc:
        console.print(f"[bold red]Run failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc

    console.print(f"[bold green]Run {metadata.run_id} -> {metadata.status}[/bold green]")
    console.print(f"  phase reached:        {metadata.phase_reached}")
    console.print(f"  total rows:            {metadata.total_rows}")
    console.print(f"  failed expectations:   {metadata.failed_expectation_count}")
    console.print(f"  candidates generated:  {metadata.candidates_generated}")


@app.command()
def candidates(
    module: str = typer.Option(..., "--module", "-m"),
    status: str = typer.Option("pending", "--status"),
):
    """List candidate fixes/rules for a module, filtered by status."""
    app_config = load_settings()
    registry = ModuleRegistry(app_config.paths.modules_root)
    store = RuleStore(registry.get(module))

    table = Table("candidate_id", "type", "category", "sandbox_passed", "run_id")
    for c in store.list_candidates(status=status):
        table.add_row(
            c.candidate_id,
            c.artifact_type,
            c.rca_result.failure_category,
            str(c.sandbox_passed),
            c.run_id,
        )
    console.print(table)


@app.command()
def approve(
    candidate_id: str,
    module: str = typer.Option(..., "--module", "-m"),
    actor: str = typer.Option("user:cli", "--actor"),
):
    """Promote a pending candidate (archives the current active version first,
    logs the promotion), then re-validates the cached run snapshot."""
    app_config = load_settings()
    registry = ModuleRegistry(app_config.paths.modules_root)
    module_config = registry.get(module)
    store = RuleStore(module_config)

    candidate = store.promote(candidate_id, actor=actor)
    console.print(f"[bold green]Promoted[/bold green] {candidate.candidate_id} ({candidate.artifact_type})")

    comparison = rerun_validation_after_promotion(module, candidate.run_id, app_config)
    console.print(f"  post-approval re-validation: {json.dumps(comparison)}")


@app.command()
def reject(
    candidate_id: str,
    module: str = typer.Option(..., "--module", "-m"),
    reason: str | None = typer.Option(None, "--reason"),
    actor: str = typer.Option("user:cli", "--actor"),
):
    """Reject a pending candidate (moved to rejected/, logged, never applied)."""
    app_config = load_settings()
    registry = ModuleRegistry(app_config.paths.modules_root)
    store = RuleStore(registry.get(module))
    candidate = store.reject(candidate_id, actor=actor, reason=reason)
    console.print(f"[bold yellow]Rejected[/bold yellow] {candidate.candidate_id}")


@app.command()
def rollback(
    module: str = typer.Option(..., "--module", "-m"),
    kind: str = typer.Option(..., "--kind", help="'rule' or 'fix'"),
    to: str = typer.Option(..., "--to", help="Path to an archived version, e.g. modules/orders/expectations/archive/2026-08-11T14-32-00__v2.json"),
    actor: str = typer.Option("user:cli", "--actor"),
):
    """Restore an archived rule/fix version as active - itself a logged, archived promotion."""
    app_config = load_settings()
    registry = ModuleRegistry(app_config.paths.modules_root)
    store = RuleStore(registry.get(module))
    restored = store.rollback(kind, to, actor=actor)
    console.print(f"[bold green]Rolled back[/bold green] {module} {kind} suite -> version {restored['version']}")


if __name__ == "__main__":
    app()

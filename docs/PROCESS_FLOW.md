# Process Flow — from `python run_pipeline.py` to the final report

This traces the actual runtime call chain, one hop at a time: which function
calls which, in what order, and what each one actually does. For the
system-level design rationale, see `docs/ARCHITECTURE.md`; this doc is the
"what happens when you hit run" companion to it.

## 1. Entry point

```
run_pipeline.py
  └─ from dq_framework.cli import app
     app()
```

[`run_pipeline.py`](../run_pipeline.py) is a two-line wrapper — the
assignment's required `python run_pipeline.py` deliverable. All real
behavior lives in the library (`src/dq_framework/`), not here.

## 2. Library / framework setup (not project-specific)

`app()` is a Typer application. Calling it triggers Typer/Click's own
internal startup (argument parsing, `completion_init()` registering shell
tab-completion handlers) — this is third-party library code, identical for
any Typer CLI, and has nothing to do with this project's logic. Once
argument parsing resolves which subcommand was invoked, Typer routes into
the matching function in [`cli.py`](../src/dq_framework/cli.py) — this is
where project-specific code starts.

## 3. CLI command → `runner.run()`

Running `python run_pipeline.py run-pipeline --module orders` hits
[`cli.py`](../src/dq_framework/cli.py)'s `run_pipeline()` command:

```python
@app.command()
def run_pipeline(module: str = ..., source_ref: str | None = ...):
    metadata = run(module, source_ref=source_ref)   # <- pipeline/runner.py::run()
    console.print(...)
```

It does nothing itself except call
[`pipeline/runner.py`](../src/dq_framework/pipeline/runner.py)`::run()` and
print the result — the CLI, the FastAPI backend (`POST /api/modules/{name}/runs`),
the APScheduler cron trigger, and the filesystem watcher all call this
exact same function. This is the single pipeline entry point regardless of
what triggered it.

## 4. `runner.run()` — the orchestrator

Everything below happens inside one `with _ModuleLock(...):` block (a
cross-process, self-healing, PID-aware file lock — see
`docs/ARCHITECTURE.md` section 4/9 — so two runs of the same module never
execute concurrently).

### 4.1 Setup

| Step | Calls | Does |
|---|---|---|
| Load config | `load_settings()`, `ModuleRegistry.get(module_name)` | Parses `config/settings.yaml` + `modules/orders/module.yaml` into validated Pydantic objects |
| Start Spark | `get_spark_session()` | Returns the (cached, process-wide) `SparkSession` |
| Init collaborators | `get_data_source(...)`, `GXEngine(...)`, `RuleStore(module)` | The CSV reader, the GX adapter, and the versioned rule/candidate store for this module |

### 4.2 Phase 1 — Baseline calibration

```
baseline_df = source.read_baseline(spark, module)
suite = calibrate_or_load_baseline(engine, rule_store, module, baseline_df)
```

`stages.py::calibrate_or_load_baseline()` checks the rule store's active
suite version. If it's still `0` (never calibrated), it profiles the clean
baseline CSV and writes the inferred `ExpectationSuite` as version 1. On
every later run it's a no-op read — the suite may already include rules
promoted by prior runs.

### 4.3 Phase 2 — Validate the incoming (dirty) data

```
dirty_df = source.read(spark, module, actual_source_ref)
engine_result = run_validation(engine, module, dirty_df, suite)
```

`stages.py::run_validation()` calls `GXEngine.validate()`, which builds a
GX `ExpectationSuite` from the stored JSON, runs `batch.validate()` against
the live Spark DataFrame (no CSV round-trip through GX), and converts the
raw GX result into the framework's own `EngineValidationResult` — including,
per failing expectation, the **full** set of primary keys that trip it (not
just a capped sample), used later for the convergence loop's stop condition
and the Quarantine export.

### 4.4 Phase 3 — Extract failures → `failure_report.json`

```
failure_report = extract_failures(engine_result, module, dirty_df, ...)
_write_json(run_dir / "failure_report.json", ...)
```

`stages.py::extract_failures()` → `failure_extraction/extractor.py::build_failure_report()`:
computes per-column profiles (native Spark aggregations, one combined job),
masks PII in the sample failing rows, and packages everything the LLM needs
to diagnose the failures. **This file always represents the pre-remediation
state** — the `/api/runs/{id}/compare` endpoint's "before" side reads it
directly, so it's never overwritten by a later loop.

If `engine_result.success` here (the incoming file was already clean), the
run ends immediately: `phase_reached = "loop_closed_no_failures"`, no LLM
call, no further phases.

### 4.5 Phases 4-7, repeated — the convergence loop

```
provider = build_llm_provider(app_config)
outcome = run_remediation_loop(engine, rule_store, module, provider,
                                baseline_df, dirty_df, engine_result,
                                run_id, actual_source_ref,
                                fix_mode, rule_mode,
                                loop_till_no_exception, max_loops, ...)
```

`stages.py::run_remediation_loop()` (see `docs/ARCHITECTURE.md` section 6.2
for the design rationale) repeats phases 4-7 as a `while not
engine_result.success:` loop:

| Phase | Calls | Does |
|---|---|---|
| 4. LLM RCA | `run_rca()` → `llm/rca_service.py::generate_rca_report()` | One structured-output LLM call per distinct failure type (Pydantic + Instructor — auto-retries on a schema-invalid response); a diagnosis that fails after retries is logged and skipped, never aborts the run |
| 5. Guarded remediation | `process_remediation()` | For each `RCAResult`: AST-validates + sandbox-runs the suggested fix on a sample, then the full frame (`remediation/applier.py`); records a `Candidate` either way. A passing fix updates the in-memory DataFrame regardless of promotion mode — in `manual` mode this is *provisional* (for looping purposes only); `rule_store.promote()` (persisting it into `active/cleaning_fixes.json`) still only fires in `auto` mode |
| 6. Rule promotion | `process_remediation()` (same call) | Each proposed new GE rule is dry-run against the *clean baseline* (`rules/rule_validator.py`) before being allowed to promote — rejects a rule that would flag good data as bad |
| 7. Loop closure | `close_loop()` | Re-validates the current DataFrame against whatever suite is active **right now** (may have grown mid-loop via rule promotion) |

After each loop's phase 7, the loop checks (in order): is it now clean? →
stop (`"clean"`). Is the set of still-failing primary keys identical to the
previous loop's? → stop (`"no_progress"`, nothing is helping). Has
`max_loops` been hit? → stop (`"max_loops_reached"`). Is
`loop_till_no_exception` false? → stop after this one loop
(`"single_pass_mode"`). Otherwise, loop again with a fresh RCA round against
whatever's still failing.

### 4.6 Wrap-up

```
_write_json(run_dir / "rca_report.json", outcome.rca_reports[-1]...)   # most recent loop's diagnosis
if export_quarantined:
    quarantine_path = export_quarantine_csv(outcome.final_df, outcome.final_result, ...)
_write_json(run_dir / "post_remediation_result.json", {...loop history...})
```

`remediation/quarantine_export.py::export_quarantine_csv()` writes the rows
still failing when the loop closed (if any) — original columns + a `Reason`
column — to
`artifacts/runs/<run_id>/fileOutput/QuarantineData/<run_id>_<source-stem>_Quarantine_loop<N>.csv`,
written once (the final loop's leftovers), gated by `export_quarantined`.
The final cleaned DataFrame itself is **never** written to disk — it only
ever exists in-memory for the run's duration.

`run_metadata.json` is written last, in a `finally` block, whether the run
succeeded or raised — it always reflects however far the run actually got
(`phase_reached`, `status`, and now also `iterations_run` / `stop_reason` /
`quarantine_file`).

## 5. The separate "approve" flow

Approving a pending candidate (CLI `dq approve <id> --module orders`, or the
UI) does **not** re-run the whole pipeline:

```
cli.py::approve()
  ├─ RuleStore.promote(candidate_id, actor)        # archives + activates + logs
  └─ runner.py::rerun_validation_after_promotion()
       ├─ source.read(spark, module, source_ref)   # re-read the ORIGINAL run's source file
       ├─ apply_active_fixes(rule_store, df)        # replay every promoted fix, in order
       └─ close_loop(engine, module, rule_store, cleaned_df)   # phase 7 only
```

`stages.py::apply_active_fixes()` re-reads the plain source CSV (never a
cached DataFrame snapshot — see that function's docstring for why: Spark's
file *writers* need a real `winutils.exe` on Windows, reads don't) and
replays every currently-promoted fix from `active/cleaning_fixes.json` in
order, so re-validation always reflects everything promoted since the
original run — not a frozen snapshot from it. This is cheap: no LLM call, no
re-diagnosis, just one Spark read + N sandboxed snippet replays + one GX
validate.

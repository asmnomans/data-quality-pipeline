# Data Quality & Self-Healing Pipeline — Architecture

Status: **Implemented, tested, and running.** This started as a pre-code design
reference; it has since been kept up to date against the real, working
implementation (verified end-to-end against the real dataset — see README's
"Verified behavior" section) rather than left as a planning artifact.

## 1. Goal

Ingest a dataset, validate it against a calibrated Great Expectations suite, capture
failures, use an LLM to diagnose root cause and propose a fix + a new rule, apply the
fix and promote the rule under guardrails, and close the loop by re-validating. This
must work for the `orders` dataset today and for any dataset domain added later
without touching core code.

## 2. Core design principles

1. **Every external dependency sits behind an interface.** `DataSource`,
   `ValidationEngine`, `LLMProvider`, `RuleStore`, `Trigger`, `JobRunner`. Swapping an
   implementation (GX version, OpenAI ↔ local LLaMA, filesystem ↔ S3, in-process job
   runner ↔ Celery) never touches pipeline logic.
2. **A "module" is config, not code.** A module is one data domain (`orders` today).
   Onboarding a new one means adding a folder under `modules/`, not editing core
   files. The pipeline, scheduler, and file-watcher all discover modules by scanning
   the registry at startup.
3. **The LLM never touches production data directly.** Its proposed PySpark fix is
   AST-validated (no `import os`, `eval`, `exec`, filesystem/network calls) and
   dry-run against a *sample* before being applied to the full quarantine set. Its
   proposed GE rule is dry-run against the *clean baseline* before promotion, so it
   can't get promoted while it would flag good data as bad.
4. **Promotion gate is configurable, not hardcoded.** `auto` or `manual`, settable
   per artifact type (fix vs. rule), per module. Both code paths exist for real —
   this isn't a stub.
5. **Artifact-first, no database.** Every run writes JSON/Parquet to
   `artifacts/runs/<run_id>/`. Every pending candidate is its own JSON file under
   `modules/<name>/expectations/candidate/pending/`. CLI, API, scheduler, and watcher
   all read/write the same files — one source of truth, no state drift between
   clients.
6. **Triggers are adapters, not pipeline logic.** A run starts because of a cron
   tick, a new file landing, or a UI click — all three call the identical
   `runner.run(module, source_ref)`. Adding a Kafka-consumer trigger later means
   writing one new adapter, not touching the pipeline.
7. **Stages are stateless and idempotent**, orchestrated by a thin runner — so the
   same core logic can run under the CLI today and under Airflow/Databricks Jobs
   later with no rewrite.

## 3. Workflow — phase by phase

| Phase | What happens | Key artifact |
|---|---|---|
| 0. Bootstrap | Load `module.yaml`, init GX data context | — |
| 1. Baseline profiling | Ingest clean/control dataset via Spark → GX profiler infers/calibrates baseline `ExpectationSuite` | `expectations/active/baseline_suite.json` (version 1) |
| 2. Test validation | Ingest dirty/test dataset → `batch.validate(suite)` directly (no GX Checkpoint object - simpler, and all we needed) | raw GX `ValidationResult` |
| 3. Failure extraction | Normalize result + column profile + top-N sample rows/failure (PII-masked) into internal schema | `failure_report.json` |
| 4. LLM RCA | Batch failures by expectation type (token-budget aware) → structured-output call (both OpenAI & local LLaMA providers available, configurable primary/fallback) → `{failure_category, root_cause, pyspark_fix, new_ge_rule}` per failure | `rca_report.json` |
| 5. Guarded remediation | AST-validate + sandbox-run each `pyspark_fix` on a sample; if clean, apply per `fix_promotion_mode` (auto = applied immediately **and** promoted; manual = applied provisionally in-memory so looping can still proceed, but never promoted without an explicit approve) | cleaned DataFrame + quarantine log |
| 6. Rule promotion | Dry-run each `new_ge_rule` against the clean baseline (no false positives); promote per `rule_promotion_mode` | `expectations/active/baseline_suite.json` bumped to the next version; prior version moved to `expectations/archive/` |
| 7. Loop closure | Re-validate cleaned data against the (possibly just-grown) active suite; phases 3-7 repeat as a convergence loop rather than a single pass — see 6.2 | `post_remediation_result.json` (per-loop history) + `fileOutput/QuarantineData/*.csv` (still-failing rows, if any) + before/after comparison (rendered by the React UI's Run Detail page - GX's own Data Docs HTML generation was never wired in; JSON artifacts + the frontend are the reporting layer) |

Approving a pending candidate (via CLI `dq approve` or the UI) re-runs **only**
phase 7: it re-reads the run's original source file (a plain CSV read - cheap,
no LLM call) and replays every currently-active promoted fix onto it, rather
than restoring a cached DataFrame snapshot. This isn't just a Windows
workaround — see section 9's note on why a snapshot-cache was deliberately
rejected — it also means re-validation always reflects everything promoted
since the original run, not a frozen point-in-time copy. A separate explicit
"re-run full pipeline" action exists for when the source data itself changes.

## 4. Trigger layer

Three trigger modes, all real, all funneling into the same `runner.run(module,
source_ref)`:

- **Manual:** `POST /api/modules/{name}/upload` saves an uploaded file to
  `data/incoming/{module}/`, returns a `source_ref`; UI then calls
  `POST /api/modules/{name}/runs {source_ref}`.
- **Scheduled:** `APScheduler` running in the FastAPI process reads each module's
  `trigger.schedule` cron expression at startup and calls the same run function on
  interval. Swappable later for Airflow/Databricks Jobs calling the CLI instead.
- **Event-driven:** a `watchdog` folder watcher on `data/incoming/{module}/` fires a
  run when a new file matching the module's pattern appears. Swappable later for an
  S3 event notification or queue consumer — same interface.

Guardrails required regardless of trigger source:
- **Per-module run lock** — one in-flight Spark job per module; a second trigger
  (e.g. schedule fires mid manual-run) is rejected instead of contending for the
  driver. Self-healing: the lock stores its owning process's PID, and a stale
  lock left behind by a killed process (stop button, closed terminal, crash —
  not a graceful shutdown) is detected and reclaimed automatically on the next
  attempt, rather than requiring a human to delete the file. See section 9.
- **Idempotency by content hash** — a file re-appearing (watcher re-trigger, re-upload
  of the same file) is detected and treated as a duplicate, not reprocessed blindly.

```yaml
# module.yaml — trigger section
trigger:
  modes: [manual, scheduled, event_driven]
  schedule: "0 2 * * *"
  watch_path: data/incoming/orders/
```

## 5. LLM orchestration

Both providers implemented for real, behind `LLMProvider` — and as one shared
class (`OpenAICompatibleProvider` in `llm/openai_compatible.py`), not two
separate implementations: Ollama serves an OpenAI-compatible `/v1` endpoint, so
"OpenAI" and "local LLaMA" differ only in `base_url`/`model`/`api_key`, all
config, never code. See `config/settings.yaml` for the live values (illustrative
shape only below — check that file for what's actually active):

```yaml
llm:
  primary_provider: local_llama    # no OpenAI key yet - avoid wasting retries hitting it
  fallback_provider: null          # set to local_llama (or vice versa) once a real key exists
  providers:
    openai:      { model: gpt-4o-mini, temperature: 0, api_key_env: OPENAI_API_KEY }
    local_llama: { base_url_env: LOCAL_LLAMA_BASE_URL, model_env: LOCAL_LLAMA_MODEL }
```

Structured output enforced via Pydantic + Instructor (`llm/rca_service.py`
calls `provider.generate_structured(..., RCAResult)`); every response is
schema-validated with automatic retries (Instructor re-prompts the model with
the validation error) before being accepted - confirmed working against a real
model response that was missing a required field. Row samples sent to the LLM
are capped (token-budget aware) and PII-masked (emails, structurally - letters
become `x`, digits become `N`, punctuation/length preserved so format
anomalies stay diagnosable) before leaving the process.

## 6. Promotion gate

```yaml
# config/settings.yaml - check that file for the currently-active values,
# these are illustrative. Per-module overrides are also supported (see
# ModuleConfig fields of the same names in core/config.py).
remediation:
  fix_promotion_mode: auto        # auto | manual
  rule_promotion_mode: auto       # auto | manual - can be set independently of fix_promotion_mode
  loop_till_no_exception: true    # see 6.2 - false = exactly one RCA->fix->re-validate pass
  max_loops: 5                    # hard cap regardless of loop_till_no_exception
  export_quarantined: true        # see 6.2 - write still-failing rows to a CSV when the loop closes
```

Both branches share the same underlying `rule_store` / `applier` calls; the only
difference is whether the outcome of the automated dry-run checks is applied
immediately or written to `pending/` awaiting an explicit approve/reject (via CLI
`dq approve|reject` or the UI's Candidate Review page — same code path either way).

### 6.2 Convergence loop — repeating phases 3-7 until stable

The original single-pass design (validate → RCA → remediate → re-validate,
once) left rows unfixed whenever one round of LLM-suggested fixes couldn't
resolve everything in a single try, even though further RCA rounds against
the *remaining* failures sometimes could. `pipeline/stages.py::run_remediation_loop`
repeats phases 3-7 until one of three conditions is met:

- **Fully clean** — the active suite reports zero failures. `stop_reason: "clean"`.
- **No progress** — the exact same set of primary keys (`ModuleConfig.primary_key`,
  e.g. `order_id`) is still failing after this loop as after the previous one.
  Whatever the LLM proposed this round didn't help, and there's no reason to
  expect the next round to differ. `stop_reason: "no_progress"`.
- **`max_loops` reached** (default 5, configurable) — a hard cap regardless of
  ongoing progress, so an LLM that keeps finding *something* new to try (never
  repeating the identical failing set, but never converging either) can't loop
  indefinitely. `stop_reason: "max_loops_reached"`.

Progress is measured by **primary-key set**, not failure count: in
`rule_promotion_mode: auto`, a newly promoted rule can legitimately surface a
brand-new failure on the very next loop that didn't exist before (a stricter
suite catching something the old one missed) — that's real adaptation, not a
stuck loop, and comparing raw counts alone could mistake it for either.

`remediation.loop_till_no_exception: false` disables the repeat entirely —
exactly one RCA → remediate → re-validate pass runs regardless of outcome,
matching the original single-pass behavior (`stop_reason: "single_pass_mode"`,
or `"clean"` if that one pass happened to resolve everything).

**Manual promotion mode inside the loop:** a fix is applied to the in-memory
DataFrame every loop regardless of `fix_promotion_mode`, so the loop can keep
making real progress across rounds — but `rule_store.promote()` (the step that
actually persists a fix into `active/cleaning_fixes.json`) still only fires in
`auto` mode. In `manual` mode every candidate from every loop is recorded as
`pending` (tagged with which loop produced it via `Candidate.iteration`) and
still requires an explicit `approve` afterward; nothing from the loop's
provisional application is persisted on its own.

**Quarantine export** (`remediation/quarantine_export.py`, gated by
`export_quarantined`): once the loop closes, the rows still failing at that
point (if any) are written once — not per loop — to
`artifacts/runs/<run_id>/fileOutput/QuarantineData/<run_id>_<source-file-stem>_Quarantine_loop<N>.csv`,
where `<N>` is whichever loop the process actually stopped at. Each row keeps
its original columns plus a `Reason` column (every `"<expectation_type> on
'<column>'"` it's still failing), with no PII masking (an internal ops file,
not LLM-facing). Written via `.toPandas()` on just the quarantined subset —
never the full dataset - for the same winutils.exe reason documented in
section 9 for Spark's native writers. The final cleaned DataFrame itself is
**not** written to disk anywhere; it only ever exists in-memory for the
run's duration, same as before this feature.

`post_remediation_result.json` now carries the whole loop's history
(`loops_run`, `stop_reason`, `quarantine_file`, and an `iterations` array of
`{loop, remaining_failures, remaining_rows, candidates}`), and
`run_metadata.json` gained matching `iterations_run` / `stop_reason` /
`quarantine_file` fields — additive changes only, so the existing
`/api/runs/{id}/compare` endpoint keeps working unmodified.

### 6.1 Archiving & audit trail — every promotion is reversible and traceable

Both suite promotions (new GE rules) and remediation-code promotions (new PySpark
fixes accumulated into the "refined engine") go through the **same versioning
discipline** before the change lands:

1. Copy the current active version to `archive/<UTC-timestamp>__v<N>.json` —
   the old version is never overwritten in place, only ever moved sideways
   into the archive. This applies to both rules and fixes.
2. Write the new version as the active one (`active/baseline_suite.json` for
   rules, `active/cleaning_fixes.json` for fixes). Fixes are stored as a JSON
   registry of `{fix_id, description, code, added_at}` records - deliberately
   **not** an importable `.py` module. An LLM-authored fix is never imported
   or executed as real application code; it's always a stored string, run
   through the same AST-sandboxed `exec()` (`remediation/sandbox.py`) whether
   it's being dry-run for the first time or replayed on a later run.
3. Append one line to `promotion_log.jsonl` (append-only, one file per module):
   `{timestamp, run_id, candidate_id, artifact_type: rule|fix, action:
   auto_promoted|approved|rejected, before_ref, after_ref, actor}`.

This gives us, for free:
- **A full history** of how the ruleset/cleaning code evolved run over run —
  answers "why did this expectation start firing last Tuesday?".
- **Rollback** — `dq rules rollback --module orders --to <archive_ref>` restores an
  archived version as active again (itself archiving the version it replaces, so
  rollback is just another logged promotion event, not a special case).
- **A demo-friendly artifact** — the UI's Run Detail page can render the
  promotion log as a timeline (rule vX → vX+1 → vX+2, who/what promoted each).

```
modules/orders/
├── expectations/
│   ├── active/baseline_suite.json         # always the current suite
│   ├── archive/2026-08-11T14-32-00__v2.json
│   ├── candidate/{pending,approved,rejected}/
│   └── promotion_log.jsonl                # append-only audit trail
└── remediation_code/
    ├── active/cleaning_fixes.json         # accumulated, promoted PySpark fixes (JSON registry, not .py)
    └── archive/2026-08-11T14-32-00__v2.json
```

## 7. Interfaces (backend, frontend, CLI) — three clients, one core

```
                    ┌──────────────────┐
   CLI (dq run,     │                  │
   dq approve) ────▶│  dq_framework    │◀──── FastAPI backend ──▶ React (Vite) frontend
                     │  core library    │      (jobs, candidates,
   Scheduler/        │                  │       runs REST API)
   Watcher ─────────▶│                  │
                     └──────────────────┘
```

REST contract (backend wraps core, holds no business logic of its own):

| Endpoint | Does |
|---|---|
| `GET /api/modules` | List onboarded modules (name, description, primary key, trigger modes) |
| `POST /api/modules/{name}/upload` | Save uploaded file, return `source_ref` |
| `POST /api/modules/{name}/runs` | Start a pipeline run (background job), return `job_id` |
| `GET /api/jobs/{job_id}` | Poll job status |
| `GET /api/runs?module=` | List past runs for a module (scans `artifacts/runs/*/run_metadata.json` - no DB) |
| `GET /api/runs/{run_id}` | Run summary: pass/fail counts, phase reached, artifact links |
| `GET /api/runs/{run_id}/failures` | Failure report |
| `GET /api/runs/{run_id}/rca` | RCA report (proposed fixes + rules) |
| `GET /api/candidates?status=pending` | Everything awaiting approval, across runs |
| `POST /api/candidates/{id}/approve` | Promote (same code CLI uses) → triggers re-validation job |
| `POST /api/candidates/{id}/reject` | Move to `rejected/`, log reason |
| `GET /api/runs/{run_id}/compare?before=&after=` | Before/after pass-rate comparison |

Frontend: React + Vite + TypeScript SPA. Pages: Runs list, Run detail (failures + RCA
side by side), Candidate Review (approve/reject queue with before/after diff).

## 8. Project structure

```
DataQualityProject/
├── docs/
│   └── ARCHITECTURE.md                 # this file
├── run_pipeline.py                     # CLI entrypoint
├── config/
│   └── settings.yaml                   # llm, remediation, paths, thresholds
├── .vscode/                              # one-click debug/run configs (see README "VS Code")
│   ├── settings.json                     # interpreter path, pytest discovery, ESLint scope
│   ├── launch.json                       # CLI/backend/frontend debug configs + full-stack compound
│   ├── tasks.json                        # background server tasks + health-check-gated "Run the App"
│   └── extensions.json                   # recommended extensions
│
├── modules/                             # << extensibility point: one folder per data domain
│   └── orders/
│       ├── module.yaml                  # source, schema, critical_columns, trigger, promotion overrides
│       ├── expectations/
│       │   ├── active/baseline_suite.json      # current active suite
│       │   ├── archive/<timestamp>__v<N>.json   # every prior version, never overwritten
│       │   ├── candidate/{pending,approved,rejected}/
│       │   └── promotion_log.jsonl              # append-only audit trail
│       └── remediation_code/
│           ├── active/cleaning_fixes.json       # accumulated, promoted PySpark fixes (JSON registry, not .py)
│           └── archive/<timestamp>__v<N>.json
│
├── src/dq_framework/
│   ├── core/            # config.py, models.py (Pydantic), exceptions.py
│   ├── ingestion/       # base.py (DataSource protocol, get_spark_session), spark_reader.py
│   ├── validation/      # base.py, gx_engine.py, profiler.py
│   ├── failure_extraction/  # extractor.py
│   ├── llm/             # base.py, openai_compatible.py (ONE shared class for both providers),
│   │                    # provider_factory.py (primary/fallback wiring), prompt_templates/, rca_service.py
│   ├── remediation/     # sandbox.py (AST safety + exec), applier.py (sample-then-full apply),
│   │                    # quarantine_export.py (still-failing rows -> CSV, see 6.2)
│   ├── rules/           # rule_validator.py, rule_store.py
│   ├── triggers/        # base.py, scheduler.py, file_watcher.py
│   ├── pipeline/        # runner.py (orchestration + self-healing lock),
│   │                    # stages.py (incl. apply_active_fixes, run_remediation_loop - see 6.2)
│   └── cli.py
│
├── backend/                             # FastAPI service
│   └── app/
│       ├── main.py
│       ├── api/         # runs.py (+ modules/jobs list), candidates.py, jobs.py
│       ├── schemas.py
│       ├── jobs/        # base.py (JobRunner protocol), inprocess_runner.py
│       └── deps.py
│
├── frontend/                             # React + Vite SPA
│   └── src/
│       ├── pages/        # RunsList, RunDetail (failure drill-down + inline approve/reject), CandidateReview
│       └── api/client.ts
│
├── great_expectations/                   # GX data context (generated)
├── data/
│   ├── baseline/                         # control dataset
│   └── incoming/orders/                  # dirty/test dataset + uploaded files land here
├── artifacts/runs/<run_id>/              # failure_report.json, rca_report.json (most recent loop),
│                                          # post_remediation_result.json (full loop history), run_metadata.json,
│                                          # fileOutput/QuarantineData/*.csv (still-failing rows, if any - see 6.2)
└── tests/
    └── unit/                             # 32 tests: sandbox safety, rule_store archiving/idempotency/locking,
                                           # config, PII masking, remediation-loop stop conditions
```

Note: `reporting/` (a `report_writer.py` module) was planned in early design
but never built - Data Docs generation and reporting currently happen inline
in `pipeline/runner.py` and the frontend's Run Detail page, not as a separate
layer. Listed here so this doc doesn't claim something that doesn't exist.

## 9. Environment requirements (confirmed by spike)

- **Python 3.12** (constraint: `>=3.10,<3.14` — `great_expectations==1.20.0` refuses
  3.14 outright via `requires_python`). This dev machine ships only Python 3.14 by
  default; 3.12 was installed separately via `py install 3.12` and the project venv
  must be built on it explicitly (`py -3.12 -m venv .venv`), not on the system
  default `python`.
- **Java 17** (Temurin) — already present, required by PySpark's JVM.
- **GX version: pin to current `great_expectations==1.20.0`**, not an old 0.18.x
  release. Verified directly: `context.data_sources.add_spark(...)` →
  `add_dataframe_asset` → `batch_definition.get_batch(batch_parameters={"dataframe":
  df})` → `batch.validate(suite)` validates a live Spark DataFrame with no CSV
  round-trip through GX, and correctly flagged the assignment's injected anomalies
  (5,000/50,000 null `customer_id`, 6/50,000 out-of-bounds `order_amount`) against
  `pyspark==4.1.3`. `gx_engine.py` should be built directly against this Fluent API.
- Expect a harmless `Did not find winutils.exe` warning on Windows local-mode Spark
  runs — no Hadoop install needed, doesn't affect local file reads.
- **No Spark-native file *writer* is used anywhere in this pipeline, by design.**
  Reads work fine on Windows without `winutils.exe` (harmless WARN only), but
  any write (e.g. a `.write.parquet()` snapshot cache) routes through Hadoop's
  output-commit protocol, which on Windows *executes* `winutils.exe` — not
  just checks it exists. An empty placeholder satisfies the existence check
  but fails at execution (`CreateProcess error=193, %1 is not a valid Win32
  application`); a real one means downloading an unsigned third-party binary,
  which this project won't do. The one place that needed a write (caching a
  DataFrame snapshot for cheap post-approval re-validation) was redesigned
  instead: re-read the source CSV + replay active fixes (see
  `pipeline/stages.py::apply_active_fixes`) — portable, no Hadoop write path,
  and arguably more correct anyway (reflects all fixes promoted since the
  original run, not a frozen snapshot).
- **GX's file-backed context is not safe for concurrent use, and the backend
  runs jobs concurrently by design.** `GXEngine._batch_definition_for()` used
  a check-then-create pattern ("does this datasource exist? no? create it")
  for the module's Spark datasource/asset/batch_definition. Two threads
  hitting this at once (two backend jobs, or a run overlapping with an
  approve action's re-validation) can both decide it's missing and both try
  to create it — one raises `DataContextError`, or worse, one reads a
  half-written config and silently gets back a validation result where
  *every* expectation shows `unexpected_count: 0` on an empty `r.result`
  (which our code was defaulting silently instead of surfacing). Reproduced
  directly with two threads validating different DataFrames concurrently
  before diagnosing it. Fixed by serializing all GX operations in
  `gx_engine.py` behind one process-wide `threading.Lock()` — confirmed fixed
  with the same repro. Spark's own scheduler already queues real computation,
  so this costs little beyond what was already implicit.
- **`promote()` was not idempotent.** A double-submitted approval (a slow
  network response plus an impatient second click is an ordinary way for this
  to happen, not an edge case) re-archived and re-applied the same fix/rule
  every time it was called, appending duplicate entries and bumping the
  version each time. `rule_store.py::promote()`/`reject()` now check the
  candidate's current status first and return it unchanged if it's already
  been decided — promotion is a one-way transition, not a repeatable action.
  Covered by a dedicated test (`test_promote_is_idempotent_under_double_submission`).
- **A killed process left a stale run lock that blocked every future run
  indefinitely.** `pipeline/runner.py`'s per-module lock released correctly on
  normal completion (including handled exceptions) but had nothing to release
  it if the *process itself* was killed first (VS Code stop button, closed
  terminal, crash) - hit twice during development, each time requiring manual
  deletion of the lock file. Fixed: the lock file now stores its owning
  process's PID; before blocking on an existing lock, a cross-platform
  liveness check (`_pid_is_alive`, no extra dependency) confirms whether that
  process is still around, and a lock whose owner is dead (or whose age
  exceeds a 3-hour safety net, in case of PID reuse) is reclaimed
  automatically. Verified with a real dead-process test (spawns a subprocess,
  waits for it to exit, confirms its now-dead PID is detected) - see
  `tests/unit/test_pipeline_lock.py`.

## 10. Open items — resolved during implementation

Both items originally listed here as open are done; kept as a record of what
was decided, not as outstanding work:

- **`module.yaml` schema** — finalized as the `ModuleConfig` / `SourceConfig` /
  `ColumnSchemaEntry` / `TriggerConfig` models in `core/config.py`. See
  `modules/orders/module.yaml` for the real, in-use example (source paths,
  primary key, critical columns, trigger config, promotion-mode overrides).
- **Dataset schema enforcement** — implemented as an explicit `StructType`
  built from `module.yaml`'s `source.columns` list, applied in
  `ingestion/spark_reader.py` (not `inferSchema`, deliberately — see that
  file's docstring). The 7 columns (`order_id, customer_id, order_amount,
  customer_email, payment_method, country_code, order_timestamp`) are declared
  there with their types and nullability.

## 11. Deliverables checklist (from the assignment)

- [x] Modular code, README, setup script, dependencies
- [x] CLI entrypoint (`run_pipeline.py`) — end-to-end raw ingest → clean output.
      Verified against the real 50k-row dataset (see README "Verified behavior").
- [x] FastAPI + React UI (upload/run, review/approve, before/after) — beyond the
      literal ask, agreed as valuable for the demo. Verified live in-browser:
      run trigger, candidate review, approve → before/after (5 → 4 failures).
- [x] Scheduled + event-driven triggers, implemented AND verified firing for
      real (`triggers/file_watcher.py`): dropping a file into
      `data/incoming/orders/` while the watcher ran caused `failure_report.json`
      to appear with zero CLI/API calls — the trigger correctly invoked
      `runner.run()` on its own. The verification window was too short for
      that particular run's multi-minute LLM phase to finish before the
      watcher script itself exited (killing its daemon thread) — a property
      of a short-lived test script, not the trigger: a real deployment runs
      the trigger host as a genuinely long-lived process (systemd/Windows
      service/container), exactly like the `while True` loop in the README's
      example.
- [x] Convergence loop (beyond the literal single-pass ask) — repeats RCA →
      remediate → re-validate until clean, until two consecutive loops leave
      the identical set of rows still failing, or a configurable `max_loops`
      cap, whichever comes first (`remediation.loop_till_no_exception`, see
      section 6.2). Still-failing rows are exported once, per run, to
      `fileOutput/QuarantineData/*.csv` with a Reason column
      (`remediation.export_quarantined`).
- [ ] Demo video: architecture walkthrough, initial GX failure, LLM RCA log, repaired
      data output, UI approve flow

A more granular, spreadsheet form of this same tracking (39 line items across
10 categories, with descriptions) lives at `Deliverables_Checklist.xlsx` in
the project root.

## 12. Companion docs

- **`docs/PROCESS_FLOW.md`** — the runtime call chain, one hop at a time:
  entry point → Typer routing → `runner.run()`'s phases → the convergence
  loop → the separate `approve` flow. Written for debugging: "I ran the CLI,
  what actually executes and in what order?"
- **`docs/UNIT_TESTS.md`** — all 32 unit tests, grouped by file, with what
  each one actually verifies.

# Data Quality & Self-Healing Pipeline

Automated Data Quality & RCA Agent: PySpark ingestion + Great Expectations
validation + LLM-driven root-cause analysis and guarded self-remediation,
built as pluggable **modules** (one per data domain) so a new dataset can be
onboarded without touching core pipeline code.

Full design rationale, workflow phases, and extensibility model:
**[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** - read that first.

## What it does

1. Profiles a clean baseline dataset and calibrates a Great Expectations suite from it.
2. Validates a dirty/test dataset against that suite, capturing every failure with
   a column profile and masked sample rows.
3. Sends each failed expectation to an LLM (OpenAI or a local LLaMA model via
   Ollama - both implemented, with automatic fallback) for root-cause analysis,
   a suggested PySpark fix, and a proposed new GE rule.
4. Sandbox-validates every LLM-suggested fix (AST safety check + dry-run on a
   sample) and dry-runs every proposed rule against the clean baseline (so it
   can't be promoted while it would flag good data as bad).
5. Promotes fixes/rules - automatically or with human approval, configurable
   per artifact type - archiving the prior version and logging every
   promotion, so the whole rule/code history is auditable and reversible.
6. Re-validates to prove the loop closed.

## Project layout

```
config/settings.yaml       global config (LLM providers, promotion modes, paths)
modules/orders/             the one onboarded data module (module.yaml + versioned rules)
src/dq_framework/           the core library - every layer behind an interface
backend/app/                FastAPI service wrapping the core library
frontend/                   React + Vite SPA (run pipeline, review candidates)
data/baseline, data/incoming/orders/   the clean/dirty datasets
artifacts/runs/<run_id>/    every run's JSON artifacts
tests/unit/                 pytest suite
run_pipeline.py             CLI entrypoint (thin wrapper over src/dq_framework/cli.py)
```

## Setup

Requires **Python 3.10-3.13** (Great Expectations 1.20.0 does not support 3.14+
yet) and **Java 17+** (for PySpark). On Windows, if only Python 3.14 is
installed:

```bash
py install 3.12
```

Then, from the project root:

```bash
py -3.12 -m venv .venv
./.venv/Scripts/python.exe -m pip install --upgrade pip
./.venv/Scripts/python.exe -m pip install -e ".[dev]"
```

Copy `.env.example` to `.env` and fill in credentials:

```bash
cp .env.example .env
```

- **OpenAI path**: set `OPENAI_API_KEY`.
- **Local LLaMA path**: install [Ollama](https://ollama.com), run `ollama pull llama3.2:3b`
  (or any model you prefer - update `LOCAL_LLAMA_MODEL` to match), leave
  `LOCAL_LLAMA_BASE_URL` at its default (`http://127.0.0.1:11434/v1`).
- Both are configured in `config/settings.yaml` under `llm.primary_provider` /
  `llm.fallback_provider` - if the primary fails (missing key, model not
  pulled, timeout), the pipeline automatically falls back to the other.

## Running it

```bash
# Full pipeline, end to end, against the latest file in data/incoming/orders/
python run_pipeline.py run-pipeline --module orders

# Against a specific file
python run_pipeline.py run-pipeline --module orders --source-ref data/incoming/orders/dirty_orders_50k.csv

# List/approve/reject candidate fixes & rules
python run_pipeline.py candidates --module orders --status pending
python run_pipeline.py approve <candidate_id> --module orders
python run_pipeline.py reject <candidate_id> --module orders --reason "..."

# Roll back to an archived rule/fix version
python run_pipeline.py rollback --module orders --kind rule --to modules/orders/expectations/archive/<file>.json
```

Every run writes its artifacts to `artifacts/runs/<run_id>/`: `failure_report.json`,
`rca_report.json`, `post_remediation_result.json`, `run_metadata.json`. Approving
a candidate later re-reads the run's original source file and replays every
currently-active fix (see `pipeline/stages.py::apply_active_fixes`) rather than
restoring a cached snapshot - deliberately, not a Windows workaround; see
docs/ARCHITECTURE.md section 9 for why a DataFrame snapshot cache was rejected.

### Backend + frontend

```bash
# Terminal 1 - API
./.venv/Scripts/python.exe -m uvicorn backend.app.main:app --reload --port 8000

# Terminal 2 - UI
cd frontend && npm install && npm run dev
```

Open `http://localhost:5173`: run the pipeline (against the latest incoming
file, or upload your own CSV), then review/approve/reject candidates in real
time - approving re-validates immediately without re-ingesting the source file.

### Triggers (scheduled + event-driven)

Both are configured per module in `modules/orders/module.yaml` under
`trigger.modes`. They're started from a small standalone process (not the
API or CLI process) so they keep running independently:

```bash
./.venv/Scripts/python.exe -c "
from dq_framework.core.config import load_settings, ModuleRegistry
from dq_framework.triggers.scheduler import SchedulerTrigger
from dq_framework.triggers.file_watcher import FileWatcherTrigger
import time

config = load_settings()
registry = ModuleRegistry(config.paths.modules_root)
triggers = [SchedulerTrigger(registry, config), FileWatcherTrigger(registry, config)]
for t in triggers: t.start()
print('Triggers running. Ctrl+C to stop.')
try:
    while True: time.sleep(1)
except KeyboardInterrupt:
    for t in triggers: t.stop()
"
```

Drop a new `*.csv` file into `data/incoming/orders/` while this is running
and it fires automatically (event-driven); it also runs on the cron schedule
declared in `module.yaml` (`0 2 * * *` by default).

## Tests

```bash
./.venv/Scripts/python.exe -m pytest tests/unit -q
```

## VS Code

Open the project folder directly - `.vscode/` ships with everything preconfigured:

- **settings.json** points VS Code at `.venv` and `tests/unit` for the Testing
  sidebar (pytest), and scopes ESLint to `frontend/`.
- **launch.json** has debug configs for the CLI, the backend (no `--reload` -
  breakpoints don't survive uvicorn's reload subprocess), pytest, and a
  Chrome launch for the frontend, plus a **"Full stack: Backend + Frontend"**
  compound that starts both together.
- **tasks.json** has background tasks to start the backend/frontend servers
  without a debugger attached (`Terminal > Run Task...`).

First time: `Ctrl+Shift+P` → *Python: Select Interpreter* → pick
`.venv/Scripts/python.exe` if it isn't picked up automatically. Recommended
extensions are declared in `.vscode/extensions.json` - VS Code will prompt to
install them on first open.

## Adding a new module (new data domain)

1. Create `modules/<name>/module.yaml` (copy `modules/orders/module.yaml` as
   a template - source paths, column schema, critical columns, trigger config).
2. Drop the baseline/incoming data under `data/baseline/` and
   `data/incoming/<name>/`.
3. Run `python run_pipeline.py run-pipeline --module <name>`.

No core code changes required - see docs/ARCHITECTURE.md section 2 ("A module
is config, not code").

## Verified behavior (not just designed - actually run against the real data)

- **Validation is exact.** Against `dirty_orders_50k.csv`, the calibrated suite
  flags precisely the anomalies the dataset's key describes: 5,000/50,000 null
  `customer_id` (10.00%), 6/50,000 out-of-bounds `order_amount`, 9/50,000
  malformed `customer_email`, 6/50,000 categorical-drift `payment_method`, and
  51/50,000 temporal-anomaly `order_timestamp` - and the clean baseline passes
  with zero failures.
- **The guardrails are real, not decorative.** With a small local model
  (`llama3.2:3b` via Ollama), most RCA attempts fail schema validation or
  produce genuinely unsafe/invalid output - and the pipeline handles this
  correctly every time: a fix containing a banned `import` statement was
  rejected by the AST sandbox before ever executing; a proposed rule using
  `expect_column_values_to_be_between` (snake_case) instead of the required
  `ExpectColumnValuesToBeBetween` (the actual GX class name) was rejected by
  the structural check. Neither touched real data or the active suite.
  **A weak model is exactly the useful test case for this layer** - it proves
  the guardrails, not just the happy path. For materially better RCA quality,
  point `llm.primary_provider` at OpenAI (`gpt-4o-mini` or better) or a larger
  local model with strong tool-calling support.
- **The full approve loop works end to end.** A fix that did pass sandboxing
  (imputing null `customer_id`) was approved through the live UI and correctly
  dropped `remaining_failures` from 5 to 4 on re-validation - a real,
  measured improvement, not a mocked one.
- **Found and fixed 5 real bugs by actually running this**, not just code
  review - see docs/ARCHITECTURE.md section 9 for the Windows/Spark/GX
  specifics, the `promote()` idempotency fix in `rules/rule_store.py` (a
  double-submitted approval no longer re-applies a fix multiple times), and
  a GX concurrency race (two backend jobs running at once could corrupt each
  other's validation results) reproduced directly with a two-thread repro
  script and fixed with a lock in `validation/gx_engine.py`.

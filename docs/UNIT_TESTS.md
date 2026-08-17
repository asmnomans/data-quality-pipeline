# Unit Test Coverage

32 tests across 6 files, all under `tests/unit/`. All are pure-Python/fast —
none spin up a real Spark session, GX context, or LLM call (that behavior is
instead verified via real end-to-end runs, see README's "Verified behavior"
section and `docs/ARCHITECTURE.md` section 9's bug write-ups). Run with:

```bash
python -m pytest tests/unit -q
```

## `test_config.py` — config loading (3 tests)

Config loading is the one place YAML/env parsing happens; a typo in
`settings.yaml` or `module.yaml` should fail a test, not a demo run.

| Test | Verifies |
|---|---|
| `test_load_settings_resolves_paths_absolute` | `load_settings()` turns every relative path in `settings.yaml` (`data_root`, `artifacts_root`, ...) into an absolute path |
| `test_orders_module_loads_with_expected_shape` | `modules/orders/module.yaml` parses into a `ModuleConfig` with the right `primary_key`, `critical_columns`, source format, and column set |
| `test_unknown_module_raises` | Asking the registry for a module that doesn't exist raises `ModuleNotFoundError_`, not a `KeyError` |

## `test_extractor.py` — PII masking (3 tests)

PII masking is *structural*, not blanket redaction: it must preserve enough
shape (punctuation, length) for the LLM to still diagnose the anomaly
without ever leaking a real value into a third-party API call.

| Test | Verifies |
|---|---|
| `test_mask_structural_preserves_punctuation_and_length` | `jane.doe@example.com` masks to `xxxx.xxx@xxxxxxx.xxx` — same length, `@`/`.` preserved |
| `test_mask_structural_preserves_missing_at_sign` | A malformed email missing `@` still has no `@` after masking — the anomaly itself stays visible |
| `test_mask_pii_in_row_only_touches_configured_columns` | Masking a row only rewrites columns flagged as PII (e.g. `customer_email`); non-PII/non-string values (`order_id`, `order_amount`) pass through untouched |

## `test_pipeline_lock.py` — self-healing per-module run lock (5 tests)

The whole point of `_ModuleLock`: a lock whose owning process was killed
before it could release the lock itself (stop button, closed terminal,
crash — not a graceful shutdown) must be reclaimed automatically on the
next attempt, not require a human to notice and delete the file. This is
the exact failure mode hit repeatedly during development before the fix.

| Test | Verifies |
|---|---|
| `test_lock_blocks_a_second_acquisition_while_holder_is_alive` | A second acquisition attempt while the first is still open (and its process — this test's own — is alive) correctly raises `PipelineLockError` |
| `test_lock_releases_normally_on_exit` | Exiting the `with` block removes the lock file; a fresh acquisition afterward succeeds |
| `test_lock_is_reclaimed_when_owning_process_is_dead` | A lock file stamped with a PID that's since exited (a real subprocess spawned, then waited on) is reclaimed without raising |
| `test_lock_is_reclaimed_when_file_is_corrupt` | A lock file containing unparseable content (`"not-a-pid"`) is treated as reclaimable, not fatal |
| `test_lock_is_reclaimed_when_stale_by_age` | Even a *live* PID's lock is reclaimed once its file is older than `_MAX_LOCK_AGE_SECONDS` (a safety net against PID reuse) |

## `test_rule_store.py` — versioned rule/fix store (6 tests)

Every promotion must archive the prior version (never overwrite in place)
and append an audit-log entry; rollback is just another logged promotion,
not a special case.

| Test | Verifies |
|---|---|
| `test_active_suite_starts_at_version_zero` | A brand-new module's suite starts at version 0 (uncalibrated) |
| `test_promote_archives_before_overwriting` | Promoting a rule candidate bumps the suite version, and the pre-promotion version is preserved untouched under `archive/` |
| `test_promote_logs_to_promotion_log` | A promotion appends exactly one `action: auto_promoted` line to `promotion_log.jsonl` |
| `test_promote_is_idempotent_under_double_submission` | Calling `promote()` three times on the same candidate_id (a slow network + an impatient double-click is ordinary, not exotic) only archives/applies/logs once — not three times |
| `test_reject_never_touches_active_artifact` | Rejecting a candidate leaves the active suite completely unchanged and moves the candidate to `rejected/` |
| `test_rollback_restores_archived_version_and_is_itself_logged` | `rollback()` restores an archived suite as active again, and is itself recorded as a `rolled_back` promotion-log entry |

## `test_sandbox.py` — AST safety guardrail (7 tests)

`remediation/sandbox.py` must reject unsafe LLM-suggested code before it
ever executes, and must correctly execute+materialize genuinely safe code.
This is the guardrail that makes "apply an LLM's suggested PySpark fix
automatically" defensible at all.

| Test | Verifies |
|---|---|
| `test_rejects_import` | `import os` is rejected |
| `test_rejects_disallowed_identifier` | Calling `open(...)` is rejected |
| `test_rejects_dunder_attribute_access` | Accessing `df.__class__` is rejected |
| `test_rejects_with_statement` | A `with open(...) as f:` block is rejected |
| `test_rejects_syntax_error` | Unparseable code fails safely with a clear "Syntax error" note, not a crash |
| `test_allows_plain_filter_expression` | A legitimate `cleaned_df = df.filter(...)` snippet passes |
| `test_allows_fillna_style_impute` | A legitimate `withColumn(...).when(...).otherwise(...)` impute snippet passes |

## `test_remediation_loop.py` — convergence loop stop conditions (8 tests)

See `docs/ARCHITECTURE.md` section 6.2. `run_remediation_loop`'s three real
dependencies (`extract_failures`, `run_rca`, `process_remediation` — each of
which needs a real Spark DataFrame + GX engine + LLM call in production) are
monkeypatched out, so what's under test is purely the loop's own control
flow: does it read the sequence of `close_loop` results and decide correctly
when to stop.

| Test | Verifies |
|---|---|
| `test_stops_clean_as_soon_as_a_loop_fully_resolves` | Loop stops the moment a re-validation comes back with zero failures (`stop_reason="clean"`) |
| `test_stops_on_no_progress_when_two_consecutive_loops_match` | Two consecutive loops leaving the identical set of still-failing primary keys stops the loop with `stop_reason="no_progress"` |
| `test_progress_every_loop_keeps_going_until_max_loops` | A *different* single row failing every loop (never repeating, never empty) keeps looping until `max_loops` is hit, not before (`stop_reason="max_loops_reached"`) |
| `test_loop_till_no_exception_false_stops_after_one_loop_regardless` | `loop_till_no_exception=False` stops after exactly one loop even though the same failing set would otherwise trigger a second loop |
| `test_loop_till_no_exception_false_still_reports_clean_if_the_first_loop_resolves_it` | In single-pass mode, if that one loop happens to fully resolve everything, it's reported as `"clean"`, not the less-informative `"single_pass_mode"` |
| `test_candidates_are_tagged_with_their_loop_number` | Every candidate produced carries the correct `iteration` number for which loop generated it |
| `test_iterations_history_records_remaining_row_counts_per_loop` | The returned per-loop history (`loop`, `remaining_rows`) matches the actual sequence of `close_loop` results |
| `test_already_clean_initial_result_never_enters_the_loop` | If the very first validation (before any remediation) is already clean, the loop body never executes at all (`loops_run == 0`) |

"""Prompt construction for Module 2 (RCA & Remediation). One call per failed
expectation, not one call for the whole report - keeps each prompt small and
focused (token budget discipline) and lets failures fail/retry independently.

A module can override SYSTEM_PROMPT via modules/<name>/module.yaml in the
future (not wired yet - see ARCHITECTURE.md "prompts" field) without this
file changing.
"""
from __future__ import annotations

import json

from dq_framework.core.config import ModuleConfig
from dq_framework.core.models import ColumnProfile, FailureDetail

# The expectation vocabulary this framework actually profiles and validates in
# (see validation/profiler.py). Naming these in the prompt is what stops the
# model inventing `expect_string_to_match_regex` / `expect_timestamp_to_be_in_range`
# - neither exists in GX under any casing, though both have a real equivalent
# in this list. rule_validator still accepts ANY genuine GX expectation, so
# this steers without narrowing what's promotable.
# Each entry is (signature, which column types it accepts). The type column is
# load-bearing: given only the names, models reach for ...ToBeBetween on a
# string column - min_value="APPLE_PAY", or a regex as both bounds - which GX
# rejects outright, since Between is numeric/date only. The failing column's
# actual dtype is already in the payload's column_profile, so naming the
# applicable type per expectation is all that's needed to match them up.
_SUGGESTED_EXPECTATIONS = (
    ("ExpectColumnValuesToNotBeNull(column)", "any type"),
    ("ExpectColumnValuesToBeUnique(column)", "any type"),
    ("ExpectColumnValuesToBeInSet(column, value_set)", "categorical - use for string columns with few distinct values"),
    ("ExpectColumnValuesToMatchRegex(column, regex)", "string - use for format/pattern rules such as emails"),
    ("ExpectColumnValuesToBeBetween(column, min_value, max_value)", "NUMERIC or TIMESTAMP ONLY - never a string column"),
    ("ExpectColumnValueLengthsToBeBetween(column, min_value, max_value)", "string - bounds are integer lengths"),
)

SYSTEM_PROMPT = """\
You are a senior data reliability engineer performing automated root-cause \
analysis on a Great Expectations validation failure from a PySpark pipeline.

Given one failed expectation, its column profile, and a handful of masked \
sample failing rows, you must return:

- failure_category: a short label, e.g. "Schema Drift", "Upstream System Bug", \
"Sensor Anomaly", "Data Entry Error", "Categorical Drift", "Temporal Anomaly".
- root_cause_explanation: a concise 2-3 sentence executive summary of *why* \
the data likely failed - reason from the sample values and profile stats, \
don't just restate the expectation.
This is PySpark - PYTHON, not Scala. Spark's Scala API is far more common \
online, so be deliberate: write `&` not `&&`, `|` not `||`, `~` not `!`, and \
parenthesise every comparison you join - (F.col("a") > 1) & (F.col("b") < 2). \
No `$"col"`, no `val`, no `.as[...]`. It is also not pandas: no `df["c"].str`, \
no `.apply()`, no `inplace=`.

- suggested_pyspark_fix: a syntactically valid PySpark snippet that operates \
on an existing DataFrame variable named `df` and produces a cleaned \
DataFrame variable named `cleaned_df`. Only use pyspark.sql.functions \
(imported as F) and DataFrame methods - filter, impute with a sensible \
default, or quarantine the offending rows. Never import anything, never \
read/write files or network, never call exec/eval, never use .collect() on \
the full DataFrame.
- new_ge_expectation: a NEW Great Expectations expectation (different from \
the one that already caught this failure) designed to catch related edge \
cases earlier. Return it as {"expectation_type": "<PascalCase class name>", \
"kwargs": {...}}.

Pick expectation_type from this list - these are the only names that exist, \
and inventing one (e.g. "expect_string_to_match_regex") gets the rule thrown \
away. Match your choice to the column's dtype (given in column_profile):
%(expectations)s

Pass ONLY the kwargs shown in parentheses above, plus optionally \
"mostly": <0.0-1.0> to allow a tolerance. Any other key - "threshold", \
"batch_id", "expectation_type" nested inside kwargs - is rejected as an \
unpermitted field.

Worked example of the shape both fields must take:

  suggested_pyspark_fix:
    cleaned_df = df.filter(F.col("order_amount").isNotNull())
  new_ge_expectation:
    {"expectation_type": "ExpectColumnValuesToBeBetween",
     "kwargs": {"column": "order_amount", "min_value": 0, "max_value": 1000}}

Note the snippet assigns to `cleaned_df`, not to `df`, and has no import \
line - `F` and `df` are already defined for you. A snippet that assigns only \
`df`, or that starts with "from pyspark.sql import ...", is discarded.

Your fix MUST resolve the specific failed_expectation shown below, using its \
own kwargs. If it failed on a range, filter or repair against THAT range - a \
generic isNotNull() filter leaves out-of-range rows untouched and fixes \
nothing.

Prefer repairing over discarding. When only one column is bad and the rest of \
the row is valid, impute column_profile's "median_value" (an outlier-robust \
value taken from this very dataset) instead of dropping the row:

  cleaned_df = df.withColumn("order_timestamp",
      F.when(F.col("order_timestamp") > F.lit("2026-08-05 16:00:00").cast("timestamp"),
             F.lit("2026-08-02 03:58:13").cast("timestamp"))
       .otherwise(F.col("order_timestamp")))

Substitute the real values from failed_expectation and column_profile. Write \
the literal value only - never wrap it in angle brackets or any other \
placeholder marker, or the cast fails.

Drop rows only when the record is unusable as a whole. Never .select() a \
subset of columns - the fix is replayed on every future run, so a dropped \
column is gone permanently.

Some sample values have been structurally masked for privacy (letters -> x, \
digits -> N) but punctuation and length are preserved - malformed emails \
etc. are still diagnosable from the masked shape.

Diagnose from those samples, but NEVER copy one into a rule or a fix. \
"xxxx_xxx@xxxxxxx.xxx" is not a real value and a rule built from masked \
samples can never match real data. Take literal values from \
failed_expectation.kwargs and column_profile instead, which are unmasked. In \
particular do not build a value_set out of the failing samples - a value_set \
enumerates the values that are ALLOWED, not the ones that failed.
""" % {"expectations": "\n".join(f"  - {sig}\n      applies to: {types}" for sig, types in _SUGGESTED_EXPECTATIONS)}


_REPAIR_FIELDS = {
    "fix": "suggested_pyspark_fix",
    "rule": "new_ge_expectation",
}

_REPAIR_SYSTEM = {
    "fix": """\
You correct a single broken PySpark snippet. Return only the corrected snippet \
in `suggested_pyspark_fix`.

Rules: read `df`, assign the result to `cleaned_df`. `F` and `df` already \
exist - never import. Keep every column (no .select() of a subset). In PySpark \
`&` binds tighter than `>=`/`<=`, so parenthesize each comparison and make sure \
the parentheses balance: (F.col("t") >= a) & (F.col("t") <= b).\
""",
    "rule": """\
You correct a single malformed Great Expectations expectation. Return only the \
corrected object in `new_ge_expectation`, shaped \
{"expectation_type": "<PascalCase name>", "kwargs": {...}}.

Pass only the kwargs that expectation accepts, plus optionally "mostly". Keys \
like "threshold" or "batch_id" are rejected as unpermitted fields.\
""",
}


def build_repair_prompts(rca_result, artifact: str, error_note: str) -> tuple[str, str]:
    """Prompts for a second attempt at ONE artifact the guardrails rejected.

    Deliberately NOT the full SYSTEM_PROMPT: asking a small model to restate an
    entire RCAResult to correct one field makes it echo the JSON schema instead
    of an instance, which fails validation and wastes the retry. Narrow ask,
    narrow response model (FixRepair / RuleRepair).
    """
    field = _REPAIR_FIELDS[artifact]
    rejected = getattr(rca_result, field)
    user_prompt = (
        f"This {artifact} was REJECTED by an automated guardrail.\n\n"
        f"Rejected value:\n{json.dumps(rejected, indent=2, default=str)}\n\n"
        f"Rejection reason:\n{error_note}\n\n"
        "Correct that specific problem. Do not return the rejected value unchanged."
    )
    return _REPAIR_SYSTEM[artifact], user_prompt


def build_prompts(
    failure: FailureDetail, module: ModuleConfig, column_profile: ColumnProfile | None
) -> tuple[str, str]:
    payload = {
        "module": module.name,
        "primary_key": module.primary_key,
        "failed_expectation": {
            "expectation_type": failure.expectation_type,
            "column": failure.column,
            "kwargs": failure.expectation_kwargs,
        },
        "unexpected_count": failure.unexpected_count,
        "element_count": failure.element_count,
        "unexpected_percent": round(failure.unexpected_percent, 4),
        "column_profile": column_profile.model_dump() if column_profile else None,
        "sample_failed_rows": failure.sample_failed_rows,
    }
    user_prompt = (
        "Diagnose this Great Expectations validation failure and propose a fix + a new rule.\n\n"
        f"{json.dumps(payload, indent=2, default=str)}"
    )
    return SYSTEM_PROMPT, user_prompt

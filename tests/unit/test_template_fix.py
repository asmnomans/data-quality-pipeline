"""A range failure on a repair-only column has exactly one sensible repair, and
every input is already known - the expectation's own bounds and the profile's
median. There is nothing to reason about, yet llama3.2:3b spent five runs
failing to write this two-line snippet (Scala operators, unbalanced parens,
nested F.when chains). The template is the deterministic backstop; it only runs
after the LLM and its repair attempts are exhausted, and still faces every gate.
"""
from dq_framework.core.models import ColumnProfile, FailureDetail
from dq_framework.pipeline.stages import build_template_fix


def _failure(column, lo, hi):
    return FailureDetail(
        expectation_type="expect_column_values_to_be_between",
        column=column,
        expectation_kwargs={"column": column, "min_value": lo, "max_value": hi},
        element_count=50000,
        unexpected_count=51,
        unexpected_percent=0.1,
    )


def _profile(column, dtype, median):
    return ColumnProfile(
        column=column, dtype=dtype, row_count=50000, null_count=0, null_ratio=0.0, median_value=median
    )


def test_timestamp_bounds_and_median_are_cast():
    """A bare string literal compares as a string, not a timestamp."""
    out = build_template_fix(
        _failure("order_timestamp", "2026-07-29T16:00:01", "2026-08-05T16:00:00"),
        _profile("order_timestamp", "timestamp", "2026-08-02T03:58:13"),
    )
    assert out.count(".cast('timestamp')") == 3  # min, max, and the replacement
    assert "withColumn" in out and "otherwise" in out
    assert "filter" not in out  # must repair, never drop


def test_numeric_values_are_not_cast():
    out = build_template_fix(
        _failure("order_amount", 10.01, 500.0), _profile("order_amount", "double", 253.9)
    )
    assert "cast" not in out
    assert "253.9" in out and "10.01" in out and "500.0" in out


def test_comparisons_are_parenthesised():
    """The precedence trap that killed five LLM attempts - & binds tighter than
    the comparisons, so each side needs its own parens."""
    out = build_template_fix(
        _failure("order_amount", 1, 2), _profile("order_amount", "double", 1.5)
    )
    assert "(F.col('order_amount') < 1) | (F.col('order_amount') > 2)" in out
    assert out.count("(") == out.count(")")


def test_no_median_means_no_template():
    assert build_template_fix(_failure("order_amount", 1, 2), _profile("order_amount", "double", None)) is None


def test_no_profile_means_no_template():
    assert build_template_fix(_failure("order_amount", 1, 2), None) is None


def test_non_range_failure_is_left_to_the_llm():
    """Only min/max failures have a single obvious repair - a regex or set
    failure does not, so the template must decline rather than guess."""
    failure = FailureDetail(
        expectation_type="expect_column_values_to_match_regex",
        column="customer_email",
        expectation_kwargs={"column": "customer_email", "regex": "^x$"},
        element_count=1,
        unexpected_count=1,
        unexpected_percent=0.0,
    )
    assert build_template_fix(failure, _profile("customer_email", "string", "x")) is None

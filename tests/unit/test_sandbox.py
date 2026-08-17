"""remediation/sandbox.py must reject unsafe code before it ever executes,
and must correctly execute+materialize safe code. These are the guardrails
that make 'apply an LLM's suggested code automatically' defensible at all.
"""
from dq_framework.remediation.sandbox import check_ast_safety


def test_rejects_import():
    passed, notes = check_ast_safety("import os\ncleaned_df = df")
    assert not passed
    assert "Import" in notes


def test_rejects_disallowed_identifier():
    passed, notes = check_ast_safety('cleaned_df = df\nopen("x.txt", "w")')
    assert not passed
    assert "open" in notes


def test_rejects_dunder_attribute_access():
    passed, notes = check_ast_safety("x = df.__class__\ncleaned_df = df")
    assert not passed
    assert "__class__" in notes


def test_rejects_py4j_gateway_escape():
    """`df` is handed to the snippet, so `df.sparkSession._jvm` needs no import
    and trips no banned name - but it reaches the JVM, which in local mode is
    the driver process. Single underscore, so the old dunder-only check let it
    through."""
    passed, notes = check_ast_safety("gw = df.sparkSession._jvm\ncleaned_df = df")
    assert not passed
    assert "_jvm" in notes


def test_still_allows_ordinary_pyspark():
    passed, _ = check_ast_safety(
        'cleaned_df = df.filter(F.col("order_amount").isNotNull())\n'
        'cleaned_df = cleaned_df.fillna({"payment_method": "UNKNOWN"})'
    )
    assert passed


def test_rejects_with_statement():
    passed, notes = check_ast_safety('with open("x") as f:\n    pass\ncleaned_df = df')
    assert not passed


class _FakeDF:
    """Enough DataFrame surface for the schema guard - no Spark needed."""

    def __init__(self, columns):
        self.columns = columns

    def select(self, *cols):
        return _FakeDF(list(cols))

    def limit(self, n):
        return self

    def collect(self):
        return []

    def count(self):
        return 1


def test_rejects_a_fix_that_drops_columns(monkeypatch):
    """`.select('one_col')` is a real DataFrame that materializes, so the old
    gate promoted it - and apply_active_fixes then replayed it on every run,
    destroying every other column permanently."""
    import dq_framework.remediation.sandbox as sb

    monkeypatch.setattr(sb, "DataFrame", _FakeDF)
    df = _FakeDF(["order_id", "customer_id", "payment_method"])
    result = sb.run_snippet("cleaned_df = df.select('payment_method')", df)
    assert not result.passed
    assert "order_id" in result.notes and "customer_id" in result.notes


def test_allows_a_fix_that_preserves_every_column(monkeypatch):
    import dq_framework.remediation.sandbox as sb

    monkeypatch.setattr(sb, "DataFrame", _FakeDF)
    df = _FakeDF(["order_id", "payment_method"])
    result = sb.run_snippet("cleaned_df = df.select('order_id', 'payment_method')", df)
    assert result.passed, result.notes


def test_rejects_a_broken_column_expression_that_count_alone_would_miss(monkeypatch):
    """Spark prunes unreferenced columns, so .count() succeeds on a DataFrame
    whose withColumn expression is invalid (e.g. a malformed .cast()). That let
    a broken fix pass the sandbox and crash the whole run later in close_loop.
    Pulling real rows is what forces the expression to evaluate."""
    import dq_framework.remediation.sandbox as sb

    class LazyBomb:
        columns = ["order_id"]

        def withColumn(self, *a):
            return self

        def limit(self, n):
            return self

        def collect(self):
            raise ValueError("CAST_INVALID_INPUT: '<2026-08-02>' cannot be cast to TIMESTAMP")

        def count(self):
            return 50000  # count never touches the broken column

    monkeypatch.setattr(sb, "DataFrame", LazyBomb)
    result = sb.run_snippet("cleaned_df = df.withColumn('t', 1)", LazyBomb())
    assert not result.passed
    assert "CAST_INVALID_INPUT" in result.notes


def test_translates_scala_boolean_operators():
    """Spark's canonical API is Scala, so models emit `&&`/`||` for Column
    conditions. The snippet is otherwise correct - right column, right bounds."""
    from dq_framework.remediation.sandbox import normalize_dialect

    scala = 'cleaned_df = df.filter(F.col("t") >= F.lit("a") && F.col("t") <= F.lit("b"))'
    out = normalize_dialect(scala)
    assert "&&" not in out and "&" in out
    assert check_ast_safety(out)[0] is True


def test_scala_operators_also_get_the_grouping_fixed():
    """Swapping && for & fixes the token but not the precedence: & binds tighter
    than the comparisons, so Python reads a chained comparison and Column.__bool__
    raises CANNOT_CONVERT_COLUMN_INTO_BOOL. Both halves must be repaired."""
    from dq_framework.remediation.sandbox import normalize_dialect

    out = normalize_dialect('cleaned_df = df.filter(F.col("t") >= F.lit("a") && F.col("t") <= F.lit("b"))')
    assert out == "cleaned_df = df.filter((F.col('t') >= F.lit('a')) & (F.col('t') <= F.lit('b')))"


def test_scala_or_is_handled_too():
    from dq_framework.remediation.sandbox import normalize_dialect

    out = normalize_dialect('cleaned_df = df.filter(F.col("t") < F.lit("a") || F.col("t") > F.lit("b"))')
    assert out == "cleaned_df = df.filter((F.col('t') < F.lit('a')) | (F.col('t') > F.lit('b')))"


def test_single_comparison_is_left_alone():
    """Only 2-op chains are the broken form; a lone comparison must not be touched."""
    from dq_framework.remediation.sandbox import normalize_dialect

    code = 'cleaned_df = df.filter(F.col("a").isNotNull())'
    assert normalize_dialect(code) == code


def test_pandas_str_contains_becomes_rlike():
    """Unlike the Scala forms, pandas idioms PARSE - they only fail at execution,
    so there is no syntax error to catch them. This one survived a prompt that
    explicitly banned pandas."""
    from dq_framework.remediation.sandbox import normalize_dialect

    out = normalize_dialect('cleaned_df = df.filter(F.col("e").str.contains("^a", regex=True))')
    assert out == "cleaned_df = df.filter(F.col('e').rlike('^a'))"


def test_pandas_str_replace_becomes_regexp_replace():
    from dq_framework.remediation.sandbox import normalize_dialect

    out = normalize_dialect('cleaned_df = df.withColumn("e", F.col("e").str.replace("a", "b"))')
    assert out == "cleaned_df = df.withColumn('e', F.regexp_replace(F.col('e'), 'a', 'b'))"


def test_pandas_str_accessors_become_functions():
    from dq_framework.remediation.sandbox import normalize_dialect

    out = normalize_dialect('cleaned_df = df.withColumn("e", F.col("e").str.strip())')
    assert out == "cleaned_df = df.withColumn('e', F.trim(F.col('e')))"


def test_isbetween_becomes_between():
    from dq_framework.remediation.sandbox import normalize_dialect

    out = normalize_dialect('cleaned_df = df.filter(F.col("a").isBetween(1, 2))')
    assert out == "cleaned_df = df.filter(F.col('a').between(1, 2))"


def test_a_real_pyspark_contains_is_not_touched():
    """Column.contains() genuinely exists - only the `.str.` accessor form is
    pandas. Translating both would break working code."""
    from dq_framework.remediation.sandbox import normalize_dialect

    code = 'cleaned_df = df.filter(F.col("e").contains("@"))'
    assert normalize_dialect(code) == code


def test_valid_python_is_never_rewritten():
    from dq_framework.remediation.sandbox import normalize_dialect

    code = 'cleaned_df = df.filter((F.col("a") >= 1) & (F.col("b") <= 2))'
    assert normalize_dialect(code) == code


def test_rewrite_is_discarded_when_it_does_not_parse():
    """Only a rewrite that parses is accepted, so a snippet can never be
    silently corrupted - the original error is reported instead."""
    from dq_framework.remediation.sandbox import normalize_dialect

    broken = 'cleaned_df = df.filter(F.col("t") && '  # unbalanced regardless
    assert normalize_dialect(broken) == broken


def test_double_ampersand_inside_a_string_literal_is_left_alone():
    from dq_framework.remediation.sandbox import normalize_dialect

    code = 'cleaned_df = df.filter(F.col("t").rlike("a&&b"))'  # already parses
    assert normalize_dialect(code) == code


def test_rejects_syntax_error():
    passed, notes = check_ast_safety("cleaned_df = df.filter(")
    assert not passed
    assert "Syntax error" in notes


def test_allows_plain_filter_expression():
    passed, _ = check_ast_safety('cleaned_df = df.filter(F.col("customer_id").isNotNull())')
    assert passed


def test_allows_fillna_style_impute():
    passed, _ = check_ast_safety(
        'cleaned_df = df.withColumn("order_amount", F.when(F.col("order_amount") < 0, 0.0).otherwise(F.col("order_amount")))'
    )
    assert passed

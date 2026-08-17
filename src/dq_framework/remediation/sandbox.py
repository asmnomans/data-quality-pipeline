"""Never trust LLM-generated code with production data.

Two independent gates before an LLM's `suggested_pyspark_fix` touches
anything: (1) an AST walk that rejects imports, private/dunder attribute
access, exec/eval, os/sys/subprocess/socket names, and `with` blocks -
purely static, no code runs yet; (2) actual execution inside a namespace with a stripped-down
`__builtins__` (no open/eval/exec/__import__), against a DataFrame, in a
try/except that requires the result to be a real Spark DataFrame that
materializes without error. Only code that clears both gates ever reaches
`applier.py`, and even there it's tried on a sample before the full dataset.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass

import pyspark.sql.functions as F  # noqa: N812 - exposed to the sandbox namespace as `F`
from pyspark.sql import DataFrame

_DISALLOWED_NODE_TYPES = (
    ast.Import,
    ast.ImportFrom,
    ast.Global,
    ast.Nonlocal,
    ast.With,
    ast.AsyncWith,
    ast.Lambda,  # not needed for filter/impute/quarantine snippets; keeps the grammar small
)

_DISALLOWED_NAMES = {
    "eval", "exec", "compile", "__import__", "open", "input", "breakpoint",
    "getattr", "setattr", "delattr", "globals", "locals", "vars", "help",
    "os", "sys", "subprocess", "socket", "shutil", "pathlib", "pickle", "importlib",
}

_MATERIALIZE_SAMPLE_ROWS = 20

_SAFE_BUILTINS = {
    "len": len, "range": range, "str": str, "int": int, "float": float,
    "bool": bool, "dict": dict, "list": list, "tuple": tuple, "set": set,
    "min": min, "max": max, "sorted": sorted, "abs": abs, "round": round,
    "True": True, "False": False, "None": None,
}


@dataclass
class SandboxResult:
    passed: bool
    notes: str
    cleaned_df: DataFrame | None = None


_DIALECT_SUBSTITUTIONS = (("&&", "&"), ("||", "|"))

# pandas `.str.<x>()` accessors whose PySpark equivalent is a module-level F.<y>
_STR_TO_FUNCTION = {"strip": "trim", "lower": "lower", "upper": "upper", "len": "length"}


def normalize_dialect(code: str) -> str:
    """Translate Scala-Spark boolean operators into their PySpark equivalents.

    Spark's original API is Scala and the public corpus is saturated with it, so
    models reliably reach for `&&`/`||` when joining Column conditions - the
    snippet is otherwise correct (right column, right bounds), it just isn't
    Python. Same species of boundary mismatch as the GX snake_case/PascalCase
    translation in validation/base.py: a known, deterministic dialect gap,
    translated once rather than argued with via prompts.

    Only ever returns a rewrite that PARSES, so this cannot silently corrupt a
    snippet - notably `&&` inside a string literal (a regex, say) yields either
    unparseable code or a genuine no-op, and the original is kept. Everything
    downstream - safety walk, execution, schema and efficacy gates - still runs
    on the result; this buys a parse, not a pass.
    """
    swapped = False
    try:
        tree = ast.parse(code, mode="exec")
    except SyntaxError:
        # Scala operators are a SYNTAX error, so they have to be repaired
        # textually before there is any tree to work on.
        rewritten = code
        for scala_op, python_op in _DIALECT_SUBSTITUTIONS:
            rewritten = rewritten.replace(scala_op, python_op)
        if rewritten == code:
            return code
        try:
            tree = ast.parse(rewritten, mode="exec")
        except SyntaxError:
            return code  # rewrite didn't help - report the original error
        swapped = True

    # pandas forms, unlike Scala ones, PARSE cleanly and only fail at execution,
    # so this pass has to run over valid code too - not just over what failed.
    transformers = [_UnchainComparisons(), _PandasToPySpark()]
    for transformer in transformers:
        tree = transformer.visit(tree)
    if not swapped and not any(t.changed for t in transformers):
        return code  # nothing to do - hand back the original, unreformatted
    return ast.unparse(ast.fix_missing_locations(tree))


class _PandasToPySpark(ast.NodeTransformer):
    """Translate pandas idioms with exactly one PySpark equivalent.

    pandas shares the `df` variable name and much of the surface, so models
    blend the two - `.str.contains(..., regex=True)` for `.rlike(...)` was
    still being emitted after the prompt explicitly banned pandas forms. These
    parse fine and die at execution, so unlike the Scala case there is no
    syntax error to catch them.

    Only unambiguous one-to-one mappings belong here. Anything with a judgement
    call in it (pandas `.replace()` on a whole frame, `.apply()`, `inplace=`)
    is left to fail and reach the repair loop, which can ask about intent.
    """

    def __init__(self):
        self.changed = False

    def visit_Call(self, node: ast.Call) -> ast.AST:
        self.generic_visit(node)
        func = node.func
        if not isinstance(func, ast.Attribute):
            return node

        # X.isBetween(a, b) -> X.between(a, b)
        if func.attr == "isBetween":
            self.changed = True
            return ast.Call(
                func=ast.Attribute(value=func.value, attr="between", ctx=ast.Load()),
                args=node.args,
                keywords=[],
            )

        # everything below is a `.str.<method>` accessor, which PySpark has no
        # equivalent of - the methods live directly on Column, or on F.
        if not (isinstance(func.value, ast.Attribute) and func.value.attr == "str"):
            return node
        target = func.value.value  # the Column that `.str` was hung off

        # X.str.contains(pat, regex=...) -> X.rlike(pat); pandas defaults to
        # regex=True, so rlike is the faithful translation either way.
        if func.attr == "contains" and node.args:
            self.changed = True
            return ast.Call(
                func=ast.Attribute(value=target, attr="rlike", ctx=ast.Load()),
                args=node.args[:1],
                keywords=[],
            )

        # X.str.replace(pat, repl) -> F.regexp_replace(X, pat, repl)
        if func.attr == "replace" and len(node.args) >= 2:
            self.changed = True
            return ast.Call(
                func=ast.Attribute(
                    value=ast.Name(id="F", ctx=ast.Load()), attr="regexp_replace", ctx=ast.Load()
                ),
                args=[target, node.args[0], node.args[1]],
                keywords=[],
            )

        # X.str.strip()/lower()/upper() -> F.trim(X)/F.lower(X)/F.upper(X)
        if func.attr in _STR_TO_FUNCTION and not node.args:
            self.changed = True
            return ast.Call(
                func=ast.Attribute(
                    value=ast.Name(id="F", ctx=ast.Load()),
                    attr=_STR_TO_FUNCTION[func.attr],
                    ctx=ast.Load(),
                ),
                args=[target],
                keywords=[],
            )

        # X.str.startswith(a) -> X.startswith(a); same name, the accessor goes
        if func.attr in ("startswith", "endswith"):
            self.changed = True
            return ast.Call(
                func=ast.Attribute(value=target, attr=func.attr, ctx=ast.Load()),
                args=node.args,
                keywords=[],
            )

        return node


class _UnchainComparisons(ast.NodeTransformer):
    """Rewrite `a >= b & c <= d` into `(a >= b) & (c <= d)`.

    Swapping Scala's `&&` for `&` fixes the token but not the grouping: `&` is a
    BITWISE operator, which binds tighter than the comparisons around it, so
    Python parses the result as a chained comparison over `(b & c)`. Chaining
    implies `and`, `and` calls `__bool__`, and a Column raises there - the
    CANNOT_CONVERT_COLUMN_INTO_BOOL every one of these snippets died on.

    Safe to rewrite because the input form is ALWAYS broken - there is no
    PySpark program where chain-comparing Columns does something useful - so
    this can only ever turn a guaranteed runtime failure into the single
    reading it could have meant. The result still faces every downstream gate.
    """

    def __init__(self):
        self.changed = False

    def visit_Compare(self, node: ast.Compare) -> ast.AST:
        self.generic_visit(node)
        if len(node.ops) != 2 or not isinstance(node.comparators[0], ast.BinOp):
            return node
        middle = node.comparators[0]
        if not isinstance(middle.op, (ast.BitAnd, ast.BitOr)):
            return node
        self.changed = True
        left = ast.Compare(left=node.left, ops=[node.ops[0]], comparators=[middle.left])
        right = ast.Compare(left=middle.right, ops=[node.ops[1]], comparators=[node.comparators[1]])
        return ast.copy_location(ast.BinOp(left=left, op=middle.op, right=right), node)


def check_ast_safety(code: str) -> tuple[bool, str]:
    try:
        tree = ast.parse(code, mode="exec")
    except SyntaxError as exc:
        return False, f"Syntax error: {exc}"

    for node in ast.walk(tree):
        if isinstance(node, _DISALLOWED_NODE_TYPES):
            return False, f"Disallowed statement type: {type(node).__name__}"
        if isinstance(node, ast.Name) and node.id in _DISALLOWED_NAMES:
            return False, f"Disallowed identifier: '{node.id}'"
        # Single underscore, not dunder: `df.sparkSession._jvm` is a plain
        # attribute walk off the DataFrame we hand in - no import, no banned
        # name - and it reaches the py4j gateway, i.e. out of Python entirely.
        # `_sc` and `_jdf` are the same story. No legitimate filter/impute
        # snippet touches a private attribute, so ban the whole prefix.
        if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            return False, f"Disallowed private attribute access: '.{node.attr}'"

    return True, "Passed static safety check."


def run_snippet(code: str, df: DataFrame) -> SandboxResult:
    """Execute `code` against `df`. The snippet must read `df` and assign a
    Spark DataFrame to `cleaned_df` - nothing else about its shape is
    prescribed, so filter/impute/quarantine strategies are all valid."""
    code = normalize_dialect(code)
    safe, notes = check_ast_safety(code)
    if not safe:
        return SandboxResult(passed=False, notes=notes)

    namespace: dict = {"df": df, "F": F}
    try:
        compiled = compile(ast.parse(code, mode="exec"), "<llm_suggested_fix>", "exec")
        exec(compiled, {"__builtins__": _SAFE_BUILTINS}, namespace)  # noqa: S102 - gated by check_ast_safety above
    except Exception as exc:
        return SandboxResult(passed=False, notes=f"Execution error: {exc}")

    cleaned = namespace.get("cleaned_df")
    if cleaned is None:
        return SandboxResult(passed=False, notes="Snippet did not define a 'cleaned_df' variable.")
    if not isinstance(cleaned, DataFrame):
        return SandboxResult(passed=False, notes="'cleaned_df' is not a pyspark DataFrame.")

    # A snippet ending in .select('one_column') is still a DataFrame and still
    # counts, so "is it a DataFrame that materializes" passes it - and once
    # promoted, apply_active_fixes replays it on every future run, so every
    # other column is silently destroyed for good. Adding columns is fine;
    # losing one is never a data-quality fix.
    dropped = set(df.columns) - set(cleaned.columns)
    if dropped:
        return SandboxResult(
            passed=False,
            notes=f"'cleaned_df' dropped column(s) {sorted(dropped)} - a fix must preserve the schema.",
        )

    try:
        # .count() alone is NOT enough: Spark prunes unreferenced columns, so a
        # broken withColumn expression (e.g. a malformed .cast()) survives it and
        # only detonates later during real validation - taking the whole run down
        # with it. Pulling actual rows forces every column expression to evaluate.
        # ponytail: bounded sample, not the full frame - a malformed expression
        # fails on any row, and scanning 50k rows per candidate isn't worth it.
        cleaned.limit(_MATERIALIZE_SAMPLE_ROWS).collect()
        cleaned.count()
    except Exception as exc:
        return SandboxResult(passed=False, notes=f"'cleaned_df' failed to materialize: {exc}")

    return SandboxResult(passed=True, notes="Passed sandboxed execution.", cleaned_df=cleaned)

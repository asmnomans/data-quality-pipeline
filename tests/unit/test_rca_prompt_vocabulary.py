"""The prompt now hands the model a list of expectation names to choose from.
If any name in that list isn't real, the prompt is actively teaching the model
to produce rules the validator will reject - the exact failure it exists to
prevent. Pin every one against GX itself.
"""
from dq_framework.llm.prompt_templates.rca_prompt import _SUGGESTED_EXPECTATIONS, SYSTEM_PROMPT
from dq_framework.validation.base import resolve_expectation_class


def test_every_suggested_expectation_actually_exists():
    for signature, _types in _SUGGESTED_EXPECTATIONS:
        name = signature.split("(")[0]
        assert resolve_expectation_class(name) is not None, f"{name} is not a real GX expectation"


def test_suggested_names_are_rendered_into_the_prompt():
    for signature, types in _SUGGESTED_EXPECTATIONS:
        assert signature in SYSTEM_PROMPT
        assert types in SYSTEM_PROMPT
    assert "%(expectations)s" not in SYSTEM_PROMPT  # template actually interpolated


def test_between_is_flagged_as_numeric_only():
    """Models put min_value="APPLE_PAY" on a string column otherwise - GX
    rejects it with 9 validation errors and the rule is lost."""
    types = dict(_SUGGESTED_EXPECTATIONS)["ExpectColumnValuesToBeBetween(column, min_value, max_value)"]
    assert "NUMERIC" in types and "never a string" in types


def test_prompt_shows_the_two_things_models_get_wrong():
    # A worked example beats prose for small models - these are the exact
    # buckets that dominated the rejection counts.
    assert "cleaned_df = df.filter" in SYSTEM_PROMPT
    assert "mostly" in SYSTEM_PROMPT

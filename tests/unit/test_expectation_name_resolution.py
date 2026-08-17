"""LLMs answer with GX 0.18 snake_case names far more often than the PascalCase
the prompt asks for; before this was handled, every such rule candidate was
rejected as "not a known expectation" and no rule ever got promoted.
"""
from dq_framework.validation.base import resolve_expectation_class


def test_pascal_case_resolves():
    cls = resolve_expectation_class("ExpectColumnValuesToBeInSet")
    assert cls is not None and cls.__name__ == "ExpectColumnValuesToBeInSet"


def test_legacy_snake_case_resolves_to_the_same_class():
    assert (
        resolve_expectation_class("expect_column_values_to_be_in_set")
        is resolve_expectation_class("ExpectColumnValuesToBeInSet")
    )


def test_unknown_name_is_still_rejected():
    assert resolve_expectation_class("expect_string_to_match_regex") is None
    assert resolve_expectation_class("TotallyMadeUpExpectation") is None

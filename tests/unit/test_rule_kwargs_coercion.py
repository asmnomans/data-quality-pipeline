"""value_set arrives as a list-shaped STRING from JSON-mode generation.

GX rejects it with "value is not a valid list" and the rule is lost, even
though the text unambiguously denotes the container. Same boundary translation
as the Scala/pandas handling in sandbox.py - parse the encoding artifact, don't
argue with it in the prompt.
"""
from dq_framework.rules.rule_validator import _coerce_stringified_containers


def test_stringified_list_becomes_a_list():
    out = _coerce_stringified_containers({"column": "payment_method", "value_set": "['A', 'B']"})
    assert out["value_set"] == ["A", "B"]
    assert out["column"] == "payment_method"  # untouched


def test_real_list_is_left_alone():
    kwargs = {"value_set": ["A", "B"]}
    assert _coerce_stringified_containers(kwargs)["value_set"] == ["A", "B"]


def test_ordinary_strings_are_not_parsed():
    """A regex is a string that must stay a string."""
    kwargs = {"regex": "^[A-Za-z]+@x\\.com$"}
    assert _coerce_stringified_containers(kwargs)["regex"] == "^[A-Za-z]+@x\\.com$"


def test_unparseable_text_is_left_for_the_normal_check_to_reject():
    kwargs = {"value_set": "[not, valid, python"}
    assert _coerce_stringified_containers(kwargs)["value_set"] == "[not, valid, python"


def test_literal_eval_does_not_execute_code():
    """literal_eval, never eval - a call expression raises rather than running."""
    kwargs = {"value_set": "[__import__('os').system('echo pwned')]"}
    assert _coerce_stringified_containers(kwargs)["value_set"] == "[__import__('os').system('echo pwned')]"


def test_numbers_are_not_coerced():
    kwargs = {"min_value": "10.01"}  # not container-shaped
    assert _coerce_stringified_containers(kwargs)["min_value"] == "10.01"

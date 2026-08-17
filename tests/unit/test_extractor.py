"""PII masking must preserve structural shape (punctuation, length pattern)
so a malformed-email failure is still diagnosable, while never leaking a
real value into a prompt sent to a third-party LLM API.
"""
from dq_framework.failure_extraction.extractor import _mask_pii_in_row, _mask_structural


def test_mask_structural_preserves_punctuation_and_length():
    masked = _mask_structural("jane.doe@example.com")
    assert masked == "xxxx.xxx@xxxxxxx.xxx"
    assert len(masked) == len("jane.doe@example.com")


def test_mask_structural_preserves_missing_at_sign():
    # exactly the "malformed email" case this pipeline exists to diagnose
    masked = _mask_structural("invalid_email_at_google.com")
    assert "@" not in masked
    assert masked.count(".") == 1


def test_median_value_accepts_a_timestamp():
    """mean_value is float-typed, so a timestamp column had nowhere to put a
    central value and the LLM got min/max/mean all None on the very column it
    was asked to repair a range failure on. median_value must stay Any-typed.
    """
    from datetime import datetime

    from dq_framework.core.models import ColumnProfile

    profile = ColumnProfile(
        column="order_timestamp", dtype="timestamp", row_count=1, null_count=0, null_ratio=0.0,
        median_value=datetime(2026, 8, 1, 21, 58, 13),
    )
    assert profile.median_value.year == 2026


def test_mask_pii_in_row_only_touches_configured_columns():
    row = {"order_id": "ORD-000010", "customer_email": "jane@example.com", "order_amount": 42.5}
    masked = _mask_pii_in_row(row, pii_columns={"customer_email"})
    assert masked["order_id"] == "ORD-000010"  # untouched
    assert masked["order_amount"] == 42.5  # untouched, non-string
    assert masked["customer_email"] != "jane@example.com"
    assert "@" in masked["customer_email"]

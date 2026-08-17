"""Config loading is the one place YAML/env parsing happens - worth pinning
down directly so a typo in settings.yaml or module.yaml fails a test, not a
demo run.
"""
from pathlib import Path

from dq_framework.core.config import ModuleRegistry, load_settings

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_load_settings_resolves_paths_absolute():
    config = load_settings(PROJECT_ROOT / "config" / "settings.yaml")
    assert config.paths.data_root.is_absolute()
    assert config.paths.artifacts_root.is_absolute()


def test_orders_module_loads_with_expected_shape():
    config = load_settings(PROJECT_ROOT / "config" / "settings.yaml")
    registry = ModuleRegistry(config.paths.modules_root)
    orders = registry.get("orders")

    assert orders.primary_key == "order_id"
    assert "customer_id" in orders.critical_columns
    assert orders.source.format == "csv"
    assert {c.name for c in orders.source.columns} == {
        "order_id", "customer_id", "order_amount", "customer_email",
        "payment_method", "country_code", "order_timestamp",
    }


def test_unknown_module_raises():
    from dq_framework.core.exceptions import ModuleNotFoundError_

    config = load_settings(PROJECT_ROOT / "config" / "settings.yaml")
    registry = ModuleRegistry(config.paths.modules_root)
    try:
        registry.get("does-not-exist")
        assert False, "expected ModuleNotFoundError_"
    except ModuleNotFoundError_:
        pass

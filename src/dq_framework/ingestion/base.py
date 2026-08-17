"""DataSource abstraction: how raw bytes become a Spark DataFrame.

Swapping CSV for Parquet/Delta/JDBC later means adding one new class here
and a new `format:` value in module.yaml - the validation/failure/LLM/rules
layers never see a file path, only a DataFrame.
"""
from __future__ import annotations

import os
import sys
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession

from dq_framework.core.config import ModuleConfig

_spark_session: SparkSession | None = None


def get_spark_session(app_name: str = "dq-framework") -> SparkSession:
    """Process-wide SparkSession, created lazily on first use.

    Local-mode master for the POC; swap for a cluster master URI (or drop
    this factory in favor of a Databricks-provided session) without any
    caller needing to change - they only ever call get_spark_session().

    PYSPARK_PYTHON is pinned to sys.executable explicitly: on Windows, the
    bare `python` command frequently resolves to the Microsoft Store's
    execution-alias stub rather than a real interpreter, which breaks any
    Spark operation that spawns a Python worker (createDataFrame from local
    data, Python UDFs, `.rdd`, ...) with an opaque "Python worker failed to
    connect back" error. Pinning removes the dependency on PATH entirely.

    Deliberately does NOT touch HADOOP_HOME/winutils.exe: this pipeline never
    calls a Spark-native file *writer* (see stages.apply_active_fixes for
    why), and reads work fine on Windows without it - Spark only logs a
    harmless WARN. Writing would need a real winutils.exe executed by Hadoop's
    output-commit protocol, which we won't fetch from a third-party source.
    """
    global _spark_session
    if _spark_session is None:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        _spark_session = (
            SparkSession.builder.master("local[*]")
            .appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .getOrCreate()
        )
    return _spark_session


def stop_spark_session() -> None:
    global _spark_session
    if _spark_session is not None:
        _spark_session.stop()
        _spark_session = None


class DataSource(Protocol):
    """Something that can turn a module's configured source into a DataFrame."""

    def read_baseline(self, spark: SparkSession, module: ModuleConfig) -> DataFrame:
        """Read the module's clean/control dataset (used for calibration)."""
        ...

    def read(self, spark: SparkSession, module: ModuleConfig, source_ref: str) -> DataFrame:
        """Read one specific input file (the dirty/test dataset, an uploaded file, etc.)."""
        ...

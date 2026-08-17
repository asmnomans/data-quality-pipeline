"""CSV DataSource implementation, backed by an explicit schema when the
module declares one (module.yaml `source.columns`) - avoids `inferSchema`,
which forces Spark to do an extra full pass over the data and can silently
mis-infer types on a messy/dirty file (the exact kind of input this
pipeline exists to validate).
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dq_framework.core.config import ColumnSchemaEntry, ModuleConfig
from dq_framework.core.exceptions import IngestionError

_SPARK_TYPES = {
    "string": StringType(),
    "double": DoubleType(),
    "integer": IntegerType(),
    "long": LongType(),
    "timestamp": TimestampType(),
    "boolean": BooleanType(),
}


def _build_struct_type(columns: list[ColumnSchemaEntry]) -> StructType:
    return StructType(
        [StructField(c.name, _SPARK_TYPES[c.type], nullable=c.nullable) for c in columns]
    )


class CSVDataSource:
    """DataSource for `format: csv` modules."""

    def read_baseline(self, spark: SparkSession, module: ModuleConfig) -> DataFrame:
        path = self._resolve(module.module_dir, module.source.baseline_path)
        return self._read_csv(spark, module, path)

    def read(self, spark: SparkSession, module: ModuleConfig, source_ref: str) -> DataFrame:
        path = self._resolve(module.module_dir, source_ref)
        return self._read_csv(spark, module, path)

    def _read_csv(self, spark: SparkSession, module: ModuleConfig, path: Path) -> DataFrame:
        if not path.exists():
            raise IngestionError(f"Source file not found: {path}")

        reader = spark.read.options(**module.source.options)
        if module.source.columns:
            reader = reader.schema(_build_struct_type(module.source.columns))
        else:
            reader = reader.option("inferSchema", "true")

        try:
            return reader.csv(str(path))
        except Exception as exc:  # pragma: no cover - defensive, Spark exceptions are broad
            raise IngestionError(f"Failed to read CSV at {path}: {exc}") from exc

    @staticmethod
    def _resolve(module_dir: Path | None, raw_path: str) -> Path:
        p = Path(raw_path)
        if p.is_absolute():
            return p
        # module.yaml paths are relative to the project root (module_dir's grandparent),
        # not to the module folder itself - keeps module.yaml portable/readable.
        project_root = module_dir.parents[1] if module_dir else Path.cwd()
        return (project_root / raw_path).resolve()


def get_data_source(fmt: str) -> CSVDataSource:
    """Format registry. Add 'parquet'/'delta' here + a new class when needed."""
    registry = {"csv": CSVDataSource}
    if fmt not in registry:
        raise IngestionError(f"Unsupported source format: '{fmt}'. Supported: {list(registry)}")
    return registry[fmt]()

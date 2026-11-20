"""Versioned publication of Gold Delta tables."""

import json
import os
from pathlib import Path
from uuid import uuid4
import shutil

from pyspark.sql import DataFrame, SparkSession


GOLD_TABLE_NAMES = (
    "dim_equipment",
    "dim_sensor",
    "fact_sensor_readings",
    "agg_sensor_minute",
    "agg_sensor_hourly",
)


def get_active_gold_version(
    gold_path: str = "data/gold",
) -> str | None:
    """Return the currently published Gold version."""

    manifest_path = Path(gold_path) / "current.json"

    if not manifest_path.exists():
        return None

    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    return manifest["run_id"]


def validate_persisted_gold_table(
    expected_df: DataFrame,
    persisted_df: DataFrame,
    table_name: str,
) -> None:
    """Validate a persisted Gold table before publication."""

    expected_count = expected_df.count()
    persisted_count = persisted_df.count()

    if expected_count != persisted_count:
        raise ValueError(
            f"Gold publication validation failed for "
            f"{table_name}: expected {expected_count} rows, "
            f"found {persisted_count}"
        )

    expected_schema = [
        (field.name, field.dataType)
        for field in expected_df.schema.fields
    ]

    persisted_schema = [
        (field.name, field.dataType)
        for field in persisted_df.schema.fields
    ]

    if expected_schema != persisted_schema:
        raise ValueError(
            f"Gold publication schema mismatch: {table_name}"
        )


def publish_gold_tables(
    spark: SparkSession,
    gold_tables: dict[str, DataFrame],
    gold_path: str = "data/gold",
) -> str:
    """Write and publish a complete version of Gold."""

    missing = set(GOLD_TABLE_NAMES) - set(gold_tables)

    if missing:
        raise ValueError(
            f"Missing Gold tables: {sorted(missing)}"
        )

    base_path = Path(gold_path)
    versions_path = base_path / "versions"

    run_id = uuid4().hex
    version_path = versions_path / run_id

    version_path.mkdir(parents=True, exist_ok=False)

    try:
        # Write all five tables into an isolated version.
        for table_name in GOLD_TABLE_NAMES:
            target = version_path / table_name

            (
                gold_tables[table_name]
                .write
                .format("delta")
                .mode("errorifexists")
                .save(str(target))
            )

        # Validate all persisted tables.
        for table_name in GOLD_TABLE_NAMES:
            target = version_path / table_name

            persisted_df = spark.read.format("delta").load(
                str(target)
            )

            validate_persisted_gold_table(
                gold_tables[table_name],
                persisted_df,
                table_name,
            )

        # Publish only after successful validation.
        manifest_path = base_path / "current.json"

        temporary_manifest = base_path / (
            f".current.{run_id}.tmp"
        )

        manifest = {
            "run_id": run_id,
            "tables": list(GOLD_TABLE_NAMES),
        }

        try:
            temporary_manifest.write_text(
                json.dumps(manifest, indent=2),
                encoding="utf-8",
            )

            os.replace(
                temporary_manifest,
                manifest_path,
            )

        finally:
            temporary_manifest.unlink(missing_ok=True)

    except Exception:
        # Remove the incomplete version.
        shutil.rmtree(version_path, ignore_errors=True)
        raise

    return run_id


def read_active_gold_table(
    spark: SparkSession,
    table_name: str,
    gold_path: str = "data/gold",
) -> DataFrame:
    """Read a table from the currently published Gold version."""

    if table_name not in GOLD_TABLE_NAMES:
        raise ValueError(
            f"Unknown Gold table: {table_name}"
        )

    run_id = get_active_gold_version(gold_path)

    if run_id is None:
        raise ValueError(
            "No published Gold version exists"
        )

    table_path = (
        Path(gold_path)
        / "versions"
        / run_id
        / table_name
    )

    return spark.read.format("delta").load(
        str(table_path)
    )
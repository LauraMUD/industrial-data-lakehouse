"""Bronze ingestion pipeline for industrial sensor readings."""

import argparse
import json
import re
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.utils.spark_session import create_spark_session

BASE_DIR = Path(__file__).resolve().parents[2]

DEFAULT_SOURCE = BASE_DIR / "data" / "raw" / "sensor_readings" / "sensors_batch_001.csv"

DEFAULT_TARGET = BASE_DIR / "data" / "bronze" / "sensor_readings"

CONTRACT_PATH = BASE_DIR / "config" / "contracts" / "sensor_readings.json"


def validate_batch_id(batch_id: str) -> str:
    """Validate batch identifiers used in Delta predicates."""

    if not re.fullmatch(r"[A-Za-z0-9_-]+", batch_id):
        raise ValueError(
            f"Invalid batch_id: {batch_id!r}. "
            "Only letters, numbers, underscores and hyphens are allowed."
        )

    return batch_id


def load_contract(path: Path) -> dict:
    """Load the dataset contract."""
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def read_source(
    spark: SparkSession,
    source_path: Path,
    contract: dict,
) -> DataFrame:
    """Read CSV while preserving source values as strings."""

    if not source_path.is_file():
        raise FileNotFoundError(source_path)

    expected_columns = contract["columns"]

    with source_path.open(encoding="utf-8-sig") as file:
        header = file.readline().rstrip("\r\n").split(contract["delimiter"])

    if header != expected_columns:
        raise ValueError(
            f"CSV header mismatch. Expected: {expected_columns}. Received: {header}"
        )

    schema = StructType(
        [StructField(column, StringType(), True) for column in expected_columns]
    )

    return (
        spark.read.option("header", contract["header"])
        .option("sep", contract["delimiter"])
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(str(source_path))
    )


def enrich_bronze(
    df: DataFrame,
    source_path: Path,
    batch_id: str,
) -> DataFrame:
    """Add technical ingestion metadata."""

    return (
        df.withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.lit(source_path.name))
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp(),
        )
    )


def write_bronze(
    spark: SparkSession,
    df: DataFrame,
    target_path: Path,
    batch_id: str,
) -> None:
    """Write a batch to Delta without duplicating its records."""

    target = str(target_path)

    if DeltaTable.isDeltaTable(spark, target):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option(
                "replaceWhere",
                f"batch_id = '{batch_id}'",
            )
            .save(target)
        )
    else:
        (df.write.format("delta").mode("overwrite").save(target))


def ingest(
    spark: SparkSession,
    source_path: Path,
    target_path: Path,
    contract_path: Path,
) -> int:
    """Execute a complete Bronze ingestion."""

    contract = load_contract(contract_path)
    batch_id = validate_batch_id(source_path.stem)

    source_df = read_source(spark, source_path, contract)

    bronze_df = enrich_bronze(
        source_df,
        source_path,
        batch_id,
    )

    bronze_df = bronze_df.cache()

    try:
        record_count = bronze_df.count()

        if record_count == 0:
            raise ValueError("Source batch is empty")

        write_bronze(
            spark,
            bronze_df,
            target_path,
            batch_id,
        )

        return record_count
    finally:
        bronze_df.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser(description="Industrial sensor Bronze ingestion")

    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE,
    )

    parser.add_argument(
        "--target",
        type=Path,
        default=DEFAULT_TARGET,
    )

    args = parser.parse_args()

    spark = create_spark_session("BronzeSensorIngestion")

    try:
        count = ingest(
            spark,
            args.source,
            args.target,
            CONTRACT_PATH,
        )

        print(f"SUCCESS: Ingested {count} sensor readings")
        print(f"Target: {args.target}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""Data quality and Silver transformations for industrial sensors."""

import json
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.ingestion.bronze_sensors import validate_batch_id

BASE_DIR = Path(__file__).resolve().parents[2]

SILVER_CONTRACT_PATH = BASE_DIR / "config" / "contracts" / "sensor_readings_silver.json"


def load_silver_contract(path: Path = SILVER_CONTRACT_PATH) -> dict:
    """Load Silver data quality rules."""
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def validate_required_fields(
    df: DataFrame,
    required_columns: list[str],
) -> DataFrame:
    """Flag records with null or blank required fields."""

    missing_columns = [
        column for column in required_columns if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    invalid_conditions = [
        F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")
        for column in required_columns
    ]

    invalid_record = invalid_conditions[0]

    for condition in invalid_conditions[1:]:
        invalid_record = invalid_record | condition

    return df.withColumn(
        "dq_errors",
        F.when(
            invalid_record,
            F.array(F.lit("DQ001_NULL_REQUIRED")),
        ).otherwise(F.array().cast("array<string>")),
    )


def validate_numeric_measurement(df: DataFrame) -> DataFrame:
    """Validate numeric sensor readings and accumulate DQ errors."""

    required = {"measurement_value", "dq_errors"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns for numeric validation: {sorted(missing)}")

    numeric_value = F.expr("try_cast(measurement_value AS DOUBLE)")

    result = df.withColumn(
        "measurement_value_numeric",
        numeric_value,
    )

    invalid_numeric = (
        F.col("measurement_value_numeric").isNull()
        | F.isnan(F.col("measurement_value_numeric"))
        | (F.abs(F.col("measurement_value_numeric")) == F.lit(float("inf")))
    )

    return result.withColumn(
        "dq_errors",
        F.when(
            invalid_numeric,
            F.concat(
                F.col("dq_errors"),
                F.array(F.lit("DQ003_INVALID_NUMERIC")),
            ),
        ).otherwise(F.col("dq_errors")),
    )


def validate_event_timestamp(df: DataFrame) -> DataFrame:
    """Validate ISO 8601 UTC timestamps and accumulate DQ errors."""

    required = {"event_timestamp", "dq_errors"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns for timestamp validation: {sorted(missing)}")

    # Require an explicit UTC designator.
    valid_format = F.coalesce(
        F.col("event_timestamp").rlike(
            r"^\d{4}-\d{2}-\d{2}T"
            r"\d{2}:\d{2}:\d{2}"
            r"(?:\.\d+)?Z$"
        ),
        F.lit(False),
    )

    parsed_timestamp = F.expr("try_to_timestamp(event_timestamp)")

    result = df.withColumn(
        "event_timestamp_utc",
        F.when(valid_format, parsed_timestamp),
    )

    invalid_timestamp = F.col("event_timestamp_utc").isNull()

    return result.withColumn(
        "dq_errors",
        F.when(
            invalid_timestamp,
            F.concat(
                F.col("dq_errors"),
                F.array(F.lit("DQ002_INVALID_TIMESTAMP")),
            ),
        ).otherwise(F.col("dq_errors")),
    )


def validate_measurement_type(
    df: DataFrame,
    contract: dict,
) -> DataFrame:
    """Validate measurement types against the Silver contract."""

    required = {"measurement_type", "dq_errors"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    allowed_types = list(contract["measurement_rules"])

    invalid_type = F.col("measurement_type").isNotNull() & ~F.col(
        "measurement_type"
    ).isin(allowed_types)

    return df.withColumn(
        "dq_errors",
        F.when(
            invalid_type,
            F.concat(
                F.col("dq_errors"),
                F.array(F.lit("DQ004_UNKNOWN_MEASUREMENT_TYPE")),
            ),
        ).otherwise(F.col("dq_errors")),
    )


def validate_measurement_unit(
    df: DataFrame,
    contract: dict,
) -> DataFrame:
    """Validate units for recognized measurement types."""

    required = {"measurement_type", "unit", "dq_errors"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    rules = contract["measurement_rules"]

    invalid_unit = F.lit(False)

    for measurement_type, rule in rules.items():
        condition = (
            (F.col("measurement_type") == measurement_type)
            & F.col("unit").isNotNull()
            & (F.col("unit") != rule["unit"])
        )

        invalid_unit = invalid_unit | condition

    return df.withColumn(
        "dq_errors",
        F.when(
            invalid_unit,
            F.concat(
                F.col("dq_errors"),
                F.array(F.lit("DQ005_INVALID_UNIT")),
            ),
        ).otherwise(F.col("dq_errors")),
    )


def validate_duplicate_events(df: DataFrame) -> DataFrame:
    """Flag duplicate event IDs using deterministic ordering."""

    required = {"event_id", "dq_errors"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    # Rank records with fewer existing quality errors first.
    quality_score = F.size(F.col("dq_errors"))

    # Stable ordering based on the complete record content.
    content_columns = sorted(column for column in df.columns if column != "dq_errors")

    content_key = F.to_json(
        F.struct(*[F.col(column) for column in content_columns]),
        options={"ignoreNullFields": "false"},
    )

    window = Window.partitionBy("event_id").orderBy(
        quality_score.asc(),
        content_key.asc(),
    )

    result = df.withColumn(
        "_duplicate_rank",
        F.row_number().over(window),
    )

    return result.withColumn(
        "dq_errors",
        F.when(
            F.col("event_id").isNotNull() & (F.col("_duplicate_rank") > 1),
            F.concat(
                F.col("dq_errors"),
                F.array(F.lit("DQ006_DUPLICATE_EVENT_ID")),
            ),
        ).otherwise(F.col("dq_errors")),
    ).drop("_duplicate_rank")


def validate_measurement_range(
    df: DataFrame,
    contract: dict,
) -> DataFrame:
    """Validate measurement values against configured ranges."""

    required = {
        "measurement_type",
        "measurement_value_numeric",
        "dq_errors",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    rules = contract["measurement_rules"]

    invalid_range = F.lit(False)

    for measurement_type, rule in rules.items():
        value = F.col("measurement_value_numeric")

        condition = (
            (F.col("measurement_type") == measurement_type)
            & value.isNotNull()
            & ~F.isnan(value)
            & ((value < F.lit(rule["min"])) | (value > F.lit(rule["max"])))
        )

        invalid_range = invalid_range | condition

    return df.withColumn(
        "dq_errors",
        F.when(
            invalid_range,
            F.concat(
                F.col("dq_errors"),
                F.array(F.lit("DQ007_OUT_OF_RANGE")),
            ),
        ).otherwise(F.col("dq_errors")),
    )


def apply_quality_rules(
    df: DataFrame,
    contract: dict,
) -> DataFrame:
    """Apply all Silver data quality rules."""

    result = validate_required_fields(
        df,
        contract["required_columns"],
    )

    result = validate_event_timestamp(result)

    result = validate_numeric_measurement(result)

    result = validate_measurement_type(
        result,
        contract,
    )

    result = validate_measurement_unit(
        result,
        contract,
    )

    result = validate_measurement_range(
        result,
        contract,
    )

    result = validate_duplicate_events(result)

    return result


def split_quality_results(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Separate valid and rejected sensor readings."""

    if "dq_errors" not in df.columns:
        raise ValueError("Missing required column: dq_errors")

    valid_df = df.filter(F.size(F.col("dq_errors")) == 0)

    rejected_df = df.filter(F.size(F.col("dq_errors")) > 0)

    return valid_df, rejected_df


def prepare_silver(df: DataFrame) -> DataFrame:
    """Prepare validated sensor readings for the Silver layer."""

    required = {
        "event_id",
        "plant_id",
        "equipment_id",
        "sensor_id",
        "event_timestamp_utc",
        "measurement_type",
        "measurement_value_numeric",
        "unit",
        "batch_id",
        "source_file",
        "ingestion_timestamp",
        "dq_errors",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns for Silver: {sorted(missing)}")

    return df.filter(F.size(F.col("dq_errors")) == 0).select(
        "event_id",
        "plant_id",
        "equipment_id",
        "sensor_id",
        F.col("event_timestamp_utc").cast("timestamp").alias("event_timestamp"),
        "measurement_type",
        F.col("measurement_value_numeric").cast("double").alias("measurement_value"),
        "unit",
        "batch_id",
        "source_file",
        F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
    )


def prepare_quarantine(df: DataFrame) -> DataFrame:
    """Prepare rejected sensor readings for quarantine."""

    required = {
        "event_id",
        "plant_id",
        "equipment_id",
        "sensor_id",
        "event_timestamp",
        "measurement_type",
        "measurement_value",
        "unit",
        "batch_id",
        "source_file",
        "ingestion_timestamp",
        "dq_errors",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns for Quarantine: {sorted(missing)}")

    return df.filter(F.size(F.col("dq_errors")) > 0).select(
        "event_id",
        "plant_id",
        "equipment_id",
        "sensor_id",
        "event_timestamp",
        "measurement_type",
        "measurement_value",
        "unit",
        "batch_id",
        "source_file",
        "ingestion_timestamp",
        "dq_errors",
        F.current_timestamp().alias("quarantine_timestamp"),
    )


def write_delta_batch(
    spark: SparkSession,
    df: DataFrame,
    target_path: Path,
    batch_id: str,
) -> None:
    """Write or replace one batch in a Delta table."""

    validate_batch_id(batch_id)

    if "batch_id" not in df.columns:
        raise ValueError("Missing required column: batch_id")

    target = str(target_path)

    invalid_batch_df = df.filter(
        F.col("batch_id").isNull() | (F.col("batch_id") != batch_id)
    )

    if invalid_batch_df.limit(1).count() > 0:
        raise ValueError(f"DataFrame contains records outside batch_id={batch_id}")

    batch_df = df

    if DeltaTable.isDeltaTable(spark, target):
        (
            batch_df.write.format("delta")
            .mode("overwrite")
            .option(
                "replaceWhere",
                f"batch_id = '{batch_id}'",
            )
            .save(target)
        )
    else:
        (batch_df.write.format("delta").mode("overwrite").save(target))


def process_bronze_batch(
    spark: SparkSession,
    bronze_path: Path,
    batch_id: str,
) -> tuple[DataFrame, DataFrame, dict]:
    """Validate one Bronze batch and prepare Silver/Quarantine."""

    validate_batch_id(batch_id)

    bronze_df = (
        spark.read.format("delta")
        .load(str(bronze_path))
        .filter(F.col("batch_id") == batch_id)
    )

    bronze_count = bronze_df.count()

    if bronze_count == 0:
        raise ValueError(f"No Bronze records found for batch_id={batch_id}")

    contract = load_silver_contract()

    validated_df = apply_quality_rules(
        bronze_df,
        contract,
    )

    silver_df = prepare_silver(validated_df)
    quarantine_df = prepare_quarantine(validated_df)

    silver_count = silver_df.count()
    quarantine_count = quarantine_df.count()

    if bronze_count != silver_count + quarantine_count:
        raise RuntimeError(
            "Data reconciliation failed: "
            f"bronze={bronze_count}, "
            f"silver={silver_count}, "
            f"quarantine={quarantine_count}"
        )

    metrics = {
        "batch_id": batch_id,
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "quarantine_count": quarantine_count,
    }

    return silver_df, quarantine_df, metrics


def run_silver_pipeline(
    spark: SparkSession,
    bronze_path: Path,
    silver_path: Path,
    quarantine_path: Path,
    batch_id: str,
) -> dict:
    """Process and persist one Bronze batch into Silver and Quarantine."""

    validate_batch_id(batch_id)

    if silver_path == quarantine_path:
        raise ValueError("Silver and Quarantine must have different paths")

    silver_df, quarantine_df, metrics = process_bronze_batch(
        spark,
        bronze_path,
        batch_id,
    )

    # Each Delta write is atomic individually.
    # The two writes are not a single transaction.
    write_delta_batch(
        spark,
        silver_df,
        silver_path,
        batch_id,
    )

    write_delta_batch(
        spark,
        quarantine_df,
        quarantine_path,
        batch_id,
    )

    silver_written = (
        spark.read.format("delta")
        .load(str(silver_path))
        .filter(F.col("batch_id") == batch_id)
        .count()
    )

    quarantine_written = (
        spark.read.format("delta")
        .load(str(quarantine_path))
        .filter(F.col("batch_id") == batch_id)
        .count()
    )

    if (
        silver_written != metrics["silver_count"]
        or quarantine_written != metrics["quarantine_count"]
    ):
        raise RuntimeError(
            "Post-write reconciliation failed: "
            f"silver={silver_written}, "
            f"quarantine={quarantine_written}"
        )

    return {
        **metrics,
        "silver_written": silver_written,
        "quarantine_written": quarantine_written,
    }

"""Shared Gold temporal aggregation logic."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


REQUIRED_COLUMNS = [
    "plant_id",
    "equipment_id",
    "sensor_id",
    "measurement_type",
    "unit",
    "event_timestamp",
    "measurement_value",
]

DIMENSION_COLUMNS = [
    "plant_id",
    "equipment_id",
    "sensor_id",
    "measurement_type",
    "unit",
]


def build_temporal_aggregation(
    fact_df: DataFrame,
    granularity: str,
) -> DataFrame:
    """Aggregate industrial sensor readings by minute or hour."""

    if granularity not in ("minute", "hour"):
        raise ValueError(
            f"Unsupported temporal granularity: {granularity}"
        )

    missing_columns = set(REQUIRED_COLUMNS) - set(
        fact_df.columns
    )

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    expected_types = {
        "event_timestamp": "timestamp",
        "measurement_value": "double",
    }

    for column, expected_type in expected_types.items():
        actual_type = (
            fact_df.schema[column].dataType.simpleString()
        )

        if actual_type != expected_type:
            raise ValueError(
                f"Invalid type for {column}: "
                f"expected {expected_type}, got {actual_type}"
            )

    invalid_condition = F.lit(False)

    for column in REQUIRED_COLUMNS:
        condition = F.col(column).isNull()

        if column in DIMENSION_COLUMNS:
            condition = condition | (
                F.trim(F.col(column)) == ""
            )

        invalid_condition = invalid_condition | condition

    invalid_numeric = (
        F.isnan(F.col("measurement_value"))
        | (
            F.abs(F.col("measurement_value"))
            == F.lit(float("inf"))
        )
    )

    invalid_condition = invalid_condition | invalid_numeric

    if fact_df.filter(invalid_condition).limit(1).count() > 0:
        raise ValueError(
            "Fact contains invalid aggregation attributes"
        )

    timestamp_column = f"{granularity}_start_utc"

    temporal_df = fact_df.withColumn(
        timestamp_column,
        F.date_trunc(
            granularity,
            F.col("event_timestamp"),
        ),
    )

    return (
        temporal_df
        .groupBy(
            *DIMENSION_COLUMNS,
            timestamp_column,
        )
        .agg(
            F.count("*").alias("reading_count"),
            F.avg("measurement_value").alias("avg_value"),
            F.min("measurement_value").alias("min_value"),
            F.max("measurement_value").alias("max_value"),
            F.stddev_pop("measurement_value").alias(
                "stddev_value"
            ),
        )
    )
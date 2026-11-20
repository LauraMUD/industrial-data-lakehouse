"""Gold industrial sensor readings fact transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


FACT_COLUMNS = [
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
]


def build_fact_sensor_readings(
    silver_df: DataFrame,
) -> DataFrame:
    """Build one fact record per validated Silver event."""

    missing_columns = set(FACT_COLUMNS) - set(
        silver_df.columns
    )

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    # Preserve the physical types established in Silver.
    expected_types = {
        "event_timestamp": "timestamp",
        "measurement_value": "double",
        "ingestion_timestamp": "timestamp",
    }

    for column, expected_type in expected_types.items():
        actual_type = silver_df.schema[column].dataType.simpleString()

        if actual_type != expected_type:
            raise ValueError(
                f"Invalid type for {column}: "
                f"expected {expected_type}, got {actual_type}"
            )

    result = silver_df.select(*FACT_COLUMNS)

    # Required analytical and lineage attributes.
    required_columns = FACT_COLUMNS

    invalid_condition = F.lit(False)

    for column in required_columns:
        invalid_condition = (
            invalid_condition | F.col(column).isNull()
        )

    if result.filter(invalid_condition).limit(1).count() > 0:
        raise ValueError(
            "Silver contains null fact attributes"
        )

    # Reject invalid numerical values.
    invalid_measurement = (
        F.isnan(F.col("measurement_value"))
        | (F.abs(F.col("measurement_value")) == F.lit(float("inf")))
    )

    if result.filter(invalid_measurement).limit(1).count() > 0:
        raise ValueError(
            "Silver contains invalid numerical measurements"
        )

    # An event must represent exactly one reading.
    duplicates = (
        result.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count() > 0:
        raise ValueError(
            "Duplicate event_id detected in Silver"
        )

    return result
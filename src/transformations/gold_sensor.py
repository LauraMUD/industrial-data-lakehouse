"""Gold industrial sensor dimension transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SENSOR_COLUMNS = [
    "plant_id",
    "equipment_id",
    "sensor_id",
    "measurement_type",
    "unit",
]

SENSOR_KEY = [
    "plant_id",
    "equipment_id",
    "sensor_id",
]


def build_dim_sensor(silver_df: DataFrame) -> DataFrame:
    """Build a consistent sensor dimension from Silver readings."""

    missing_columns = set(SENSOR_COLUMNS) - set(silver_df.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    invalid_conditions = [
        F.col(column).isNull() | (F.trim(F.col(column)) == "")
        for column in SENSOR_COLUMNS
    ]

    invalid = invalid_conditions[0]

    for condition in invalid_conditions[1:]:
        invalid = invalid | condition

    if silver_df.filter(invalid).limit(1).count() > 0:
        raise ValueError("Silver contains invalid sensor attributes")

    sensor_df = silver_df.select(*SENSOR_COLUMNS).distinct()

    inconsistent_sensors = (
        sensor_df.groupBy(*SENSOR_KEY).count().filter(F.col("count") > 1)
    )

    if inconsistent_sensors.limit(1).count() > 0:
        raise ValueError("Inconsistent measurement type or unit for sensor")

    return sensor_df

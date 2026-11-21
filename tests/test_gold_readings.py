"""Tests for the Gold industrial sensor readings fact."""

from datetime import datetime

import pytest

from src.transformations.gold_readings import (
    FACT_COLUMNS,
    build_fact_sensor_readings,
)
from src.utils.spark_session import create_spark_session

FACT_SCHEMA = (
    "event_id string, "
    "plant_id string, "
    "equipment_id string, "
    "sensor_id string, "
    "event_timestamp timestamp, "
    "measurement_type string, "
    "measurement_value double, "
    "unit string, "
    "batch_id string, "
    "source_file string, "
    "ingestion_timestamp timestamp"
)


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session("TestGoldReadings")
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def make_record(
    event_id="EVT001",
    measurement_value=68.5,
):
    return (
        event_id,
        "PLANT_ES_01",
        "CRUSHER_01",
        "TEMP_01",
        datetime(2026, 11, 19, 7, 0),
        "temperature",
        measurement_value,
        "C",
        "sensors_batch_001",
        "sensors_batch_001.csv",
        datetime(2026, 11, 19, 8, 0),
    )


def test_fact_preserves_all_events(spark):
    """Every unique Silver event must produce one Gold fact."""

    df = spark.createDataFrame(
        [
            make_record("EVT001"),
            make_record("EVT002"),
            make_record("EVT003"),
        ],
        FACT_SCHEMA,
    )

    result = build_fact_sensor_readings(df)

    assert result.count() == 3
    assert result.select("event_id").distinct().count() == 3


def test_fact_rejects_duplicate_events(spark):
    """Duplicate event identifiers must be rejected."""

    df = spark.createDataFrame(
        [
            make_record("EVT001"),
            make_record("EVT001"),
        ],
        FACT_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="Duplicate event_id",
    ):
        build_fact_sensor_readings(df)


def test_fact_rejects_null_attributes(spark):
    """Required fact attributes cannot be null."""

    record = list(make_record())
    record[2] = None

    df = spark.createDataFrame(
        [tuple(record)],
        FACT_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="null fact attributes",
    ):
        build_fact_sensor_readings(df)


@pytest.mark.parametrize(
    "invalid_value",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
    ],
)
def test_fact_rejects_invalid_numeric_values(
    spark,
    invalid_value,
):
    """NaN and infinite measurements must be rejected."""

    df = spark.createDataFrame(
        [make_record(measurement_value=invalid_value)],
        FACT_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="invalid numerical measurements",
    ):
        build_fact_sensor_readings(df)


def test_fact_rejects_invalid_measurement_type(spark):
    """The measurement value must be stored as double."""

    invalid_schema = FACT_SCHEMA.replace(
        "measurement_value double",
        "measurement_value string",
    )

    record = list(make_record())
    record[6] = "68.5"

    df = spark.createDataFrame(
        [tuple(record)],
        invalid_schema,
    )

    with pytest.raises(
        ValueError,
        match="Invalid type for measurement_value",
    ):
        build_fact_sensor_readings(df)


def test_fact_rejects_missing_columns(spark):
    """Missing required columns must raise an error."""

    df = spark.createDataFrame(
        [("EVT001",)],
        ["event_id"],
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):
        build_fact_sensor_readings(df)


def test_fact_preserves_lineage(spark):
    """Gold must preserve source batch and file information."""

    df = spark.createDataFrame(
        [make_record()],
        FACT_SCHEMA,
    )

    result = build_fact_sensor_readings(df)
    row = result.first()

    assert row["batch_id"] == "sensors_batch_001"
    assert row["source_file"] == "sensors_batch_001.csv"
    assert row["ingestion_timestamp"] == datetime(2026, 11, 19, 8, 0)


def test_fact_expected_schema(spark):
    """Gold must expose the expected fact columns."""

    df = spark.createDataFrame(
        [make_record()],
        FACT_SCHEMA,
    )

    result = build_fact_sensor_readings(df)

    assert result.columns == FACT_COLUMNS

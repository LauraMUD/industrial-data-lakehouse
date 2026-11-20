"""Tests for Gold hourly sensor aggregations."""

from datetime import datetime

import pytest

from src.transformations.gold_hourly import build_agg_sensor_hourly
from src.utils.spark_session import create_spark_session


SCHEMA = (
    "plant_id string, "
    "equipment_id string, "
    "sensor_id string, "
    "measurement_type string, "
    "unit string, "
    "event_timestamp timestamp, "
    "measurement_value double"
)


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session("TestGoldHourly")
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def make_reading(
    hour=7,
    minute=0,
    value=60.0,
    sensor_id="TEMP_01",
    unit="C",
):
    return (
        "PLANT_ES_01",
        "CRUSHER_01",
        sensor_id,
        "temperature",
        unit,
        datetime(2026, 11, 19, hour, minute),
        value,
    )


def test_hourly_aggregates_different_minutes(spark):
    """Readings in different minutes of the same hour aggregate."""

    df = spark.createDataFrame(
        [
            make_reading(minute=0, value=60.0),
            make_reading(minute=20, value=70.0),
            make_reading(minute=40, value=80.0),
        ],
        SCHEMA,
    )

    result = build_agg_sensor_hourly(df)
    row = result.first()

    assert result.count() == 1
    assert row["reading_count"] == 3
    assert row["avg_value"] == pytest.approx(70.0)
    assert row["min_value"] == 60.0
    assert row["max_value"] == 80.0
    assert row["stddev_value"] == pytest.approx(
        (200.0 / 3.0) ** 0.5
    )


def test_hourly_separates_hours(spark):
    """Different hours must produce separate groups."""

    df = spark.createDataFrame(
        [
            make_reading(hour=7, minute=59),
            make_reading(hour=8, minute=0),
        ],
        SCHEMA,
    )

    result = build_agg_sensor_hourly(df)

    assert result.count() == 2


def test_hourly_separates_sensors(spark):
    """Different sensors must remain separate."""

    df = spark.createDataFrame(
        [
            make_reading(sensor_id="TEMP_01"),
            make_reading(sensor_id="TEMP_02"),
        ],
        SCHEMA,
    )

    result = build_agg_sensor_hourly(df)

    assert result.count() == 2


def test_hourly_separates_units(spark):
    """Measurements with different units cannot be combined."""

    df = spark.createDataFrame(
        [
            make_reading(unit="C"),
            make_reading(unit="F"),
        ],
        SCHEMA,
    )

    result = build_agg_sensor_hourly(df)

    assert result.count() == 2


def test_hourly_single_reading(spark):
    """A single reading has zero population deviation."""

    df = spark.createDataFrame(
        [make_reading(value=68.5)],
        SCHEMA,
    )

    row = build_agg_sensor_hourly(df).first()

    assert row["reading_count"] == 1
    assert row["avg_value"] == 68.5
    assert row["stddev_value"] == 0.0


def test_hourly_rejects_null_values(spark):
    """Null measurement values must be rejected."""

    df = spark.createDataFrame(
        [make_reading(value=None)],
        SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="invalid aggregation attributes",
    ):
        build_agg_sensor_hourly(df)


@pytest.mark.parametrize(
    "invalid_value",
    [float("nan"), float("inf"), float("-inf")],
)
def test_hourly_rejects_invalid_numeric_values(
    spark,
    invalid_value,
):
    """Non-finite measurements must be rejected."""

    df = spark.createDataFrame(
        [make_reading(value=invalid_value)],
        SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="invalid aggregation attributes",
    ):
        build_agg_sensor_hourly(df)


def test_hourly_rejects_missing_columns(spark):
    """Required columns must exist."""

    df = spark.createDataFrame(
        [("PLANT_ES_01",)],
        ["plant_id"],
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):
        build_agg_sensor_hourly(df)


def test_hourly_expected_timestamp(spark):
    """Timestamp must be truncated to the hour."""

    df = spark.createDataFrame(
        [make_reading(hour=7, minute=42)],
        SCHEMA,
    )

    row = build_agg_sensor_hourly(df).first()

    assert row["hour_start_utc"] == datetime(
        2026, 11, 19, 7, 0, 0
    )
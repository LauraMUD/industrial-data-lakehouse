"""Tests for Gold minute-level sensor aggregations."""

from datetime import datetime

import pytest

from src.transformations.gold_minute import (
    build_agg_sensor_minute,
)
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
    session = create_spark_session("TestGoldMinute")
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def make_reading(
    second,
    value,
    sensor_id="TEMP_01",
    unit="C",
):
    return (
        "PLANT_ES_01",
        "CRUSHER_01",
        sensor_id,
        "temperature",
        unit,
        datetime(2026, 11, 19, 7, 0, second),
        value,
    )


def test_minute_aggregates_multiple_readings(spark):
    """Readings in the same minute must be aggregated."""

    df = spark.createDataFrame(
        [
            make_reading(0, 60.0),
            make_reading(20, 70.0),
            make_reading(40, 80.0),
        ],
        SCHEMA,
    )

    result = build_agg_sensor_minute(df)
    row = result.first()

    assert result.count() == 1
    assert row["reading_count"] == 3
    assert row["avg_value"] == pytest.approx(70.0)
    assert row["min_value"] == 60.0
    assert row["max_value"] == 80.0
    assert row["stddev_value"] == pytest.approx((200.0 / 3.0) ** 0.5)


def test_minute_separates_different_minutes(spark):
    """Different minutes must produce separate groups."""

    df = spark.createDataFrame(
        [
            make_reading(0, 60.0),
            (
                "PLANT_ES_01",
                "CRUSHER_01",
                "TEMP_01",
                "temperature",
                "C",
                datetime(2026, 11, 19, 7, 1, 0),
                70.0,
            ),
        ],
        SCHEMA,
    )

    result = build_agg_sensor_minute(df)

    assert result.count() == 2


def test_minute_separates_sensors(spark):
    """Different sensors must not be aggregated together."""

    df = spark.createDataFrame(
        [
            make_reading(0, 60.0, "TEMP_01"),
            make_reading(0, 80.0, "TEMP_02"),
        ],
        SCHEMA,
    )

    result = build_agg_sensor_minute(df)

    assert result.count() == 2


def test_minute_separates_units(spark):
    """Different measurement units must remain separate."""

    df = spark.createDataFrame(
        [
            make_reading(0, 60.0, unit="C"),
            make_reading(20, 140.0, unit="F"),
        ],
        SCHEMA,
    )

    result = build_agg_sensor_minute(df)

    assert result.count() == 2


def test_minute_rejects_null_values(spark):
    """Null measurements must be rejected."""

    df = spark.createDataFrame(
        [make_reading(0, None)],
        SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="invalid aggregation attributes",
    ):
        build_agg_sensor_minute(df)


@pytest.mark.parametrize(
    "invalid_value",
    [float("nan"), float("inf"), float("-inf")],
)
def test_minute_rejects_invalid_numeric_values(
    spark,
    invalid_value,
):
    """Non-finite measurements must be rejected."""

    df = spark.createDataFrame(
        [make_reading(0, invalid_value)],
        SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="invalid aggregation attributes",
    ):
        build_agg_sensor_minute(df)


def test_minute_rejects_missing_columns(spark):
    """Required columns must exist."""

    df = spark.createDataFrame(
        [("PLANT_ES_01",)],
        ["plant_id"],
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):
        build_agg_sensor_minute(df)


def test_minute_expected_timestamp(spark):
    """Timestamp must be truncated to the minute."""

    df = spark.createDataFrame(
        [make_reading(42, 68.5)],
        SCHEMA,
    )

    result = build_agg_sensor_minute(df)
    row = result.first()

    assert row["minute_start_utc"] == datetime(2026, 11, 19, 7, 0, 0)
    assert row["stddev_value"] == 0.0

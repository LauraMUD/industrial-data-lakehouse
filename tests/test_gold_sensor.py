"""Tests for the Gold industrial sensor dimension."""

import pytest

from src.transformations.gold_sensor import build_dim_sensor
from src.utils.spark_session import create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session("TestGoldSensor")
    yield session
    session.stop()


SENSOR_SCHEMA = (
    "plant_id string, "
    "equipment_id string, "
    "sensor_id string, "
    "measurement_type string, "
    "unit string"
)


def test_dim_sensor_distinct_records(spark):
    """Repeated readings must not duplicate sensors."""

    data = [
        ("PLANT_ES_01", "CRUSHER_01", "TEMP_01", "temperature", "C"),
        ("PLANT_ES_01", "CRUSHER_01", "TEMP_01", "temperature", "C"),
        ("PLANT_ES_01", "CRUSHER_01", "VIB_01", "vibration", "mm/s"),
    ]

    df = spark.createDataFrame(data, SENSOR_SCHEMA)

    result = build_dim_sensor(df)

    assert result.count() == 2


def test_dim_sensor_composite_key(spark):
    """The same sensor ID may exist on different equipment."""

    data = [
        ("PLANT_ES_01", "CRUSHER_01", "TEMP_01", "temperature", "C"),
        ("PLANT_ES_01", "CONVEYOR_01", "TEMP_01", "temperature", "C"),
        ("PLANT_CL_01", "CRUSHER_01", "TEMP_01", "temperature", "C"),
    ]

    df = spark.createDataFrame(data, SENSOR_SCHEMA)

    result = build_dim_sensor(df)

    assert result.count() == 3


def test_dim_sensor_rejects_null_identifiers(spark):
    """Null sensor identifiers must be rejected."""

    data = [
        ("PLANT_ES_01", "CRUSHER_01", None, "temperature", "C"),
    ]

    df = spark.createDataFrame(data, SENSOR_SCHEMA)

    with pytest.raises(
        ValueError,
        match="invalid sensor attributes",
    ):
        build_dim_sensor(df)


def test_dim_sensor_rejects_blank_attributes(spark):
    """Blank sensor attributes must be rejected."""

    data = [
        ("PLANT_ES_01", "CRUSHER_01", "TEMP_01", "temperature", " "),
    ]

    df = spark.createDataFrame(data, SENSOR_SCHEMA)

    with pytest.raises(
        ValueError,
        match="invalid sensor attributes",
    ):
        build_dim_sensor(df)


def test_dim_sensor_rejects_inconsistent_units(spark):
    """A sensor cannot have two different measurement units."""

    data = [
        ("PLANT_ES_01", "CRUSHER_01", "TEMP_01", "temperature", "C"),
        ("PLANT_ES_01", "CRUSHER_01", "TEMP_01", "temperature", "F"),
    ]

    df = spark.createDataFrame(data, SENSOR_SCHEMA)

    with pytest.raises(
        ValueError,
        match="Inconsistent measurement type or unit",
    ):
        build_dim_sensor(df)


def test_dim_sensor_rejects_inconsistent_types(spark):
    """A sensor cannot represent contradictory measurement types."""

    data = [
        ("PLANT_ES_01", "CRUSHER_01", "SENSOR_01", "temperature", "C"),
        ("PLANT_ES_01", "CRUSHER_01", "SENSOR_01", "pressure", "bar"),
    ]

    df = spark.createDataFrame(data, SENSOR_SCHEMA)

    with pytest.raises(
        ValueError,
        match="Inconsistent measurement type or unit",
    ):
        build_dim_sensor(df)


def test_dim_sensor_missing_columns(spark):
    """Missing required columns must raise an error."""

    df = spark.createDataFrame(
        [("PLANT_ES_01", "CRUSHER_01")],
        ["plant_id", "equipment_id"],
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):
        build_dim_sensor(df)


def test_dim_sensor_expected_schema(spark):
    """The dimension must expose exactly five columns."""

    data = [
        (
            "PLANT_ES_01",
            "CRUSHER_01",
            "TEMP_01",
            "temperature",
            "C",
            "EVT001",
        ),
    ]

    df = spark.createDataFrame(
        data,
        SENSOR_SCHEMA + ", event_id string",
    )

    result = build_dim_sensor(df)

    assert result.columns == [
        "plant_id",
        "equipment_id",
        "sensor_id",
        "measurement_type",
        "unit",
    ]
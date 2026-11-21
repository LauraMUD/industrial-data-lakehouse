"""Tests for the Gold equipment dimension."""

import pytest
from pyspark.sql import SparkSession

from src.transformations.gold_equipment import build_dim_equipment


@pytest.fixture(scope="module")
def spark():
    from src.utils.spark_session import create_spark_session

    session = create_spark_session("TestGoldEquipment")
    yield session
    session.stop()


def test_dim_equipment_distinct_records(spark: SparkSession):
    """Duplicate readings must not duplicate equipment."""

    data = [
        ("PLANT_ES_01", "CRUSHER_01"),
        ("PLANT_ES_01", "CRUSHER_01"),
        ("PLANT_ES_01", "CONVEYOR_01"),
        ("PLANT_CL_01", "MILL_01"),
    ]

    df = spark.createDataFrame(
        data,
        ["plant_id", "equipment_id"],
    )

    result = build_dim_equipment(df)

    assert result.count() == 3


def test_dim_equipment_composite_key(spark: SparkSession):
    """Equipment IDs may be reused across different plants."""

    data = [
        ("PLANT_ES_01", "PUMP_01"),
        ("PLANT_CL_01", "PUMP_01"),
    ]

    df = spark.createDataFrame(
        data,
        ["plant_id", "equipment_id"],
    )

    result = build_dim_equipment(df)

    assert result.count() == 2


def test_dim_equipment_rejects_null_identifiers(
    spark: SparkSession,
):
    """Null identifiers must be rejected."""

    data = [
        ("PLANT_ES_01", "CRUSHER_01"),
        (None, "PUMP_01"),
    ]

    df = spark.createDataFrame(
        data,
        "plant_id string, equipment_id string",
    )

    with pytest.raises(
        ValueError,
        match="invalid equipment identifiers",
    ):
        build_dim_equipment(df)


def test_dim_equipment_rejects_blank_identifiers(
    spark: SparkSession,
):
    """Blank identifiers must be rejected."""

    data = [
        ("PLANT_ES_01", "CRUSHER_01"),
        ("PLANT_ES_01", "   "),
    ]

    df = spark.createDataFrame(
        data,
        ["plant_id", "equipment_id"],
    )

    with pytest.raises(
        ValueError,
        match="invalid equipment identifiers",
    ):
        build_dim_equipment(df)


def test_dim_equipment_missing_columns(spark: SparkSession):
    """Missing required columns must raise an error."""

    df = spark.createDataFrame(
        [("PLANT_ES_01",)],
        ["plant_id"],
    )

    with pytest.raises(
        ValueError,
        match="Missing required columns",
    ):
        build_dim_equipment(df)


def test_dim_equipment_expected_schema(spark: SparkSession):
    """Gold must expose only the expected dimension columns."""

    df = spark.createDataFrame(
        [
            ("PLANT_ES_01", "CRUSHER_01", "EVT001"),
        ],
        ["plant_id", "equipment_id", "event_id"],
    )

    result = build_dim_equipment(df)

    assert result.columns == [
        "plant_id",
        "equipment_id",
    ]

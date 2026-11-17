import os
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    builder = (
        SparkSession.builder
        .appName("IndustrialLakehouseTests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def test_delta_write_and_read(spark, tmp_path: Path):
    delta_path = tmp_path / "production_delta"

    data = [
        ("PLANT_01", "CRUSHER_01", 125.5),
        ("PLANT_01", "CRUSHER_02", 132.8),
        ("PLANT_02", "MILL_01", 210.3),
    ]

    df = spark.createDataFrame(
        data,
        ["plant_id", "equipment_id", "production_tons"],
    )

    df.write.format("delta").mode("overwrite").save(str(delta_path))

    result = spark.read.format("delta").load(str(delta_path))

    assert result.count() == 3
    assert set(result.columns) == {
        "plant_id",
        "equipment_id",
        "production_tons",
    }

    assert result.filter(
        result.plant_id == "PLANT_02"
    ).first()["production_tons"] == pytest.approx(210.3)

    assert (delta_path / "_delta_log").exists()

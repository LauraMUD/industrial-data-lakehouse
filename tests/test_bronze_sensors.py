"""Integration tests for industrial Bronze ingestion."""

from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.ingestion.bronze_sensors import ingest
from src.utils.spark_session import create_spark_session


BASE_DIR = Path(__file__).resolve().parents[1]

FIXTURES = BASE_DIR / "tests" / "fixtures" / "sensor_readings"
CONTRACT = BASE_DIR / "config" / "contracts" / "sensor_readings.json"


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session("BronzeIntegrationTests")
    yield session
    session.stop()


def test_initial_ingestion(spark, tmp_path):
    target = tmp_path / "bronze"

    count = ingest(
        spark,
        FIXTURES / "sensors_batch_001.csv",
        target,
        CONTRACT,
    )

    df = spark.read.format("delta").load(str(target))

    assert count == 6
    assert df.count() == 6

    assert df.filter(
        (F.col("event_id") == "EVT005")
        & (F.col("measurement_value") == "ERROR")
    ).count() == 1

    assert df.filter(
        F.col("ingestion_timestamp").isNull()
    ).count() == 0

    assert (target / "_delta_log").exists()


def test_reprocessing_same_batch(spark, tmp_path):
    target = tmp_path / "bronze"
    source = FIXTURES / "sensors_batch_001.csv"

    ingest(spark, source, target, CONTRACT)
    ingest(spark, source, target, CONTRACT)

    df = spark.read.format("delta").load(str(target))

    assert df.count() == 6
    assert df.select("event_id").distinct().count() == 6


def test_multiple_batches(spark, tmp_path):
    target = tmp_path / "bronze"

    ingest(
        spark,
        FIXTURES / "sensors_batch_001.csv",
        target,
        CONTRACT,
    )

    ingest(
        spark,
        FIXTURES / "sensors_batch_002.csv",
        target,
        CONTRACT,
    )

    df = spark.read.format("delta").load(str(target))

    batch_counts = {
        row["batch_id"]: row["count"]
        for row in df.groupBy("batch_id").count().collect()
    }

    assert df.count() == 10

    assert batch_counts == {
        "sensors_batch_001": 6,
        "sensors_batch_002": 4,
    }


def test_invalid_header_is_rejected(spark, tmp_path):
    source = tmp_path / "invalid.csv"
    target = tmp_path / "bronze"

    source.write_text(
        "wrong_column,another_column\nvalue1,value2\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="CSV header mismatch"):
        ingest(spark, source, target, CONTRACT)

    assert not target.exists()


def test_missing_source_is_rejected(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        ingest(
            spark,
            tmp_path / "missing.csv",
            tmp_path / "bronze",
            CONTRACT,
        )

def test_invalid_batch_id_is_rejected(spark, tmp_path):
    source = tmp_path / "invalid'batch.csv"

    source.write_text(
        (FIXTURES / "sensors_batch_001.csv").read_text(
            encoding="utf-8"
        ),
        encoding="utf-8",
    )

    target = tmp_path / "bronze"

    with pytest.raises(ValueError, match="Invalid batch_id"):
        ingest(spark, source, target, CONTRACT)

    assert not target.exists()

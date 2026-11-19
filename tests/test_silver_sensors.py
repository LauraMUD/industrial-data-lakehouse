"""Tests for Silver data quality transformations."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pytest
from src.utils.spark_session import create_spark_session

from unittest.mock import patch

import src.transformations.silver_sensors as silver_module

from src.transformations.silver_sensors import (
    load_silver_contract,
    validate_required_fields,
    validate_numeric_measurement,
    validate_event_timestamp,
    validate_measurement_type,
    validate_measurement_unit,
    validate_duplicate_events,
    validate_measurement_range,
    apply_quality_rules,
    split_quality_results,
    prepare_silver,
    prepare_quarantine,
    write_delta_batch,
    process_bronze_batch,
    run_silver_pipeline,
)




def test_silver_contract_loads():
    contract = load_silver_contract()

    assert contract["layer"] == "silver"
    assert contract["primary_key"] == ["event_id"]
    assert len(contract["required_columns"]) == 8


def test_validate_required_fields():
    spark = create_spark_session("SilverQualityTest")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        data = [
            ("EVT001", "PLANT_ES_01"),
            ("EVT002", None),
            ("EVT003", "   "),
        ]

        df = spark.createDataFrame(
            data,
            ["event_id", "plant_id"],
        )

        result = validate_required_fields(
            df,
            ["event_id", "plant_id"],
        )

        rows = {
            row["event_id"]: row["dq_errors"]
            for row in result.collect()
        }

        assert rows["EVT001"] == []
        assert rows["EVT002"] == ["DQ001_NULL_REQUIRED"]
        assert rows["EVT003"] == ["DQ001_NULL_REQUIRED"]

    finally:
        spark.stop()

def test_validate_numeric_measurement():
    spark = create_spark_session("SilverQualityTest")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        data = [
            ("EVT001", "68.5", []),
            ("EVT005", "ERROR", []),
            ("EVT006", None, ["DQ001_NULL_REQUIRED"]),
            ("EVT007", "NaN", []),
            ("EVT008", "Infinity", []),
        ]

        schema = (
            "event_id STRING, "
            "measurement_value STRING, "
            "dq_errors ARRAY<STRING>"
        )

        df = spark.createDataFrame(data, schema=schema)

        result = validate_numeric_measurement(df)

        rows = {
            row["event_id"]: row
            for row in result.collect()
        }

        assert rows["EVT001"]["measurement_value_numeric"] == 68.5
        assert rows["EVT001"]["dq_errors"] == []

        assert rows["EVT005"]["measurement_value_numeric"] is None
        assert rows["EVT005"]["dq_errors"] == [
            "DQ003_INVALID_NUMERIC"
        ]

        assert rows["EVT006"]["dq_errors"] == [
            "DQ001_NULL_REQUIRED",
            "DQ003_INVALID_NUMERIC",
        ]

        assert rows["EVT007"]["dq_errors"] == [
            "DQ003_INVALID_NUMERIC"
        ]

        assert rows["EVT008"]["dq_errors"] == [
            "DQ003_INVALID_NUMERIC"
        ]

    finally:
        spark.stop()

    
def test_validate_event_timestamp():
    spark = create_spark_session("SilverQualityTest")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        data = [
            ("EVT001", "2026-11-19T07:00:00Z", []),
            ("EVT002", "INVALID_DATE", []),
            ("EVT003", None, ["DQ001_NULL_REQUIRED"]),
            ("EVT004", "2026-02-30T07:00:00Z", []),
            ("EVT005", "2026-11-19T08:00:00", []),
        ]

        schema = (
            "event_id STRING, "
            "event_timestamp STRING, "
            "dq_errors ARRAY<STRING>"
        )

        df = spark.createDataFrame(data, schema=schema)

        result = validate_event_timestamp(df)

        rows = {
            row["event_id"]: row
            for row in result.collect()
        }

        assert rows["EVT001"]["event_timestamp_utc"] is not None
        assert rows["EVT001"]["dq_errors"] == []

        for event_id in ["EVT002", "EVT004", "EVT005"]:
            assert rows[event_id]["event_timestamp_utc"] is None
            assert rows[event_id]["dq_errors"] == [
                "DQ002_INVALID_TIMESTAMP"
            ]

        assert rows["EVT003"]["dq_errors"] == [
            "DQ001_NULL_REQUIRED",
            "DQ002_INVALID_TIMESTAMP",
        ]

    finally:
        spark.stop()

def test_validate_measurement_type_and_unit():
    spark = create_spark_session("SilverQualityTest")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        contract = load_silver_contract()

        data = [
            ("EVT001", "temperature", "C", []),
            ("EVT002", "vibration", "mm/s", []),
            ("EVT003", "pressure", "bar", []),
            ("EVT004", "temperature", "bar", []),
            ("EVT005", "humidity", "%", []),
            ("EVT006", "pressure", "psi", []),
        ]

        schema = (
            "event_id STRING, "
            "measurement_type STRING, "
            "unit STRING, "
            "dq_errors ARRAY<STRING>"
        )

        df = spark.createDataFrame(data, schema=schema)

        result = validate_measurement_type(df, contract)
        result = validate_measurement_unit(result, contract)

        rows = {
            row["event_id"]: row["dq_errors"]
            for row in result.collect()
        }

        assert rows["EVT001"] == []
        assert rows["EVT002"] == []
        assert rows["EVT003"] == []

        assert rows["EVT004"] == [
            "DQ005_INVALID_UNIT"
        ]

        assert rows["EVT005"] == [
            "DQ004_UNKNOWN_MEASUREMENT_TYPE"
        ]

        assert rows["EVT006"] == [
            "DQ005_INVALID_UNIT"
        ]

    finally:
        spark.stop()

def test_validate_duplicate_events():
    spark = create_spark_session("SilverQualityTest")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        data = [
            ("EVT001", "68.5", []),
            ("EVT001", "ERROR", ["DQ003_INVALID_NUMERIC"]),
            ("EVT002", "42.1", []),
            ("EVT003", "70.0", []),
            ("EVT003", "71.0", []),
        ]

        schema = (
            "event_id STRING, "
            "measurement_value STRING, "
            "dq_errors ARRAY<STRING>"
        )

        df = spark.createDataFrame(data, schema=schema)

        result = validate_duplicate_events(df)

        rows = result.collect()

        valid_rows = [
            row for row in rows
            if not row["dq_errors"]
        ]

        duplicate_rows = [
            row for row in rows
            if "DQ006_DUPLICATE_EVENT_ID" in row["dq_errors"]
        ]

        assert len(rows) == 5
        assert len(valid_rows) == 3
        assert len(duplicate_rows) == 2

        # The valid EVT001 takes priority.
        valid_evt001 = [
            row for row in valid_rows
            if row["event_id"] == "EVT001"
        ]

        assert len(valid_evt001) == 1
        assert valid_evt001[0]["measurement_value"] == "68.5"

        # The invalid duplicate retains both error codes.
        invalid_evt001 = [
            row for row in duplicate_rows
            if row["event_id"] == "EVT001"
        ]

        assert invalid_evt001[0]["dq_errors"] == [
            "DQ003_INVALID_NUMERIC",
            "DQ006_DUPLICATE_EVENT_ID",
        ]

        # EVT003 retains one deterministic candidate.
        valid_evt003 = [
            row for row in valid_rows
            if row["event_id"] == "EVT003"
        ]

        assert len(valid_evt003) == 1
        assert valid_evt003[0]["measurement_value"] == "70.0"

    finally:
        spark.stop()

def test_validate_measurement_range():
    spark = create_spark_session("SilverQualityTest")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        contract = load_silver_contract()

        data = [
            ("EVT001", "temperature", 68.5, []),
            ("EVT002", "temperature", 250.0, []),
            ("EVT003", "vibration", -2.0, []),
            ("EVT004", "pressure", 500.0, []),
            ("EVT005", "pressure", 600.0, []),
            (
                "EVT006",
                "vibration",
                None,
                ["DQ003_INVALID_NUMERIC"],
            ),
            ("EVT007", "temperature", -50.0, []),
            ("EVT008", "vibration", 100.0, []),
        ]

        schema = (
            "event_id STRING, "
            "measurement_type STRING, "
            "measurement_value_numeric DOUBLE, "
            "dq_errors ARRAY<STRING>"
        )

        df = spark.createDataFrame(data, schema=schema)

        result = validate_measurement_range(
            df,
            contract,
        )

        rows = {
            row["event_id"]: row["dq_errors"]
            for row in result.collect()
        }

        # Valid measurements
        assert rows["EVT001"] == []
        assert rows["EVT004"] == []
        assert rows["EVT007"] == []
        assert rows["EVT008"] == []

        # Out-of-range measurements
        for event_id in ["EVT002", "EVT003", "EVT005"]:
            assert rows[event_id] == [
                "DQ007_OUT_OF_RANGE"
            ]

        # Preserve previous numeric errors
        assert rows["EVT006"] == [
            "DQ003_INVALID_NUMERIC"
        ]

    finally:
        spark.stop()

def test_apply_quality_rules_integration():
    spark = create_spark_session("SilverQualityTest")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        contract = load_silver_contract()

        data = [
            (
                "EVT001",
                "PLANT_ES_01",
                "CRUSHER_01",
                "TEMP_01",
                "2026-11-19T07:00:00Z",
                "temperature",
                "68.5",
                "C",
            ),
            (
                "EVT005",
                "PLANT_CL_01",
                "MILL_01",
                "VIB_02",
                "2026-11-19T07:04:00Z",
                "vibration",
                "ERROR",
                "mm/s",
            ),
        ]

        df = spark.createDataFrame(
            data,
            schema=contract["required_columns"],
        )

        result = apply_quality_rules(df, contract)

        rows = {
            row["event_id"]: row
            for row in result.collect()
        }

        assert len(rows) == 2

        assert rows["EVT001"]["dq_errors"] == []
        assert (
            rows["EVT001"]["measurement_value_numeric"]
            == 68.5
        )

        assert rows["EVT005"]["dq_errors"] == [
            "DQ003_INVALID_NUMERIC"
        ]

        assert (
            rows["EVT005"]["measurement_value_numeric"]
            is None
        )

    finally:
        spark.stop()

def test_split_quality_results():
    spark = create_spark_session("SilverQualityTest")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        data = [
            ("EVT001", []),
            ("EVT002", []),
            ("EVT005", ["DQ003_INVALID_NUMERIC"]),
            ("EVT007", []),
        ]

        schema = (
            "event_id STRING, "
            "dq_errors ARRAY<STRING>"
        )

        df = spark.createDataFrame(data, schema=schema)

        valid_df, rejected_df = split_quality_results(df)

        assert valid_df.count() == 3
        assert rejected_df.count() == 1

        assert (
            valid_df.count() + rejected_df.count()
            == df.count()
        )

        rejected = rejected_df.collect()

        assert rejected[0]["event_id"] == "EVT005"
        assert rejected[0]["dq_errors"] == [
            "DQ003_INVALID_NUMERIC"
        ]

    finally:
        spark.stop()

def test_prepare_silver():
    from datetime import datetime

    spark = create_spark_session("SilverQualityTest")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        data = [
            (
                "EVT001",
                "PLANT_ES_01",
                "CRUSHER_01",
                "TEMP_01",
                datetime(2026, 11, 19, 7, 0),
                "temperature",
                68.5,
                "C",
                "sensors_batch_001",
                "sensors_batch_001.csv",
                datetime(2026, 11, 19, 9, 0),
                [],
            ),
            (
                "EVT005",
                "PLANT_CL_01",
                "MILL_01",
                "VIB_02",
                datetime(2026, 11, 19, 7, 4),
                "vibration",
                None,
                "mm/s",
                "sensors_batch_001",
                "sensors_batch_001.csv",
                datetime(2026, 11, 19, 9, 0),
                ["DQ003_INVALID_NUMERIC"],
            ),
        ]

        schema = (
            "event_id STRING, "
            "plant_id STRING, "
            "equipment_id STRING, "
            "sensor_id STRING, "
            "event_timestamp_utc TIMESTAMP, "
            "measurement_type STRING, "
            "measurement_value_numeric DOUBLE, "
            "unit STRING, "
            "batch_id STRING, "
            "source_file STRING, "
            "ingestion_timestamp TIMESTAMP, "
            "dq_errors ARRAY<STRING>"
        )

        df = spark.createDataFrame(data, schema=schema)

        silver_df = prepare_silver(df)

        assert silver_df.count() == 1

        row = silver_df.first()

        assert row["event_id"] == "EVT001"
        assert row["measurement_value"] == 68.5

        assert "dq_errors" not in silver_df.columns
        assert "measurement_value_numeric" not in silver_df.columns
        assert "event_timestamp_utc" not in silver_df.columns

        assert (
            silver_df.schema["measurement_value"]
            .dataType.simpleString()
            == "double"
        )

        assert (
            silver_df.schema["event_timestamp"]
            .dataType.simpleString()
            == "timestamp"
        )

        assert len(silver_df.columns) == 11

    finally:
        spark.stop()

def test_prepare_quarantine():
    from datetime import datetime

    spark = create_spark_session("SilverQualityTest")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        data = [
            (
                "EVT001",
                "PLANT_ES_01",
                "CRUSHER_01",
                "TEMP_01",
                "2026-11-19T07:00:00Z",
                "temperature",
                "68.5",
                "C",
                "sensors_batch_001",
                "sensors_batch_001.csv",
                datetime(2026, 11, 19, 9, 0),
                [],
            ),
            (
                "EVT005",
                "PLANT_CL_01",
                "MILL_01",
                "VIB_02",
                "2026-11-19T07:04:00Z",
                "vibration",
                "ERROR",
                "mm/s",
                "sensors_batch_001",
                "sensors_batch_001.csv",
                datetime(2026, 11, 19, 9, 0),
                ["DQ003_INVALID_NUMERIC"],
            ),
        ]

        schema = (
            "event_id STRING, "
            "plant_id STRING, "
            "equipment_id STRING, "
            "sensor_id STRING, "
            "event_timestamp STRING, "
            "measurement_type STRING, "
            "measurement_value STRING, "
            "unit STRING, "
            "batch_id STRING, "
            "source_file STRING, "
            "ingestion_timestamp TIMESTAMP, "
            "dq_errors ARRAY<STRING>"
        )

        df = spark.createDataFrame(data, schema=schema)

        quarantine_df = prepare_quarantine(df)

        assert quarantine_df.count() == 1

        row = quarantine_df.first()

        assert row["event_id"] == "EVT005"
        assert row["measurement_value"] == "ERROR"
        assert row["event_timestamp"] == (
            "2026-11-19T07:04:00Z"
        )

        assert row["dq_errors"] == [
            "DQ003_INVALID_NUMERIC"
        ]

        assert row["batch_id"] == "sensors_batch_001"
        assert row["source_file"] == (
            "sensors_batch_001.csv"
        )

        assert row["quarantine_timestamp"] is not None

        assert (
            quarantine_df.schema["measurement_value"]
            .dataType.simpleString()
            == "string"
        )

        assert len(quarantine_df.columns) == 13

    finally:
        spark.stop()

def test_write_delta_batch_idempotent(tmp_path):
    from src.utils.spark_session import create_spark_session

    spark = create_spark_session(
        "SilverDeltaIdempotencyTest"
    )

    try:
        target = tmp_path / "silver_delta"

        schema = (
            "event_id STRING, "
            "batch_id STRING, "
            "measurement_value DOUBLE"
        )

        first_df = spark.createDataFrame(
            [
                ("EVT001", "batch_001", 68.5),
                ("EVT002", "batch_001", 42.1),
            ],
            schema=schema,
        )

        write_delta_batch(
            spark, first_df, target, "batch_001"
        )

        write_delta_batch(
            spark, first_df, target, "batch_001"
        )

        result = (
            spark.read
            .format("delta")
            .load(str(target))
        )

        assert result.count() == 2

        second_df = spark.createDataFrame(
            [
                ("EVT003", "batch_002", 70.1),
            ],
            schema=schema,
        )

        write_delta_batch(
            spark, second_df, target, "batch_002"
        )

        result = (
            spark.read
            .format("delta")
            .load(str(target))
        )

        assert result.count() == 3

        # Reprocessing batch_001 must not duplicate records.
        write_delta_batch(
            spark, first_df, target, "batch_001"
        )

        result = (
            spark.read
            .format("delta")
            .load(str(target))
        )

        assert result.count() == 3

        assert (
            result.filter(F.col("batch_id") == "batch_001")
            .count()
            == 2
        )

    finally:
        spark.stop()

def test_write_delta_batch_empty_reprocessing(tmp_path):
    from src.utils.spark_session import create_spark_session

    spark = create_spark_session(
        "SilverEmptyBatchReprocessingTest"
    )

    try:
        target = tmp_path / "quarantine_delta"

        schema = (
            "event_id STRING, "
            "batch_id STRING, "
            "measurement_value STRING"
        )

        initial_df = spark.createDataFrame(
            [
                ("EVT005", "batch_001", "ERROR"),
                ("EVT011", "batch_002", "INVALID"),
            ],
            schema=schema,
        )

        write_delta_batch(
            spark,
            initial_df.filter(F.col("batch_id") == "batch_001"),
            target,
            "batch_001",
        )

        write_delta_batch(
            spark,
            initial_df.filter(F.col("batch_id") == "batch_002"),
            target,
            "batch_002",
        )
        result = (
            spark.read
            .format("delta")
            .load(str(target))
        )

        assert result.count() == 2

        # The corrected batch no longer contains rejected records.
        empty_df = spark.createDataFrame(
            [],
            schema=schema,
        )

        write_delta_batch(
            spark,
            empty_df,
            target,
            "batch_001",
        )

        result = (
            spark.read
            .format("delta")
            .load(str(target))
        )

        assert result.count() == 1

        remaining = result.collect()

        assert remaining[0]["event_id"] == "EVT011"
        assert remaining[0]["batch_id"] == "batch_002"

    finally:
        spark.stop()

def test_write_delta_batch_replaces_modified_records(tmp_path):
    from src.utils.spark_session import create_spark_session

    spark = create_spark_session(
        "SilverModifiedBatchTest"
    )

    try:
        target = tmp_path / "silver_modified_delta"

        schema = (
            "event_id STRING, "
            "batch_id STRING, "
            "measurement_value DOUBLE"
        )

        initial_df = spark.createDataFrame(
            [
                ("EVT001", "batch_001", 68.5),
                ("EVT002", "batch_001", 42.1),
                ("EVT010", "batch_002", 75.4),
            ],
            schema=schema,
        )

        write_delta_batch(
            spark,
            initial_df.filter(F.col("batch_id") == "batch_001"),
            target,
            "batch_001",
        )

        write_delta_batch(
            spark,
            initial_df.filter(F.col("batch_id") == "batch_002"),
            target,
            "batch_002",
        )

        # Updated content for batch_001.
        corrected_df = spark.createDataFrame(
            [
                ("EVT001", "batch_001", 69.8),
                ("EVT003", "batch_001", 44.0),
            ],
            schema=schema,
        )

        write_delta_batch(
            spark, corrected_df, target, "batch_001"
        )

        result = (
            spark.read
            .format("delta")
            .load(str(target))
        )

        records = {
            row["event_id"]: row["measurement_value"]
            for row in result.collect()
        }

        assert records == {
            "EVT001": 69.8,
            "EVT003": 44.0,
            "EVT010": 75.4,
        }

        assert result.count() == 3

    finally:
        spark.stop()

def test_write_delta_batch_rejects_mixed_batches(tmp_path):
    from src.utils.spark_session import create_spark_session

    spark = create_spark_session(
        "SilverMixedBatchValidationTest"
    )

    try:
        target = tmp_path / "mixed_batch_delta"

        schema = (
            "event_id STRING, "
            "batch_id STRING, "
            "measurement_value DOUBLE"
        )

        mixed_df = spark.createDataFrame(
            [
                ("EVT001", "batch_001", 68.5),
                ("EVT002", "batch_002", 42.1),
            ],
            schema=schema,
        )

        with pytest.raises(
            ValueError,
            match="outside batch_id=batch_001",
        ):
            write_delta_batch(
                spark,
                mixed_df,
                target,
                "batch_001",
            )

        from delta.tables import DeltaTable

        assert not DeltaTable.isDeltaTable(
            spark,
            str(target),
        )

    finally:
        spark.stop()

def test_process_bronze_batch_integration(tmp_path):
    spark = create_spark_session(
        "SilverBatchIntegrationTest"
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        bronze_path = tmp_path / "bronze"

        schema = (
            "event_id STRING, "
            "plant_id STRING, "
            "equipment_id STRING, "
            "sensor_id STRING, "
            "event_timestamp STRING, "
            "measurement_type STRING, "
            "measurement_value STRING, "
            "unit STRING, "
            "batch_id STRING, "
            "source_file STRING, "
            "ingestion_timestamp TIMESTAMP"
        )

        from datetime import datetime

        rows = [
            (
                "EVT001", "PLANT_01", "MILL_01", "TEMP_01",
                "2026-11-19T07:00:00Z",
                "temperature", "68.5", "C",
                "batch_001", "batch_001.csv",
                datetime(2026, 11, 19, 9, 0),
            ),
            (
                "EVT005", "PLANT_01", "MILL_01", "VIB_01",
                "2026-11-19T07:04:00Z",
                "vibration", "ERROR", "mm/s",
                "batch_001", "batch_001.csv",
                datetime(2026, 11, 19, 9, 0),
            ),
        ]

        bronze_df = spark.createDataFrame(
            rows,
            schema=schema,
        )

        (
            bronze_df.write
            .format("delta")
            .mode("overwrite")
            .save(str(bronze_path))
        )

        silver_df, quarantine_df, metrics = (
            process_bronze_batch(
                spark,
                bronze_path,
                "batch_001",
            )
        )

        assert metrics == {
            "batch_id": "batch_001",
            "bronze_count": 2,
            "silver_count": 1,
            "quarantine_count": 1,
        }

        silver_row = silver_df.first()
        quarantine_row = quarantine_df.first()

        assert silver_row["event_id"] == "EVT001"
        assert silver_row["measurement_value"] == 68.5

        assert quarantine_row["event_id"] == "EVT005"
        assert quarantine_row["measurement_value"] == "ERROR"
        assert quarantine_row["dq_errors"] == [
            "DQ003_INVALID_NUMERIC"
        ]

    finally:
        spark.stop()

def test_run_silver_pipeline_integration(tmp_path):
    from datetime import datetime

    spark = create_spark_session(
        "SilverPipelineIntegrationTest"
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        bronze_path = tmp_path / "bronze"
        silver_path = tmp_path / "silver"
        quarantine_path = tmp_path / "quarantine"

        schema = (
            "event_id STRING, "
            "plant_id STRING, "
            "equipment_id STRING, "
            "sensor_id STRING, "
            "event_timestamp STRING, "
            "measurement_type STRING, "
            "measurement_value STRING, "
            "unit STRING, "
            "batch_id STRING, "
            "source_file STRING, "
            "ingestion_timestamp TIMESTAMP"
        )

        rows = [
            (
                "EVT001", "PLANT_01", "MILL_01", "TEMP_01",
                "2026-11-19T07:00:00Z",
                "temperature", "68.5", "C",
                "batch_001", "batch_001.csv",
                datetime(2026, 11, 19, 9, 0),
            ),
            (
                "EVT005", "PLANT_01", "MILL_01", "VIB_01",
                "2026-11-19T07:04:00Z",
                "vibration", "ERROR", "mm/s",
                "batch_001", "batch_001.csv",
                datetime(2026, 11, 19, 9, 0),
            ),
        ]

        bronze_df = spark.createDataFrame(
            rows,
            schema=schema,
        )

        (
            bronze_df.write
            .format("delta")
            .mode("overwrite")
            .save(str(bronze_path))
        )

        metrics = run_silver_pipeline(
            spark,
            bronze_path,
            silver_path,
            quarantine_path,
            "batch_001",
        )

        assert metrics["bronze_count"] == 2
        assert metrics["silver_count"] == 1
        assert metrics["quarantine_count"] == 1
        assert metrics["silver_written"] == 1
        assert metrics["quarantine_written"] == 1

        silver_df = spark.read.format("delta").load(
            str(silver_path)
        )

        quarantine_df = spark.read.format("delta").load(
            str(quarantine_path)
        )

        assert silver_df.first()["event_id"] == "EVT001"
        assert silver_df.first()["measurement_value"] == 68.5

        assert quarantine_df.first()["event_id"] == "EVT005"
        assert quarantine_df.first()["measurement_value"] == "ERROR"
        assert quarantine_df.first()["dq_errors"] == [
            "DQ003_INVALID_NUMERIC"
        ]

        # Reprocessing must not duplicate records.
        run_silver_pipeline(
            spark,
            bronze_path,
            silver_path,
            quarantine_path,
            "batch_001",
        )

        assert spark.read.format("delta").load(
            str(silver_path)
        ).count() == 1

        assert spark.read.format("delta").load(
            str(quarantine_path)
        ).count() == 1

    finally:
        spark.stop()

def test_run_silver_pipeline_recovers_after_partial_failure(
    tmp_path,
):
    from datetime import datetime

    spark = create_spark_session(
        "SilverPartialFailureRecoveryTest"
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        bronze_path = tmp_path / "bronze"
        silver_path = tmp_path / "silver"
        quarantine_path = tmp_path / "quarantine"

        schema = (
            "event_id STRING, "
            "plant_id STRING, "
            "equipment_id STRING, "
            "sensor_id STRING, "
            "event_timestamp STRING, "
            "measurement_type STRING, "
            "measurement_value STRING, "
            "unit STRING, "
            "batch_id STRING, "
            "source_file STRING, "
            "ingestion_timestamp TIMESTAMP"
        )

        rows = [
            (
                "EVT001", "PLANT_01", "MILL_01", "TEMP_01",
                "2026-11-19T07:00:00Z",
                "temperature", "68.5", "C",
                "batch_001", "batch_001.csv",
                datetime(2026, 11, 19, 9, 0),
            ),
            (
                "EVT005", "PLANT_01", "MILL_01", "VIB_01",
                "2026-11-19T07:04:00Z",
                "vibration", "ERROR", "mm/s",
                "batch_001", "batch_001.csv",
                datetime(2026, 11, 19, 9, 0),
            ),
        ]

        bronze_df = spark.createDataFrame(
            rows,
            schema=schema,
        )

        (
            bronze_df.write
            .format("delta")
            .mode("overwrite")
            .save(str(bronze_path))
        )

        original_write = silver_module.write_delta_batch

        def fail_quarantine_write(
            spark_session,
            df,
            target_path,
            batch_id,
        ):
            if str(target_path) == str(quarantine_path):
                raise RuntimeError(
                    "Simulated Quarantine write failure"
                )

            return original_write(
                spark_session,
                df,
                target_path,
                batch_id,
            )

        # First attempt: Silver succeeds, Quarantine fails.
        with patch.object(
            silver_module,
            "write_delta_batch",
            side_effect=fail_quarantine_write,
        ):
            with pytest.raises(
                RuntimeError,
                match="Simulated Quarantine write failure",
            ):
                run_silver_pipeline(
                    spark,
                    bronze_path,
                    silver_path,
                    quarantine_path,
                    "batch_001",
                )

        # Silver was written before the failure.
        silver_df = (
            spark.read
            .format("delta")
            .load(str(silver_path))
        )

        assert silver_df.count() == 1
        assert silver_df.first()["event_id"] == "EVT001"

        # Retry without the simulated failure.
        metrics = run_silver_pipeline(
            spark,
            bronze_path,
            silver_path,
            quarantine_path,
            "batch_001",
        )

        assert metrics["silver_written"] == 1
        assert metrics["quarantine_written"] == 1

        silver_final = (
            spark.read
            .format("delta")
            .load(str(silver_path))
        )

        quarantine_final = (
            spark.read
            .format("delta")
            .load(str(quarantine_path))
        )

        assert silver_final.count() == 1
        assert quarantine_final.count() == 1

        assert silver_final.first()["event_id"] == "EVT001"

        quarantine_row = quarantine_final.first()

        assert quarantine_row["event_id"] == "EVT005"
        assert quarantine_row["measurement_value"] == "ERROR"
        assert quarantine_row["dq_errors"] == [
            "DQ003_INVALID_NUMERIC"
        ]

    finally:
        spark.stop()
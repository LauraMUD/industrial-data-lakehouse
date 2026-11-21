"""Validate real Bronze records using Silver quality rules."""

from pathlib import Path

from pyspark.sql import functions as F

from src.transformations.silver_sensors import (
    apply_quality_rules,
    load_silver_contract,
)
from src.utils.spark_session import create_spark_session

BASE_DIR = Path(__file__).resolve().parents[1]

BRONZE_PATH = BASE_DIR / "data" / "bronze" / "sensor_readings"


def main():
    spark = create_spark_session("BronzeSilverQualityValidation")

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        contract = load_silver_contract()

        bronze_df = spark.read.format("delta").load(str(BRONZE_PATH))

        bronze_count = bronze_df.count()

        print(f"\nBRONZE RECORDS: {bronze_count}")

        if bronze_count != 10:
            raise AssertionError(f"Expected 10 Bronze records, found {bronze_count}")

        validated_df = apply_quality_rules(
            bronze_df,
            contract,
        ).cache()

        try:
            valid_df = validated_df.filter(F.size(F.col("dq_errors")) == 0)

            rejected_df = validated_df.filter(F.size(F.col("dq_errors")) > 0)

            valid_count = valid_df.count()
            rejected_count = rejected_df.count()

            print(f"VALID RECORDS: {valid_count}")
            print(f"REJECTED RECORDS: {rejected_count}")

            print("\nREJECTED EVENTS:")

            rejected_df.select(
                "event_id",
                "measurement_type",
                "measurement_value",
                "dq_errors",
            ).show(truncate=False)

            rejected_events = rejected_df.select(
                "event_id",
                "dq_errors",
            ).collect()

            assert valid_count == 9
            assert rejected_count == 1

            assert valid_count + rejected_count == bronze_count

            assert len(rejected_events) == 1
            assert rejected_events[0]["event_id"] == "EVT005"
            assert rejected_events[0]["dq_errors"] == ["DQ003_INVALID_NUMERIC"]

            print("\nPASS: Bronze quality validation successful")

        finally:
            validated_df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

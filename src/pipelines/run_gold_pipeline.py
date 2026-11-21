"""Gold industrial analytics pipeline."""

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.pipelines.gold_publication import (
    publish_gold_tables,
    read_active_gold_table,
)
from src.transformations.gold_equipment import build_dim_equipment
from src.transformations.gold_hourly import build_agg_sensor_hourly
from src.transformations.gold_minute import build_agg_sensor_minute
from src.transformations.gold_readings import build_fact_sensor_readings
from src.transformations.gold_sensor import build_dim_sensor


def build_gold_tables(
    spark: SparkSession,
    silver_df: DataFrame,
) -> dict[str, DataFrame]:
    """Build and validate all five Gold analytical tables."""

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    dim_equipment = build_dim_equipment(silver_df)
    dim_sensor = build_dim_sensor(silver_df)
    fact_readings = build_fact_sensor_readings(silver_df)

    agg_minute = build_agg_sensor_minute(fact_readings)
    agg_hourly = build_agg_sensor_hourly(fact_readings)

    # Validate equipment references.
    missing_equipment = fact_readings.join(
        dim_equipment,
        on=["plant_id", "equipment_id"],
        how="left_anti",
    )

    if missing_equipment.limit(1).count() > 0:
        raise ValueError("Gold fact contains unknown equipment references")

    # Validate sensor references, including type and unit.
    sensor_key = [
        "plant_id",
        "equipment_id",
        "sensor_id",
        "measurement_type",
        "unit",
    ]

    missing_sensors = fact_readings.join(
        dim_sensor,
        on=sensor_key,
        how="left_anti",
    )

    if missing_sensors.limit(1).count() > 0:
        raise ValueError("Gold fact contains unknown sensor references")

    # Reconcile source readings with both temporal aggregates.
    fact_count = fact_readings.count()

    for name, aggregated_df in [
        ("minute", agg_minute),
        ("hourly", agg_hourly),
    ]:
        total = aggregated_df.agg(F.sum("reading_count").alias("total")).first()[
            "total"
        ]

        total = total or 0

        if total != fact_count:
            raise ValueError(
                f"Gold {name} reconciliation failed: expected {fact_count}, got {total}"
            )

    return {
        "dim_equipment": dim_equipment,
        "dim_sensor": dim_sensor,
        "fact_sensor_readings": fact_readings,
        "agg_sensor_minute": agg_minute,
        "agg_sensor_hourly": agg_hourly,
    }


GOLD_TABLE_NAMES = (
    "dim_equipment",
    "dim_sensor",
    "fact_sensor_readings",
    "agg_sensor_minute",
    "agg_sensor_hourly",
)


def write_gold_tables(
    gold_tables: dict[str, DataFrame],
    gold_path: str = "data/gold",
) -> None:
    """Persist all Gold tables using Delta Lake."""

    missing = set(GOLD_TABLE_NAMES) - set(gold_tables)

    if missing:
        raise ValueError(f"Missing Gold tables: {sorted(missing)}")

    base_path = Path(gold_path)
    base_path.mkdir(parents=True, exist_ok=True)

    for table_name in GOLD_TABLE_NAMES:
        target_path = base_path / table_name

        (
            gold_tables[table_name]
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(target_path))
        )


def run_gold_pipeline(
    spark: SparkSession,
    silver_path: str = "data/silver/sensor_readings",
    gold_path: str = "data/gold",
) -> dict[str, int]:
    """Execute and publish a versioned Gold pipeline."""

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # Read validated Silver data.
    silver_df = spark.read.format("delta").load(silver_path)

    # Build and validate the five Gold tables.
    gold_tables = build_gold_tables(spark, silver_df)

    # Publish a complete, isolated Gold version.
    run_id = publish_gold_tables(
        spark,
        gold_tables,
        gold_path=gold_path,
    )

    print(f"\nPublished Gold version: {run_id}")

    # Read counts from the published version.
    counts = {}

    for table_name in GOLD_TABLE_NAMES:
        persisted_df = read_active_gold_table(
            spark,
            table_name,
            gold_path=gold_path,
        )

        counts[table_name] = persisted_df.count()

    return counts


def main() -> None:
    """Run the Gold pipeline from the command line."""

    from src.utils.spark_session import create_spark_session

    parser = argparse.ArgumentParser(description="Industrial Gold analytics pipeline")

    parser.add_argument(
        "--silver-path",
        default="data/silver/sensor_readings",
        help="Input Silver Delta table path",
    )

    parser.add_argument(
        "--gold-path",
        default="data/gold",
        help="Output Gold publication directory",
    )

    args = parser.parse_args()

    spark = create_spark_session("IndustrialGoldPipeline")

    try:
        counts = run_gold_pipeline(
            spark,
            silver_path=args.silver_path,
            gold_path=args.gold_path,
        )

        print("\n=== GOLD PIPELINE COMPLETED ===")

        for table_name, count in counts.items():
            print(f"{table_name}: {count} rows")

        print("\nGOLD PIPELINE SUCCESS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

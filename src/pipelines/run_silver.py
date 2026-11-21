"""Execute the Bronze to Silver/Quarantine sensor pipeline."""

import argparse
from pathlib import Path

from src.transformations.silver_sensors import run_silver_pipeline
from src.utils.spark_session import create_spark_session

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "sensor_readings"
DEFAULT_SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "sensor_readings"
DEFAULT_QUARANTINE_PATH = PROJECT_ROOT / "data" / "quarantine" / "sensor_readings"


def discover_batches(spark, bronze_path: Path) -> list[str]:
    """Return sorted batch IDs present in Bronze."""

    bronze_df = spark.read.format("delta").load(str(bronze_path))

    batch_rows = bronze_df.select("batch_id").distinct().orderBy("batch_id").collect()

    batch_ids = [row["batch_id"] for row in batch_rows]

    if not batch_ids:
        raise ValueError("No batches found in Bronze")

    if any(batch_id is None for batch_id in batch_ids):
        raise ValueError("Bronze contains null batch IDs")

    return batch_ids


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process industrial sensor Bronze batches."
    )
    parser.add_argument(
        "--batch-id",
        help="Process only the specified batch; default: all batches.",
    )
    parser.add_argument(
        "--bronze-path",
        type=Path,
        default=DEFAULT_BRONZE_PATH,
    )
    parser.add_argument(
        "--silver-path",
        type=Path,
        default=DEFAULT_SILVER_PATH,
    )
    parser.add_argument(
        "--quarantine-path",
        type=Path,
        default=DEFAULT_QUARANTINE_PATH,
    )

    args = parser.parse_args()

    paths = [
        args.bronze_path.resolve(),
        args.silver_path.resolve(),
        args.quarantine_path.resolve(),
    ]

    if len(set(paths)) != 3:
        raise ValueError("Bronze, Silver and Quarantine paths must be different")

    spark = create_spark_session("IndustrialSilverPipeline")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        batch_ids = (
            [args.batch_id]
            if args.batch_id
            else discover_batches(spark, args.bronze_path)
        )

        print("\n=== SILVER PIPELINE ===")
        print(f"Bronze:     {args.bronze_path}")
        print(f"Silver:     {args.silver_path}")
        print(f"Quarantine: {args.quarantine_path}")
        print(f"Batches:    {batch_ids}")

        totals = {
            "bronze_count": 0,
            "silver_count": 0,
            "quarantine_count": 0,
        }

        for batch_id in batch_ids:
            print(f"\nProcessing batch: {batch_id}")

            metrics = run_silver_pipeline(
                spark=spark,
                bronze_path=args.bronze_path,
                silver_path=args.silver_path,
                quarantine_path=args.quarantine_path,
                batch_id=batch_id,
            )

            for key in totals:
                totals[key] += metrics[key]

            print(f"  Bronze:     {metrics['bronze_count']}")
            print(f"  Silver:     {metrics['silver_written']}")
            print(f"  Quarantine: {metrics['quarantine_written']}")

        print("\n=== EXECUTION TOTALS ===")
        print(f"Bronze:     {totals['bronze_count']}")
        print(f"Silver:     {totals['silver_count']}")
        print(f"Quarantine: {totals['quarantine_count']}")

        if (
            totals["bronze_count"]
            != totals["silver_count"] + totals["quarantine_count"]
        ):
            raise RuntimeError("Global reconciliation failed")

        print("\nPIPELINE COMPLETED SUCCESSFULLY")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

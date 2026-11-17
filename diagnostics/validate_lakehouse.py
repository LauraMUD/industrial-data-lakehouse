from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]
DELTA_PATH = BASE_DIR / "data" / "bronze" / "production_validation"

builder = (
    SparkSession.builder
    .appName("IndustrialLakehouseValidation")
    .master("local[2]")
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

spark = None

try:
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("Spark version:", spark.version)

    data = [
        ("PLANT_01", "CRUSHER_01", 125.5),
        ("PLANT_01", "CRUSHER_02", 132.8),
        ("PLANT_02", "MILL_01", 210.3),
    ]

    df = spark.createDataFrame(
        data,
        ["plant_id", "equipment_id", "production_tons"],
    )

    print("Writing Delta table...")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(str(DELTA_PATH))
    )

    print("Reading Delta table...")

    result = spark.read.format("delta").load(str(DELTA_PATH))

    result.show(truncate=False)

    assert result.count() == 3

    print("SUCCESS: Spark + Delta Lake validation passed")

finally:
    if spark is not None:
        spark.stop()

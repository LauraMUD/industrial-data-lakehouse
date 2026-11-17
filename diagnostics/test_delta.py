import os
import sys
import tempfile
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

builder = (
    SparkSession.builder
    .appName("IndustrialLakehouseDeltaTest")
    .master("local[1]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.python.use.daemon", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    data = [
        ("PLANT_01", 1250.5),
        ("PLANT_02", 980.2),
        ("PLANT_03", 1430.8),
    ]

    df = spark.createDataFrame(
        data,
        ["plant_id", "production_tons"],
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        delta_path = Path(temp_dir) / "production"

        print("\n=== WRITING DELTA TABLE ===")

        df.write.format("delta").mode("overwrite").save(
            delta_path.as_uri()
        )

        print("DELTA WRITE: OK")

        print("\n=== READING DELTA TABLE ===")

        result = spark.read.format("delta").load(
            delta_path.as_uri()
        )

        result.show()

        assert result.count() == 3

        print("DELTA READ: OK")
        print("DELTA LAKE TEST: PASSED")

finally:
    spark.stop()
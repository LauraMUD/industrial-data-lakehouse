import os
import sys
import traceback

from pyspark.sql import SparkSession

# Windows configuration
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("SparkWindowsDiagnostics")
    .master("local[1]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.python.use.daemon", "false")
    .config("spark.python.worker.reuse", "false")
    .config("spark.python.worker.faulthandler.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n=== ENVIRONMENT ===")
    print("Python:", sys.version.split()[0])
    print("Spark:", spark.version)
    print("Python executable:", sys.executable)

    print("\n=== TEST 1: SPARK SQL ===")
    spark.sql("SELECT 1 AS test").show()
    print("TEST 1: OK")

    print("\n=== TEST 2: PYTHON WORKER ===")
    rdd = spark.sparkContext.parallelize([1, 2, 3], 1)
    result = rdd.map(lambda x: x * 2).collect()
    print("Result:", result)
    print("TEST 2: OK")

    print("\n=== TEST 3: DATAFRAME ===")
    data = [
        ("PLANT_01", 1250.5),
        ("PLANT_02", 980.2),
        ("PLANT_03", 1430.8),
    ]

    df = spark.createDataFrame(
        data,
        ["plant_id", "production_tons"],
    )

    df.show()
    print("TEST 3: OK")

except Exception:
    print("\nDIAGNOSTIC FAILED")
    traceback.print_exc()

finally:
    spark.stop()
import os
import sys

from pyspark.sql import SparkSession

# ==========================================
# PYTHON WORKER CONFIGURATION
# ==========================================

python_executable = sys.executable

os.environ["PYSPARK_PYTHON"] = python_executable
os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

print("Python executable:", python_executable)

# ==========================================
# SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder.appName("IndustrialDataLakehouse")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.python.worker.faulthandler.enabled", "true")
    .config("spark.python.worker.reuse", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Python:", sys.version.split()[0])
print("Spark:", spark.version)

# ==========================================
# TEST DATAFRAME
# ==========================================

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

spark.stop()

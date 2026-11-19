"""Shared SparkSession configuration for the industrial lakehouse."""

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "IndustrialDataLakehouse",
) -> SparkSession:
    """Create a local SparkSession with Delta Lake support."""

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    builder = (
        SparkSession.builder
        .appName(app_name)
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

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark

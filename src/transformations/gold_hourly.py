"""Gold hourly industrial sensor aggregations."""

from pyspark.sql import DataFrame

from src.transformations.gold_temporal import (
    build_temporal_aggregation,
)


def build_agg_sensor_hourly(
    fact_df: DataFrame,
) -> DataFrame:
    """Aggregate sensor readings by UTC hour."""

    return build_temporal_aggregation(
        fact_df,
        granularity="hour",
    )
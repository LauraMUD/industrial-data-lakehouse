"""Gold minute-level industrial sensor aggregations."""

from pyspark.sql import DataFrame

from src.transformations.gold_temporal import (
    build_temporal_aggregation,
)


def build_agg_sensor_minute(
    fact_df: DataFrame,
) -> DataFrame:
    """Aggregate sensor readings by UTC minute."""

    return build_temporal_aggregation(
        fact_df,
        granularity="minute",
    )

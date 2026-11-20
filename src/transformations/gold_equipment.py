"""Gold equipment dimension transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


EQUIPMENT_COLUMNS = [
    "plant_id",
    "equipment_id",
]


def build_dim_equipment(silver_df: DataFrame) -> DataFrame:
    """Build one equipment record per plant and equipment."""

    missing_columns = set(EQUIPMENT_COLUMNS) - set(
        silver_df.columns
    )

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    invalid_records = silver_df.filter(
        F.col("plant_id").isNull()
        | F.col("equipment_id").isNull()
        | (F.trim(F.col("plant_id")) == "")
        | (F.trim(F.col("equipment_id")) == "")
    )

    if invalid_records.limit(1).count() > 0:
        raise ValueError(
            "Silver contains invalid equipment identifiers"
        )

    return (
        silver_df
        .select(*EQUIPMENT_COLUMNS)
        .distinct()
    )
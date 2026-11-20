
"""Prefect orchestration for the industrial Data Lakehouse."""

from pathlib import Path

from prefect import flow, get_run_logger, task

from pipeline.runner import StageResult, run_stage


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@task(name="bronze-ingestion", retries=0)
def bronze_task(
    source: str,
    bronze_path: str,
) -> StageResult:
    """Execute Bronze ingestion."""

    logger = get_run_logger()
    logger.info("Starting Bronze ingestion")

    result = run_stage(
        stage="bronze",
        module="src.ingestion.bronze_sensors",
        args=[
            "--source", source,
            "--target", bronze_path,
        ],
    )

    logger.info(
        "Bronze completed in %.2f seconds",
        result.duration_seconds,
    )

    return result


@task(name="silver-transformation", retries=0)
def silver_task(
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
) -> StageResult:
    """Execute Silver transformation and validation."""

    logger = get_run_logger()
    logger.info("Starting Silver transformation")

    result = run_stage(
        stage="silver",
        module="src.pipelines.run_silver",
        args=[
            "--bronze-path", bronze_path,
            "--silver-path", silver_path,
            "--quarantine-path", quarantine_path,
        ],
    )

    logger.info(
        "Silver completed in %.2f seconds",
        result.duration_seconds,
    )

    return result


@task(name="gold-publication", retries=0)
def gold_task(
    silver_path: str,
    gold_path: str,
) -> StageResult:
    """Execute versioned Gold publication."""

    logger = get_run_logger()
    logger.info("Starting Gold publication")

    result = run_stage(
        stage="gold",
        module="src.pipelines.run_gold_pipeline",
        args=[
            "--silver-path", silver_path,
            "--gold-path", gold_path,
        ],
    )

    logger.info(
        "Gold completed in %.2f seconds",
        result.duration_seconds,
    )

    return result


@flow(
    name="industrial-lakehouse",
    log_prints=True,
)
def industrial_lakehouse_flow(
    source: str = "data/raw/sensor_readings/sensors_batch_001.csv",
    bronze_path: str = "data/bronze/sensor_readings",
    silver_path: str = "data/silver/sensor_readings",
    quarantine_path: str = "data/quarantine/sensor_readings",
    gold_path: str = "data/gold",
) -> dict[str, StageResult]:
    """Execute the complete Bronze-Silver-Gold pipeline."""

    logger = get_run_logger()

    logger.info("Starting industrial lakehouse pipeline")

    bronze_result = bronze_task(
        source=source,
        bronze_path=bronze_path,
    )

    silver_result = silver_task(
        bronze_path=bronze_path,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )

    gold_result = gold_task(
        silver_path=silver_path,
        gold_path=gold_path,
    )

    logger.info(
        "Industrial lakehouse pipeline completed successfully"
    )

    return {
        "bronze": bronze_result,
        "silver": silver_result,
        "gold": gold_result,
    }


if __name__ == "__main__":
    industrial_lakehouse_flow()

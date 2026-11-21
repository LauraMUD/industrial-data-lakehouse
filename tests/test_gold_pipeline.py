"""Integration tests for the Gold Delta pipeline."""

from pathlib import Path

import pytest

from src.ingestion.bronze_sensors import ingest
from src.pipelines.run_gold_pipeline import (
    build_gold_tables,
    write_gold_tables,
)
from src.transformations.silver_sensors import (
    apply_quality_rules,
    load_silver_contract,
    prepare_silver,
)
from src.utils.spark_session import create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session("TestGoldPipeline")
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


@pytest.fixture
def silver_df(spark, tmp_path):
    """Build deterministic Silver test data from versioned CSV fixtures."""

    base_dir = Path(__file__).resolve().parents[1]
    fixtures_dir = base_dir / "tests" / "fixtures" / "sensor_readings"
    bronze_contract = base_dir / "config" / "contracts" / "sensor_readings.json"
    bronze_path = tmp_path / "bronze"

    for filename in (
        "sensors_batch_001.csv",
        "sensors_batch_002.csv",
    ):
        ingest(
            spark,
            fixtures_dir / filename,
            bronze_path,
            bronze_contract,
        )

    bronze_df = spark.read.format("delta").load(str(bronze_path))

    assert bronze_df.count() == 10

    validated_df = apply_quality_rules(
        bronze_df,
        load_silver_contract(),
    )

    silver_df = prepare_silver(validated_df)

    assert silver_df.count() == 9
    assert validated_df.filter("size(dq_errors) > 0").count() == 1

    return silver_df


EXPECTED_COUNTS = {
    "dim_equipment": 4,
    "dim_sensor": 6,
    "fact_sensor_readings": 9,
    "agg_sensor_minute": 9,
    "agg_sensor_hourly": 9,
}


def read_table(spark, base_path, name):
    return spark.read.format("delta").load(str(base_path / name))


def test_gold_pipeline_builds_five_tables(spark, silver_df):
    gold_tables = build_gold_tables(spark, silver_df)

    assert set(gold_tables) == set(EXPECTED_COUNTS)

    for name, expected_count in EXPECTED_COUNTS.items():
        assert gold_tables[name].count() == expected_count


def test_gold_pipeline_writes_delta(
    spark,
    silver_df,
    tmp_path,
):
    gold_tables = build_gold_tables(spark, silver_df)

    write_gold_tables(
        gold_tables,
        gold_path=str(tmp_path / "gold"),
    )

    for name, expected_count in EXPECTED_COUNTS.items():
        df = read_table(
            spark,
            tmp_path / "gold",
            name,
        )

        assert df.count() == expected_count


def test_gold_pipeline_is_idempotent(
    spark,
    silver_df,
    tmp_path,
):
    gold_path = tmp_path / "gold"

    first_tables = build_gold_tables(spark, silver_df)
    write_gold_tables(first_tables, str(gold_path))

    first_results = {
        name: sorted(tuple(row) for row in read_table(spark, gold_path, name).collect())
        for name in EXPECTED_COUNTS
    }

    second_tables = build_gold_tables(spark, silver_df)
    write_gold_tables(second_tables, str(gold_path))

    second_results = {
        name: sorted(tuple(row) for row in read_table(spark, gold_path, name).collect())
        for name in EXPECTED_COUNTS
    }

    assert first_results == second_results


def test_gold_pipeline_rejects_missing_tables(tmp_path):
    with pytest.raises(
        ValueError,
        match="Missing Gold tables",
    ):
        write_gold_tables(
            {},
            gold_path=str(tmp_path / "gold"),
        )


def test_gold_publication_preserves_previous_version_on_failure(
    spark,
    silver_df,
    tmp_path,
):
    from src.pipelines.gold_publication import (
        get_active_gold_version,
        publish_gold_tables,
        read_active_gold_table,
    )

    gold_path = tmp_path / "gold"

    # First publication: valid Gold data.
    gold_tables = build_gold_tables(spark, silver_df)

    first_version = publish_gold_tables(
        spark,
        gold_tables,
        gold_path=str(gold_path),
    )

    assert get_active_gold_version(str(gold_path)) == first_version

    # Verify the published fact table.
    original_count = read_active_gold_table(
        spark,
        "fact_sensor_readings",
        gold_path=str(gold_path),
    ).count()

    assert original_count == 9

    # Second publication: inject a failure into one table.
    invalid_tables = dict(gold_tables)

    invalid_tables["agg_sensor_minute"] = None

    with pytest.raises(AttributeError, match="has no attribute 'write'"):
        publish_gold_tables(
            spark,
            invalid_tables,
            gold_path=str(gold_path),
        )

    # The previous version must remain active.
    assert get_active_gold_version(str(gold_path)) == first_version

    # Previously published data must remain readable.
    recovered_count = read_active_gold_table(
        spark,
        "fact_sensor_readings",
        gold_path=str(gold_path),
    ).count()

    assert recovered_count == original_count


def test_gold_publication_cleans_failed_version(
    spark,
    silver_df,
    tmp_path,
):
    from src.pipelines.gold_publication import (
        get_active_gold_version,
        publish_gold_tables,
    )

    gold_path = tmp_path / "gold"

    gold_tables = build_gold_tables(spark, silver_df)

    # Publish the first valid version.
    first_version = publish_gold_tables(
        spark,
        gold_tables,
        gold_path=str(gold_path),
    )

    versions_path = gold_path / "versions"

    versions_before = {path.name for path in versions_path.iterdir() if path.is_dir()}

    assert versions_before == {first_version}

    # Simulate a failure during the next publication.
    invalid_tables = dict(gold_tables)
    invalid_tables["agg_sensor_minute"] = None

    with pytest.raises(AttributeError, match="has no attribute 'write'"):
        publish_gold_tables(
            spark,
            invalid_tables,
            gold_path=str(gold_path),
        )

    # No incomplete version should remain.
    versions_after = {path.name for path in versions_path.iterdir() if path.is_dir()}

    assert versions_after == {first_version}

    # The original version must still be active.
    assert get_active_gold_version(str(gold_path)) == first_version


def test_gold_publication_rejects_validation_failure(
    spark,
    silver_df,
    tmp_path,
    monkeypatch,
):
    from src.pipelines import gold_publication
    from src.pipelines.gold_publication import (
        get_active_gold_version,
        publish_gold_tables,
        read_active_gold_table,
    )

    gold_path = tmp_path / "gold"
    gold_tables = build_gold_tables(spark, silver_df)

    # Publish a valid baseline.
    first_version = publish_gold_tables(
        spark,
        gold_tables,
        gold_path=str(gold_path),
    )

    original_count = read_active_gold_table(
        spark,
        "fact_sensor_readings",
        gold_path=str(gold_path),
    ).count()

    # Inject a validation failure after Delta writes.
    original_validate = gold_publication.validate_persisted_gold_table

    validated_tables = []

    def failing_validation(
        expected_df,
        persisted_df,
        table_name,
    ):
        original_validate(
            expected_df,
            persisted_df,
            table_name,
        )

        validated_tables.append(table_name)

        if table_name == "agg_sensor_hourly":
            raise ValueError("Simulated Gold validation failure")

    monkeypatch.setattr(
        gold_publication,
        "validate_persisted_gold_table",
        failing_validation,
    )

    with pytest.raises(
        ValueError,
        match="Simulated Gold validation failure",
    ):
        publish_gold_tables(
            spark,
            gold_tables,
            gold_path=str(gold_path),
        )

    # Confirm the failure happened at the last validation.
    assert validated_tables == list(gold_publication.GOLD_TABLE_NAMES)

    # Previous version remains active.
    assert get_active_gold_version(str(gold_path)) == first_version

    # Previously published data remains readable.
    assert (
        read_active_gold_table(
            spark,
            "fact_sensor_readings",
            gold_path=str(gold_path),
        ).count()
        == original_count
    )

    # Failed version was removed.
    versions_path = gold_path / "versions"

    remaining_versions = {
        path.name for path in versions_path.iterdir() if path.is_dir()
    }

    assert remaining_versions == {first_version}

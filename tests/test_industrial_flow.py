"""Tests for the industrial Prefect orchestration flow."""

import pytest
from prefect.testing.utilities import prefect_test_harness

from pipeline import industrial_flow
from pipeline.runner import StageExecutionError, StageResult


@pytest.fixture
def prefect_environment():
    """Provide an isolated Prefect testing environment."""
    with prefect_test_harness():
        yield


def make_result(stage: str) -> StageResult:
    """Create a successful simulated stage result."""
    return StageResult(
        stage=stage,
        command=("python", "-m", f"fake.{stage}"),
        duration_seconds=0.1,
        return_code=0,
        stdout=f"{stage} completed",
        stderr="",
    )


def test_flow_executes_stages_in_order(
    monkeypatch,
    prefect_environment,
):
    """Bronze, Silver and Gold execute sequentially."""

    executed = []

    def fake_run_stage(stage, module, args=()):
        executed.append(stage)
        return make_result(stage)

    monkeypatch.setattr(
        industrial_flow,
        "run_stage",
        fake_run_stage,
    )

    results = industrial_flow.industrial_lakehouse_flow()

    assert executed == ["bronze", "silver", "gold"]
    assert set(results) == {"bronze", "silver", "gold"}

    for stage in executed:
        assert results[stage].return_code == 0


def test_bronze_failure_blocks_downstream(
    monkeypatch,
    prefect_environment,
):
    """A Bronze failure prevents Silver and Gold execution."""

    executed = []

    def fake_run_stage(stage, module, args=()):
        executed.append(stage)

        if stage == "bronze":
            raise StageExecutionError("Simulated Bronze failure")

        return make_result(stage)

    monkeypatch.setattr(
        industrial_flow,
        "run_stage",
        fake_run_stage,
    )

    with pytest.raises(
        StageExecutionError,
        match="Simulated Bronze failure",
    ):
        industrial_flow.industrial_lakehouse_flow()

    assert executed == ["bronze"]


def test_silver_failure_blocks_gold(
    monkeypatch,
    prefect_environment,
):
    """A Silver failure prevents Gold execution."""

    executed = []

    def fake_run_stage(stage, module, args=()):
        executed.append(stage)

        if stage == "silver":
            raise StageExecutionError("Simulated Silver failure")

        return make_result(stage)

    monkeypatch.setattr(
        industrial_flow,
        "run_stage",
        fake_run_stage,
    )

    with pytest.raises(
        StageExecutionError,
        match="Simulated Silver failure",
    ):
        industrial_flow.industrial_lakehouse_flow()

    assert executed == ["bronze", "silver"]


def test_gold_failure_propagates(
    monkeypatch,
    prefect_environment,
):
    """A Gold failure propagates to the Prefect flow."""

    executed = []

    def fake_run_stage(stage, module, args=()):
        executed.append(stage)

        if stage == "gold":
            raise StageExecutionError("Simulated Gold failure")

        return make_result(stage)

    monkeypatch.setattr(
        industrial_flow,
        "run_stage",
        fake_run_stage,
    )

    with pytest.raises(
        StageExecutionError,
        match="Simulated Gold failure",
    ):
        industrial_flow.industrial_lakehouse_flow()

    assert executed == ["bronze", "silver", "gold"]


def test_flow_propagates_custom_paths(
    monkeypatch,
    prefect_environment,
):
    """Each stage receives the correct isolated paths."""

    calls = []

    def fake_run_stage(stage, module, args=()):
        calls.append(
            {
                "stage": stage,
                "module": module,
                "args": list(args),
            }
        )
        return make_result(stage)

    monkeypatch.setattr(
        industrial_flow,
        "run_stage",
        fake_run_stage,
    )

    results = industrial_flow.industrial_lakehouse_flow(
        source="data/raw/test.csv",
        bronze_path="data/test/bronze",
        silver_path="data/test/silver",
        quarantine_path="data/test/quarantine",
        gold_path="data/test/gold",
    )

    assert [call["stage"] for call in calls] == [
        "bronze",
        "silver",
        "gold",
    ]

    assert calls[0]["module"] == ("src.ingestion.bronze_sensors")
    assert calls[0]["args"] == [
        "--source",
        "data/raw/test.csv",
        "--target",
        "data/test/bronze",
    ]

    assert calls[1]["module"] == ("src.pipelines.run_silver")
    assert calls[1]["args"] == [
        "--bronze-path",
        "data/test/bronze",
        "--silver-path",
        "data/test/silver",
        "--quarantine-path",
        "data/test/quarantine",
    ]

    assert calls[2]["module"] == ("src.pipelines.run_gold_pipeline")
    assert calls[2]["args"] == [
        "--silver-path",
        "data/test/silver",
        "--gold-path",
        "data/test/gold",
    ]

    assert set(results) == {
        "bronze",
        "silver",
        "gold",
    }


"""Unit tests for the isolated pipeline stage runner."""

import subprocess
import sys
import pytest
from pipeline import runner

from pipeline.runner import (
    PROJECT_ROOT,
    StageExecutionError,
    StageResult,
    run_stage,
)


def test_run_stage_success(monkeypatch):
    """A successful process returns execution metadata."""

    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs

        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout="Pipeline completed successfully",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = run_stage(
        stage="bronze",
        module="src.ingestion.bronze_sensors",
    )

    assert isinstance(result, StageResult)
    assert result.stage == "bronze"
    assert result.return_code == 0
    assert result.duration_seconds >= 0
    assert "successfully" in result.stdout
    assert result.stderr == ""

    assert captured["command"] == [
        sys.executable,
        "-m",
        "src.ingestion.bronze_sensors",
    ]

    assert captured["kwargs"]["cwd"] == PROJECT_ROOT
    assert captured["kwargs"]["capture_output"] is True
    assert captured["kwargs"]["text"] is True
    assert captured["kwargs"]["check"] is False


def test_run_stage_failure(monkeypatch):
    """A nonzero exit code raises StageExecutionError."""

    def fake_run(command, **kwargs):
        return subprocess.CompletedProcess(
            args=command,
            returncode=1,
            stdout="",
            stderr="Simulated Spark failure",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(
        StageExecutionError,
        match="Stage silver failed with exit code 1",
    ):
        run_stage(
            stage="silver",
            module="src.pipelines.run_silver",
        )


def test_run_stage_process_start_error(monkeypatch):
    """An OS error is converted into StageExecutionError."""

    def fake_run(command, **kwargs):
        raise OSError("Simulated process startup failure")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(
        StageExecutionError,
        match="Stage gold could not start",
    ):
        run_stage(
            stage="gold",
            module="src.pipelines.run_gold_pipeline",
        )




def test_run_stage_timeout(monkeypatch):
    """A timed-out subprocess raises StageTimeoutError."""

    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(
            cmd=args[0],
            timeout=kwargs["timeout"],
        )

    monkeypatch.setattr(
        runner.subprocess,
        "run",
        fake_run,
    )

    with pytest.raises(
        runner.StageTimeoutError,
        match="Stage bronze exceeded 5 seconds",
    ) as exc_info:
        runner.run_stage(
            stage="bronze",
            module="src.ingestion.bronze_sensors",
            timeout_seconds=5,
        )

    assert isinstance(
        exc_info.value.__cause__,
        subprocess.TimeoutExpired,
    )


def test_run_stage_rejects_invalid_timeout(monkeypatch):
    """Invalid timeouts are rejected before execution."""

    def unexpected_run(*args, **kwargs):
        pytest.fail(
            "subprocess.run must not be called"
        )

    monkeypatch.setattr(
        runner.subprocess,
        "run",
        unexpected_run,
    )

    with pytest.raises(
        ValueError,
        match="timeout_seconds must be greater than zero",
    ):
        runner.run_stage(
            stage="bronze",
            module="src.ingestion.bronze_sensors",
            timeout_seconds=0,
        )




def test_run_stage_timeout(monkeypatch):
    """A timed-out subprocess raises StageTimeoutError."""

    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(
            cmd=args[0],
            timeout=kwargs["timeout"],
        )

    monkeypatch.setattr(
        runner.subprocess,
        "run",
        fake_run,
    )

    with pytest.raises(
        runner.StageTimeoutError,
        match="Stage bronze exceeded 5 seconds",
    ) as exc_info:
        runner.run_stage(
            stage="bronze",
            module="src.ingestion.bronze_sensors",
            timeout_seconds=5,
        )

    assert isinstance(
        exc_info.value.__cause__,
        subprocess.TimeoutExpired,
    )


def test_run_stage_rejects_invalid_timeout(monkeypatch):
    """Invalid timeouts are rejected before execution."""

    def unexpected_run(*args, **kwargs):
        pytest.fail(
            "subprocess.run must not be called"
        )

    monkeypatch.setattr(
        runner.subprocess,
        "run",
        unexpected_run,
    )

    with pytest.raises(
        ValueError,
        match="timeout_seconds must be greater than zero",
    ):
        runner.run_stage(
            stage="bronze",
            module="src.ingestion.bronze_sensors",
            timeout_seconds=0,
        )


def test_run_stage_passes_timeout_to_subprocess(
    monkeypatch,
):
    """The configured timeout reaches subprocess.run."""

    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["timeout"] = kwargs["timeout"]

        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout="OK",
            stderr="",
        )

    monkeypatch.setattr(
        runner.subprocess,
        "run",
        fake_run,
    )

    result = runner.run_stage(
        stage="silver",
        module="src.pipelines.run_silver",
        timeout_seconds=120,
    )

    assert captured["timeout"] == 120
    assert captured["command"] == [
        runner.sys.executable,
        "-m",
        "src.pipelines.run_silver",
    ]
    assert result.return_code == 0


def test_run_stage_failure_preserves_metadata(
    monkeypatch,
):
    """Failed stages expose structured execution metadata."""

    def fake_run(command, **kwargs):
        return subprocess.CompletedProcess(
            args=command,
            returncode=42,
            stdout="Partial processing completed",
            stderr="Simulated Delta write failure",
        )

    monkeypatch.setattr(
        runner.subprocess,
        "run",
        fake_run,
    )

    with pytest.raises(
        runner.StageExecutionError,
        match="Stage gold failed with exit code 42",
    ) as exc_info:
        runner.run_stage(
            stage="gold",
            module="src.pipelines.run_gold_pipeline",
        )

    result = exc_info.value.result

    assert result is not None
    assert result.stage == "gold"
    assert result.return_code == 42
    assert result.duration_seconds >= 0
    assert result.stdout == "Partial processing completed"
    assert result.stderr == "Simulated Delta write failure"

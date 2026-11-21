"""Execute pipeline stages in isolated Python processes."""

import logging
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_TIMEOUT_SECONDS = 600

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StageResult:
    """Execution metadata for a completed pipeline stage."""

    stage: str
    command: tuple[str, ...]
    duration_seconds: float
    return_code: int
    stdout: str
    stderr: str


class StageExecutionError(RuntimeError):
    """Raised when a pipeline stage fails."""

    def __init__(
        self,
        message: str,
        result: StageResult | None = None,
    ) -> None:
        super().__init__(message)
        self.result = result


class StageTimeoutError(StageExecutionError):
    """Raised when a pipeline stage exceeds its timeout."""


def run_stage(
    stage: str,
    module: str,
    args: Sequence[str] = (),
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> StageResult:
    """Execute one pipeline stage in a separate Python process."""

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than zero")

    command = [
        sys.executable,
        "-m",
        module,
        *args,
    ]

    logger.info(
        "Starting stage=%s timeout=%ss command=%s",
        stage,
        timeout_seconds,
        command,
    )

    start = time.monotonic()

    try:
        completed = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )

    except subprocess.TimeoutExpired as exc:
        duration = time.monotonic() - start

        logger.error(
            "Stage=%s timed out after %.2fs (limit=%ss)",
            stage,
            duration,
            timeout_seconds,
        )

        raise StageTimeoutError(
            f"Stage {stage} exceeded {timeout_seconds} seconds"
        ) from exc

    except OSError as exc:
        logger.exception(
            "Unable to start stage=%s",
            stage,
        )

        raise StageExecutionError(f"Stage {stage} could not start") from exc

    duration = time.monotonic() - start

    result = StageResult(
        stage=stage,
        command=tuple(command),
        duration_seconds=duration,
        return_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )

    if result.stdout:
        logger.info(
            "Stage=%s stdout:\n%s",
            stage,
            result.stdout,
        )

    if result.stderr:
        logger.debug(
            "Stage=%s stderr:\n%s",
            stage,
            result.stderr,
        )

    if result.return_code != 0:
        logger.error(
            "Stage=%s failed return_code=%s duration=%.2fs stderr=%s",
            stage,
            result.return_code,
            duration,
            result.stderr[-4000:],
        )

        raise StageExecutionError(
            f"Stage {stage} failed with exit code {result.return_code}",
            result=result,
        )

    logger.info(
        "Stage=%s completed return_code=%s duration=%.2fs",
        stage,
        result.return_code,
        duration,
    )

    return result

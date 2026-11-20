
# Industrial Data Lakehouse — Prefect Orchestration

## 1. Overview

The project uses Prefect 3 to orchestrate the industrial
Medallion Architecture:

**Bronze → Silver → Gold**

The workflow coordinates existing PySpark and Delta Lake
pipelines without replacing their transformation logic.

The implementation uses a local Prefect execution model.

No scheduled deployment, external Prefect server, or
production monitoring infrastructure is configured.

## 2. Architecture

```text
                  Prefect Flow
              industrial-lakehouse
                       |
                       v
                Bronze Task
                       |
                       v
                Silver Task
                       |
                       v
                 Gold Task

Each task
    |
    v
pipeline/runner.py
    |
    v
Isolated Python subprocess
    |
    v
Existing PySpark + Delta Lake CLI
```

The stages execute sequentially.

A downstream stage is not executed if an upstream
stage fails.

## 3. Components

### pipeline/industrial_flow.py

Defines the Prefect flow and its three tasks.

Responsibilities:

- Coordinate Bronze, Silver, and Gold.
- Pass configurable input and output paths.
- Enforce stage ordering.
- Propagate execution failures.
- Expose task and flow execution states.

The tasks currently use `retries=0`.

### pipeline/runner.py

Executes each stage in a separate Python subprocess.

Responsibilities:

- Construct the Python module command.
- Execute from the project root.
- Capture standard output and standard error.
- Measure execution duration.
- Validate subprocess exit codes.
- Enforce an execution timeout.
- Raise structured execution errors.

The default subprocess timeout is 600 seconds.

A successful stage returns a `StageResult` containing:

- `stage`
- `command`
- `duration_seconds`
- `return_code`
- `stdout`
- `stderr`

A failed subprocess raises `StageExecutionError`.

For nonzero exit codes, the exception exposes
the execution metadata through `exc.result`.

A timeout raises `StageTimeoutError`.

## 4. Dependencies

The project uses:

- Python 3.11
- Apache Spark / PySpark
- Delta Lake
- Prefect 3
- uv
- pytest

Install the locked dependencies:

```bash
uv sync --frozen
```

## 5. Local Prefect Configuration

The project uses a local `.env` file containing:

```dotenv
PREFECT_SERVER_ANALYTICS_ENABLED=false
```

The `.env` file is excluded from Git.

This setting disables Prefect server analytics
in the local development environment.

It was introduced after observing an intermittent
SQLite database lock during a successful pipeline run.

Disabling analytics is not a guarantee that SQLite
locking can never occur.

The project does not automatically load `.env`
through uv unless `--env-file` is specified.

## 6. Running the Workflow

Execute commands from the repository root.

The flow can be invoked directly through Python.

Example using the default paths:

```bash
uv run --env-file .env python -c \
  "from pipeline.industrial_flow import industrial_lakehouse_flow; industrial_lakehouse_flow()"
```

The workflow uses these defaults:

| Parameter | Default |
|---|---|
| `source` | `data/raw/sensor_readings/sensors_batch_001.csv` |
| `bronze_path` | `data/bronze/sensor_readings` |
| `silver_path` | `data/silver/sensor_readings` |
| `quarantine_path` | `data/quarantine/sensor_readings` |
| `gold_path` | `data/gold` |

### Isolated Validation Run

To avoid modifying existing local Delta datasets,
use dedicated validation paths.

```bash
uv run --env-file .env python - <<'PY'
from pipeline.industrial_flow import industrial_lakehouse_flow

results = industrial_lakehouse_flow(
    source="data/raw/sensor_readings/sensors_batch_001.csv",
    bronze_path="data/orchestration_validation/bronze/sensor_readings",
    silver_path="data/orchestration_validation/silver/sensor_readings",
    quarantine_path="data/orchestration_validation/quarantine/sensor_readings",
    gold_path="data/orchestration_validation/gold",
)

for stage, result in results.items():
    print(
        stage,
        result.return_code,
        round(result.duration_seconds, 2),
    )
PY
```

Generated validation data should remain excluded
from version control.

## 7. Execution and Observability

Prefect provides task and flow execution states.

The subprocess Runner provides stage-level metadata
and Python logging.

Available information includes:

- Stage name.
- Executed command.
- Execution duration.
- Exit code.
- Captured standard output and standard error.
- Failure exceptions.
- Prefect task and flow states.

The Runner logs successful subprocess output
at INFO level.

For successful stages, captured standard error
is logged at DEBUG level.

For failed stages, the Runner logs the exit code,
duration, and a bounded standard-error excerpt.

The implementation does not provide a persistent
operational audit table or centralized production
monitoring.

## 8. Failure Handling

### Bronze Failure

Bronze raises an exception.

Silver and Gold are not executed.

### Silver Failure

Silver raises an exception.

Gold is not executed.

Silver and Quarantine are separate Delta tables,
so a partial update can occur between their writes.

The existing batch-level idempotency supports
reprocessing after a handled failure.

### Gold Failure

Gold raises an exception.

The versioned publication mechanism preserves
the previous active version for handled failures
before manifest publication.

Incomplete version directories are cleaned up
on a best-effort basis.

Prefect does not add distributed multi-table
transactional guarantees.

## 9. Timeout and Retry Policy

Each subprocess has a default timeout of
600 seconds.

Timeouts are enforced by `subprocess.run`.

This timeout applies to the direct subprocess.
It does not guarantee termination of all
descendant Spark or Java processes.

The current Prefect tasks use:

```python
retries=0
```

No automatic task retries are configured.

Recovery requires diagnosing the failure
and explicitly rerunning the appropriate
stage or workflow.

## 10. Testing

Run the orchestration tests:

```bash
uv run --env-file .env pytest \
  tests/test_runner.py \
  tests/test_industrial_flow.py \
  -v
```

Run the complete regression suite:

```bash
uv run --env-file .env pytest -v
```

Latest complete validation:

**November 20, 2026 — 89 tests passed.**

The suite includes:

- Bronze ingestion and Delta Lake tests.
- Silver quality, idempotency, and recovery tests.
- Gold analytical transformation tests.
- Gold versioned publication tests.
- Prefect workflow integration tests.
- Subprocess Runner and timeout tests.

The Prefect integration tests simulate stage execution.
A separate real execution of Bronze, Silver, and Gold
was also completed successfully using isolated
validation paths.

## 11. Known Limitations

- No scheduled Prefect deployment is configured.
- Automatic task retries are disabled.
- No persistent operational audit table exists.
- No centralized production monitoring is configured.
- SQLite locking may occur in the local Prefect server.
- Subprocess timeouts do not guarantee termination
  of every Spark descendant process.
- Silver and Quarantine are not published atomically.
- Gold publication relies on local filesystem semantics.
- Concurrent Gold publishers are not coordinated.

## 12. Future Improvements

- Scheduled Prefect deployments.
- Persistent execution audit records.
- Configurable retry and recovery policies.
- Production-grade monitoring and alerting.
- Improved Spark subprocess lifecycle management.
- Cloud storage integration.
- CI/CD validation and deployment automation.

## 13. Related Documentation

- [Bronze Ingestion](bronze_ingestion.md)
- [Silver Pipeline](silver_pipeline.md)
- [Gold Pipeline](gold_pipeline.md)
- [Project README](../README.md)

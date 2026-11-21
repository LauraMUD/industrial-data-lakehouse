# Continuous Integration and Reproducibility

## Overview

Industrial Data Lakehouse uses GitHub Actions to run continuous integration (CI) on pushes to `main` and pull requests targeting `main`. The workflow checks dependency reproducibility, code quality, and automated tests. Production deployment is not implemented.

## Workflow

The workflow is defined in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).

| Setting | Value |
|---|---|
| Triggers | Push and pull request to `main` |
| Job | `quality-and-tests` |
| Runner | `ubuntu-latest` |
| Job timeout | 30 minutes |
| Permissions | `contents: read` |
| Java | Temurin 17 |
| Python | 3.11 |
| Dependencies | `uv sync --locked --dev` |

The job sets `SPARK_LOCAL_IP=127.0.0.1`, `PYSPARK_PYTHON=python`, and `PYSPARK_DRIVER_PYTHON=python` to support local Spark execution in CI.

### Steps

1. Check out the repository with `actions/checkout@v4`.
2. Install Java 17 with `actions/setup-java@v4`.
3. Configure Python 3.11 and uv with `astral-sh/setup-uv@v6` (dependency caching enabled).
4. Install dependencies using the existing lockfile.
5. Run Ruff lint checks.
6. Verify Ruff formatting without modifying files.
7. Execute the pytest suite, including PySpark and Delta Lake integration tests.

The quality gates are:

```bash
uv sync --locked --dev
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked pytest -v
```

A failing command causes the CI job to fail. The `--locked` option checks consistency with `uv.lock` rather than silently updating it.

## Test Reproducibility

The automated suite covers Bronze ingestion, Silver validation and quarantine, Gold analytical transformations and versioned publication, Prefect orchestration, and subprocess execution and failure handling.

Gold integration tests generate deterministic Silver data from version-controlled CSV fixtures and write temporary Delta datasets using pytest's `tmp_path`. They do not require a pre-existing local Silver dataset.

The CI test suite does not require a local `.env` file. Local Prefect workflow execution may use `.env` for configuration.

## Local Validation

From the repository root, use Java 17 and Python 3.11, then run the four quality-gate commands above. For a failing check, inspect its output and reproduce that command locally before committing changes.

For Spark-related failures, also verify the Java version, Python environment, and Spark logs. For lockfile failures, check that `pyproject.toml` and `uv.lock` are consistent.

## Verified Result

The local test suite passed **89 tests** on November 20, 2026. On November 21, 2026, GitHub Actions completed successfully for commit `d5a7538`, including dependency installation, Ruff checks, and pytest.

[View successful GitHub Actions run](https://github.com/LauraMUD/industrial-data-lakehouse/actions/runs/44897204138).

## Scope and Limitations

- CI runs on a GitHub-hosted Ubuntu runner; cloud deployment is not configured.
- The workflow does not deploy Prefect flows, publish artifacts, or provision infrastructure.
- Passing local-filesystem integration tests does not establish cloud-storage or distributed-production guarantees.
- Production scheduling, monitoring, and alerting remain future work.
- The successful run reported nonblocking deprecation warnings concerning Node.js 20-based Actions and `actions/setup-java@v4`; Action versions should be reviewed in a future maintenance update.

## Next Steps

Future work includes updating deprecated Actions, adding cloud-specific integration tests, and implementing deployment automation after a target environment and its secrets-management approach are defined.
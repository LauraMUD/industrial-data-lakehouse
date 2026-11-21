# Industrial Data Lakehouse

A reproducible industrial data engineering project built with Apache Spark, Delta Lake, Python, Prefect, and uv.

## Project Overview

Industrial Data Lakehouse is an end-to-end data engineering project designed to ingest, validate, transform, and analyze industrial sensor data.

The project implements the **Medallion Architecture (Bronze, Silver, Gold)** with a focus on data reliability, reproducibility, automated testing, workflow orchestration, and industrial analytics.

The current implementation includes:

- Industrial sensor ingestion into Delta Lake
- Data quality validation and quarantine
- Idempotent Silver batch processing
- Dimensional modeling in Gold
- Minute-level and hourly sensor aggregations
- Referential integrity and reconciliation checks
- Versioned Gold publication
- Prefect 3 workflow orchestration across Bronze, Silver, and Gold
- Isolated subprocess execution with timeouts and failure propagation
- Execution logging and structured stage metadata
- Automated unit and integration tests
- Ruff linting and formatting checks
- GitHub Actions continuous integration on pushes and pull requests to `main`

## Architecture

The Medallion pipeline is orchestrated by Prefect 3.

The workflow executes Bronze, Silver, and Gold sequentially. Each stage runs in a separate Python subprocess through `pipeline/runner.py`.

Prefect manages execution order and failure propagation, while each layer retains responsibility for its own transformations, validations, and Delta Lake persistence.

```text
              Prefect 3 Workflow
              industrial-lakehouse
                       |
                       v
              Industrial Sensor CSV
                       |
                       v
                  BRONZE LAYER
              Raw ingestion + metadata
                    Delta Lake
                       |
                       v
                  SILVER LAYER
              Data quality validation
                   /         \
                  v           v
            Validated       Quarantine
              Silver        Rejected data
                  |
                  v
                  GOLD LAYER
              Analytical transformations
                  |
                  +-- dim_equipment
                  +-- dim_sensor
                  +-- fact_sensor_readings
                  +-- agg_sensor_minute
                  +-- agg_sensor_hourly
                  |
                  v
             Versioned Delta Publication
                  |
                  +-- versions/<run_id>/
                  +-- current.json
```

Each workflow stage is executed through the subprocess Runner.

The Runner provides:

- Isolated Python subprocess execution
- Configurable execution timeouts
- Standard output and error capture
- Execution duration measurement
- Exit-code validation
- Structured execution metadata
- Failure propagation to Prefect

### Bronze Layer

The Bronze layer ingests industrial sensor readings from CSV files.

Responsibilities:

- Validate source file structure
- Apply ingestion schemas
- Preserve original sensor measurements
- Add ingestion metadata
- Persist data using Delta Lake
- Support reproducible batch ingestion

Bronze can execute independently or as the first Prefect task.

Documentation: [Bronze Ingestion](docs/bronze_ingestion.md)

### Silver Layer

The Silver layer applies data quality rules to Bronze sensor readings.

Responsibilities:

- Validate required fields
- Validate measurement values and timestamps
- Detect duplicate events
- Validate measurement types, units, and ranges
- Separate valid and invalid records
- Preserve data lineage
- Support idempotent batch processing
- Recover from tested partial processing failures

Invalid records are isolated in a quarantine dataset.

Silver executes after successful Bronze ingestion.

Silver and Quarantine are separate Delta tables. Their writes do not form a single atomic transaction.

Documentation: [Silver Pipeline](docs/silver_pipeline.md)

### Gold Layer

The Gold layer transforms validated Silver readings into analytical datasets.

It implements two dimensions, one fact table, and two temporal aggregations.

| Table | Description | Grain |
|---|---|---|
| `dim_equipment` | Industrial equipment dimension | Plant and equipment |
| `dim_sensor` | Sensor dimension | Plant, equipment, and sensor |
| `fact_sensor_readings` | Validated measurement events | Individual sensor event |
| `agg_sensor_minute` | Minute-level statistics | Sensor and UTC minute |
| `agg_sensor_hourly` | Hourly statistics | Sensor and UTC hour |

The temporal aggregations calculate:

- Reading count
- Average measurement value
- Minimum measurement value
- Maximum measurement value
- Population standard deviation

Gold also validates dimension references and reconciles fact counts against temporal aggregations.

Gold executes after successful Bronze and Silver processing.

The Gold publication mechanism is independent of Prefect and manages versioned Delta tables and the active-version manifest.

Documentation: [Gold Pipeline](docs/gold_pipeline.md)

### Workflow Orchestration

Prefect 3 coordinates the complete Medallion pipeline.

The workflow is defined in:

`pipeline/industrial_flow.py`

Subprocess execution is implemented in:

`pipeline/runner.py`

The workflow:

1. Executes Bronze ingestion.
2. Executes Silver validation and quarantine.
3. Executes Gold analytical transformations and publication.
4. Propagates failures and prevents downstream execution after a failed stage.

The current tasks use `retries=0`.

The default subprocess timeout is 600 seconds.

The implementation provides local execution logging and Prefect task states, but does not include persistent operational audit tables or production monitoring infrastructure.

Documentation: [Prefect Orchestration](docs/orchestration.md)

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11 | Pipeline development |
| Apache Spark / PySpark | Distributed data processing |
| Delta Lake | Transactional data storage |
| Prefect 3 | Workflow orchestration and execution states |
| uv | Dependency management |
| pytest | Automated testing |
| Ruff | Linting and code formatting |
| GitHub Actions | Continuous integration |
| Git / GitHub | Version control |
| Ubuntu / WSL | Development environment |

## Project Structure

```text
industrial-data-lakehouse/
├── config/
│   └── contracts/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── quarantine/
│   └── gold/
│       ├── versions/
│       └── current.json
├── docs/
│   ├── bronze_ingestion.md
│   ├── silver_pipeline.md
│   ├── gold_pipeline.md
│   ├── orchestration.md
│   └── ci_cd.md
├── pipeline/
│   ├── __init__.py
│   ├── industrial_flow.py
│   └── runner.py
├── src/
│   ├── ingestion/
│   │   └── bronze_sensors.py
│   ├── pipelines/
│   │   ├── run_silver.py
│   │   ├── run_gold_pipeline.py
│   │   └── gold_publication.py
│   ├── transformations/
│   │   ├── silver_sensors.py
│   │   ├── gold_equipment.py
│   │   ├── gold_sensor.py
│   │   ├── gold_readings.py
│   │   ├── gold_temporal.py
│   │   ├── gold_minute.py
│   │   └── gold_hourly.py
│   └── utils/
│       └── spark_session.py
├── tests/
│   ├── test_bronze_sensors.py
│   ├── test_delta_lake.py
│   ├── test_silver_sensors.py
│   ├── test_gold_equipment.py
│   ├── test_gold_sensor.py
│   ├── test_gold_readings.py
│   ├── test_gold_minute.py
│   ├── test_gold_hourly.py
│   ├── test_gold_pipeline.py
│   ├── test_industrial_flow.py
│   └── test_runner.py
├── .github/
│   └── workflows/
│       └── ci.yml
├── .gitignore
├── pyproject.toml
├── uv.lock
└── README.md
```

The `data/` directories contain locally generated Delta datasets.

Gold version directories and the active-version manifest are generated locally and excluded from Git.

The isolated orchestration validation datasets under `data/orchestration_validation/` are also excluded from version control.

Local environment configuration is stored in `.env`, which is not committed to Git.

## Installation

### Requirements

- Linux or WSL
- Java 17
- Python 3.11
- uv
- Git

### Clone the Repository

```bash
git clone https://github.com/LauraMUD/industrial-data-lakehouse.git
cd industrial-data-lakehouse
```

### Install Dependencies

```bash
uv sync --locked --dev
```

### Verify the Environment

```bash
uv run python --version
uv run python -c "import pyspark; print(pyspark.__version__)"
uv run python -c "import delta; print('Delta Lake available')"
uv run python -c "import prefect; print(prefect.__version__)"
```

## Pipeline Execution

The pipeline follows this processing sequence:

**Raw → Bronze → Silver → Gold**

There are two execution modes:

- Complete workflow execution using Prefect.
- Independent execution of individual pipeline stages.

### Complete Pipeline with Prefect

The complete pipeline can be executed through Prefect 3.

Create a local `.env` file containing:

```dotenv
PREFECT_SERVER_ANALYTICS_ENABLED=false
```

The `.env` file is excluded from Git.

Run the complete workflow from the repository root:

```bash
uv run --env-file .env python -c \
  "from pipeline.industrial_flow import industrial_lakehouse_flow; industrial_lakehouse_flow()"
```

The workflow executes:

1. Bronze ingestion.
2. Silver validation and quarantine.
3. Gold analytical transformation and versioned publication.

Each stage runs in a separate Python subprocess.

If a stage fails, downstream stages are not executed.

The Runner enforces a default subprocess timeout of 600 seconds.

Automatic Prefect task retries are currently disabled.

The workflow supports configurable source, Bronze, Silver, Quarantine, and Gold paths.

**Important:** Running the workflow with default paths can modify existing local Delta datasets.

For isolated validation runs, use the dedicated paths documented in [Prefect Orchestration](docs/orchestration.md).

### 1. Bronze Ingestion

Bronze ingestion is implemented in:

`src/ingestion/bronze_sensors.py`

Example:

```bash
uv run python -m src.ingestion.bronze_sensors \
  --source data/raw/sensor_readings/sensors_batch_001.csv \
  --target data/bronze/sensor_readings
```

See [Bronze Ingestion](docs/bronze_ingestion.md) for data contracts, metadata, batch processing, and idempotency.

### 2. Silver Transformation

Silver execution is implemented in:

`src/pipelines/run_silver.py`

Example:

```bash
uv run python -m src.pipelines.run_silver
```

The pipeline supports custom Bronze, Silver, and Quarantine paths.

See [Silver Pipeline](docs/silver_pipeline.md) for execution parameters, data quality rules, and failure recovery.

### 3. Gold Analytics

Gold execution is implemented in:

`src/pipelines/run_gold_pipeline.py`

Run the complete Gold pipeline:

```bash
uv run python -m src.pipelines.run_gold_pipeline
```

Custom paths:

```bash
uv run python -m src.pipelines.run_gold_pipeline \
  --silver-path data/silver/sensor_readings \
  --gold-path data/gold
```

Default Silver input:

`data/silver/sensor_readings`

Default Gold publication root:

`data/gold`

The pipeline builds five analytical tables, validates their consistency, and publishes a new version.

The published tables are stored under:

`data/gold/versions/<run_id>/`

The active version is recorded in:

`data/gold/current.json`

Consumers should resolve the active version through the manifest rather than accessing legacy Gold table paths.

## Gold Versioned Publication

The Gold pipeline uses isolated version directories to avoid exposing incomplete publication runs.

Publication steps:

1. Build and validate analytical DataFrames.
2. Write five Delta tables to a new version directory.
3. Validate persisted row counts and schema compatibility.
4. Update the active-version manifest.
5. Preserve the previously published version.

If a handled exception occurs before publication, the previous version remains active and the incomplete version is removed on a best-effort basis.

This mechanism uses local filesystem semantics and does not provide distributed multi-table transactions or concurrent-writer coordination.

Prefect orchestrates the Gold execution but does not replace or extend these transactional guarantees.

## Data Quality

Data quality checks are applied across the Medallion Architecture.

Implemented checks include:

- Required field validation
- Schema validation
- Duplicate event detection
- Measurement value validation
- Timestamp validation
- Measurement type and unit consistency
- Measurement range validation
- Business-key uniqueness
- Fact-to-dimension referential integrity
- Temporal aggregation reconciliation

Invalid Silver records are separated into Quarantine.

## Industrial Analytics

The implemented Gold layer supports sensor-based industrial monitoring.

Available analytical metrics include:

- Sensor reading frequency
- Average sensor measurement by minute
- Average sensor measurement by hour
- Minimum and maximum measurements
- Measurement variability

Additional business KPIs are planned for future development:

- Production efficiency
- Energy intensity
- Equipment availability
- Machine downtime
- Production throughput

These business KPIs are not yet implemented.

## Orchestration and Observability

The project uses Prefect 3 for local workflow orchestration.

The workflow exposes task and flow execution states.

The subprocess Runner captures:

- Stage name
- Executed Python command
- Execution duration
- Exit code
- Standard output
- Standard error
- Failure exceptions

Successful stages return a `StageResult`.

Subprocess failures raise `StageExecutionError`.

For nonzero exit codes, execution metadata is available through the exception's `result` attribute.

Timeout failures raise `StageTimeoutError`.

The current implementation does not include:

- Scheduled Prefect deployments
- Automatic task retries
- Persistent operational audit tables
- Centralized production monitoring
- Alerting infrastructure

The subprocess timeout does not guarantee termination of every descendant Spark or Java process.

See [Prefect Orchestration](docs/orchestration.md) for operational details.

## Automated Testing

The project uses pytest for unit and integration testing.

Run the complete test suite:

```bash
uv run --locked pytest -v
```

**Local validation: 89 tests passed (November 20, 2026). The test step also passed in GitHub Actions on November 21, 2026.**

| Component | Tests |
|---|---:|
| Bronze and Delta Lake | 7 |
| Silver | 18 |
| Gold transformations | 45 |
| Gold pipeline and publication | 7 |
| Prefect orchestration | 5 |
| Subprocess Runner | 7 |
| **Total** | **89** |

The tests cover:

- Bronze ingestion and Delta Lake persistence
- Silver data quality and quarantine
- Batch idempotency and failure recovery
- Gold dimensions and fact transformations
- Minute-level and hourly aggregations
- Referential integrity and reconciliation
- Gold versioned publication
- Prefect workflow execution order
- Failure propagation between stages
- Configurable pipeline paths
- Subprocess execution and timeouts
- Structured execution errors

Gold integration tests build deterministic Silver data from versioned CSV fixtures and use temporary Delta paths. They do not require a pre-existing local Silver dataset.

A real end-to-end Prefect execution was also completed successfully using isolated validation paths.

## Continuous Integration and Code Quality

GitHub Actions runs the workflow in `.github/workflows/ci.yml` on pushes to `main` and pull requests targeting `main`.

The CI job uses an Ubuntu runner, Java 17, Python 3.11, and locked Python dependencies. It performs:

1. Repository checkout and runtime setup.
2. Dependency installation with `uv sync --locked --dev`.
3. Linting with Ruff.
4. Formatting verification with Ruff.
5. Automated pytest execution, including PySpark and Delta Lake tests.

To reproduce the CI quality gates locally, run from the repository root:

```bash
uv sync --locked --dev
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked pytest -v
```

The workflow completed successfully on November 21, 2026, for commit `d5a7538`: [successful CI run](https://github.com/LauraMUD/industrial-data-lakehouse/actions/runs/44897204138).

**Scope:** continuous integration is implemented; automated production deployment is not. Current maintenance warnings concern the deprecation of Node.js 20-based Actions and `actions/setup-java@v4`.

See [CI/CD and Reproducibility](docs/ci_cd.md) for details and troubleshooting.

## Reproducibility

The project uses `uv` to manage Python dependencies.

- `pyproject.toml` defines project dependencies.
- `uv.lock` records resolved dependency versions.

To recreate the Python environment:

```bash
uv sync --locked --dev
```

The generated Delta datasets are excluded from version control.

Prefect execution can be configured locally through an ignored `.env` file. The CI test suite does not require this local file.

## Development Roadmap

- [x] Configure Python and uv
- [x] Configure Apache Spark and Delta Lake
- [x] Implement Bronze sensor ingestion
- [x] Implement Silver transformations
- [x] Implement data quality checks and quarantine
- [x] Implement Gold dimensions and fact table
- [x] Implement minute and hourly aggregations
- [x] Implement versioned Gold publication
- [x] Add automated tests
- [x] Document Bronze, Silver, and Gold
- [x] Implement Prefect workflow orchestration
- [x] Add subprocess execution, timeouts, and failure logging
- [x] Document orchestration and operational limitations
- [ ] Implement additional industrial business KPIs
- [ ] Add persistent monitoring, alerting, and scheduled deployments
- [x] Implement GitHub Actions continuous integration
- [ ] Deploy to cloud infrastructure

## Future Improvements

Potential extensions include:

- Scheduled Prefect deployments and production monitoring
- Persistent operational audit tables and alerting
- Configurable retry and recovery policies
- Streaming industrial IoT ingestion
- Incremental Gold processing
- Automated data quality monitoring
- Cloud object storage integration
- Version retention and cleanup
- Concurrent-publication control
- Additional integration-test scenarios and fixtures
- Improved Spark subprocess lifecycle management
- Business intelligence dashboards

## Project Goals

This project demonstrates practical data engineering skills through an industrial lakehouse implementation.

The main engineering principles are:

- Modular architecture
- Data reliability
- Reproducibility
- Automated testing
- Continuous integration
- Data lineage
- Industrial analytics
- Workflow orchestration
- Execution observability
- Maintainable software design

## License

License to be defined.

# Industrial Data Lakehouse

A reproducible industrial data engineering project built with Apache Spark, Delta Lake, Python, and uv.

## Project Overview

Industrial Data Lakehouse is an end-to-end data engineering project designed to ingest, validate, transform, and analyze industrial sensor data.

The project implements the **Medallion Architecture (Bronze, Silver, Gold)** with a focus on data reliability, reproducibility, automated testing, and industrial analytics.

The current implementation includes:

- Industrial sensor ingestion into Delta Lake
- Data quality validation and quarantine
- Idempotent Silver batch processing
- Dimensional modeling in Gold
- Minute-level and hourly sensor aggregations
- Referential integrity and reconciliation checks
- Versioned Gold publication
- Automated integration and unit tests

## Architecture

```text
Industrial Sensor CSV Files
            |
            v
     BRONZE LAYER
  Raw ingestion + metadata
        Delta Lake
            |
            v
     SILVER LAYER
   Data quality validation
     /              \
    v                v
Validated Silver   Quarantine
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

### Bronze Layer

The Bronze layer ingests industrial sensor readings from CSV files.

Responsibilities:

- Validate source file structure
- Apply ingestion schemas
- Preserve original sensor measurements
- Add ingestion metadata
- Persist data using Delta Lake
- Support reproducible batch ingestion

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

Documentation: [Gold Pipeline](docs/gold_pipeline.md)

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11 | Pipeline development |
| Apache Spark / PySpark | Distributed data processing |
| Delta Lake | Transactional data storage |
| uv | Dependency management |
| pytest | Automated testing |
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
│   └── gold_pipeline.md
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
├── pyproject.toml
├── uv.lock
└── README.md
```

The Gold version directories and active-version manifest are generated locally and excluded from Git.

## Installation

### Requirements

- Linux or WSL
- Compatible Java runtime
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
uv sync --frozen
```

### Verify the Environment

```bash
uv run python --version
uv run python -c "import pyspark; print(pyspark.__version__)"
uv run python -c "import delta; print('Delta Lake available')"
```

## Pipeline Execution

The pipeline follows this processing sequence:

**Raw → Bronze → Silver → Gold**

### 1. Bronze Ingestion

Bronze ingestion is implemented in:

`src/ingestion/bronze_sensors.py`

See [Bronze Ingestion](docs/bronze_ingestion.md) for the supported ingestion workflow and batch parameters.

### 2. Silver Transformation

Silver orchestration is implemented in:

`src/pipelines/run_silver.py`

See [Silver Pipeline](docs/silver_pipeline.md) for execution instructions, data quality rules, and quarantine behavior.

### 3. Gold Analytics

Run the complete Gold pipeline:

```bash
uv run python -m src.pipelines.run_gold_pipeline
```

Default Silver input:

`data/silver/sensor_readings`

Default Gold output:

`data/gold/versions/<run_id>/`

The pipeline builds five analytical tables, validates their consistency, and publishes a new version.

The active version is recorded in:

`data/gold/current.json`

Consumers should use the active-version reference rather than accessing legacy Gold table paths.

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

Invalid Silver records are separated into quarantine.

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

## Automated Testing

The project uses pytest for unit and integration testing.

Run the complete test suite:

```bash
uv run pytest -v
```

**Latest validated result: 77 tests passed (November 20, 2026).**

| Component | Tests |
|---|---:|
| Bronze and Delta | 7 |
| Silver | 18 |
| Gold transformations | 45 |
| Gold pipeline and publication | 7 |
| **Total** | **77** |

Tests cover data quality, transformations, persistence, idempotency, reconciliation, versioned publication, and simulated failure scenarios.

Some Gold integration tests currently require a locally prepared Silver Delta dataset.

## Reproducibility

The project uses `uv` to manage Python dependencies.

- `pyproject.toml` defines project dependencies.
- `uv.lock` records resolved dependency versions.

To recreate the Python environment:

```bash
uv sync --frozen
```

The generated Delta datasets are excluded from version control.

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
- [ ] Implement additional industrial business KPIs
- [ ] Add orchestration and monitoring
- [ ] Implement CI/CD
- [ ] Deploy to cloud infrastructure

## Future Improvements

Potential extensions include:

- Apache Airflow or Dagster orchestration
- Streaming industrial IoT ingestion
- Incremental Gold processing
- Automated data quality monitoring
- Cloud object storage integration
- Version retention and cleanup
- Concurrent-publication control
- Independent integration-test fixtures
- Business intelligence dashboards

## Project Goals

This project demonstrates practical data engineering skills through an industrial lakehouse implementation.

The main engineering principles are:

- Modular architecture
- Data reliability
- Reproducibility
- Automated testing
- Data lineage
- Industrial analytics
- Maintainable software design

## License

License to be defined.
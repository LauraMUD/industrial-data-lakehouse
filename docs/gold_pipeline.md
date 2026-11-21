# Gold Layer — Industrial Analytics Pipeline

## 1. Overview

The Gold layer transforms validated Silver sensor readings into analytical datasets designed for industrial monitoring and reporting.

The implementation uses PySpark and Delta Lake, following the Medallion Architecture:

**Bronze → Silver → Gold**

The Gold pipeline produces five analytical tables:

- `dim_equipment`
- `dim_sensor`
- `fact_sensor_readings`
- `agg_sensor_minute`
- `agg_sensor_hourly`

All temporal aggregations use UTC.

## 2. Architecture

The implementation separates analytical transformations from pipeline orchestration.

**Transformations**

Located in `src/transformations/`:

- `gold_equipment.py` — Equipment dimension
- `gold_sensor.py` — Sensor dimension
- `gold_readings.py` — Sensor readings fact table
- `gold_temporal.py` — Shared temporal aggregation logic
- `gold_minute.py` — Minute-level aggregation
- `gold_hourly.py` — Hourly aggregation

**Pipelines**

Located in `src/pipelines/`:

- `run_gold_pipeline.py` — Gold orchestration and execution
- `gold_publication.py` — Versioned publication and active-version resolution


**Workflow orchestration**

Located in `pipeline/`:

- `industrial_flow.py` — Prefect workflow coordinating Bronze,
  Silver, and Gold.
- `runner.py` — Subprocess execution, timeout enforcement,
  logging, and execution metadata.

Prefect controls when the Gold pipeline executes and
propagates failures.

The Gold publication module remains responsible for
writing versioned Delta tables and updating the
active-version manifest.

These responsibilities are intentionally separated.

The Gold pipeline reads validated Silver data, constructs
analytical tables, verifies referential integrity,
reconciles aggregation counts, and publishes a complete
Gold version.

## 3. Analytical Data Model

### 3.1 Equipment Dimension

**Table:** `dim_equipment`

Represents the industrial equipment associated with sensor readings.

Composite business key:

`(plant_id, equipment_id)`

The transformation removes duplicate equipment records and rejects invalid identifiers.

### 3.2 Sensor Dimension

**Table:** `dim_sensor`

Represents industrial sensors and their measurement characteristics.

Composite business key:

`(plant_id, equipment_id, sensor_id)`

Relevant attributes include:

- `measurement_type`
- `unit`

The transformation checks identifier validity and consistency of measurement characteristics.

### 3.3 Sensor Readings Fact

**Table:** `fact_sensor_readings`

Contains validated sensor events at individual-reading granularity.

Each record represents one sensor measurement.

Unique event identifier:

`event_id`

The fact table preserves event timestamps, measurement values, industrial identifiers, and available source lineage information.

### 3.4 Minute Aggregation

**Table:** `agg_sensor_minute`

Groups sensor readings into one-minute UTC intervals.

Grouping dimensions:

- Plant
- Equipment
- Sensor
- Measurement type
- Unit
- UTC minute

Calculated metrics:

- `reading_count`
- `avg_value`
- `min_value`
- `max_value`
- `stddev_value`

### 3.5 Hourly Aggregation

**Table:** `agg_sensor_hourly`

Groups sensor readings into one-hour UTC intervals.

It uses the same industrial grouping dimensions and statistical metrics as the minute aggregation.

The minute and hourly transformations share common aggregation logic to avoid duplicated implementations.

## 4. Data Quality and Consistency

The Gold pipeline validates:

- Required identifiers and attributes
- Business-key uniqueness
- Duplicate sensor events
- Numeric measurement validity
- Measurement type and unit consistency
- Referential integrity between facts and dimensions
- Reconciliation between fact records and temporal aggregation counts

Before publishing a Gold version, the publication module also checks the persisted table row counts and column names and types against the corresponding source DataFrames.

Schema compatibility validation does not require identical nullability metadata.

## 5. Delta Lake Persistence

The Gold layer uses Delta Lake for analytical table persistence.

Each Delta table provides its own transactional write guarantees.

The five tables are not written as one distributed transaction. Instead, the project implements versioned publication to avoid exposing partially completed Gold runs to consumers that follow the active-version manifest.

## 6. Versioned Gold Publication

Each successful execution creates a unique version under:

`data/gold/versions/<run_id>/`

The active version is identified by:

`data/gold/current.json`

Publication follows this sequence:

1. Build and validate the analytical DataFrames.
2. Write all five Delta tables into an isolated version directory.
3. Read back the persisted tables.
4. Validate row counts and schema compatibility.
5. Atomically replace the local active-version manifest.

A failed publication does not intentionally change the active manifest.

When an exception is caught, the pipeline attempts to remove the incomplete version directory.

Consumers must resolve the active version through the manifest rather than reading legacy Gold table paths directly.

For a multi-table analytical operation, consumers should resolve the version once and use that same version for all tables.

## 7. Execution

Run the Gold pipeline from the repository root:

```bash
uv run python -m src.pipelines.run_gold_pipeline
```


The CLI also supports configurable input and output paths:

```bash
uv run python -m src.pipelines.run_gold_pipeline \
  --silver-path data/silver/sensor_readings \
  --gold-path data/gold
```

`--silver-path` identifies the validated Silver Delta dataset.

`--gold-path` identifies the Gold publication root,
where `versions/` and `current.json` are maintained.

When these arguments are omitted, the pipeline uses
the default paths documented below.


Default input:

`data/silver/sensor_readings`

Default output:

`data/gold/versions/`

The pipeline prints the published version identifier and record counts for the five analytical tables.


### Prefect Integration

Gold runs as the third and final task of the
`industrial-lakehouse` Prefect workflow.

The Gold task executes only after successful
Bronze and Silver tasks.

Prefect invokes the Gold CLI through the subprocess
Runner, passing the configured Silver and Gold paths.

If Gold fails, Prefect reports the task and workflow
failure.

A failed Gold execution does not automatically
trigger a retry because the task uses `retries=0`.

For handled publication failures, the existing Gold
publication mechanism preserves the previously
active version and attempts to clean up incomplete
version directories.

Prefect does not provide additional transactional
guarantees for the five Gold Delta tables.

See [Orchestration](orchestration.md) for execution
and operational details.


## 8. Automated Testing

Run the complete project test suite:

```bash
uv run pytest -v
```

Complete regression validation performed on
November 20, 2026:

89 tests passed.

This includes the original Bronze, Silver, Gold,
and Delta Lake tests, together with the new Prefect
workflow and subprocess Runner tests.

Coverage includes:

- Bronze ingestion and Delta persistence
- Silver data quality and recovery
- Gold dimensions and fact transformations
- Minute and hourly aggregations
- Gold persistence and logical idempotency
- Versioned publication
- Preservation of the previous active version
- Cleanup after handled publication failures
- Rejection of simulated validation failures

Gold integration tests generate deterministic Silver data from versioned
CSV fixtures through the Bronze and Silver transformations. Temporary
Delta paths isolate each test from existing local datasets.

## 9. Current Validation Dataset

The current local validation dataset produces:

| Gold table | Records |
|---|---:|
| `dim_equipment` | 4 |
| `dim_sensor` | 6 |
| `fact_sensor_readings` | 9 |
| `agg_sensor_minute` | 9 |
| `agg_sensor_hourly` | 9 |

These counts describe the test dataset and are not production expectations.

## 10. Known Limitations

The current implementation is intended as a local engineering portfolio project, not a production-ready distributed publication service.

Known limitations include:

- The active-version manifest uses local filesystem replacement semantics.
- Concurrent publishers are not coordinated.
- Abrupt process termination may leave orphaned version directories.
- Each successful execution creates a new version, even when Silver data is unchanged.
- Version retention and garbage collection are not automated.
- Publication validation checks counts and schema compatibility but does not compare every persisted value.
- Readers must consistently use a pinned version identifier to avoid mixing versions.
- Prefect tasks currently use `retries=0`; automatic
  recovery policies are not implemented.
- The subprocess Runner enforces a timeout on the
  direct subprocess but does not guarantee termination
  of every descendant Spark process.
- Persistent operational audit tables and centralized
  production monitoring are not implemented.

## 11. Future Improvements

Potential extensions include:

- Additional integration-test scenarios and fixtures
- Version retention and cleanup policies
- Concurrent-publication control
- Data quality metrics and observability
- Scheduled Prefect deployments and production monitoring
- Incremental Gold processing
- Deployment to managed cloud storage
- Automated deployment to a defined production environment

Continuous integration is implemented with GitHub Actions and runs
Ruff checks and automated tests. See [CI/CD and Reproducibility](ci_cd.md).

## 12. Summary

The Gold layer completes the analytical stage of the industrial lakehouse.

It provides dimensional modeling, validated sensor facts, minute-level and hourly statistics, Delta Lake persistence, automated tests, and controlled version publication.

Together, Bronze, Silver, and Gold form an end-to-end industrial data engineering pipeline.

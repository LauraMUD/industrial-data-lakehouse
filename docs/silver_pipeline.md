# Industrial Sensor Lakehouse — Silver Pipeline

## 1. Overview

This pipeline transforms industrial sensor readings from Bronze
into two Delta Lake datasets:

- **Silver:** validated and typed sensor measurements.
- **Quarantine:** rejected records with their original values
  and associated data quality errors.

The pipeline is implemented using PySpark and Delta Lake 4.0.

## 2. Architecture

```text
CSV Sensor Batches
        |
        v
   Bronze Delta
        |
        v
 Data Quality Rules
        |
        +------------------+
        |                  |
        v                  v
   Silver Delta      Quarantine Delta
   Valid records     Rejected records
```

## 3. Data Quality

The Silver transformation validates:

1. Required fields.
2. Event timestamp format.
3. Numeric measurement values.
4. Measurement types.
5. Measurement units.
6. Duplicate event identifiers.
7. Measurement ranges.

Supported measurement ranges:

| Measurement | Unit | Minimum | Maximum |
|---|---|---:|---:|
| Temperature | C | -50 | 200 |
| Vibration | mm/s | 0 | 100 |
| Pressure | bar | 0 | 500 |

Invalid records are redirected to Quarantine.

Quarantine preserves the original measurement value and
includes `dq_errors` and `quarantine_timestamp`.

## 4. Delta Lake Storage

```text
data/
├── bronze/
│   └── sensor_readings/
├── silver/
│   └── sensor_readings/
└── quarantine/
    └── sensor_readings/
```

Generated Delta tables are excluded from Git.

## 5. Pipeline Execution

Run all available Bronze batches:

```bash
uv run python -m src.pipelines.run_silver
```

Run one specific batch:

```bash
uv run python -m src.pipelines.run_silver \
  --batch-id sensors_batch_001
```

The pipeline also supports custom Bronze, Silver and
Quarantine paths.


### Prefect Orchestration

Silver can run independently through its Python CLI
or as the second task of the Prefect workflow.

The workflow is defined in:

`pipeline/industrial_flow.py`

The Silver task runs after successful Bronze ingestion
and before Gold analytics.

Prefect invokes the Silver CLI through `pipeline/runner.py`,
which executes the stage in a separate Python subprocess.

The workflow passes configurable paths using:

- `--bronze-path`
- `--silver-path`
- `--quarantine-path`

If Silver fails, the workflow stops and Gold is not executed.

The current Prefect tasks use `retries=0`.
Recovery requires investigating the failure and explicitly
rerunning the workflow or the affected stage.

See [Orchestration](orchestration.md) for workflow execution,
timeouts, logging, and failure handling.


## 6. Idempotency

Each batch is written using Delta Lake.

Existing batches are replaced using `replaceWhere`.

Reprocessing the same batch does not append duplicate records.

This behavior has been verified with automated tests and
reprocessing of the industrial sensor dataset.

## 7. Failure Recovery

Silver and Quarantine are separate Delta tables.

Each individual Delta write is atomic, but the two writes
do not form a single atomic transaction.

If Quarantine fails after Silver succeeds, the pipeline
raises an error.

Reprocessing the batch can restore consistent outputs.

A simulated partial failure and subsequent successful retry
have been verified through automated testing.

### Known limitation

Consumers may observe partially updated data between
the two writes.

A production implementation should introduce a batch
completion or publication mechanism to prevent consumers
from reading incomplete results.


### Recovery under Prefect

Prefect detects a failed Silver subprocess and marks
the corresponding task as failed.

The Gold task is not executed after a Silver failure.

Prefect does not provide a multi-table transaction across
Silver and Quarantine.

Because automatic task retries are disabled, operators
must investigate the error and initiate recovery.

The existing batch-level idempotency allows a batch to
be reprocessed without accumulating duplicate records.

Recovery of a partially completed Silver/Quarantine
update has been verified through automated tests.


## 8. Validation Results

Two Bronze batches were processed:

| Batch | Bronze | Silver | Quarantine |
|---|---:|---:|---:|
| sensors_batch_001 | 6 | 5 | 1 |
| sensors_batch_002 | 4 | 4 | 0 |
| Total | 10 | 9 | 1 |

The rejected record was:

- Event ID: `EVT005`
- Measurement type: `vibration`
- Original value: `ERROR`
- Data quality error: `DQ003_INVALID_NUMERIC`

Reprocessing produced the same counts without duplicate
event identifiers in Silver.

## 9. Automated Tests

Run the complete test suite:

```bash
uv run pytest -v
```

The suite includes:

- Bronze ingestion tests.
- Delta Lake read/write tests.
- Silver data quality tests.
- Batch idempotency tests.
- Pipeline integration tests.
- Partial failure recovery tests.

The initial Bronze and Silver implementation passed 25 tests.

The complete project regression suite passed 89 tests
on November 20, 2026, including Prefect orchestration
and subprocess Runner tests.


## 10. Next Steps

The Gold analytical layer and Prefect orchestration
are implemented.

Remaining improvements include:

- Introduce batch completion and publication controls
  across Silver and Quarantine.
- Add persistent operational metrics and structured
  execution audit records.
- Implement configurable recovery and retry policies.
- Extend the pipeline with additional industrial datasets.
- Introduce cloud storage and production deployment.

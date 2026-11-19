# Bronze Layer — Industrial Sensor Ingestion

## Overview

The Bronze layer ingests industrial sensor readings from CSV files
and stores them as Delta Lake tables.

The implementation uses PySpark 4.0.1 and Delta Lake 4.0.0.

## Architecture

Source CSV
    |
    v
Data Contract Validation
    |
    v
PySpark Ingestion
    |
    v
Technical Metadata Enrichment
    |
    v
Delta Lake Bronze

## Source Dataset

Dataset: sensor_readings

Source system: industrial_scada_simulator

Source format: CSV

Data contract:
config/contracts/sensor_readings.json

## Source Fields

- event_id
- plant_id
- equipment_id
- sensor_id
- event_timestamp
- measurement_type
- measurement_value
- unit

All source fields are initially stored as strings to preserve
the original representation.

## Technical Metadata

- batch_id: identifier derived from the source filename
- source_file: original filename
- ingestion_timestamp: timestamp of ingestion

## Storage

Raw landing zone:
data/raw/sensor_readings/

Bronze Delta table:
data/bronze/sensor_readings/

Generated data is excluded from Git.

Synthetic test fixtures are versioned under:
tests/fixtures/sensor_readings/

## Batch Processing

The pipeline supports:

1. Initial ingestion into Delta Lake.
2. Reprocessing of an existing batch using replaceWhere.
3. Incremental ingestion of new batches.
4. Explicit source schema.
5. CSV header validation.
6. Batch identifier validation.

## Execution

Default batch:

    uv run python -m src.ingestion.bronze_sensors

Specific batch:

    uv run python -m src.ingestion.bronze_sensors \
      --source data/raw/sensor_readings/sensors_batch_002.csv

## Testing

Run all tests:

    uv run pytest -v

The current test suite covers:

- Initial Bronze ingestion
- Reprocessing an existing batch
- Multiple batches
- Invalid CSV headers
- Missing source files
- Invalid batch identifiers
- Delta Lake read/write integration

## Verified Results

Batch 001: 6 records
Batch 002: 4 records

Total: 10 records

The original non-numeric value ERROR is preserved in Bronze.

## Current Limitations

- Batch identity depends on the source filename.
- Source checksums are not yet implemented.
- Reprocessing updates ingestion metadata.
- CSV parsing and malformed-record handling need further hardening.
- No quarantine mechanism is implemented.
- No centralized ingestion audit table exists.
- Concurrent writers have not been tested.
- Cloud storage integration is not yet implemented.

## Future Improvements

- Content-based batch identification
- Schema evolution policies
- Quarantine and dead-letter processing
- Ingestion audit logging
- Data quality monitoring
- Cloud object storage integration
- CI/CD pipeline

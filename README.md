# Industrial Data Lakehouse

A reproducible industrial data engineering project built with Apache Spark, Delta Lake, Python, and uv.

## Project Overview

Industrial Data Lakehouse is an end-to-end data engineering project designed to process, transform, validate, and analyze industrial data.

The project implements a Medallion Architecture (Bronze, Silver, Gold) to organize data pipelines and support industrial analytics.

The main focus areas are:

- Industrial production monitoring
- Energy consumption analysis
- Equipment performance
- Data quality validation
- Industrial KPI calculation
- Reproducible data pipelines

## Architecture

The project follows the Medallion Architecture:

    Industrial Data Sources
              |
              v
    RAW - Original Source Files
              |
              v
    BRONZE - Data Ingestion
    PySpark + Delta Lake
              |
              v
    SILVER - Data Transformation
    Cleaning + Validation
              |
              v
    GOLD - Business Analytics
    Industrial KPIs

### RAW Layer

Stores original industrial datasets without modifications.

Example datasets:

- Production records
- Energy consumption measurements
- Equipment sensor readings
- Machine operating events

### BRONZE Layer

Ingests raw industrial data using Apache Spark.

Responsibilities:

- Reading source files
- Applying ingestion schemas
- Adding ingestion metadata
- Writing Delta Lake tables
- Preserving source information

### SILVER Layer

Transforms and validates ingested data.

Responsibilities:

- Handling missing values
- Removing duplicates
- Standardizing timestamps
- Validating data types
- Applying business rules
- Detecting invalid measurements

### GOLD Layer

Produces curated datasets for industrial analytics.

Example KPIs:

- Total production
- Production efficiency
- Energy consumption
- Energy intensity
- Equipment availability
- Machine downtime
- Production by equipment

## Technology Stack

| Technology | Purpose |
|------------|---------|
| Python | Pipeline development |
| Apache Spark | Distributed data processing |
| PySpark | Spark Python API |
| Delta Lake | Reliable lakehouse storage |
| uv | Python dependency management |
| pytest | Automated testing |
| Git | Version control |
| Ubuntu | Development environment |

## Project Structure

    industrial-data-lakehouse/
    |
    |-- README.md
    |-- pyproject.toml
    |-- uv.lock
    |-- .python-version
    |-- .gitignore
    |
    |-- data/
    |   |-- raw/
    |   |-- bronze/
    |   |-- silver/
    |   |-- gold/
    |
    |-- src/
    |   |-- ingestion/
    |   |-- transformation/
    |   |-- analytics/
    |   |-- quality/
    |
    |-- tests/
    |
    |-- notebooks/
    |
    |-- scripts/
    |
    |-- docs/

Note: This structure represents the intended project organization.
Some directories and modules may be added progressively.

## Installation

### Requirements

- Ubuntu or another Linux distribution
- Python compatible with the project configuration
- Java compatible with the installed Spark version
- uv
- Git

### Clone the Repository

    git clone <repository-url>
    cd industrial-data-lakehouse

Replace <repository-url> with the actual Git repository URL.

### Install Dependencies

Install the project dependencies using uv:

    uv sync --frozen

This command installs the exact dependency versions recorded in uv.lock.

If the lockfile has not been generated yet, run:

    uv sync

### Verify the Environment

Check the Python version:

    uv run python --version

Check the installed PySpark version:

    uv run python -c "import pyspark; print(pyspark.__version__)"

Check the installed Delta Lake version:

    uv run python -c "import delta; print('Delta Lake available')"

## Data Pipeline

The intended pipeline consists of three main stages.

### Step 1 - Bronze Ingestion

Read industrial source files and write Delta Lake tables.

Input:

    data/raw/

Output:

    data/bronze/

### Step 2 - Silver Transformation

Clean and validate the Bronze datasets.

Input:

    data/bronze/

Output:

    data/silver/

### Step 3 - Gold Analytics

Generate industrial metrics and analytical datasets.

Input:

    data/silver/

Output:

    data/gold/

Pipeline entry points will be documented as they are implemented.

## Data Quality

Data quality is a fundamental part of the project.

Planned validations include:

- Null value detection
- Duplicate record detection
- Schema validation
- Timestamp consistency
- Valid measurement ranges
- Referential integrity
- Industrial business rule validation

Invalid records may be isolated for further analysis.

## Industrial KPIs

The Gold layer is designed to calculate industrial performance indicators.

### Production Efficiency

    Production Efficiency (%) =
        (Actual Production / Target Production) * 100

### Energy Intensity

    Energy Intensity =
        Total Energy Consumption / Total Production

### Equipment Availability

    Equipment Availability (%) =
        (Operating Time / Planned Production Time) * 100

### Downtime

    Total Downtime =
        Sum of Equipment Downtime Intervals

These metrics can support operational monitoring and industrial optimization.

## Testing

Automated testing will be implemented using pytest.

Run tests with:

    uv run pytest

Tests will cover:

- Data transformation logic
- Data quality rules
- Schema validation
- KPI calculations
- Pipeline behavior

## Reproducibility

The project uses uv for dependency management.

The pyproject.toml file defines project dependencies.

The uv.lock file records resolved dependency versions.

To reproduce the development environment:

    uv sync --frozen

This ensures consistent Python dependencies across compatible environments.

## Development Roadmap

- [ ] Configure the Python environment
- [ ] Configure Apache Spark and Delta Lake
- [ ] Create synthetic industrial datasets
- [ ] Implement Bronze ingestion pipelines
- [ ] Implement Silver transformations
- [ ] Implement data quality checks
- [ ] Implement Gold analytical tables
- [ ] Calculate industrial KPIs
- [ ] Add automated tests
- [ ] Document pipeline execution
- [ ] Add orchestration and monitoring

## Future Improvements

Potential extensions include:

- Apache Airflow orchestration
- Streaming ingestion
- Industrial IoT data processing
- Data quality monitoring
- Automated CI/CD pipelines
- Cloud object storage integration
- Business intelligence dashboards

## Project Goals

The main goal of this project is to demonstrate practical data engineering skills through a reproducible industrial lakehouse implementation.

The project emphasizes:

- Clean architecture
- Modular pipeline development
- Data reliability
- Reproducibility
- Industrial analytics
- Software engineering best practices

## License

License to be defined.

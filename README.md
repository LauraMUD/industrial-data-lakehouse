# Industrial Data Lakehouse

An end-to-end Data Engineering portfolio project using Apache Spark, Delta Lake, and Python.

## Project Overview

This project demonstrates how to transform industrial production, equipment, and energy data into analytics-ready datasets.

The solution follows a Medallion Architecture with Bronze, Silver, and Gold layers.

## Architecture

Raw Industrial Data
    |
    v
Bronze - Raw Delta Tables
    |
    v
Silver - Cleaned and Validated Data
    |
    v
Gold - Analytics-Ready Data Models

## Technology Stack

- Python 3.11
- Apache Spark 4.0.1
- Delta Lake 4.0.0
- pytest
- Ubuntu WSL2
- GitHub Actions (planned)

## Project Structure

- config/ - Configuration
- data/raw/ - Source datasets
- data/bronze/ - Raw Delta tables
- data/silver/ - Cleaned datasets
- data/gold/ - Analytical datasets
- src/ingestion/ - Data ingestion pipelines
- src/transformations/ - Data transformations
- src/utils/ - Shared utilities
- tests/ - Automated tests
- docs/ - Technical documentation
- diagnostics/ - Environment validation scripts

## Development Roadmap

- [x] Configure Python, Spark, and Delta Lake
- [x] Validate Spark and Delta Lake locally
- [x] Create project structure
- [ ] Design industrial datasets and schemas
- [ ] Implement Bronze ingestion
- [ ] Implement Silver transformations
- [ ] Build Gold analytical models
- [ ] Implement incremental processing with Delta MERGE
- [ ] Handle schema evolution
- [ ] Add automated data quality tests
- [ ] Configure GitHub Actions CI
- [ ] Document architecture and results

## Local Setup

Use Python 3.11 and install the dependencies listed in requirements.txt.

For local Spark execution under WSL2, set SPARK_LOCAL_IP=127.0.0.1 if necessary.

## Project Status

Day 1: Development environment validated. Data pipelines are not yet implemented.

## Author

Laura - Data Engineer

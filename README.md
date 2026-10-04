# Cashew Data ETL

A local Python ETL project for processing monthly transaction data from **Cashew**, using **PySpark** and **Delta Lake**.

The project is designed to ingest raw Cashew exports, validate and transform the data using Spark, and store the resulting datasets as Delta tables for downstream analytics.

## Workflow

```text
Cashew
   │
   ▼
Monthly Data Export
   │
   ▼
data/incoming/
   │
   ▼
Python / PySpark ETL
   │
   ├── Extract
   ├── Validate
   ├── Transform
   └── Load
   │
   ▼
Delta Tables
   │
   ▼
Analytics / Reporting
```

## Project Structure

```text
cashew-project/

├── .env
├── pyproject.toml
├── uv.lock
├── README.md
│
├── data/
│   ├── incoming/       # New Cashew exports
│   ├── raw/            # Delta Tables
│   ├── staging/
│   ├── transform/
│   └── reporting/
│
├── notebooks/          # Jupyter notebooks for exploration
│
└── src/
    └── cashew/
        ├── config.py
        ├── spark.py
        ├── extract.py
        ├── transform.py
        ├── validation.py
        ├── load.py
        └── pipeline.py
```

## Tech Stack

- **Python** — Application and ETL code
- **PySpark** — Distributed data processing and SQL/DataFrame transformations
- **Delta Lake** — Transactional table format and data storage
- **uv** — Python environment and package management
- **Jupyter / ipykernel** — Data exploration and development
- **Java 21** — JVM required by Spark

## Environment

The project is currently developed and run in **WSL2**.

### Python

Python is managed through `uv`.

Check the project Python version with:

```bash
uv run python --version
```

### Spark

The project currently uses:

```text
PySpark 4.2.0
Spark 4.2.0
Scala 2.13
```

### Delta Lake

Delta Lake is integrated with Spark through the `delta-spark` Python package and its corresponding JVM dependencies.

The Spark session is configured with the Delta extension and catalog:

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("CashewETL")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

## Setup

Clone the repository and install the project dependencies:

```bash
uv sync
```

Run Python code through the project environment using:

```bash
uv run python ...
```

This keeps Python and the project's dependencies managed by `uv`.

## Data Workflow

New monthly Cashew exports are placed in:

```text
data/incoming/
```

The ETL pipeline will:

1. Extract the incoming data.
2. Validate the input.
3. Transform the data using PySpark.
4. Write the processed data to Delta tables.
5. Make the resulting Delta datasets available for downstream analytics.

## Delta Tables

Delta tables are stored locally as directories containing Parquet data and Delta transaction logs.

A typical Delta table looks like:

```text
data/delta/
└── transactions/
    ├── _delta_log/
    │   ├── 00000000000000000000.json
    │   └── ...
    │
    ├── part-....parquet
    └── ...
```

The **Parquet files** contain the actual data.

The **`_delta_log`** directory contains the transaction history and metadata that allow Delta Lake to provide table-level features on top of the underlying Parquet files.

## Development

Start a Python session inside the project environment with:

```bash
uv run python
```

Run a module with:

```bash
uv run python -m cashew.<module>
```

Jupyter can be started using the project's `uv` environment so that notebooks use the same Python environment and dependencies as the ETL application.

## Project Goal

The goal of this project is to build a reliable local ETL pipeline for recurring Cashew data exports.

The pipeline will progressively cover:

- Raw data ingestion
- Data validation
- Spark DataFrame transformations
- Delta Lake storage
- Incremental processing
- Data quality checks
- Analytics-ready datasets
- Downstream reporting and dashboards

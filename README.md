# ETL OHDSI DBT

ETL OHDSI DBT is a demonstration project that loads synthetic health records generated with [Synthea](https://synthetichealth.github.io/synthea/) into a PostgreSQL database and then transforms that data into the [OMOP](https://www.ohdsi.org/data-standardization/the-common-data-model/) schema using [dbt](https://www.getdbt.com/).

The repository shows how to combine Python-based ingestion with dbt models for downstream analytics. It contains two main components:

- **`load_data/`** – Python utilities for ingesting Synthea CSV exports and storing them in a raw schema. The loader automatically creates the target schema, cleans column names, converts date columns, and appends basic metadata.
- **`omop_cancer/`** – A dbt project that turns the raw Synthea tables into OMOP tables. This project can be extended with additional models, seeds, and tests.

## Getting Started

1. **Install requirements**
   ```bash
   pip install -r requirements.txt
   ```
2. **Configure database access**
   Set the following environment variables or create a `.env` file:
   - `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
   - `DB_SCHEMA` (defaults to `raw`)
   - `CSV_DIRECTORY_PATH` (path to your Synthea CSV folder)
3. **Load the raw data**
   ```bash
   python main.py
   ```
   This reads all CSV files from the configured directory and loads them into PostgreSQL.
4. **Run dbt transformations**
   ```bash
   cd omop_cancer
   dbt run
   dbt test  # optional
   ```
   The dbt project expects a profile named `omop_cancer` pointing at the same database.

## Directory Overview

```
load_data/      # Python ETL utilities
omop_cancer/    # dbt models, macros, and seeds
main.py         # entry point for loading Synthea CSV files
requirements.txt
```

## Tags

`dbt` `python` `postgres`


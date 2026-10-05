# Implementation notes

## Included notebooks

- `00_setup_finflow.py`: extracts the dataset archive from the configured Volume path and performs initial source inspection. The dataset ZIP itself is not included in this repository bundle.
- `01_bronze_ingestion.py`: initial Bronze ingestion.
- `02_silver_transformation.py`: Silver transformations and data-quality handling.
- `03_gold_transformation.py`: Gold fact/dimension and summary construction.
- `04_Gold_SQL_Analytics.sql`: Databricks SQL analytics.
- `05_Data_Quality_Audit.py`: audit table and reconciliation logic.
- `06_Incremental_Ingestion.py`: scheduled incremental file processing and Gold refresh.
- `07_Snowflake_Export.py`: exports nine Gold datasets as CSV files to a Volume.
- `08_Idempotency_Validation.py`: row-count, audit and uniqueness validation queries.

## Environment-specific configuration

The notebooks refer to the following Unity Catalog namespaces and Volume locations:

- `workspace.finflow_bronze`
- `workspace.finflow_silver`
- `workspace.finflow_gold`
- `/Volumes/workspace/default/finflow_raw/source_data`
- `/Volumes/workspace/default/finflow_raw/incoming_transactions`
- `/Volumes/workspace/default/finflow_raw/finflow_snowflake_export`

Change these paths and table names if deploying into a different workspace.

## Notebook export format

The `.py` files are Databricks notebook-source exports. They contain Databricks markers such as `# MAGIC` and `# COMMAND ----------`; retain these markers when re-importing as Databricks notebooks.

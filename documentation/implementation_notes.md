# FinFlow Implementation Notes

## Project Scope

FinFlow is an educational Data Engineering capstone designed to demonstrate practical pipeline engineering rather than every advanced production platform feature.

The active implementation uses:

- Python
- PySpark
- Apache Spark
- Spark SQL
- Databricks Free Edition
- Delta Lake
- Unity Catalog / Volumes
- Snowflake
- Databricks Workflows
- Git / GitHub

Live API ingestion and direct database ingestion are not part of the active pipeline.

## Notebook Responsibilities

| Notebook / SQL | Responsibility |
|---|---|
| `00_setup_finflow.py` | Dataset extraction and initial inspection |
| `01_bronze_ingestion.py` | Initial Bronze ingestion |
| `02_silver_transformation.py` | Cleaning, validation and quarantine |
| `03_gold_transformation.py` | Facts, dimensions and analytical summaries |
| `04_Gold_SQL_Analytics.sql` | Databricks SQL analytics |
| `05_Data_Quality_Audit.py` | Audit logging and reconciliation |
| `06_Incremental_Ingestion.py` | Incremental transaction processing |
| `07_Snowflake_Export.py` | Gold-to-Snowflake staged export |
| `08_Idempotency_Validation.py` | Idempotency and pipeline validation |

## Environment-Specific Paths

The notebooks use:

- `workspace.finflow_bronze`
- `workspace.finflow_silver`
- `workspace.finflow_gold`
- `/Volumes/workspace/default/finflow_raw/source_data`
- `/Volumes/workspace/default/finflow_raw/incoming_transactions`
- `/Volumes/workspace/default/finflow_raw/finflow_snowflake_export`

These are environment-specific and should be adapted before deploying elsewhere.

## Initial Ingestion

The initial Bronze notebook reads the prepared source files and writes Delta tables using an overwrite pattern. This notebook represents the initial/full load; incremental processing is handled separately.

## Silver Processing

Silver transformations standardize categorical fields, convert timestamps, remove duplicate business keys where applicable, validate references, and apply business rules.

Invalid transaction and loan records are retained in quarantine tables with rejection reasons where applicable. The pipeline also performs source-to-valid-plus-rejected reconciliation.

## Gold Modelling

The Gold layer uses a dimensional model with date, customer, account, and branch dimensions plus transaction and loan facts. Analytical summary tables are derived from the Gold facts and dimensions.

The customer dimension is an initial snapshot with effective-date fields. It is not a complete historical SCD Type 2 implementation.

## SQL Analytics

The Gold SQL analytics notebook demonstrates:

- aggregations
- joins
- CTEs
- window functions
- `LAG()`
- customer analysis
- branch analysis
- loan analysis
- daily and monthly trends
- failure-rate analysis
- integrity checks
- reconciliation

## Audit and Monitoring

`pipeline_audit_log` records source, valid, rejected, reconciliation, pipeline-status, and timestamp information.

The audit design supports both datasets with quarantine populations and master tables where a count check is more appropriate.

## Incremental Processing

The incremental notebook uses file-level ingestion tracking and record-level idempotency to prevent repeated source files or records from creating duplicate downstream data.

It also refreshes affected Gold summaries rather than rebuilding every summary from scratch.

The tested workflow is intentionally kept understandable and suitable for an educational project. It is not presented as a fully transactional production framework.

## Snowflake Export

The Snowflake export is a staged file workflow:

```text
Databricks Gold
     |
     v
CSV export
     |
     v
Snowflake internal stage
     |
     v
COPY INTO
     |
     v
FINFLOW_DB.GOLD
```

Account-specific credentials and connection details are intentionally not stored in the repository.

## Reproducibility Notes

The repository does not include the raw dataset archive. A fresh deployment therefore requires the prepared dataset files and environment-specific Volume/catalog setup.

The validation numbers documented in `validation_results.md` describe the tested workspace state and should not be interpreted as guaranteed results from a fresh deployment.

## Security Notes

Do not commit passwords, access keys, private keys, connection strings, `.env` files, or raw customer datasets containing PII. Use appropriate secret-management mechanisms for environment-specific credentials.

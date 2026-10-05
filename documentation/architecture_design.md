# FinFlow Architecture Design

## 1. Overview

FinFlow is an educational end-to-end financial Data Engineering project. It demonstrates a practical flow from raw CSV/JSON files through Databricks Bronze, Silver, and Gold layers, followed by staged loading of curated Gold datasets into Snowflake for analytics.

## 2. Logical Architecture

```text
CSV / JSON Source Files
        |
        v
Databricks Volume
        |
        v
+---------------------------+
| Bronze - Raw Delta Tables |
| workspace.finflow_bronze  |
+---------------------------+
        |
        v
+----------------------------------+
| Silver - Clean / Validate       |
| workspace.finflow_silver        |
|                                  |
| Valid tables + Quarantine tables |
+----------------------------------+
        |
        v
+----------------------------------+
| Gold - Analytics Layer           |
| workspace.finflow_gold           |
|                                  |
| Dimensions + Facts + Summaries   |
+----------------------------------+
        |
        v
Databricks Gold
    |\
    | \\
    |  +--> Databricks SQL Analytics
    |
    +--> CSV Export to Databricks Volume
             |
             v
        Snowflake Internal Stage
             |
             v
        Snowflake FINFLOW_DB.GOLD
```

## 3. Medallion Layers

### Bronze

The Bronze layer preserves source-oriented data and adds ingestion metadata such as batch and source-file information.

Schema: `workspace.finflow_bronze`

Main tables include customers, accounts, branches, loans, transactions, exchange rates, and the incremental ingestion file log.

### Silver

The Silver layer standardizes, validates, and separates acceptable records from rejected records.

Schema: `workspace.finflow_silver`

Key controls include duplicate handling, mandatory-field validation, referential checks, business-rule validation, timestamp validation, and temporal checks against customer signup dates.

Rejected transactions and loans are retained in quarantine tables.

### Gold

The Gold layer provides analytics-ready dimensional and fact structures.

Schema: `workspace.finflow_gold`

Dimensions:
- `dim_date`
- `dim_customer`
- `dim_account`
- `dim_branch`

Facts:
- `fact_transaction`
- `fact_loan`

Analytical tables:
- `daily_transaction_summary`
- `customer_analytics`
- `branch_performance`
- `loan_portfolio_summary`

The customer dimension contains effective-date fields and represents an initial snapshot. It should not be described as a fully historized SCD Type 2 implementation.

## 4. Incremental Processing

The incremental notebook discovers JSON files in the configured incoming transaction directory.

The processing pattern is:

1. Discover incoming files.
2. Check the ingestion file log.
3. Recover files where Bronze exists but ingestion state requires recovery.
4. Append new Bronze records.
5. Apply record-level idempotency.
6. Validate records in Silver.
7. Quarantine invalid records.
8. Append valid records to the Gold transaction fact.
9. Refresh affected daily, customer, and branch summaries.
10. Write an audit record.
11. Verify reconciliation and pipeline status.

The implementation is an educational single-job workflow rather than a fully transactional production multi-table framework.

## 5. Data Quality and Reconciliation

The pipeline treats data quality as a first-class processing step.

Examples of transaction rules include:
- mandatory identifiers
- valid customer/account references
- positive transaction amounts
- valid timestamps
- allowed transaction types
- allowed statuses and channels
- valid currency
- transaction date not earlier than customer signup

Loan validation includes the customer signup-date temporal rule.

Reconciliation checks compare source, valid, and rejected populations where applicable.

## 6. Orchestration

The incremental pipeline is scheduled through the Databricks Workflow `FinFlow_Incremental_Pipeline` with task `incremental_pipeline`.

The tested workflow uses Serverless compute and was configured with a daily schedule at 01:00 AM Asia/Calcutta (UTC+05:30).

## 7. Snowflake Integration

The Gold datasets are exported as CSV files to a Databricks Volume, uploaded to a Snowflake internal stage, and loaded into `FINFLOW_DB.GOLD` using `COPY INTO`.

Nine curated datasets were loaded:
- FACT_TRANSACTION
- DIM_CUSTOMER
- DIM_ACCOUNT
- DIM_BRANCH
- DIM_DATE
- CUSTOMER_ANALYTICS
- BRANCH_PERFORMANCE
- DAILY_TRANSACTION_SUMMARY
- LOAN_PORTFOLIO_SUMMARY

This is a staged file-based transfer, not a direct Databricks-Snowflake connector implementation.

## 8. Azure Storage Scope

Azure Data Lake Storage Gen2 was configured as a landing-zone resource for the project. The active Databricks Free Edition pipeline reads from Databricks Volumes instead of implementing a direct ADLS-to-Databricks connection.

## 9. Security Boundary

Credentials, passwords, keys, and connection secrets are intentionally excluded from the repository. Environment-specific credentials should be supplied through appropriate secret-management mechanisms.

# FinFlow Validation Results

These results document the tested workspace state. They are evidence from the implemented project and are not guaranteed outputs from a fresh deployment.

## 1. Bronze / Silver / Gold Reconciliation

### Transactions

| Population | Rows |
|---|---:|
| Bronze transactions | 120,120 |
| Silver valid transactions | 108,710 |
| Quarantined transactions | 11,410 |

The tested Silver and Gold transaction populations reconcile as expected for the incremental project state.

The quarantine population contains records rejected for data-quality or business-rule reasons, including duplicate transaction IDs, invalid amounts, invalid or missing timestamps, invalid transaction types, missing customer IDs, and transactions occurring before customer signup.

### Loans

| Population | Rows |
|---|---:|
| Bronze loans | 5,000 |
| Silver valid loans | 2,233 |
| Quarantined loans | 2,767 |

The loan validation quarantines loans occurring before customer signup.

## 2. Gold Validation

| Gold table | Rows |
|---|---:|
| `fact_transaction` | 108,710 |
| `daily_transaction_summary` | 277 |
| `customer_analytics` | 10,000 |
| `branch_performance` | 50 |
| `loan_portfolio_summary` | 20 |

### Fact transaction integrity

- Total rows: **108,710**
- Distinct transaction IDs: **108,710**
- Duplicate transaction IDs: **0**
- Unmatched fact-to-dimension keys: **0**
- Total transaction amount: **240,709,803.28**

## 3. Incremental Pipeline Validation

The tested incremental executions produced audit records with:

| Metric | Result |
|---|---:|
| Source records | 100 |
| Valid records | 100 |
| Rejected records | 0 |
| Reconciliation | PASSED |
| Pipeline status | SUCCESS |

The workflow also recorded successful scheduled runs through `FinFlow_Incremental_Pipeline`.

## 4. Idempotency Validation

Repeated execution of the tested incremental batch did not create duplicate transaction IDs in the Gold fact.

The observed state was:

- Gold fact rows: 108,710
- Distinct transaction IDs: 108,710
- Duplicate IDs: 0

This supports idempotency for the tested scenario. It is not a formal guarantee of exactly-once processing under every possible concurrent execution or partial-failure condition.

## 5. Snowflake Validation

The tested Snowflake environment contains the nine curated Gold datasets:

- FACT_TRANSACTION
- DIM_CUSTOMER
- DIM_ACCOUNT
- DIM_BRANCH
- DIM_DATE
- CUSTOMER_ANALYTICS
- BRANCH_PERFORMANCE
- DAILY_TRANSACTION_SUMMARY
- LOAN_PORTFOLIO_SUMMARY

The tested row counts matched the corresponding Databricks Gold exports.

| Snowflake dataset | Rows |
|---|---:|
| FACT_TRANSACTION | 108,710 |
| DIM_CUSTOMER | 10,000 |
| DIM_ACCOUNT | 15,000 |
| DIM_BRANCH | 50 |
| DIM_DATE | 1,461 |
| CUSTOMER_ANALYTICS | 10,000 |
| BRANCH_PERFORMANCE | 50 |
| DAILY_TRANSACTION_SUMMARY | 277 |
| LOAN_PORTFOLIO_SUMMARY | 20 |

## 6. Workflow Validation

The Databricks Workflow `FinFlow_Incremental_Pipeline` was configured with:

- Task: `incremental_pipeline`
- Compute: Serverless
- Schedule: 01:00 AM Asia/Calcutta (UTC+05:30)

The latest scheduled execution shown in the tested workspace completed with status **Succeeded**.

## 7. Evidence

The repository's `screenshots/` directory is intended to contain selected evidence for:

1. Bronze layer
2. Silver and quarantine layer
3. Gold layer
4. SQL analytics
5. Data-quality audit
6. Incremental pipeline
7. Snowflake Gold schema
8. Databricks Workflow

Screenshots should remain sanitized and must not expose credentials, secrets, or unnecessary personal/customer information.

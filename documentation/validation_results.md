# Validation results recorded for the tested workspace

The following counts were confirmed in the user's Databricks workspace after the incremental job executions.

| Table | Confirmed row count |
|---|---:|
| `workspace.finflow_bronze.transactions` | 120,120 |
| `workspace.finflow_silver.transactions` | 108,710 |
| `workspace.finflow_silver.quarantine_transactions` | 11,410 |
| `workspace.finflow_gold.fact_transaction` | 108,710 |
| `workspace.finflow_gold.daily_transaction_summary` | 277 |
| `workspace.finflow_gold.customer_analytics` | 10,000 |
| `workspace.finflow_gold.branch_performance` | 50 |

## Gold fact uniqueness

- Total rows: 108,710
- Distinct transaction IDs: 108,710
- Duplicate transaction IDs: 0
- Total transaction amount: 240,709,803.28

## Incremental audit records

Two execution records were shown, each with:

- Source count: 100
- Valid count: 100
- Rejected count: 0
- Reconciliation status: `PASSED`
- Pipeline status: `SUCCESS`

## Interpretation

The unchanged table counts and zero duplicate transaction IDs support the conclusion that repeating the same test batch did not create duplicate Gold fact rows. This validates the tested scenario; it is not a formal proof of exactly-once processing under every possible concurrent or partial-failure condition.

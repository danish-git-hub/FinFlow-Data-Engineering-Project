# Databricks notebook source
# MAGIC %md
# MAGIC # FinFlow — Idempotency Validation
# MAGIC
# MAGIC Validation notebook for the FinFlow incremental pipeline.
# MAGIC
# MAGIC Checks:
# MAGIC - Medallion-layer transaction counts
# MAGIC - Recent pipeline audit records
# MAGIC - Duplicate transaction IDs in the Gold fact
# MAGIC - Incremental pipeline reconciliation and status

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Medallion Layer Row Counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Bronze Transactions' AS table_name, COUNT(*) AS row_count
# MAGIC FROM workspace.finflow_bronze.transactions
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Silver Transactions', COUNT(*)
# MAGIC FROM workspace.finflow_silver.transactions
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Quarantined Transactions', COUNT(*)
# MAGIC FROM workspace.finflow_silver.quarantine_transactions
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Gold Fact Transactions', COUNT(*)
# MAGIC FROM workspace.finflow_gold.fact_transaction
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Daily Summary Rows', COUNT(*)
# MAGIC FROM workspace.finflow_gold.daily_transaction_summary
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Customer Analytics Rows', COUNT(*)
# MAGIC FROM workspace.finflow_gold.customer_analytics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Branch Performance Rows', COUNT(*)
# MAGIC FROM workspace.finflow_gold.branch_performance;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Recent Pipeline Audit Records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     pipeline_name,
# MAGIC     source_table,
# MAGIC     source_count,
# MAGIC     valid_count,
# MAGIC     rejected_count,
# MAGIC     reconciliation_status,
# MAGIC     pipeline_status,
# MAGIC     run_timestamp
# MAGIC FROM workspace.finflow_gold.pipeline_audit_log
# MAGIC ORDER BY run_timestamp DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold Fact Duplicate Check
# MAGIC
# MAGIC A transaction ID should occur only once in the Gold fact table.
# MAGIC `duplicate_rows = 0` is required for the idempotency check to pass.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT transaction_id) AS unique_transactions,
# MAGIC     COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_rows,
# MAGIC     ROUND(SUM(amount), 2) AS total_transaction_amount
# MAGIC FROM workspace.finflow_gold.fact_transaction;

# COMMAND ----------

# MAGIC %md

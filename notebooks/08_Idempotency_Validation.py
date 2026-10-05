# Databricks notebook source
# MAGIC %sql
# MAGIC
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
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finflow_gold.pipeline_audit_log
# MAGIC ORDER BY run_timestamp DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check duplicate transaction IDs
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT transaction_id) AS unique_transactions,
# MAGIC     COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_rows,
# MAGIC     ROUND(SUM(amount), 2) AS total_transaction_amount
# MAGIC FROM workspace.finflow_gold.fact_transaction;
# MAGIC
# MAGIC -- Check latest incremental audit records
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     pipeline_name,
# MAGIC     source_count,
# MAGIC     valid_count,
# MAGIC     rejected_count,
# MAGIC     reconciliation_status,
# MAGIC     pipeline_status,
# MAGIC     run_timestamp
# MAGIC FROM workspace.finflow_gold.pipeline_audit_log
# MAGIC WHERE pipeline_name = 'FinFlow_Incremental'
# MAGIC ORDER BY run_timestamp DESC
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT transaction_id) AS unique_transactions,
# MAGIC     COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_rows,
# MAGIC     ROUND(SUM(amount), 2) AS total_transaction_amount
# MAGIC FROM workspace.finflow_gold.fact_transaction;
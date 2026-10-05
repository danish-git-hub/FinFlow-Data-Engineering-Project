# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.finflow_gold.pipeline_audit_log (
# MAGIC     run_id STRING,
# MAGIC     pipeline_name STRING,
# MAGIC     source_table STRING,
# MAGIC     valid_table STRING,
# MAGIC     quarantine_table STRING,
# MAGIC     source_count BIGINT,
# MAGIC     valid_count BIGINT,
# MAGIC     rejected_count BIGINT,
# MAGIC     reconciliation_status STRING,
# MAGIC     pipeline_status STRING,
# MAGIC     run_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# Generate a unique pipeline run ID
run_id = str(uuid.uuid4())

# Define datasets to audit
datasets = [
    {
        "name": "transactions",
        "source": "workspace.finflow_bronze.transactions",
        "valid": "workspace.finflow_silver.transactions",
        "quarantine": "workspace.finflow_silver.quarantine_transactions"
    },
    {
        "name": "loans",
        "source": "workspace.finflow_bronze.loans",
        "valid": "workspace.finflow_silver.loans",
        "quarantine": "workspace.finflow_silver.quarantine_loans"
    }
]

audit_records = []

for dataset in datasets:

    source_count = spark.table(dataset["source"]).count()

    valid_count = spark.table(dataset["valid"]).count()

    rejected_count = spark.table(dataset["quarantine"]).count()

    # Check reconciliation
    reconciliation_passed = (
        source_count == valid_count + rejected_count
    )

    reconciliation_status = (
        "PASSED" if reconciliation_passed else "FAILED"
    )

    pipeline_status = (
        "SUCCESS" if reconciliation_passed else "FAILED"
    )

    audit_records.append((
        run_id,
        "FinFlow_Medallion_Pipeline",
        dataset["source"],
        dataset["valid"],
        dataset["quarantine"],
        source_count,
        valid_count,
        rejected_count,
        reconciliation_status,
        pipeline_status,
        datetime.now()
    ))

# Create audit DataFrame
audit_df = spark.createDataFrame(
    audit_records,
    schema="""
        run_id STRING,
        pipeline_name STRING,
        source_table STRING,
        valid_table STRING,
        quarantine_table STRING,
        source_count LONG,
        valid_count LONG,
        rejected_count LONG,
        reconciliation_status STRING,
        pipeline_status STRING,
        run_timestamp TIMESTAMP
    """
)

display(audit_df)

# COMMAND ----------

audit_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "workspace.finflow_gold.pipeline_audit_log"
    )

print("Pipeline audit records saved successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_name,
# MAGIC     source_table,
# MAGIC     source_count,
# MAGIC     valid_count,
# MAGIC     rejected_count,
# MAGIC     reconciliation_status,
# MAGIC     pipeline_status,
# MAGIC     run_timestamp
# MAGIC FROM workspace.finflow_gold.pipeline_audit_log
# MAGIC ORDER BY run_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reusable audit framework

# COMMAND ----------

datasets = [
    {
        "name": "customers",
        "source": "workspace.finflow_bronze.customers",
        "valid": "workspace.finflow_silver.customers",
        "quarantine": None
    },
    {
        "name": "accounts",
        "source": "workspace.finflow_bronze.accounts",
        "valid": "workspace.finflow_silver.accounts",
        "quarantine": None
    },
    {
        "name": "branches",
        "source": "workspace.finflow_bronze.branches",
        "valid": "workspace.finflow_silver.branches",
        "quarantine": None
    },
    {
        "name": "loans",
        "source": "workspace.finflow_bronze.loans",
        "valid": "workspace.finflow_silver.loans",
        "quarantine": "workspace.finflow_silver.quarantine_loans"
    },
    {
        "name": "transactions",
        "source": "workspace.finflow_bronze.transactions",
        "valid": "workspace.finflow_silver.transactions",
        "quarantine": "workspace.finflow_silver.quarantine_transactions"
    },
    {
        "name": "exchange_rates",
        "source": "workspace.finflow_bronze.exchange_rates",
        "valid": "workspace.finflow_silver.exchange_rates",
        "quarantine": None
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC Create a reusable audit function

# COMMAND ----------

from datetime import datetime
import uuid

def audit_dataset(dataset, run_id):

    source_count = spark.table(
        dataset["source"]
    ).count()

    valid_count = spark.table(
        dataset["valid"]
    ).count()

    quarantine_table = dataset["quarantine"]

    if quarantine_table:

        rejected_count = spark.table(
            quarantine_table
        ).count()

        reconciliation_passed = (
            source_count == valid_count + rejected_count
        )

        reconciliation_status = (
            "PASSED" if reconciliation_passed else "FAILED"
        )

    else:

        rejected_count = 0
        reconciliation_status = "COUNT_CHECK_ONLY"

        reconciliation_passed = True

    pipeline_status = (
        "SUCCESS" if reconciliation_passed else "FAILED"
    )

    return (
        run_id,
        "FinFlow_Medallion_Pipeline",
        dataset["source"],
        dataset["valid"],
        quarantine_table,
        source_count,
        valid_count,
        rejected_count,
        reconciliation_status,
        pipeline_status,
        datetime.now()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the audit for all six datasets

# COMMAND ----------

run_id = str(uuid.uuid4())

audit_records = []

for dataset in datasets:

    record = audit_dataset(dataset, run_id)

    audit_records.append(record)

audit_df = spark.createDataFrame(
    audit_records,
    schema="""
        run_id STRING,
        pipeline_name STRING,
        source_table STRING,
        valid_table STRING,
        quarantine_table STRING,
        source_count LONG,
        valid_count LONG,
        rejected_count LONG,
        reconciliation_status STRING,
        pipeline_status STRING,
        run_timestamp TIMESTAMP
    """
)

display(audit_df)

# COMMAND ----------

# append all six records
audit_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "workspace.finflow_gold.pipeline_audit_log"
    )

print("Six-dataset audit execution completed!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     source_table,
# MAGIC     source_count,
# MAGIC     valid_count,
# MAGIC     rejected_count,
# MAGIC     reconciliation_status,
# MAGIC     pipeline_status,
# MAGIC     run_timestamp
# MAGIC FROM workspace.finflow_gold.pipeline_audit_log
# MAGIC ORDER BY run_timestamp DESC
# MAGIC LIMIT 6;
# Databricks notebook source
# MAGIC %md
# MAGIC # FinFlow — Snowflake Export
# MAGIC
# MAGIC Exports the FinFlow Gold-layer tables from Databricks to CSV files
# MAGIC in a Databricks Volume.
# MAGIC
# MAGIC These files are then uploaded to a Snowflake stage and loaded into
# MAGIC the corresponding Snowflake Gold tables using `COPY INTO`.
# MAGIC
# MAGIC This notebook intentionally uses a simple CSV-based transfer because
# MAGIC the capstone uses manual Snowflake loading rather than a direct
# MAGIC Databricks-to-Snowflake connector.

# COMMAND ----------

# ============================================================
# 1. Configuration
# ============================================================

GOLD_DB = "workspace.finflow_gold"

EXPORT_BASE = (
    "/Volumes/workspace/default/finflow_raw/"
    "finflow_snowflake_export"
)

# Gold tables exported to Snowflake.
gold_tables = [
    "fact_transaction",
    "dim_customer",
    "dim_account",
    "dim_branch",
    "dim_date",
    "customer_analytics",
    "branch_performance",
    "daily_transaction_summary",
    "loan_portfolio_summary"
]

print("Gold tables configured for export:", len(gold_tables))

# COMMAND ----------

# ============================================================
# 2. Reusable Gold Table Export Function
# ============================================================

def export_gold_table(table_name):
    """
    Export one Databricks Gold table as a single CSV part file.

    coalesce(1) is used here because the capstone uses manual CSV
    transfer into Snowflake and the Gold datasets are manageable in size.
    For large production datasets, a single partition should be avoided.
    """

    table_full_name = f"{GOLD_DB}.{table_name}"
    export_path = f"{EXPORT_BASE}/{table_name}"

    df = spark.table(table_full_name)

    record_count = df.count()

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(export_path)
    )

    print(f"Table: {table_name}")
    print(f"Records: {record_count:,}")
    print(f"Export location: {export_path}")
    print("-" * 60)

# COMMAND ----------

# ============================================================
# 3. Export All Gold Tables
# ============================================================

for table_name in gold_tables:
    export_gold_table(table_name)

print("All Gold tables exported successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Export Locations
# MAGIC
# MAGIC List the generated files for each Gold table so the CSV part file
# MAGIC can be identified before uploading it to Snowflake.

# COMMAND ----------

for table_name in gold_tables:
    export_path = f"{EXPORT_BASE}/{table_name}"

    print(f"\n{table_name}:")
    for file_info in dbutils.fs.ls(export_path):
        print(
            f"  {file_info.name} | "
            f"{file_info.size:,} bytes | "
            f"{file_info.path}"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snowflake Transfer Flow
# MAGIC
# MAGIC The exported CSV files are transferred to Snowflake using the following
# MAGIC manual workflow:
# MAGIC
# MAGIC `Databricks Gold → CSV files → Snowflake Stage → COPY INTO → Snowflake Gold`
# MAGIC
# MAGIC The Snowflake loading and validation steps are documented separately.

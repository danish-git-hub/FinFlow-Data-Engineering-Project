# Databricks notebook source
# MAGIC %md
# MAGIC ## FinFlow – Bronze Layer Ingestion
# MAGIC
# MAGIC Purpose:
# MAGIC Ingest raw source datasets into Delta tables
# MAGIC using PySpark and Delta Lake.
# MAGIC
# MAGIC Source:
# MAGIC Databricks Unity Catalog Volume
# MAGIC
# MAGIC Target:
# MAGIC workspace.finflow_bronze
# MAGIC
# MAGIC Datasets:
# MAGIC Customers, Accounts, Branches, Loans,
# MAGIC Transactions and Exchange Rates.

# COMMAND ----------


from pyspark.sql.functions import (
    lit,
    current_timestamp
)

import uuid

# Source location
SOURCE_PATH = "/Volumes/workspace/default/finflow_raw/source_data"

# Target schema
BRONZE_SCHEMA = "workspace.finflow_bronze"

# Unique ID for this pipeline execution
BATCH_ID = str(uuid.uuid4())

print("FinFlow Bronze Ingestion Started")
print("Batch ID:", BATCH_ID)
print("Source:", SOURCE_PATH)
print("Target:", BRONZE_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## # Create the Bronze schema

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}
""")

print("Bronze schema is ready!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a reusable metadata function

# COMMAND ----------

def add_bronze_metadata(df, source_file):

    return (
        df
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "_source_file",
            lit(source_file)
        )
        .withColumn(
            "_batch_id",
            lit(BATCH_ID)
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the table-writing function

# COMMAND ----------

def write_bronze_table(df, table_name, source_file):

    bronze_df = add_bronze_metadata(
        df,
        source_file
    )

    full_table_name = f"{BRONZE_SCHEMA}.{table_name}"

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    print(f"Successfully created: {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Test the metadata function

# COMMAND ----------

# MAGIC %md
# MAGIC Test the writing function

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read all six source datasets

# COMMAND ----------

# 1. Customers
customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{SOURCE_PATH}/customers.csv")
)

# 2. Accounts
accounts_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{SOURCE_PATH}/accounts.csv")
)

# 3. Branches
branches_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{SOURCE_PATH}/branches.csv")
)

# 4. Loans
loans_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{SOURCE_PATH}/loans.csv")
)

# 5. Transactions
transactions_df = spark.read.json(
    f"{SOURCE_PATH}/transactions.json"
)

# 6. Exchange rates
# Prepared/mock exchange-rate dataset for the project
exchange_rates_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{SOURCE_PATH}/api_exchange_rates.csv")
)

print("All six source datasets loaded successfully!")

# COMMAND ----------

source_dfs = {
    "customers": customers_df,
    "accounts": accounts_df,
    "branches": branches_df,
    "loans": loans_df,
    "transactions": transactions_df,
    "exchange_rates": exchange_rates_df
}

for name, df in source_dfs.items():
    print(f"{name}: {df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest data into Bronze Delta tables

# COMMAND ----------

# Customers
write_bronze_table(
    customers_df,
    "customers",
    "customers.csv"
)

# Accounts
write_bronze_table(
    accounts_df,
    "accounts",
    "accounts.csv"
)

# Branches
write_bronze_table(
    branches_df,
    "branches",
    "branches.csv"
)

# Loans
write_bronze_table(
    loans_df,
    "loans",
    "loans.csv"
)

# Transactions
write_bronze_table(
    transactions_df,
    "transactions",
    "transactions.json"
)

# Exchange rates
write_bronze_table(
    exchange_rates_df,
    "exchange_rates",
    "api_exchange_rates.csv"
)

# COMMAND ----------

spark.sql("""
SHOW TABLES IN workspace.finflow_bronze
""").show(truncate=False)

# COMMAND ----------

bronze_transactions = spark.table(
    "workspace.finflow_bronze.transactions"
)

print("Bronze transaction count:", bronze_transactions.count())

display(bronze_transactions.limit(10))

# COMMAND ----------

bronze_transactions.printSchema()

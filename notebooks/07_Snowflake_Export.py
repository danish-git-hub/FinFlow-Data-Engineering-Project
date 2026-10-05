# Databricks notebook source
# Export the Gold fact table as CSV

fact_df = spark.table(
    "workspace.finflow_gold.fact_transaction"
)

export_path = (
    "/Volumes/workspace/default/finflow_raw/"
    "finflow_snowflake_export/fact_transaction"
)

(
    fact_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .csv(export_path)
)

print("Fact transaction export completed!")
print("Records exported:", fact_df.count())
print("Export location:", export_path)

# COMMAND ----------

export_path = (
    "/Volumes/workspace/default/finflow_raw/"
    "finflow_snowflake_export/fact_transaction"
)

files = dbutils.fs.ls(export_path)

for file in files:
    print(file.name, file.size, file.path)

# COMMAND ----------

table_name = "dim_customer"

df = spark.table(
    f"workspace.finflow_gold.{table_name}"
)

export_path = (
    "/Volumes/workspace/default/finflow_raw/"
    f"finflow_snowflake_export/{table_name}"
)

(
    df.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .csv(export_path)
)

print("Table:", table_name)
print("Records:", df.count())
print("Export completed!")

# COMMAND ----------

# Inspect the original Gold customer dimension

df = spark.table(
    "workspace.finflow_gold.dim_customer"
)

print("Total columns:", len(df.columns))
print("Column names:", df.columns)

df.printSchema()

# COMMAND ----------

export_path = (
    "/Volumes/workspace/default/finflow_raw/"
    "finflow_snowflake_export/dim_customer"
)

files = dbutils.fs.ls(export_path)

csv_file = [
    f.path for f in files
    if f.name.endswith(".csv")
][0]

print("CSV header:")
print(dbutils.fs.head(csv_file, 2000))

# COMMAND ----------

table_name = "dim_account"

df = spark.table(
    f"workspace.finflow_gold.{table_name}"
)

export_path = (
    "/Volumes/workspace/default/finflow_raw/"
    f"finflow_snowflake_export/{table_name}"
)

(
    df.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .csv(export_path)
)

print("Table:", table_name)
print("Records:", df.count())
print("Export completed!")

# COMMAND ----------

table_name = "dim_branch"

df = spark.table(
    f"workspace.finflow_gold.{table_name}"
)

export_path = (
    "/Volumes/workspace/default/finflow_raw/"
    f"finflow_snowflake_export/{table_name}"
)

(
    df.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .csv(export_path)
)

print("Table:", table_name)
print("Records:", df.count())
print("Export completed!")

# COMMAND ----------

table_name = "dim_date"

df = spark.table(
    f"workspace.finflow_gold.{table_name}"
)

export_path = (
    "/Volumes/workspace/default/finflow_raw/"
    f"finflow_snowflake_export/{table_name}"
)

(
    df.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .csv(export_path)
)

print("Table:", table_name)
print("Records:", df.count())
print("Columns:", df.columns)
print("Export completed!")

# COMMAND ----------

table_name = "customer_analytics"

df = spark.table(
    f"workspace.finflow_gold.{table_name}"
)

export_path = (
    "/Volumes/workspace/default/finflow_raw/"
    f"finflow_snowflake_export/{table_name}"
)

(
    df.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .csv(export_path)
)

print("Table:", table_name)
print("Records:", df.count())
print("Columns:", df.columns)
print("Export completed!")

# COMMAND ----------

table_name = "branch_performance"

df = spark.table(
    f"workspace.finflow_gold.{table_name}"
)

export_path = (
    "/Volumes/workspace/default/finflow_raw/"
    f"finflow_snowflake_export/{table_name}"
)

(
    df.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .csv(export_path)
)

print("Table:", table_name)
print("Records:", df.count())
print("Columns:", df.columns)
print("Export completed!")

# COMMAND ----------

table_name = "daily_transaction_summary"

df = spark.table(
    f"workspace.finflow_gold.{table_name}"
)

export_path = (
    "/Volumes/workspace/default/finflow_raw/"
    f"finflow_snowflake_export/{table_name}"
)

(
    df.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .csv(export_path)
)

print("Table:", table_name)
print("Records:", df.count())
print("Columns:", df.columns)
print("Export completed!")

# COMMAND ----------

table_name = "loan_portfolio_summary"

df = spark.table(
    f"workspace.finflow_gold.{table_name}"
)

export_path = (
    "/Volumes/workspace/default/finflow_raw/"
    f"finflow_snowflake_export/{table_name}"
)

(
    df.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .csv(export_path)
)

print("Table:", table_name)
print("Records:", df.count())
print("Columns:", df.columns)
print("Export completed!")
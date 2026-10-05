# Databricks notebook source
import zipfile
import os

# Volume location
volume_path = "/Volumes/workspace/default/finflow_raw"

# ZIP file location
zip_path = f"{volume_path}/FinFlow_datasets.zip"

# Extract datasets into a directory
extract_path = f"{volume_path}/source_data"

os.makedirs(extract_path, exist_ok=True)

with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall(extract_path)

print("FinFlow datasets extracted successfully!")

# COMMAND ----------

display(
    dbutils.fs.ls("/Volumes/workspace/default/finflow_raw/source_data")
)

# COMMAND ----------

volume_path = "/Volumes/workspace/default/finflow_raw/source_data"

files = dbutils.fs.ls(volume_path)

for file in files:
    print(file.name, file.size, "bytes")

# COMMAND ----------

# Define source location
source_path = "/Volumes/workspace/default/finflow_raw/source_data"

# Read customer CSV
customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{source_path}/customers.csv")
)

# Display sample records
display(customers_df.limit(10))

# COMMAND ----------

# Record count
print("Total customers:", customers_df.count())

# Column names and data types
customers_df.printSchema()

# Basic data statistics
display(customers_df.describe())

# COMMAND ----------

transactions_df = spark.read.json(
    f"{source_path}/transactions.json"
)

display(transactions_df.limit(10))

print("Total transactions:", transactions_df.count())

transactions_df.printSchema()
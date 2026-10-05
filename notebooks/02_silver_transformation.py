# Databricks notebook source
# MAGIC %md
# MAGIC Inspect all Bronze table schemas

# COMMAND ----------

# =====================================================
# FINFLOW - SILVER TRANSFORMATION
# Step 1: Inspect Bronze Tables
# =====================================================

BRONZE_DB = "workspace.finflow_bronze"

tables = [
    "customers",
    "accounts",
    "branches",
    "loans",
    "transactions",
    "exchange_rates"
]

for table_name in tables:

    print(f"\n{'='*60}")
    print(f"TABLE: {table_name}")
    print(f"{'='*60}")

    df = spark.table(f"{BRONZE_DB}.{table_name}")

    print("Record Count:", df.count())
    df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Inspect transaction values

# COMMAND ----------

from pyspark.sql import functions as F

BRONZE_DB = "workspace.finflow_bronze"

transactions_df = spark.table(f"{BRONZE_DB}.transactions")

print("Transaction types:")
transactions_df.groupBy("transaction_type") \
    .count().orderBy(F.desc("count")).show(50, truncate=False)

print("Transaction statuses:")
transactions_df.groupBy("status") \
    .count().orderBy(F.desc("count")).show(50, truncate=False)

print("Transaction channels:")
transactions_df.groupBy("channel") \
    .count().orderBy(F.desc("count")).show(50, truncate=False)

print("Currencies:")
transactions_df.groupBy("currency") \
    .count().orderBy(F.desc("count")).show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Check transaction data quality

# COMMAND ----------

print("TRANSACTION DATA QUALITY PROFILE")

transactions_df.select(
    F.count("*").alias("total_records"),

    F.countDistinct("transaction_id").alias("unique_transaction_ids"),

    F.sum(
        F.col("transaction_id").isNull().cast("int")
    ).alias("null_transaction_ids"),

    F.sum(
        F.col("customer_id").isNull().cast("int")
    ).alias("null_customer_ids"),

    F.sum(
        F.col("account_id").isNull().cast("int")
    ).alias("null_account_ids"),

    F.sum(
        F.col("amount").isNull().cast("int")
    ).alias("null_amounts"),

    F.sum(
        (F.col("amount") < 0).cast("int")
    ).alias("negative_amounts"),

    F.sum(
        F.col("transaction_timestamp").isNull().cast("int")
    ).alias("null_timestamps")
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Inspect other entity values

# COMMAND ----------

for table_name, columns in {
    "customers": ["customer_segment", "customer_status"],
    "accounts": ["account_type", "account_status", "currency"],
    "branches": ["branch_type"],
    "loans": ["loan_type", "loan_status"]
}.items():

    print(f"\n========== {table_name.upper()} ==========")

    df = spark.table(f"{BRONZE_DB}.{table_name}")

    for column_name in columns:
        print(f"\nDistinct values in {column_name}:")

        df.groupBy(column_name) \
          .count() \
          .orderBy(F.desc("count")) \
          .show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Silver database

# COMMAND ----------

# =====================================================
# FINFLOW - SILVER LAYER
# Step 1: Create Silver Schema
# =====================================================

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.finflow_silver
""")

BRONZE_DB = "workspace.finflow_bronze"
SILVER_DB = "workspace.finflow_silver"

print("Silver schema created successfully!")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def clean_and_deduplicate(df, primary_key, uppercase_columns=None):

    uppercase_columns = uppercase_columns or []

    # Remove leading and trailing spaces from string columns
    for column_name, data_type in df.dtypes:

        if data_type == "string":
            df = df.withColumn(
                column_name,
                F.trim(F.col(column_name))
            )

    # Standardize categorical values
    for column_name in uppercase_columns:

        df = df.withColumn(
            column_name,
            F.upper(F.col(column_name))
        )

    # Keep the latest ingested record for each primary key
    window_spec = (
        Window
        .partitionBy(primary_key)
        .orderBy(
            F.col("_ingestion_timestamp").desc_nulls_last()
        )
    )

    df = (
        df.withColumn(
            "_row_number",
            F.row_number().over(window_spec)
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    # Remove records without a primary key
    df = df.filter(
        F.col(primary_key).isNotNull()
    )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC Clean and save master tables

# COMMAND ----------

# =====================================================
# Clean Customers
# =====================================================

customers_df = spark.table(f"{BRONZE_DB}.customers")

silver_customers = clean_and_deduplicate(
    customers_df,
    "customer_id",
    ["customer_segment", "customer_status"]
)

silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER_DB}.customers")


# =====================================================
# Clean Accounts
# =====================================================

accounts_df = spark.table(f"{BRONZE_DB}.accounts")

silver_accounts = clean_and_deduplicate(
    accounts_df,
    "account_id",
    ["account_type", "account_status", "currency"]
)

silver_accounts.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER_DB}.accounts")


# =====================================================
# Clean Branches
# =====================================================

branches_df = spark.table(f"{BRONZE_DB}.branches")

silver_branches = clean_and_deduplicate(
    branches_df,
    "branch_id",
    ["branch_type"]
)

silver_branches.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER_DB}.branches")


# =====================================================
# Clean Loans
# =====================================================

loans_df = spark.table(f"{BRONZE_DB}.loans")

silver_loans = clean_and_deduplicate(
    loans_df,
    "loan_id",
    ["loan_type", "loan_status"]
)

silver_loans.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER_DB}.loans")


# =====================================================
# Clean Exchange Rates
# Composite key: base_currency + currency + effective_date
# =====================================================

exchange_df = spark.table(f"{BRONZE_DB}.exchange_rates")

silver_exchange = exchange_df

for column_name, data_type in silver_exchange.dtypes:
    if data_type == "string":
        silver_exchange = silver_exchange.withColumn(
            column_name,
            F.trim(F.col(column_name))
        )

silver_exchange = silver_exchange.withColumn(
    "base_currency", F.upper(F.col("base_currency"))
).withColumn(
    "currency", F.upper(F.col("currency"))
)

silver_exchange = silver_exchange.dropDuplicates(
    ["base_currency", "currency", "effective_date"]
)

silver_exchange.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER_DB}.exchange_rates")

print("Exchange rates Silver table created successfully!")


print("All five Silver master tables created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the five Silver tables

# COMMAND ----------

silver_tables = [
    "customers",
    "accounts",
    "branches",
    "loans",
    "exchange_rates"
]

for table_name in silver_tables:

    df = spark.table(f"{SILVER_DB}.{table_name}")

    print(
        f"{table_name}: {df.count()} records"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaction cleaning and quarantine

# COMMAND ----------

# =====================================================
# FINFLOW - SILVER TRANSACTIONS
# Step 1: Load and Standardize
# =====================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

transactions_df = spark.table(
    f"{BRONZE_DB}.transactions"
)

# Trim string columns
for column_name, data_type in transactions_df.dtypes:

    if data_type == "string":
        transactions_df = transactions_df.withColumn(
            column_name,
            F.trim(F.col(column_name))
        )

# Standardize categorical values
categorical_columns = [
    "transaction_type",
    "status",
    "channel",
    "currency"
]

for column_name in categorical_columns:

    transactions_df = transactions_df.withColumn(
        column_name,
        F.upper(F.col(column_name))
    )

# Convert string timestamp into Spark timestamp
transactions_df = transactions_df.withColumn(
    "transaction_timestamp",
    F.to_timestamp(F.col("transaction_timestamp"))
)

print("Transaction data loaded and standardized!")

transactions_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Identify duplicate transactions

# COMMAND ----------

# =====================================================
# Step 2: Identify Duplicate Transactions
# =====================================================

window_spec = (
    Window
    .partitionBy("transaction_id")
    .orderBy(
        F.col("_ingestion_timestamp").desc_nulls_last()
    )
)

transactions_with_row_number = (
    transactions_df
    .withColumn(
        "_row_number",
        F.row_number().over(window_spec)
    )
)

# Keep one occurrence of each non-null transaction ID.
# Preserve every null-ID record for validation.
unique_transactions = (
    transactions_with_row_number
    .filter(
        F.col("transaction_id").isNull() |
        (F.col("_row_number") == 1)
    )
    .drop("_row_number")
)

# Identify additional occurrences of non-null IDs
duplicate_transactions = (
    transactions_with_row_number
    .filter(
        F.col("transaction_id").isNotNull() &
        (F.col("_row_number") > 1)
    )
    .drop("_row_number")
    .withColumn(
        "_rejection_reason",
        F.lit("DUPLICATE_TRANSACTION_ID")
    )
)

print("Original records:", transactions_df.count())

print(
    "Unique transaction candidates:",
    unique_transactions.count()
)

print(
    "Duplicate records:",
    duplicate_transactions.count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check customer and account reference

# COMMAND ----------

# =====================================================
# Step 3: Referential Integrity Validation
# =====================================================

customer_reference = (
    spark.table(f"{SILVER_DB}.customers")
    .select("customer_id")
    .distinct()
    .withColumn("_customer_found", F.lit(1))
)

account_reference = (
    spark.table(f"{SILVER_DB}.accounts")
    .select("account_id")
    .distinct()
    .withColumn("_account_found", F.lit(1))
)

transactions_with_references = (
    unique_transactions
    .join(
        customer_reference,
        on="customer_id",
        how="left"
    )
    .join(
        account_reference,
        on="account_id",
        how="left"
    )
)

print("Referential integrity checks completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Apply business validation rules

# COMMAND ----------

# =====================================================
# Step 4: Data Quality Validation
# =====================================================

valid_transaction_types = [
    "UPI",
    "PAYMENT",
    "TRANSFER",
    "DEPOSIT",
    "ATM",
    "WITHDRAWAL"
]

valid_statuses = [
    "SUCCESS",
    "FAILED",
    "PENDING"
]

valid_channels = [
    "MOBILE_APP",
    "BRANCH",
    "UPI",
    "ATM",
    "WEB"
]

# Generate rejection reasons
validated_transactions = (
    transactions_with_references
    .withColumn(
        "_rejection_reason",
        F.concat_ws(
            " | ",

            F.when(
                F.col("transaction_id").isNull(),
                F.lit("MISSING_TRANSACTION_ID")
            ),

            F.when(
                F.col("customer_id").isNull(),
                F.lit("MISSING_CUSTOMER_ID")
            ),

            F.when(
                F.col("customer_id").isNotNull() &
                F.col("_customer_found").isNull(),
                F.lit("UNKNOWN_CUSTOMER_ID")
            ),

            F.when(
                F.col("account_id").isNull(),
                F.lit("MISSING_ACCOUNT_ID")
            ),

            F.when(
                F.col("account_id").isNotNull() &
                F.col("_account_found").isNull(),
                F.lit("UNKNOWN_ACCOUNT_ID")
            ),

            F.when(
                F.col("amount").isNull(),
                F.lit("MISSING_AMOUNT")
            ),

            F.when(
                F.col("amount").isNotNull() &
                (F.col("amount") <= 0),
                F.lit("INVALID_AMOUNT")
            ),

            F.when(
                F.col("transaction_timestamp").isNull(),
                F.lit("INVALID_OR_MISSING_TIMESTAMP")
            ),

            F.when(
                ~F.col("transaction_type").isin(
                    valid_transaction_types
                ) |
                F.col("transaction_type").isNull(),
                F.lit("INVALID_TRANSACTION_TYPE")
            ),

            F.when(
                ~F.col("status").isin(valid_statuses) |
                F.col("status").isNull(),
                F.lit("INVALID_TRANSACTION_STATUS")
            ),

            F.when(
                ~F.col("channel").isin(valid_channels) |
                F.col("channel").isNull(),
                F.lit("INVALID_CHANNEL")
            ),

            F.when(
                F.col("currency").isNull() |
                (F.col("currency") != "INR"),
                F.lit("INVALID_CURRENCY")
            )
        )
    )
)

# Separate valid and invalid records
valid_transactions = (
    validated_transactions
    .filter(F.col("_rejection_reason") == "")
)

invalid_transactions = (
    validated_transactions
    .filter(F.col("_rejection_reason") != "")
)

print(
    "Valid transaction candidates:",
    valid_transactions.count()
)

print(
    "Invalid transaction records:",
    invalid_transactions.count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the final Silver and Quarantine tables

# COMMAND ----------

# =====================================================
# Step 5: Prepare Silver and Quarantine
# =====================================================

# Remove temporary validation columns from valid data
silver_transactions = (
    valid_transactions
    .drop(
        "_customer_found",
        "_account_found",
        "_rejection_reason"
    )
)

# Remove temporary reference flags from rejected data
quarantined_invalid = (
    invalid_transactions
    .drop(
        "_customer_found",
        "_account_found"
    )
)

# Combine duplicate records and invalid records
quarantine_transactions = (
    duplicate_transactions
    .unionByName(quarantined_invalid)
)

# Save Silver transactions
silver_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER_DB}.transactions")

# Save quarantine transactions
quarantine_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER_DB}.quarantine_transactions")

print("Silver transactions table created!")
print("Quarantine transactions table created!")

# COMMAND ----------

# MAGIC %md
# MAGIC Final reconciliation

# COMMAND ----------

# =====================================================
# Step 6: Reconciliation
# =====================================================

bronze_count = spark.table(
    f"{BRONZE_DB}.transactions"
).count()

silver_count = spark.table(
    f"{SILVER_DB}.transactions"
).count()

quarantine_count = spark.table(
    f"{SILVER_DB}.quarantine_transactions"
).count()

print("=" * 50)
print("FINFLOW TRANSACTION RECONCILIATION")
print("=" * 50)

print("Bronze Records:", bronze_count)
print("Silver Records:", silver_count)
print("Quarantine Records:", quarantine_count)

print(
    "Reconciliation:",
    "PASSED" if bronze_count == silver_count + quarantine_count
    else "FAILED"
)

# COMMAND ----------

print([
    name for name in globals()
    if "quarant" in name.lower()
])

# COMMAND ----------

quarantine_transactions.select(
    "transaction_id",
    "customer_id",
    "amount",
    "transaction_timestamp",
    "transaction_type",
    "_rejection_reason"
).show(20, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import count

spark.table("workspace.finflow_silver.quarantine_transactions") \
    .groupBy("_rejection_reason") \
    .agg(count("*").alias("record_count")) \
    .orderBy("_rejection_reason") \
    .show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Read existing Silver tables
silver_tx = spark.table("workspace.finflow_silver.transactions")

silver_customers = (
    spark.table("workspace.finflow_silver.customers")
    .select(
        "customer_id",
        F.to_date("signup_date").alias("_customer_signup_date")
    )
)

# 2. Join transactions with customer signup dates
joined = (
    silver_tx.alias("t")
    .join(
        silver_customers.alias("c"),
        F.col("t.customer_id") == F.col("c.customer_id"),
        "left"
    )
)

# 3. Identify transactions before customer signup
before_signup = (
    F.col("c._customer_signup_date").isNotNull()
    & (
        F.to_date(F.col("t.transaction_timestamp"))
        < F.col("c._customer_signup_date")
    )
)

# Keep original transaction columns
transaction_columns = [
    F.col(f"t.`{column}`").alias(column)
    for column in silver_tx.columns
]

# 4. Create new quarantine records
new_quarantine = (
    joined
    .filter(before_signup)
    .select(*transaction_columns)
    .withColumn(
        "_rejection_reason",
        F.lit("TRANSACTION_BEFORE_CUSTOMER_SIGNUP")
    )
)

# 5. Keep only transactions that pass this additional rule
updated_valid_transactions = (
    joined
    .filter(~before_signup)
    .select(*transaction_columns)
)

# 6. Preserve existing quarantine records
existing_quarantine = spark.table(
    "workspace.finflow_silver.quarantine_transactions"
)

updated_quarantine = existing_quarantine.unionByName(
    new_quarantine
)

# 7. Check counts BEFORE writing
new_count = new_quarantine.count()
valid_count = updated_valid_transactions.count()
old_quarantine_count = existing_quarantine.count()
total_quarantine_count = updated_quarantine.count()

print("New signup-date violations:", new_count)
print("Updated valid transactions:", valid_count)
print("Existing quarantine records:", old_quarantine_count)
print("Updated quarantine records:", total_quarantine_count)

# Safety checks
assert new_count == 11150, (
    f"Expected 11150 new violations, found {new_count}"
)

assert valid_count == 108610, (
    f"Expected 108610 valid records, found {valid_count}"
)

assert total_quarantine_count == 11410, (
    f"Expected 11410 quarantine records, found {total_quarantine_count}"
)

# 8. Write quarantine FIRST to avoid losing rejected records
updated_quarantine.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.finflow_silver.quarantine_transactions")

# 9. Overwrite Silver transactions with the updated valid records
updated_valid_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.finflow_silver.transactions")

print("Silver transaction validation updated successfully!")

# COMMAND ----------

spark.table(
    "workspace.finflow_silver.quarantine_transactions"
).groupBy("_rejection_reason") \
 .count() \
 .orderBy("_rejection_reason") \
 .show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

# Read Silver loans
silver_loans = spark.table(
    "workspace.finflow_silver.loans"
)

# Read customer signup dates
silver_customers = (
    spark.table("workspace.finflow_silver.customers")
    .select(
        "customer_id",
        F.col("signup_date").alias("_signup_date")
    )
)

# Join loans with customers
loan_check = (
    silver_loans.alias("l")
    .join(
        silver_customers.alias("c"),
        F.col("l.customer_id") == F.col("c.customer_id"),
        "left"
    )
)

# Business validation rule
before_signup = (
    F.col("c._signup_date").isNotNull()
    & (
        F.col("l.issue_date") < F.col("c._signup_date")
    )
)

# Preserve original loan columns
loan_columns = [
    F.col(f"l.`{column}`").alias(column)
    for column in silver_loans.columns
]

# Invalid loans
quarantine_loans = (
    loan_check
    .filter(before_signup)
    .select(*loan_columns)
    .withColumn(
        "_rejection_reason",
        F.lit("LOAN_BEFORE_CUSTOMER_SIGNUP")
    )
)

# Valid loans
valid_loans = (
    loan_check
    .filter(~before_signup)
    .select(*loan_columns)
)

print("Total source loans:", silver_loans.count())
print("Valid loans:", valid_loans.count())
print("Quarantined loans:", quarantine_loans.count())

# COMMAND ----------

# Save invalid loans separately
quarantine_loans.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.finflow_silver.quarantine_loans")

# Update valid Silver loans
valid_loans.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.finflow_silver.loans")

print("Silver loan validation completed!")

# COMMAND ----------

valid_count = spark.table(
    "workspace.finflow_silver.loans"
).count()

quarantine_count = spark.table(
    "workspace.finflow_silver.quarantine_loans"
).count()

print("Valid loans:", valid_count)
print("Quarantined loans:", quarantine_count)
print("Total reconciled:", valid_count + quarantine_count)

assert valid_count + quarantine_count == 5000

print("LOAN RECONCILIATION PASSED")
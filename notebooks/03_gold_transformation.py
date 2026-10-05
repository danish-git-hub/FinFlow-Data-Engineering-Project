# Databricks notebook source
# MAGIC %md
# MAGIC ### Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC Create the Gold schema.

# COMMAND ----------

# =====================================================
# FINFLOW - GOLD LAYER
# STEP 1: CREATE GOLD SCHEMA
# =====================================================

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.finflow_gold
""")

print("FinFlow Gold schema created successfully!")

# Verify schema
spark.sql("""
SHOW SCHEMAS IN workspace
""").show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

# Generate date range: 2023-01-01 to 2026-12-31
date_range = spark.sql("""
    SELECT explode(
        sequence(
            DATE '2023-01-01',
            DATE '2026-12-31',
            INTERVAL 1 DAY
        )
    ) AS full_date
""")

# Create date dimension
dim_date = (
    date_range
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("day_of_week", F.dayofweek("full_date"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
    .withColumn("month_end_date", F.last_day("full_date"))
)

# Verify BEFORE saving
print("Generated date records:", dim_date.count())

dim_date.select(
    F.min("full_date").alias("earliest_date"),
    F.max("full_date").alias("latest_date")
).show()

# Save updated dimension
dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.finflow_gold.dim_date")

print("Gold dim_date updated successfully!")

# COMMAND ----------

# =====================================================
# FINFLOW GOLD LAYER
# STEP 3: CREATE CUSTOMER DIMENSION
# SCD TYPE 2 INITIAL LOAD
# =====================================================

from pyspark.sql.functions import (
    col,
    lit,
    current_date,
    to_date
)

# Read Silver customers
customers_silver = spark.table(
    "workspace.finflow_silver.customers"
)

# Create customer dimension
dim_customer = (
    customers_silver
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "state",
        "customer_segment",
        "signup_date",
        "customer_status"
    )
    .withColumn(
        "effective_from",
        current_date()
    )
    .withColumn(
        "effective_to",
        to_date(lit("9999-12-31"))
    )
    .withColumn(
        "is_current",
        lit(True)
    )
    .withColumn(
        "customer_key",
        col("customer_id")
    )
)

# Save customer dimension
dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.finflow_gold.dim_customer")

print("dim_customer created successfully!")

print("Total customer records:", dim_customer.count())

dim_customer.printSchema()

dim_customer.show(10, truncate=False)

# COMMAND ----------


## STEP 3: Create dim_account
from pyspark.sql.functions import col, upper, trim

# Read Silver accounts
silver_accounts = spark.table("workspace.finflow_silver.accounts")

# Create Gold dimension
dim_account = (
    silver_accounts
    .select(
        col("account_id"),
        col("customer_id"),
        col("branch_id"),
        col("account_type"),
        col("balance"),
        col("currency"),
        col("account_status"),
        col("opened_date")
    )
    .withColumn("account_key", col("account_id"))
)

# Save as Delta table
(
    dim_account.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.finflow_gold.dim_account")
)

print("dim_account created successfully!")
print(f"Total account records: {dim_account.count()}")

dim_account.printSchema()

# COMMAND ----------

# creat dim_branch
# Read Silver branches
silver_branches = spark.table("workspace.finflow_silver.branches")

# Create Gold dimension
dim_branch = (
    silver_branches
    .select(
        col("branch_id"),
        col("branch_name"),
        col("city"),
        col("state"),
        col("branch_type"),
        col("opening_date")
    )
    .withColumn("branch_key", col("branch_id"))
)

# Save as Delta table
(
    dim_branch.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.finflow_gold.dim_branch")
)

print("dim_branch created successfully!")
print(f"Total branch records: {dim_branch.count()}")

dim_branch.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Build fact_transaction

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, date_format, when
)

# 1. Read Silver transactions
silver_transactions = spark.table(
    "workspace.finflow_silver.transactions"
)

# 2. Read Gold dimensions
customers = spark.table(
    "workspace.finflow_gold.dim_customer"
)

accounts = spark.table(
    "workspace.finflow_gold.dim_account"
)

branches = spark.table(
    "workspace.finflow_gold.dim_branch"
)

dates = spark.table(
    "workspace.finflow_gold.dim_date"
)

# 3. Convert transaction timestamp into date
transactions = silver_transactions.withColumn(
    "transaction_date",
    to_date(col("transaction_timestamp"))
)

# 4. Join transactions with customer dimension
fact = (
    transactions.alias("t")

    .join(
        customers.alias("c"),
        (
            (col("t.customer_id") == col("c.customer_id")) &
            (col("t.transaction_date") >= col("c.effective_from")) &
            (col("t.transaction_date") <= col("c.effective_to"))
        ),
        "left"
    )

    # 5. Join account dimension
    .join(
        accounts.alias("a"),
        col("t.account_id") == col("a.account_id"),
        "left"
    )

    # 6. Join branch dimension through account
    .join(
        branches.alias("b"),
        col("a.branch_id") == col("b.branch_id"),
        "left"
    )

    # 7. Join date dimension
    .join(
        dates.alias("d"),
        col("t.transaction_date") == col("d.full_date"),
        "left"
    )

    # 8. Select fact table columns
    .select(
        col("t.transaction_id"),
        col("c.customer_key"),
        col("a.account_key"),
        col("b.branch_key"),
        col("d.date_key"),
        col("t.amount"),
        col("t.currency"),
        col("t.transaction_type"),
        col("t.channel"),
        col("t.status"),
        col("t.transaction_timestamp")
    )
)

print("Transaction fact data prepared!")
print(f"Total fact records: {fact.count()}")

fact.printSchema()

# COMMAND ----------

fact.select(
    F.count("*").alias("total_records"),

    F.sum(
        F.when(F.col("customer_key").isNull(), 1)
         .otherwise(0)
    ).alias("missing_customer_key"),

    F.sum(
        F.when(F.col("account_key").isNull(), 1)
         .otherwise(0)
    ).alias("missing_account_key"),

    F.sum(
        F.when(F.col("branch_key").isNull(), 1)
         .otherwise(0)
    ).alias("missing_branch_key"),

    F.sum(
        F.when(F.col("date_key").isNull(), 1)
         .otherwise(0)
    ).alias("missing_date_key")
).show()

# COMMAND ----------

# =====================================================
# FINFLOW - FIX CUSTOMER DIMENSION
# Initial historical load
# =====================================================

from pyspark.sql import functions as F

customers_silver = spark.table(
    "workspace.finflow_silver.customers"
)

dim_customer = (
    customers_silver
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "state",
        "customer_segment",
        "signup_date",
        "customer_status"
    )
    .withColumn(
        "effective_from",
        F.col("signup_date")
    )
    .withColumn(
        "effective_to",
        F.to_date(F.lit("9999-12-31"))
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
    .withColumn(
        "customer_key",
        F.col("customer_id")
    )
)

dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.finflow_gold.dim_customer")

print("Customer dimension corrected!")
print("Records:", dim_customer.count())

dim_customer.select(
    "customer_id",
    "signup_date",
    "effective_from",
    "effective_to",
    "is_current"
).show(10, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

silver_transactions = spark.table("workspace.finflow_silver.transactions")
silver_customers = spark.table("workspace.finflow_silver.customers")

tx = (
    silver_transactions
    .withColumn(
        "transaction_date",
        F.to_date("transaction_timestamp")
    )
)

customer_check = (
    tx.alias("t")
    .join(
        silver_customers.alias("c"),
        F.col("t.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .withColumn(
        "match_reason",
        F.when(
            F.col("c.customer_id").isNull(),
            "CUSTOMER_NOT_FOUND"
        )
        .when(
            F.col("c.signup_date").isNull(),
            "NULL_SIGNUP_DATE"
        )
        .when(
            F.col("t.transaction_date") < F.col("c.signup_date"),
            "TRANSACTION_BEFORE_SIGNUP"
        )
        .otherwise("VALID_CUSTOMER_DATE")
    )
)

customer_check.groupBy("match_reason").count().show()

# COMMAND ----------

customer_check.filter(
    F.col("match_reason") == "TRANSACTION_BEFORE_SIGNUP"
).select(
    F.col("t.transaction_id").alias("transaction_id"),
    F.col("t.customer_id").alias("customer_id"),
    F.col("t.transaction_date").alias("transaction_date"),
    F.col("c.signup_date").alias("signup_date")
).show(10, truncate=False)

# COMMAND ----------

# Check existing Silver transaction schema
spark.table(
    "workspace.finflow_silver.transactions"
).printSchema()

# Check existing quarantine schema
spark.table(
    "workspace.finflow_silver.quarantine_transactions"
).printSchema()

# COMMAND ----------

# Check existing transaction and quarantine counts
print("Valid Silver transactions:")
print(
    spark.table("workspace.finflow_silver.transactions").count()
)

print("Existing quarantined transactions:")
print(
    spark.table("workspace.finflow_silver.quarantine_transactions").count()
)

# COMMAND ----------

from pyspark.sql import functions as F

# Read valid Silver transactions
silver_transactions = spark.table(
    "workspace.finflow_silver.transactions"
)

# Read Gold dimensions
customers = spark.table("workspace.finflow_gold.dim_customer")
accounts = spark.table("workspace.finflow_gold.dim_account")
branches = spark.table("workspace.finflow_gold.dim_branch")
dates = spark.table("workspace.finflow_gold.dim_date")

print("Silver transactions:", silver_transactions.count())
print("Customer dimension:", customers.count())
print("Account dimension:", accounts.count())
print("Branch dimension:", branches.count())
print("Date dimension:", dates.count())

# COMMAND ----------

# Add transaction date
transactions = silver_transactions.withColumn(
    "transaction_date",
    F.to_date("transaction_timestamp")
)

# Join transactions with dimensions
fact = (
    transactions.alias("t")

    # Customer dimension: temporal join
    .join(
        customers.alias("c"),
        (F.col("t.customer_id") == F.col("c.customer_id")) &
        (F.col("t.transaction_date") >= F.col("c.effective_from")) &
        (F.col("t.transaction_date") <= F.col("c.effective_to")),
        "left"
    )

    # Account dimension
    .join(
        accounts.alias("a"),
        F.col("t.account_id") == F.col("a.account_id"),
        "left"
    )

    # Branch dimension
    .join(
        branches.alias("b"),
        F.col("a.branch_id") == F.col("b.branch_id"),
        "left"
    )

    # Date dimension
    .join(
        dates.alias("d"),
        F.col("t.transaction_date") == F.col("d.full_date"),
        "left"
    )

    # Select fact table columns
    .select(
        F.col("t.transaction_id"),
        F.col("c.customer_key"),
        F.col("a.account_key"),
        F.col("b.branch_key"),
        F.col("d.date_key"),
        F.col("t.amount"),
        F.col("t.currency"),
        F.col("t.transaction_type"),
        F.col("t.channel"),
        F.col("t.status"),
        F.col("t.transaction_timestamp")
    )
)

print("Fact records:", fact.count())

# COMMAND ----------

fact.select(
    F.count("*").alias("total_records"),

    F.sum(
        F.when(F.col("customer_key").isNull(), 1).otherwise(0)
    ).alias("missing_customer_key"),

    F.sum(
        F.when(F.col("account_key").isNull(), 1).otherwise(0)
    ).alias("missing_account_key"),

    F.sum(
        F.when(F.col("branch_key").isNull(), 1).otherwise(0)
    ).alias("missing_branch_key"),

    F.sum(
        F.when(F.col("date_key").isNull(), 1).otherwise(0)
    ).alias("missing_date_key")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### save the gold table

# COMMAND ----------

# Save Gold fact transaction table

fact.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.finflow_gold.fact_transaction")

print("fact_transaction table created successfully!")

# COMMAND ----------

gold_fact = spark.table(
    "workspace.finflow_gold.fact_transaction"
)

print("Total Gold transactions:", gold_fact.count())

gold_fact.printSchema()

display(gold_fact.limit(10))

# COMMAND ----------

silver_loans = spark.table(
    "workspace.finflow_silver.loans"
)

print("Silver loan records:", silver_loans.count())

silver_loans.printSchema()

display(silver_loans.limit(10))

# COMMAND ----------

print("Customer dimension:")
spark.table(
    "workspace.finflow_gold.dim_customer"
).printSchema()

print("Branch dimension:")
spark.table(
    "workspace.finflow_gold.dim_branch"
).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare the loan fact DataFrame

# COMMAND ----------

from pyspark.sql import functions as F

# Read Silver loans
silver_loans = spark.table(
    "workspace.finflow_silver.loans"
)

# Read Gold dimensions
customers = spark.table(
    "workspace.finflow_gold.dim_customer"
)

dates = spark.table(
    "workspace.finflow_gold.dim_date"
)

# Join loans with customer and date dimensions
fact_loan = (
    silver_loans.alias("l")

    # Customer dimension: temporal join
    .join(
        customers.alias("c"),
        (F.col("l.customer_id") == F.col("c.customer_id")) &
        (F.col("l.issue_date") >= F.col("c.effective_from")) &
        (F.col("l.issue_date") <= F.col("c.effective_to")),
        "left"
    )

    # Date dimension
    .join(
        dates.alias("d"),
        F.col("l.issue_date") == F.col("d.full_date"),
        "left"
    )

    # Select fact table columns
    .select(
        F.col("l.loan_id"),
        F.col("c.customer_key"),
        F.col("d.date_key"),
        F.col("l.loan_amount"),
        F.col("l.interest_rate"),
        F.col("l.tenure_months"),
        F.col("l.loan_type"),
        F.col("l.loan_status"),
        F.col("l.issue_date")
    )
)

print("Prepared loan fact records:", fact_loan.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Validate dimension keys

# COMMAND ----------

from pyspark.sql import functions as F

silver_loans = spark.table("workspace.finflow_silver.loans")

dim_customer = spark.table("workspace.finflow_gold.dim_customer")

dim_date = spark.table("workspace.finflow_gold.dim_date")

print("Valid Silver loans:", silver_loans.count())
print("Customer dimension:", dim_customer.count())
print("Date dimension:", dim_date.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Create the loan fact table

# COMMAND ----------

fact_loan = (
    silver_loans.alias("l")
    .join(
        dim_customer.alias("c"),
        (F.col("l.customer_id") == F.col("c.customer_id")) &
        (F.col("l.issue_date") >= F.col("c.effective_from")) &
        (F.col("l.issue_date") <= F.col("c.effective_to")),
        "left"
    )
    .join(
        dim_date.alias("d"),
        F.col("l.issue_date") == F.col("d.full_date"),
        "left"
    )
    .select(
        F.col("l.loan_id"),
        F.col("c.customer_key"),
        F.col("d.date_key"),
        F.col("l.loan_amount"),
        F.col("l.interest_rate"),
        F.col("l.tenure_months"),
        F.col("l.loan_type"),
        F.col("l.loan_status"),
        F.col("l.issue_date")
    )
)

print("Fact loan records:", fact_loan.count())

# COMMAND ----------

fact_loan.select(
    F.count("*").alias("total_records"),
    F.countDistinct("loan_id").alias("distinct_loan_ids"),
    F.sum(F.col("customer_key").isNull().cast("int")).alias("missing_customer_key"),
    F.sum(F.col("date_key").isNull().cast("int")).alias("missing_date_key"),
    F.sum(F.col("loan_id").isNull().cast("int")).alias("missing_loan_id")
).show()

# COMMAND ----------

fact_loan.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.finflow_gold.fact_loan")

print("Gold fact_loan table created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Analytical Tables — Business Aggregations.

# COMMAND ----------

# MAGIC %md
# MAGIC Read the Gold fact table

# COMMAND ----------

from pyspark.sql import functions as F

fact_transaction = spark.table(
    "workspace.finflow_gold.fact_transaction"
)

print("Transaction records:", fact_transaction.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Create daily transaction aggregations

# COMMAND ----------

daily_transaction_summary = (
    fact_transaction
    .groupBy(
        F.to_date("transaction_timestamp").alias("transaction_date")
    )
    .agg(
        F.count("*").alias("total_transactions"),

        F.sum("amount").alias("total_transaction_amount"),

        F.sum(
            F.when(F.col("status") == "SUCCESS", 1).otherwise(0)
        ).alias("successful_transactions"),

        F.sum(
            F.when(F.col("status") == "FAILED", 1).otherwise(0)
        ).alias("failed_transactions"),

        F.sum(
            F.when(F.col("status") == "PENDING", 1).otherwise(0)
        ).alias("pending_transactions"),

        F.sum(
            F.when(F.col("status") == "SUCCESS", F.col("amount"))
             .otherwise(0)
        ).alias("successful_transaction_amount")
    )
    .withColumn(
        "success_rate",
        F.round(
            F.col("successful_transactions") /
            F.col("total_transactions") * 100,
            2
        )
    )
    .orderBy("transaction_date")
)

display(daily_transaction_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC Validate the aggregation

# COMMAND ----------

daily_transaction_summary.select(
    F.sum("total_transactions").alias("total_transactions"),
    F.sum("successful_transactions").alias("successful"),
    F.sum("failed_transactions").alias("failed"),
    F.sum("pending_transactions").alias("pending")
).show()

# COMMAND ----------

daily_transaction_summary = (
    daily_transaction_summary
    .withColumn(
        "total_transaction_amount",
        F.round("total_transaction_amount", 2)
    )
    .withColumn(
        "successful_transaction_amount",
        F.round("successful_transaction_amount", 2)
    )
)

display(daily_transaction_summary)

# COMMAND ----------

daily_transaction_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.finflow_gold.daily_transaction_summary"
    )

print("daily_transaction_summary created successfully!")

# COMMAND ----------

daily_transaction_summary.select(
    F.sum("total_transactions").alias("total_transactions"),
    F.sum("successful_transactions").alias("successful"),
    F.sum("failed_transactions").alias("failed"),
    F.sum("pending_transactions").alias("pending")
).show()

# COMMAND ----------

daily_transaction_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.finflow_gold.daily_transaction_summary"
    )

print("daily_transaction_summary created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC # Customer Analytics.

# COMMAND ----------

# Read the Gold tables
from pyspark.sql import functions as F

fact_transaction = spark.table(
    "workspace.finflow_gold.fact_transaction"
)

dim_customer = spark.table(
    "workspace.finflow_gold.dim_customer"
)

print("Transactions:", fact_transaction.count())
print("Customers:", dim_customer.count())

# COMMAND ----------

#Aggregate customer transactions

customer_transaction_metrics = (
    fact_transaction
    .groupBy("customer_key")
    .agg(
        F.count("*").alias("total_transactions"),

        F.sum(
            F.when(F.col("status") == "SUCCESS", 1).otherwise(0)
        ).alias("successful_transactions"),

        F.sum(
            F.when(F.col("status") == "FAILED", 1).otherwise(0)
        ).alias("failed_transactions"),

        F.sum(
            F.when(F.col("status") == "PENDING", 1).otherwise(0)
        ).alias("pending_transactions"),

        F.sum(
            F.when(
                F.col("status") == "SUCCESS",
                F.col("amount")
            ).otherwise(0)
        ).alias("total_successful_amount"),

        F.avg(
            F.when(
                F.col("status") == "SUCCESS",
                F.col("amount")
            )
        ).alias("average_successful_transaction_amount"),

        F.max("transaction_timestamp").alias("last_transaction_date")
    )
)

# COMMAND ----------

# Join customer details
customer_analytics = (
    dim_customer.alias("c")
    .join(
        customer_transaction_metrics.alias("t"),
        F.col("c.customer_key") == F.col("t.customer_key"),
        "left"
    )
    .select(
        F.col("c.customer_key"),
        F.col("c.first_name"),
        F.col("c.last_name"),
        F.col("c.city"),
        F.col("c.state"),
        F.col("c.customer_segment"),
        F.col("c.customer_status"),

        F.coalesce(
            F.col("t.total_transactions"), F.lit(0)
        ).alias("total_transactions"),

        F.coalesce(
            F.col("t.successful_transactions"), F.lit(0)
        ).alias("successful_transactions"),

        F.coalesce(
            F.col("t.failed_transactions"), F.lit(0)
        ).alias("failed_transactions"),

        F.coalesce(
            F.col("t.pending_transactions"), F.lit(0)
        ).alias("pending_transactions"),

        F.round(
            F.coalesce(
                F.col("t.total_successful_amount"), F.lit(0)
            ), 2
        ).alias("total_successful_amount"),

        F.round(
            F.col("t.average_successful_transaction_amount"), 2
        ).alias("average_successful_transaction_amount"),

        F.col("t.last_transaction_date")
    )
    .withColumn(
        "success_rate",
        F.when(
            F.col("total_transactions") > 0,
            F.round(
                F.col("successful_transactions") /
                F.col("total_transactions") * 100,
                2
            )
        ).otherwise(0)
    )
)

# COMMAND ----------

# inspect the result
print("Customer analytics records:", customer_analytics.count())

display(
    customer_analytics.orderBy(
        F.desc("total_successful_amount")
    )
)

# COMMAND ----------

customer_analytics.select(
    F.count("*").alias("total_customers"),
    F.countDistinct("customer_key").alias("distinct_customers"),
    F.sum("total_transactions").alias("total_transactions"),
    F.sum("successful_transactions").alias("successful_transactions"),
    F.sum("failed_transactions").alias("failed_transactions"),
    F.sum("pending_transactions").alias("pending_transactions")
).show()

# COMMAND ----------

customer_analytics.filter(
    F.col("total_transactions") == 0
).count()

# COMMAND ----------

# save gold table
customer_analytics.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.finflow_gold.customer_analytics"
    )

print("customer_analytics created successfully!")

# COMMAND ----------

spark.table(
    "workspace.finflow_gold.customer_analytics"
).count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Branch Performance Analytics

# COMMAND ----------

fact_transaction = spark.table(
    "workspace.finflow_gold.fact_transaction"
)

dim_branch = spark.table(
    "workspace.finflow_gold.dim_branch"
)

# COMMAND ----------

# Aggregate transactions by branch
branch_metrics = (
    fact_transaction
    .groupBy("branch_key")
    .agg(
        F.count("*").alias("total_transactions"),

        F.sum("amount").alias("total_transaction_amount"),

        F.sum(
            F.when(F.col("status") == "SUCCESS", 1).otherwise(0)
        ).alias("successful_transactions"),

        F.sum(
            F.when(F.col("status") == "FAILED", 1).otherwise(0)
        ).alias("failed_transactions"),

        F.sum(
            F.when(F.col("status") == "PENDING", 1).otherwise(0)
        ).alias("pending_transactions"),

        F.sum(
            F.when(
                F.col("status") == "SUCCESS",
                F.col("amount")
            ).otherwise(0)
        ).alias("successful_transaction_amount")
    )
)

# COMMAND ----------

# Join branch information
branch_performance = (
    dim_branch.alias("b")
    .join(
        branch_metrics.alias("m"),
        F.col("b.branch_key") == F.col("m.branch_key"),
        "left"
    )
    .select(
        F.col("b.branch_key"),
        F.col("b.branch_name"),
        F.col("b.city"),
        F.col("b.state"),
        F.col("b.branch_type"),

        F.coalesce(
            F.col("m.total_transactions"), F.lit(0)
        ).alias("total_transactions"),

        F.round(
            F.coalesce(
                F.col("m.total_transaction_amount"), F.lit(0)
            ), 2
        ).alias("total_transaction_amount"),

        F.coalesce(
            F.col("m.successful_transactions"), F.lit(0)
        ).alias("successful_transactions"),

        F.coalesce(
            F.col("m.failed_transactions"), F.lit(0)
        ).alias("failed_transactions"),

        F.coalesce(
            F.col("m.pending_transactions"), F.lit(0)
        ).alias("pending_transactions"),

        F.round(
            F.coalesce(
                F.col("m.successful_transaction_amount"), F.lit(0)
            ), 2
        ).alias("successful_transaction_amount")
    )
    .withColumn(
        "success_rate",
        F.when(
            F.col("total_transactions") > 0,
            F.round(
                F.col("successful_transactions") /
                F.col("total_transactions") * 100,
                2
            )
        ).otherwise(0)
    )
)

# COMMAND ----------

# display Result
print("Branch performance records:", branch_performance.count())

display(
    branch_performance.orderBy(
        F.desc("total_transaction_amount")
    )
)

# COMMAND ----------

branch_performance.select(
    F.count("*").alias("total_branches"),
    F.countDistinct("branch_key").alias("distinct_branches"),
    F.sum("total_transactions").alias("total_transactions"),
    F.sum("successful_transactions").alias("successful_transactions"),
    F.sum("failed_transactions").alias("failed_transactions"),
    F.sum("pending_transactions").alias("pending_transactions")
).show()

# COMMAND ----------

branch_performance.filter(
    F.col("total_transactions") == 0
).count()

# COMMAND ----------

branch_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.finflow_gold.branch_performance"
    )

print("branch_performance created successfully!")

# COMMAND ----------

spark.table(
    "workspace.finflow_gold.branch_performance"
).count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Loan Portfolio Analytics.

# COMMAND ----------

# read gold fact table
from pyspark.sql import functions as F

fact_loan = spark.table(
    "workspace.finflow_gold.fact_loan"
)

print("Valid loan records:", fact_loan.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Create the loan portfolio summary

# COMMAND ----------

loan_portfolio_summary = (
    fact_loan
    .groupBy(
        "loan_type",
        "loan_status"
    )
    .agg(
        F.count("*").alias("total_loans"),

        F.sum("loan_amount").alias("total_loan_amount"),

        F.avg("loan_amount").alias("average_loan_amount"),

        F.avg("interest_rate").alias("average_interest_rate"),

        F.avg("tenure_months").alias("average_tenure_months"),

        F.min("loan_amount").alias("minimum_loan_amount"),

        F.max("loan_amount").alias("maximum_loan_amount")
    )
    .withColumn(
        "total_loan_amount",
        F.round("total_loan_amount", 2)
    )
    .withColumn(
        "average_loan_amount",
        F.round("average_loan_amount", 2)
    )
    .withColumn(
        "average_interest_rate",
        F.round("average_interest_rate", 2)
    )
    .withColumn(
        "average_tenure_months",
        F.round("average_tenure_months", 2)
    )
    .withColumn(
        "minimum_loan_amount",
        F.round("minimum_loan_amount", 2)
    )
    .withColumn(
        "maximum_loan_amount",
        F.round("maximum_loan_amount", 2)
    )
    .orderBy("loan_type", "loan_status")
)

display(loan_portfolio_summary)

# COMMAND ----------

# validation
loan_portfolio_summary.select(
    F.sum("total_loans").alias("total_loans"),
    F.sum("total_loan_amount").alias("total_loan_amount")
).show()

# COMMAND ----------

fact_loan.groupBy("loan_status").count().orderBy("loan_status").show()

# COMMAND ----------

fact_loan.select(
    F.sum("loan_amount").alias("fact_total_loan_amount")
).show()

loan_portfolio_summary.select(
    F.sum("total_loan_amount").alias("summary_total_loan_amount")
).show()

# COMMAND ----------

loan_portfolio_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.finflow_gold.loan_portfolio_summary"
    )

print("loan_portfolio_summary created successfully!")

# COMMAND ----------

spark.table(
    "workspace.finflow_gold.loan_portfolio_summary"
).count()
# Databricks notebook source
# MAGIC %md
# MAGIC # FinFlow — Gold Layer
# MAGIC
# MAGIC Build the analytical Gold layer from validated Silver data.
# MAGIC
# MAGIC Outputs:
# MAGIC - Dimensions: `dim_date`, `dim_customer`, `dim_account`, `dim_branch`
# MAGIC - Facts: `fact_transaction`, `fact_loan`
# MAGIC - Analytics: daily transaction summary, customer analytics, branch performance, loan portfolio summary
# MAGIC
# MAGIC The customer dimension is an **SCD2-compatible initial snapshot** for this capstone; a full historical SCD Type 2 change-tracking process is outside the implemented scope.

# COMMAND ----------

from pyspark.sql import functions as F

GOLD_DB = "workspace.finflow_gold"
SILVER_DB = "workspace.finflow_silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_DB}")
print("Gold schema is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Date Dimension

# COMMAND ----------

date_range = spark.sql("""
    SELECT explode(
        sequence(
            DATE '2023-01-01',
            DATE '2026-12-31',
            INTERVAL 1 DAY
        )
    ) AS full_date
""")

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

print("Date dimension records:", dim_date.count())

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD_DB}.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customer Dimension
# MAGIC
# MAGIC Initial SCD2-compatible snapshot using customer signup date as `effective_from`.

# COMMAND ----------

customers_silver = spark.table(f"{SILVER_DB}.customers")

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
    .withColumn("effective_from", F.to_date("signup_date"))
    .withColumn("effective_to", F.to_date(F.lit("9999-12-31")))
    .withColumn("is_current", F.lit(True))
    .withColumn("customer_key", F.col("customer_id"))
)

dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD_DB}.dim_customer")

print("Customer dimension records:", dim_customer.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Account Dimension

# COMMAND ----------

silver_accounts = spark.table(f"{SILVER_DB}.accounts")

dim_account = (
    silver_accounts
    .select(
        "account_id",
        "customer_id",
        "branch_id",
        "account_type",
        "balance",
        "currency",
        "account_status",
        "opened_date"
    )
    .withColumn("account_key", F.col("account_id"))
)

dim_account.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD_DB}.dim_account")

print("Account dimension records:", dim_account.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Branch Dimension

# COMMAND ----------

silver_branches = spark.table(f"{SILVER_DB}.branches")

dim_branch = (
    silver_branches
    .select(
        "branch_id",
        "branch_name",
        "city",
        "state",
        "branch_type",
        "opening_date"
    )
    .withColumn("branch_key", F.col("branch_id"))
)

dim_branch.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD_DB}.dim_branch")

print("Branch dimension records:", dim_branch.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transaction Fact
# MAGIC
# MAGIC Grain: one row per valid Silver transaction.

# COMMAND ----------

silver_transactions = spark.table(f"{SILVER_DB}.transactions")
customers = spark.table(f"{GOLD_DB}.dim_customer")
accounts = spark.table(f"{GOLD_DB}.dim_account")
branches = spark.table(f"{GOLD_DB}.dim_branch")
dates = spark.table(f"{GOLD_DB}.dim_date")

transactions = silver_transactions.withColumn(
    "transaction_date",
    F.to_date("transaction_timestamp")
)

fact_transaction = (
    transactions.alias("t")
    .join(
        customers.alias("c"),
        (F.col("t.customer_id") == F.col("c.customer_id")) &
        (F.col("t.transaction_date") >= F.col("c.effective_from")) &
        (F.col("t.transaction_date") <= F.col("c.effective_to")),
        "left"
    )
    .join(
        accounts.alias("a"),
        F.col("t.account_id") == F.col("a.account_id"),
        "left"
    )
    .join(
        branches.alias("b"),
        F.col("a.branch_id") == F.col("b.branch_id"),
        "left"
    )
    .join(
        dates.alias("d"),
        F.col("t.transaction_date") == F.col("d.full_date"),
        "left"
    )
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

print("Transaction fact records:", fact_transaction.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate transaction dimension keys

# COMMAND ----------

fact_transaction.select(
    F.count("*").alias("total_records"),
    F.sum(F.col("customer_key").isNull().cast("int")).alias("missing_customer_key"),
    F.sum(F.col("account_key").isNull().cast("int")).alias("missing_account_key"),
    F.sum(F.col("branch_key").isNull().cast("int")).alias("missing_branch_key"),
    F.sum(F.col("date_key").isNull().cast("int")).alias("missing_date_key")
).show()

missing_transaction_keys = fact_transaction.filter(
    F.col("customer_key").isNull() |
    F.col("account_key").isNull() |
    F.col("branch_key").isNull() |
    F.col("date_key").isNull()
).count()

assert missing_transaction_keys == 0, (
    f"Transaction fact contains {missing_transaction_keys} rows with missing dimension keys."
)

fact_transaction.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD_DB}.fact_transaction")

print("fact_transaction created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Loan Fact
# MAGIC
# MAGIC Grain: one row per valid Silver loan.

# COMMAND ----------

silver_loans = spark.table(f"{SILVER_DB}.loans")

fact_loan = (
    silver_loans.alias("l")
    .join(
        customers.alias("c"),
        (F.col("l.customer_id") == F.col("c.customer_id")) &
        (F.col("l.issue_date") >= F.col("c.effective_from")) &
        (F.col("l.issue_date") <= F.col("c.effective_to")),
        "left"
    )
    .join(
        dates.alias("d"),
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

print("Loan fact records:", fact_loan.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate loan dimension keys

# COMMAND ----------

fact_loan.select(
    F.count("*").alias("total_records"),
    F.countDistinct("loan_id").alias("distinct_loan_ids"),
    F.sum(F.col("customer_key").isNull().cast("int")).alias("missing_customer_key"),
    F.sum(F.col("date_key").isNull().cast("int")).alias("missing_date_key"),
    F.sum(F.col("loan_id").isNull().cast("int")).alias("missing_loan_id")
).show()

missing_loan_keys = fact_loan.filter(
    F.col("loan_id").isNull() |
    F.col("customer_key").isNull() |
    F.col("date_key").isNull()
).count()

assert missing_loan_keys == 0, (
    f"Loan fact contains {missing_loan_keys} rows with missing required keys."
)

fact_loan.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD_DB}.fact_loan")

print("fact_loan created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Daily Transaction Summary

# COMMAND ----------

daily_transaction_summary = (
    fact_transaction
    .groupBy(
        F.to_date("transaction_timestamp").alias("transaction_date")
    )
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount").alias("total_transaction_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))
            .alias("successful_transactions"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))
            .alias("failed_transactions"),
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0))
            .alias("pending_transactions"),
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
    .withColumn("total_transaction_amount", F.round("total_transaction_amount", 2))
    .withColumn("successful_transaction_amount", F.round("successful_transaction_amount", 2))
    .orderBy("transaction_date")
)

daily_transaction_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD_DB}.daily_transaction_summary")

print("Daily transaction summary records:", daily_transaction_summary.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Customer Analytics

# COMMAND ----------

customer_transaction_metrics = (
    fact_transaction
    .groupBy("customer_key")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))
            .alias("successful_transactions"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))
            .alias("failed_transactions"),
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0))
            .alias("pending_transactions"),
        F.sum(
            F.when(F.col("status") == "SUCCESS", F.col("amount"))
             .otherwise(0)
        ).alias("total_successful_amount"),
        F.avg(
            F.when(F.col("status") == "SUCCESS", F.col("amount"))
        ).alias("average_successful_transaction_amount"),
        F.max("transaction_timestamp").alias("last_transaction_date")
    )
)

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
        F.coalesce(F.col("t.total_transactions"), F.lit(0))
            .alias("total_transactions"),
        F.coalesce(F.col("t.successful_transactions"), F.lit(0))
            .alias("successful_transactions"),
        F.coalesce(F.col("t.failed_transactions"), F.lit(0))
            .alias("failed_transactions"),
        F.coalesce(F.col("t.pending_transactions"), F.lit(0))
            .alias("pending_transactions"),
        F.round(
            F.coalesce(F.col("t.total_successful_amount"), F.lit(0)), 2
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

customer_analytics.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD_DB}.customer_analytics")

print("Customer analytics records:", customer_analytics.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Branch Performance

# COMMAND ----------

branch_metrics = (
    fact_transaction
    .groupBy("branch_key")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount").alias("total_transaction_amount"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))
            .alias("successful_transactions"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))
            .alias("failed_transactions"),
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0))
            .alias("pending_transactions"),
        F.sum(
            F.when(F.col("status") == "SUCCESS", F.col("amount"))
             .otherwise(0)
        ).alias("successful_transaction_amount")
    )
)

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
        F.coalesce(F.col("m.total_transactions"), F.lit(0))
            .alias("total_transactions"),
        F.round(
            F.coalesce(F.col("m.total_transaction_amount"), F.lit(0)), 2
        ).alias("total_transaction_amount"),
        F.coalesce(F.col("m.successful_transactions"), F.lit(0))
            .alias("successful_transactions"),
        F.coalesce(F.col("m.failed_transactions"), F.lit(0))
            .alias("failed_transactions"),
        F.coalesce(F.col("m.pending_transactions"), F.lit(0))
            .alias("pending_transactions"),
        F.round(
            F.coalesce(F.col("m.successful_transaction_amount"), F.lit(0)), 2
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

branch_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD_DB}.branch_performance")

print("Branch performance records:", branch_performance.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Loan Portfolio Summary

# COMMAND ----------

loan_portfolio_summary = (
    fact_loan
    .groupBy("loan_type", "loan_status")
    .agg(
        F.count("*").alias("total_loans"),
        F.sum("loan_amount").alias("total_loan_amount"),
        F.avg("loan_amount").alias("average_loan_amount"),
        F.avg("interest_rate").alias("average_interest_rate"),
        F.avg("tenure_months").alias("average_tenure_months"),
        F.min("loan_amount").alias("minimum_loan_amount"),
        F.max("loan_amount").alias("maximum_loan_amount")
    )
    .withColumn("total_loan_amount", F.round("total_loan_amount", 2))
    .withColumn("average_loan_amount", F.round("average_loan_amount", 2))
    .withColumn("average_interest_rate", F.round("average_interest_rate", 2))
    .withColumn("average_tenure_months", F.round("average_tenure_months", 2))
    .withColumn("minimum_loan_amount", F.round("minimum_loan_amount", 2))
    .withColumn("maximum_loan_amount", F.round("maximum_loan_amount", 2))
    .orderBy("loan_type", "loan_status")
)

loan_portfolio_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{GOLD_DB}.loan_portfolio_summary")

print("Loan portfolio summary records:", loan_portfolio_summary.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Gold Layer Validation

# COMMAND ----------

gold_tables = [
    "dim_date",
    "dim_customer",
    "dim_account",
    "dim_branch",
    "fact_transaction",
    "fact_loan",
    "daily_transaction_summary",
    "customer_analytics",
    "branch_performance",
    "loan_portfolio_summary"
]

for table_name in gold_tables:
    count = spark.table(f"{GOLD_DB}.{table_name}").count()
    print(f"{table_name}: {count:,} records")

# Core reconciliation checks
silver_transaction_count = spark.table(
    f"{SILVER_DB}.transactions"
).count()

gold_transaction_count = spark.table(
    f"{GOLD_DB}.fact_transaction"
).count()

silver_loan_count = spark.table(
    f"{SILVER_DB}.loans"
).count()

gold_loan_count = spark.table(
    f"{GOLD_DB}.fact_loan"
).count()

assert gold_transaction_count == silver_transaction_count, (
    f"Transaction reconciliation failed: Silver={silver_transaction_count}, "
    f"Gold={gold_transaction_count}"
)

assert gold_loan_count == silver_loan_count, (
    f"Loan reconciliation failed: Silver={silver_loan_count}, "
    f"Gold={gold_loan_count}"
)

print("Gold transaction reconciliation: PASSED")
print("Gold loan reconciliation: PASSED")
print("FinFlow Gold layer completed successfully.")

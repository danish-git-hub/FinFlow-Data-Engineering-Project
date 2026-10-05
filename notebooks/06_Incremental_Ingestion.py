# Databricks notebook source
# ============================================================
# FinFlow - Incremental Ingestion (Job-ready educational version)
# ============================================================
# This notebook:
# 1. Discovers JSON files in the incoming Volume directory.
# 2. Loads files not yet present in the Bronze file log.
# 3. Recovers a Bronze write if the file log was not written.
# 4. Processes only source records not already accounted for in
#    Silver or quarantine.
# 5. Updates Gold facts and affected analytical summaries.
# 6. Writes a unique audit record for this execution.
#
# Note: This is an educational, single-job workflow. It is not
# a fully transactional multi-table production framework.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from uuid import uuid4
import uuid

# COMMAND ----------

# ------------------------------------------------------------
# 1. Configuration
# ------------------------------------------------------------

incoming_path = "/Volumes/workspace/default/finflow_raw/incoming_transactions"

bronze_table = "workspace.finflow_bronze.transactions"
file_log_table = "workspace.finflow_bronze.ingestion_file_log"
silver_table = "workspace.finflow_silver.transactions"
quarantine_table = "workspace.finflow_silver.quarantine_transactions"
fact_table = "workspace.finflow_gold.fact_transaction"
audit_table_name = "workspace.finflow_gold.pipeline_audit_log"

dbutils.fs.mkdirs(incoming_path)

transaction_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("channel", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("status", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("transaction_type", StringType(), True)
])

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {file_log_table} (
    file_name STRING,
    batch_id STRING,
    source_table STRING,
    records_processed BIGINT,
    ingestion_status STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------
# 2. Discover incoming JSON files
# ------------------------------------------------------------

incoming_files = sorted([
    f.path for f in dbutils.fs.ls(incoming_path)
    if f.path.lower().endswith(".json")
])

print("Incoming JSON files discovered:", len(incoming_files))
for path in incoming_files:
    print(path)

# COMMAND ----------

# ------------------------------------------------------------
# 3. Incremental Bronze ingestion + restart recovery
# ------------------------------------------------------------

processed_files = {
    row["file_name"]
    for row in (
        spark.table(file_log_table)
        .filter(F.col("ingestion_status") == "SUCCESS")
        .select("file_name")
        .distinct()
        .collect()
    )
}

newly_logged_files = []

for file_path in incoming_files:
    if file_path in processed_files:
        print("Already logged; Bronze ingestion skipped:", file_path)
        continue

    batch_id = str(uuid.uuid5(uuid.NAMESPACE_URL, file_path))
    print("\nChecking file:", file_path)

    bronze_file_exists = (
        spark.table(bronze_table)
        .filter(F.col("_source_file") == file_path)
        .limit(1)
        .count() > 0
    )

    if not bronze_file_exists:
        source_df = (
            spark.read
            .schema(transaction_schema)
            .option("multiLine", "false")
            .json(file_path)
        )

        source_count_for_file = source_df.count()
        if source_count_for_file == 0:
            raise ValueError(f"Input file is empty: {file_path}")

        bronze_df = (
            source_df
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source_file", F.lit(file_path))
            .withColumn("_batch_id", F.lit(batch_id))
        )

        (
            bronze_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(bronze_table)
        )
        print("Bronze records appended:", source_count_for_file)

    else:
        source_count_for_file = (
            spark.table(bronze_table)
            .filter(F.col("_source_file") == file_path)
            .count()
        )
        print(
            "Bronze records already exist; recovering missing file-log entry:",
            source_count_for_file
        )

    log_df = spark.createDataFrame(
        [(
            file_path,
            batch_id,
            "transactions",
            int(source_count_for_file),
            "SUCCESS"
        )],
        [
            "file_name",
            "batch_id",
            "source_table",
            "records_processed",
            "ingestion_status"
        ]
    ).withColumn("ingestion_timestamp", F.current_timestamp())

    (
        log_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(file_log_table)
    )

    newly_logged_files.append(file_path)
    print("Bronze file log written:", file_path)

print("\nBronze ingestion stage completed.")
print("New file-log entries in this run:", len(newly_logged_files))

# All JSON files currently in the incoming folder are considered
# for downstream recovery. This lets a retry finish Silver/Gold
# work even if Bronze/file-log succeeded in an earlier attempt.
available_bronze_files = [
    path for path in incoming_files
    if (
        spark.table(bronze_table)
        .filter(F.col("_source_file") == path)
        .limit(1)
        .count() > 0
    )
]

print("Incoming files available in Bronze:", len(available_bronze_files))

# COMMAND ----------

# ------------------------------------------------------------
# 4. Identify source records not yet processed by Silver
#    or quarantine. The file + record key prevents replay.
# ------------------------------------------------------------

record_columns = [
    "account_id", "amount", "channel", "currency", "customer_id",
    "merchant_category", "status", "transaction_id",
    "transaction_timestamp", "transaction_type"
]

def add_record_key(df):
    # transaction_id is the normal record key. For a missing ID,
    # use a hash of the source business fields to avoid replaying
    # the same malformed row on every execution.
    fallback_hash = F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>"))
              for c in record_columns]
        ),
        256
    )
    return df.withColumn(
        "_record_key",
        F.when(
            F.col("transaction_id").isNotNull() &
            (F.trim(F.col("transaction_id")) != ""),
            F.col("transaction_id")
        ).otherwise(fallback_hash)
    )

bronze_for_incoming = (
    spark.table(bronze_table)
    .filter(F.col("_source_file").isin(available_bronze_files))
)

silver_processed_keys = (
    add_record_key(spark.table(silver_table))
    .select("_source_file", "_record_key")
)

quarantine_processed_keys = (
    add_record_key(spark.table(quarantine_table))
    .select("_source_file", "_record_key")
)

processed_record_keys = (
    silver_processed_keys
    .unionByName(quarantine_processed_keys)
    .distinct()
)

pending_bronze = (
    add_record_key(bronze_for_incoming)
    .join(
        processed_record_keys,
        on=["_source_file", "_record_key"],
        how="left_anti"
    )
    .drop("_record_key")
)

pending_source_count = pending_bronze.count()
print("\nUnprocessed Bronze records:", pending_source_count)

# COMMAND ----------

# ------------------------------------------------------------
# 5. Silver cleaning, validation and quarantine
# ------------------------------------------------------------

if pending_source_count > 0:
    cleaned = (
        pending_bronze
        .withColumn("account_id", F.trim("account_id"))
        .withColumn("customer_id", F.trim("customer_id"))
        .withColumn("transaction_id", F.trim("transaction_id"))
        .withColumn("channel", F.upper(F.trim("channel")))
        .withColumn("currency", F.upper(F.trim("currency")))
        .withColumn(
            "merchant_category",
            F.upper(F.trim("merchant_category"))
        )
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn(
            "transaction_type",
            F.upper(F.trim("transaction_type"))
        )
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp("transaction_timestamp")
        )
    )

    existing_ids = (
        spark.table(silver_table)
        .select("transaction_id")
        .where(F.col("transaction_id").isNotNull())
        .distinct()
        .withColumn("_already_in_silver", F.lit(True))
    )

    window_spec = (
        Window.partitionBy("transaction_id")
        .orderBy(F.col("_ingestion_timestamp").desc())
    )

    cleaned = (
        cleaned
        .withColumn("_row_number", F.row_number().over(window_spec))
        .join(existing_ids, on="transaction_id", how="left")
    )

    customers = (
        spark.table("workspace.finflow_silver.customers")
        .select(
            "customer_id",
            F.to_date("signup_date").alias("_signup_date")
        )
    )

    accounts = (
        spark.table("workspace.finflow_silver.accounts")
        .select(
            "account_id",
            F.col("customer_id").alias("_account_customer_id")
        )
    )

    checked = (
        cleaned
        .join(customers, on="customer_id", how="left")
        .join(accounts, on="account_id", how="left")
    )

    valid_types = [
        "UPI", "PAYMENT", "TRANSFER",
        "DEPOSIT", "ATM", "WITHDRAWAL"
    ]

    silver_reference = spark.table(silver_table)

    valid_statuses = [
        r["status"] for r in
        silver_reference.select("status").distinct().collect()
        if r["status"] is not None
    ]

    valid_channels = [
        r["channel"] for r in
        silver_reference.select("channel").distinct().collect()
        if r["channel"] is not None
    ]

    checked = checked.withColumn(
        "_rejection_reason",
        F.when(
            F.col("transaction_id").isNull() |
            (F.col("transaction_id") == ""),
            "MISSING_TRANSACTION_ID"
        )
        .when(
            F.col("_already_in_silver").isNotNull() |
            (F.col("_row_number") > 1),
            "DUPLICATE_TRANSACTION_ID"
        )
        .when(
            F.col("customer_id").isNull() |
            (F.col("customer_id") == ""),
            "MISSING_CUSTOMER_ID"
        )
        .when(
            F.col("account_id").isNull() |
            (F.col("account_id") == ""),
            "MISSING_ACCOUNT_ID"
        )
        .when(
            F.col("_signup_date").isNull(),
            "INVALID_CUSTOMER_REFERENCE"
        )
        .when(
            F.col("_account_customer_id").isNull() |
            (F.col("customer_id") != F.col("_account_customer_id")),
            "INVALID_ACCOUNT_REFERENCE"
        )
        .when(
            F.col("amount").isNull() | (F.col("amount") <= 0),
            "INVALID_AMOUNT"
        )
        .when(
            F.col("transaction_timestamp").isNull(),
            "INVALID_OR_MISSING_TIMESTAMP"
        )
        .when(
            ~F.col("transaction_type").isin(valid_types),
            "INVALID_TRANSACTION_TYPE"
        )
        .when(
            ~F.col("status").isin(valid_statuses),
            "INVALID_STATUS"
        )
        .when(
            ~F.col("channel").isin(valid_channels),
            "INVALID_CHANNEL"
        )
        .when(
            F.col("currency") != "INR",
            "INVALID_CURRENCY"
        )
        .when(
            F.to_date("transaction_timestamp") < F.col("_signup_date"),
            "TRANSACTION_BEFORE_CUSTOMER_SIGNUP"
        )
    )

    business_columns = [
        "account_id", "customer_id", "amount", "channel",
        "currency", "merchant_category", "status",
        "transaction_id", "transaction_timestamp",
        "transaction_type", "_ingestion_timestamp",
        "_source_file", "_batch_id"
    ]

    incremental_valid_df = (
        checked
        .filter(F.col("_rejection_reason").isNull())
        .select(*business_columns)
    )

    incremental_quarantine_df = (
        checked
        .filter(F.col("_rejection_reason").isNotNull())
        .select(*business_columns, "_rejection_reason")
    )

    incremental_valid_count = incremental_valid_df.count()
    incremental_rejected_count = incremental_quarantine_df.count()

    print("Pending source records:", pending_source_count)
    print("Valid records:", incremental_valid_count)
    print("Rejected records:", incremental_rejected_count)

    if pending_source_count != (
        incremental_valid_count + incremental_rejected_count
    ):
        raise ValueError("Silver reconciliation failed.")

    if incremental_valid_count > 0:
        (
            incremental_valid_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(silver_table)
        )
        print("Silver valid records appended:", incremental_valid_count)

    if incremental_rejected_count > 0:
        (
            incremental_quarantine_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(quarantine_table)
        )
        print("Quarantined records appended:", incremental_rejected_count)

else:
    print("No unprocessed Bronze records. Silver writes skipped.")

# COMMAND ----------

# ------------------------------------------------------------
# 6. Gold fact incremental processing
#    Include all Silver rows from incoming files so a retry can
#    recover if Silver succeeded but Gold did not.
# ------------------------------------------------------------

new_silver_for_incoming = (
    spark.table(silver_table)
    .filter(F.col("_source_file").isin(available_bronze_files))
)

existing_fact_ids = (
    spark.table(fact_table)
    .select("transaction_id")
    .distinct()
)

silver_not_in_fact = new_silver_for_incoming.join(
    existing_fact_ids,
    on="transaction_id",
    how="left_anti"
)

dim_customer = spark.table(
    "workspace.finflow_gold.dim_customer"
).select("customer_id", "customer_key")

dim_account = spark.table(
    "workspace.finflow_gold.dim_account"
).select("account_id", "branch_id", "account_key")

dim_branch = spark.table(
    "workspace.finflow_gold.dim_branch"
).select("branch_id", "branch_key")

dim_date = spark.table(
    "workspace.finflow_gold.dim_date"
).select("full_date", "date_key")

incremental_fact_df = (
    silver_not_in_fact.alias("t")
    .join(
        dim_customer.alias("c"),
        F.col("t.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .join(
        dim_account.alias("a"),
        F.col("t.account_id") == F.col("a.account_id"),
        "left"
    )
    .join(
        dim_branch.alias("b"),
        F.col("a.branch_id") == F.col("b.branch_id"),
        "left"
    )
    .join(
        dim_date.alias("d"),
        F.to_date(F.col("t.transaction_timestamp")) == F.col("d.full_date"),
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

missing_keys = incremental_fact_df.filter(
    F.col("customer_key").isNull() |
    F.col("account_key").isNull() |
    F.col("branch_key").isNull() |
    F.col("date_key").isNull()
).count()

new_fact_count = incremental_fact_df.count()
print("\nNew Gold fact records:", new_fact_count)
print("Records with missing dimension keys:", missing_keys)

if missing_keys > 0:
    raise ValueError(
        "Dimension key resolution failed. Gold fact append stopped."
    )

if new_fact_count > 0:
    (
        incremental_fact_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(fact_table)
    )
    print("Gold fact records appended:", new_fact_count)
else:
    print("No new Gold fact records to append.")

# COMMAND ----------

# ------------------------------------------------------------
# 7. Recalculate affected Gold summaries
#    Use all Silver transactions from incoming files joined to
#    Gold facts. This supports recovery after a partial run.
# ------------------------------------------------------------

fact = spark.table(fact_table)

batch_transaction_ids = (
    new_silver_for_incoming
    .select("transaction_id")
    .distinct()
)

gold_batch = fact.join(
    batch_transaction_ids,
    on="transaction_id",
    how="inner"
)

gold_batch_count = gold_batch.count()
print("Gold transactions included in summary refresh:", gold_batch_count)

if gold_batch_count > 0:
    affected_dates = (
        gold_batch
        .select(F.to_date("transaction_timestamp").alias("transaction_date"))
        .distinct()
    )

    affected_customers = gold_batch.select("customer_key").distinct()
    affected_branches = gold_batch.select("branch_key").distinct()

    daily_updates = (
        fact.alias("f")
        .join(
            affected_dates.alias("d"),
            F.to_date(F.col("f.transaction_timestamp")) ==
            F.col("d.transaction_date"),
            "inner"
        )
        .groupBy(
            F.to_date("f.transaction_timestamp").alias("transaction_date")
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
    )

    customer_updates = (
        fact.alias("f")
        .join(affected_customers.alias("a"), "customer_key", "inner")
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
        .withColumn(
            "success_rate",
            F.round(
                F.col("successful_transactions") /
                F.col("total_transactions") * 100,
                2
            )
        )
        
        .join(
            spark.table("workspace.finflow_gold.dim_customer").select(
                "customer_key",
                "first_name",
                "last_name",
                "city",
                "state",
                "customer_segment",
                "customer_status"
            ),
            "customer_key",
            "inner"
        )

    )

    branch_updates = (
        fact.alias("f")
        .join(affected_branches.alias("a"), "branch_key", "inner")
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
        .withColumn(
            "success_rate",
            F.round(
                F.col("successful_transactions") /
                F.col("total_transactions") * 100,
                2
            )
        )
        
        .join(
            spark.table("workspace.finflow_gold.dim_branch").select(
                "branch_key",
                "branch_name",
                "city",
                "state",
                "branch_type"
            ),
            "branch_key",
            "inner"
        )

    )

    daily_target = DeltaTable.forName(
        spark, "workspace.finflow_gold.daily_transaction_summary"
    )
    (
        daily_target.alias("t")
        .merge(
            daily_updates.alias("s"),
            "t.transaction_date = s.transaction_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    customer_target = DeltaTable.forName(
        spark, "workspace.finflow_gold.customer_analytics"
    )
    (
        customer_target.alias("t")
        .merge(
            customer_updates.alias("s"),
            "t.customer_key = s.customer_key"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    branch_target = DeltaTable.forName(
        spark, "workspace.finflow_gold.branch_performance"
    )
    (
        branch_target.alias("t")
        .merge(
            branch_updates.alias("s"),
            "t.branch_key = s.branch_key"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Gold daily, customer and branch summaries refreshed.")

else:
    print("No incoming Silver transactions in Gold; summary MERGE skipped.")

# COMMAND ----------

# ------------------------------------------------------------
# 8. Audit and reconciliation for all incoming JSON files
# ------------------------------------------------------------

source_count = (
    spark.table(bronze_table)
    .filter(F.col("_source_file").isin(available_bronze_files))
    .count()
)

valid_count = (
    spark.table(silver_table)
    .filter(F.col("_source_file").isin(available_bronze_files))
    .count()
)

rejected_count = (
    spark.table(quarantine_table)
    .filter(F.col("_source_file").isin(available_bronze_files))
    .count()
)

reconciliation_passed = (
    source_count == valid_count + rejected_count
)

reconciliation_status = (
    "PASSED" if reconciliation_passed else "FAILED"
)

pipeline_status = (
    "SUCCESS" if reconciliation_passed else "FAILED"
)

run_id = str(uuid4())
pipeline_name = "FinFlow_Incremental"

audit_schema = spark.table(audit_table_name).schema

audit_record = [(
    run_id,
    pipeline_name,
    bronze_table,
    silver_table,
    quarantine_table,
    int(source_count),
    int(valid_count),
    int(rejected_count),
    reconciliation_status,
    pipeline_status,
    None
)]

audit_df = (
    spark.createDataFrame(audit_record, schema=audit_schema)
    .withColumn("run_timestamp", F.current_timestamp())
)

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(audit_table_name)
)

print("\nAUDIT RESULTS")
print("Run ID:", run_id)
print("Source records:", source_count)
print("Valid records:", valid_count)
print("Rejected records:", rejected_count)
print("Reconciliation:", reconciliation_status)
print("Pipeline status:", pipeline_status)

if not reconciliation_passed:
    raise ValueError("Pipeline audit reconciliation failed.")

# COMMAND ----------

# ------------------------------------------------------------
# 9. Final verification
# ------------------------------------------------------------

print("\nFINAL TABLE COUNTS")

tables_to_check = [
    bronze_table,
    silver_table,
    quarantine_table,
    fact_table,
    "workspace.finflow_gold.daily_transaction_summary",
    "workspace.finflow_gold.customer_analytics",
    "workspace.finflow_gold.branch_performance"
]

for table_name in tables_to_check:
    print(table_name, "->", spark.table(table_name).count())

daily_total = (
    spark.table("workspace.finflow_gold.daily_transaction_summary")
    .agg(F.sum("total_transactions").alias("total"))
    .first()["total"]
)

gold_fact_total = spark.table(fact_table).count()

print("Gold fact transaction count:", gold_fact_total)
print("Transactions represented in daily summary:", daily_total)

if daily_total != gold_fact_total:
    raise ValueError(
        "Gold fact and daily summary transaction totals do not reconcile."
    )

print("\nFINFLOW INCREMENTAL PIPELINE COMPLETED SUCCESSFULLY.")
-- Databricks notebook source
SELECT
    loan_type,
    round(SUM(total_loans),2) AS total_loans,
    round(SUM(total_loan_amount),2) AS total_loan_amount,
    ROUND(AVG(average_loan_amount), 2) AS avg_loan_amount
FROM workspace.finflow_gold.loan_portfolio_summary
GROUP BY loan_type
ORDER BY total_loan_amount DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What percentage of loans are defaulted?

-- COMMAND ----------

SELECT
    loan_status,
    SUM(total_loans) AS total_loans,
    ROUND(
        SUM(total_loans) * 100.0 /
        SUM(SUM(total_loans)) OVER (),
        2
    ) AS percentage_of_loans
FROM workspace.finflow_gold.loan_portfolio_summary
GROUP BY loan_status
ORDER BY percentage_of_loans DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Who are the top 10 customers by successful transaction amount?

-- COMMAND ----------

SELECT
    customer_key,
    first_name,
    last_name,
    city,
    customer_segment,
    total_transactions,
    successful_transactions,
    total_successful_amount
FROM workspace.finflow_gold.customer_analytics
ORDER BY total_successful_amount DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Which customer segments generate the highest transaction value?

-- COMMAND ----------

SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_key) AS total_customers,
    SUM(c.total_transactions) AS total_transactions,
    SUM(c.successful_transactions) AS successful_transactions,
    ROUND(SUM(c.total_successful_amount), 2) AS total_successful_amount,
    ROUND(AVG(c.total_successful_amount), 2) AS avg_customer_transaction_amount
FROM workspace.finflow_gold.customer_analytics c
GROUP BY c.customer_segment
ORDER BY total_successful_amount DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Which customers have not made any valid transactions?

-- COMMAND ----------

SELECT
    customer_key,
    first_name,
    last_name,
    city,
    customer_segment,
    customer_status
FROM workspace.finflow_gold.customer_analytics
WHERE total_transactions = 0
ORDER BY customer_key;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Find customers with more than 20 transactions

-- COMMAND ----------

SELECT
    customer_key,
    first_name,
    last_name,
    customer_segment,
    total_transactions,
    successful_transactions,
    success_rate
FROM workspace.finflow_gold.customer_analytics
WHERE total_transactions > 20
ORDER BY total_transactions DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Compare customer segments by average transaction success rate

-- COMMAND ----------

SELECT
    customer_segment,
    COUNT(*) AS total_customers,
    ROUND(AVG(success_rate), 2) AS avg_success_rate,
    SUM(failed_transactions) AS total_failed_transactions
FROM workspace.finflow_gold.customer_analytics
WHERE total_transactions > 0
GROUP BY customer_segment
ORDER BY avg_success_rate DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Which branches process the highest transaction volume?

-- COMMAND ----------

SELECT
    branch_key,
    branch_name,
    city,
    state,
    branch_type,
    total_transactions
FROM workspace.finflow_gold.branch_performance
ORDER BY total_transactions DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Which branches generate the highest transaction value?

-- COMMAND ----------

SELECT
    branch_name,
    city,
    branch_type,
    total_transactions,
    ROUND(total_transaction_amount, 2) AS total_transaction_amount,
    ROUND(successful_transaction_amount, 2) AS successful_transaction_amount
FROM workspace.finflow_gold.branch_performance
ORDER BY successful_transaction_amount DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Which branches have the highest transaction failure rates?

-- COMMAND ----------

SELECT
    branch_name,
    city,
    total_transactions,
    failed_transactions,
    ROUND(
        failed_transactions * 100.0 /
        NULLIF(total_transactions, 0),
        2
    ) AS failure_rate
FROM workspace.finflow_gold.branch_performance
WHERE total_transactions > 0
ORDER BY failure_rate DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC How do Metro, Urban and Semi-Urban branches perform?

-- COMMAND ----------

SELECT
    branch_type,
    COUNT(*) AS total_branches,
    SUM(total_transactions) AS total_transactions,
    ROUND(SUM(total_transaction_amount), 2) AS total_transaction_amount,
    ROUND(
        SUM(successful_transactions) * 100.0 /
        NULLIF(SUM(total_transactions), 0),
        2
    ) AS success_rate
FROM workspace.finflow_gold.branch_performance
GROUP BY branch_type
ORDER BY total_transaction_amount DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What is the daily transaction success rate?

-- COMMAND ----------

SELECT
    transaction_date,
    total_transactions,
    successful_transactions,
    failed_transactions,
    pending_transactions,
    success_rate
FROM workspace.finflow_gold.daily_transaction_summary
ORDER BY transaction_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Which are the top 10 days by transaction volume?

-- COMMAND ----------

SELECT
    transaction_date,
    total_transactions,
    total_transaction_amount,
    successful_transaction_amount,
    success_rate
FROM workspace.finflow_gold.daily_transaction_summary
ORDER BY total_transactions DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What is the monthly transaction performance?

-- COMMAND ----------

SELECT
    DATE_FORMAT(transaction_date, 'yyyy-MM') AS transaction_month,
    SUM(total_transactions) AS total_transactions,
    SUM(successful_transactions) AS successful_transactions,
    SUM(failed_transactions) AS failed_transactions,
    SUM(pending_transactions) AS pending_transactions,
    ROUND(SUM(total_transaction_amount), 2) AS total_transaction_amount,
    ROUND(
        SUM(successful_transactions) * 100.0 /
        NULLIF(SUM(total_transactions), 0),
        2
    ) AS success_rate
FROM workspace.finflow_gold.daily_transaction_summary
GROUP BY DATE_FORMAT(transaction_date, 'yyyy-MM')
ORDER BY transaction_month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Is transaction volume increasing or decreasing each month?

-- COMMAND ----------

WITH monthly_transactions AS (
    SELECT
        DATE_FORMAT(transaction_date, 'yyyy-MM') AS transaction_month,
        SUM(total_transactions) AS total_transactions
    FROM workspace.finflow_gold.daily_transaction_summary
    GROUP BY DATE_FORMAT(transaction_date, 'yyyy-MM')
),
monthly_comparison AS (
    SELECT
        transaction_month,
        total_transactions,
        LAG(total_transactions) OVER (
            ORDER BY transaction_month
        ) AS previous_month_transactions
    FROM monthly_transactions
)
SELECT
    transaction_month,
    total_transactions,
    previous_month_transactions,
    ROUND(
        (total_transactions - previous_month_transactions) * 100.0 /
        NULLIF(previous_month_transactions, 0),
        2
    ) AS month_over_month_growth_percentage
FROM monthly_comparison
ORDER BY transaction_month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What is the average loan amount by loan type?

-- COMMAND ----------

SELECT
    loan_type,
    SUM(total_loans) AS total_loans,
    ROUND(
        SUM(total_loan_amount) / SUM(total_loans),
        2
    ) AS average_loan_amount
FROM workspace.finflow_gold.loan_portfolio_summary
GROUP BY loan_type
ORDER BY average_loan_amount DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Which loan types have the highest default rates?

-- COMMAND ----------

SELECT
    loan_type,
    SUM(total_loans) AS total_loans,
    SUM(
        CASE WHEN loan_status = 'DEFAULTED'
             THEN total_loans ELSE 0 END
    ) AS defaulted_loans,
    ROUND(
        SUM(
            CASE WHEN loan_status = 'DEFAULTED'
                 THEN total_loans ELSE 0 END
        ) * 100.0 / SUM(total_loans),
        2
    ) AS default_rate
FROM workspace.finflow_gold.loan_portfolio_summary
GROUP BY loan_type
ORDER BY default_rate DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What is the total active loan portfolio?

-- COMMAND ----------

SELECT
    SUM(total_loans) AS active_loans,
    ROUND(SUM(total_loan_amount), 2) AS active_loan_amount,
    ROUND(AVG(average_interest_rate), 2) AS average_interest_rate
FROM workspace.finflow_gold.loan_portfolio_summary
WHERE loan_status = 'ACTIVE';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Verify transaction fact-table integrity
-- MAGIC Does the Gold transaction fact table contain any missing dimension keys?

-- COMMAND ----------

SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT transaction_id) AS distinct_transactions,
    SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS missing_customer_keys,
    SUM(CASE WHEN account_key IS NULL THEN 1 ELSE 0 END) AS missing_account_keys,
    SUM(CASE WHEN branch_key IS NULL THEN 1 ELSE 0 END) AS missing_branch_keys,
    SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) AS missing_date_keys
FROM workspace.finflow_gold.fact_transaction;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Were all valid Silver transactions loaded into the Gold fact table?

-- COMMAND ----------

SELECT
    (SELECT COUNT(*)
     FROM workspace.finflow_silver.transactions) AS silver_records,

    (SELECT COUNT(*)
     FROM workspace.finflow_gold.fact_transaction) AS gold_records;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Do valid loans and quarantined loans account for the entire original loan dataset?

-- COMMAND ----------

SELECT
    (SELECT COUNT(*)
     FROM workspace.finflow_bronze.loans) AS bronze_loans,

    (SELECT COUNT(*)
     FROM workspace.finflow_silver.loans) AS valid_silver_loans,

    (SELECT COUNT(*)
     FROM workspace.finflow_silver.quarantine_loans) AS quarantined_loans;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Which transaction channels have the highest failure rates?

-- COMMAND ----------

SELECT
    channel,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_transactions,
    ROUND(
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*),
        2
    ) AS failure_rate
FROM workspace.finflow_gold.fact_transaction
GROUP BY channel
ORDER BY failure_rate DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
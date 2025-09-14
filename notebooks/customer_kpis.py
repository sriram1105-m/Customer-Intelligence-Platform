# Databricks notebook source
import pandas as pd

base_url = "https://raw.githubusercontent.com/sriram1105-m/Customer-Intelligence-Platform/main/data/final/"

# Load customer segments table
file_name = "customer_scores.csv"
pdf = pd.read_csv(base_url + file_name)
customer_scores_df = spark.createDataFrame(pdf)

# Quick check
print("Customer Scores Table Loaded:")
print(f"Rows: {customer_scores_df.count()} | Columns: {len(customer_scores_df.columns)}")

# COMMAND ----------

import pandas as pd

base_url = "https://raw.githubusercontent.com/sriram1105-m/Customer-Intelligence-Platform/main/data/cleaned/"

# Load Transactions Cleaned Data and Products Cleaned Data
# Transaction data
transactions_file = "transactions_clean.csv"
pdf_transactions = pd.read_csv(base_url + transactions_file)
transactions_clean_df = spark.createDataFrame(pdf_transactions)

# Products data
products_file = "products_clean.csv"
pdf_products = pd.read_csv(base_url + products_file)
products_clean_df = spark.createDataFrame(pdf_products)

# Quick Check - Row Counts
print(f"Transactions Rows: {transactions_clean_df.count()}")
print(f"Products Rows: {products_clean_df.count()}")

# Optional: Preview top rows
display(transactions_clean_df.limit(5))
display(products_clean_df.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Total Revenue & Average Order Value
kpi_totals_df = (customer_scores_df
                 .agg(
                     F.sum("monetary_value").alias("total_revenue"),
                     F.round(F.avg("monetary_value"), 2).alias("avg_order_value")
                     )
                )

# 2. Customer Activity KPIs
total_customers = customer_scores_df.count()
active_customers = customer_scores_df.filter(F.col("segment") != "Churn Risk").count()
loyal_customers = customer_scores_df.filter(F.col("segment").isin("Loyal", "High Value")).count()
churn_customers = customer_scores_df.filter(F.col("segment") == "Churn Risk").count()

kpi_activity_df = spark.createDataFrame([{
    "total_customers" : total_customers,
    "active_customers" : active_customers,
    "active_customer_pct" : round((active_customers / total_customers) * 100, 2),
    "loyal_customers" : loyal_customers,
    "loyal_customer_pct" : round((loyal_customers / total_customers) * 100, 2),
    "churn_customers" : churn_customers,
    "churn_rate_pct" : round((churn_customers / total_customers) * 100, 2)
}])

# 3. Revenue by Segment
kpi_segment_df = (customer_scores_df
                  .groupBy("segment")
                  .agg(
                      F.sum("monetary_value").alias("revenue_by_segment"),
                      F.round(F.avg("monetary_value"), 2).alias("avg_order_value_segment")
                      )
                  .orderBy(F.col("revenue_by_segment").desc())
                  )

# 4. Monthly Revenue Trend
monthly_revenue_df = (transactions_clean_df
                      .withColumn("month", F.date_format("transaction_date", "yyyy-MM"))
                      .groupBy("month")
                      .agg(F.sum("amount").alias("monthly_revenue"))
                      .orderBy("month")
                      )

# 5. Top Customers (Top 10 by spend)
top_customers_df = (customer_scores_df
                    .select("customer_id", "monetary_value")
                    .orderBy(F.col("monetary_value").desc())
                    .limit(10)
                    )

# 6. Top Products (Top 10 by Sales)
top_products_df = (transactions_clean_df
                   .groupBy("product_id")
                   .agg(F.sum("amount").alias("total_sales"))
                   .join(products_clean_df, "product_id", "left")
                   .orderBy(F.col("total_sales").desc())
                   .limit(10)
                   )


# COMMAND ----------

# 7. Customer Lifetime Value (CLV)
# Step A: First purchase date per customer
first_purchase_df = (transactions_clean_df
    .groupBy("customer_id")
    .agg(F.min("transaction_date").alias("first_purchase_date"))
)

# Step B: Add months_active for each customer
transactions_with_first = (transactions_clean_df
    .join(first_purchase_df, "customer_id", "left")
    .withColumn("months_active", 
        F.round(F.months_between(F.current_date(), F.col("first_purchase_date")), 1)
    )
)

# Step C: Average Order Value per customer
avg_order_df = (transactions_with_first
    .groupBy("customer_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_order_value"),
         F.max("months_active").alias("months_active"))
)

# Step D: Purchase frequency per month
purchase_freq_df = (transactions_with_first
    .groupBy("customer_id")
    .agg(F.round(F.count("transaction_id") / F.max("months_active"), 2).alias("purchase_freq_month"))
)

# Step E: CLV = AOV * Frequency * Months Active
clv_df = (avg_order_df
    .join(purchase_freq_df, "customer_id", "left")
    .withColumn("CLV", 
        F.round(F.col("avg_order_value") * F.col("purchase_freq_month") * F.col("months_active"), 2))
    .fillna(0)
)

# COMMAND ----------

# 8. Customer Retention Cohort Analysis
# Step A: Extract Cohort Month
transactions_with_cohort = (
    transactions_clean_df
    .withColumn("transaction_month", F.date_format("transaction_date", "yyyy-MM"))
    .withColumn("cohort_month",
        F.date_format(F.min("transaction_date").over(Window.partitionBy("customer_id")), "yyyy-MM")
    )
)

# Step B: Count customers per cohort & month
cohort_counts = (transactions_with_cohort
                 .groupBy("cohort_month", "transaction_month")
                 .agg(F.countDistinct("customer_id").alias("customer_count"))
                 .orderBy("cohort_month", "transaction_month")
                 )

# Step C: Pivot for retention matrix
retention_df = (cohort_counts
                .groupBy("cohort_month")
                .pivot("transaction_month")
                .sum("customer_count")
                .fillna(0)
                )

# COMMAND ----------

# 9. Display all KPIs
print(" Total KPIs: ")
display(kpi_totals_df)

print("Activity KPIs: ")
display(kpi_activity_df)

print("Revenueby Segment: ")
display(kpi_segment_df)

print("Monthly Revenue Trend: ")
display(monthly_revenue_df)

print("Top Customers: ")
display(top_customers_df)

print("Top Products: ")
display(top_products_df)

print("Customer Lifetime Value (CLV): ")
display(clv_df)

print("Customer Retention Cohort Matrix: ")
display(retention_df)

# COMMAND ----------

# Save all KPI tables separately
kpi_totals_df.toPandas().to_csv("kpi_totals.csv", index=False)
kpi_activity_df.toPandas().to_csv("kpi_activity.csv", index=False)
kpi_segment_df.toPandas().to_csv("kpi_revenue_segment.csv", index=False)
monthly_revenue_df.toPandas().to_csv("kpi_monthly_revenue.csv", index=False)
top_customers_df.toPandas().to_csv("kpi_top_customers.csv", index=False)
top_products_df.toPandas().to_csv("kpi_top_products.csv", index=False)
clv_df.toPandas().to_csv("kpi_clv.csv", index=False)
retention_df.toPandas().to_csv("kpi_retention.csv", index=False)

print("All KPI tables saved separately!")


# COMMAND ----------

from pyspark.sql import functions as F

# Get all columns except cohort_month safely
numeric_cols = [c for c in retention_df.columns if c != "cohort_month"]

# If cohort_month is not present, numeric_cols will just have month columns like 2023-09, etc.
if "cohort_month" not in retention_df.columns:
    numeric_cols = retention_df.columns  # use all month columns

# Calculate retention rate = (sum of all cells) / (number of cohorts * first month customers)
total_customers = retention_df.select(F.sum(F.col(numeric_cols[0]))).collect()[0][0]
total_values = retention_df.select([F.sum(F.col(c)) for c in numeric_cols]).collect()
sum_all = sum([r[0] for r in total_values])
num_periods = len(numeric_cols)

# Average retention across periods
retention_rate = round(sum_all / (total_customers * num_periods), 2)
print("Retention Rate:", retention_rate)


# COMMAND ----------

# Combining all the KPIs into a single Master KPI Dataset
from pyspark.sql import functions as F

final_kpis_df = spark.createDataFrame([{
    "total_customers": kpi_activity_df.agg(F.sum("total_customers")).collect()[0][0],
    "active_customers": kpi_activity_df.agg(F.sum("active_customers")).collect()[0][0],
    "active_customer_pct": round(kpi_activity_df.agg(F.avg("active_customer_pct")).collect()[0][0], 2),
    "loyal_customers": kpi_activity_df.agg(F.sum("loyal_customers")).collect()[0][0],
    "loyal_customer_pct": round(kpi_activity_df.agg(F.avg("loyal_customer_pct")).collect()[0][0], 2),
    "churn_customers": kpi_activity_df.agg(F.sum("churn_customers")).collect()[0][0],
    "churn_rate_pct": round(kpi_activity_df.agg(F.avg("churn_rate_pct")).collect()[0][0], 2),
    "total_revenue": kpi_segment_df.agg(F.sum("revenue_by_segment")).collect()[0][0],
    "avg_order_value": kpi_segment_df.agg(F.avg("avg_order_value_segment")).collect()[0][0],
    "avg_clv": round(clv_df.agg(F.avg("CLV")).collect()[0][0], 2),
    "retention_rate": retention_rate
}])


# COMMAND ----------

# Save Final KPIs Table
final_kpis_df.toPandas().to_csv("customer_kpis.csv", index=False)

print("Final KPIs Table saved as customer_kpis.csv")
display(final_kpis_df)

# COMMAND ----------

# Count rows and columns for Spark DataFrame
rows = final_kpis_df.count()
cols = len(final_kpis_df.columns)

print(f"Final KPIs Table → {rows} rows × {cols} columns")
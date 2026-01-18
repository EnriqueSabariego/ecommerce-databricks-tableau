# =====================================
# Gold Layer - Data Aggregation
# Databricks Community Edition
# =====================================
# Databricks notebook source
# Use default database
spark.sql("USE default")

# Read Silver table
df_silver = spark.table("silver_retail_clean")
df_silver.display()
df_silver.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

#Aggregate KPIs per day
df_gold_daily = (
    df_silver
    .withColumn("date", F.to_date("invoice_date"))
    .groupBy("date")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("invoice_id").alias("total_orders"),
        F.sum("quantity").alias("total_items"),
        F.avg("revenue").alias("avg_order_value")
    )
    .orderBy("date")
)

#Show results
df_gold_daily.display()

#Save as a Delta Table
df_gold_daily.write.format("delta").mode("overwrite").saveAsTable("gold_sales_daily")

# COMMAND ----------

# Aggregate KPIs per country
df_gold_country = (
    df_silver
    .groupBy("country")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("invoice_id").alias("total_orders"),
        F.sum("quantity").alias("total_items"),
        F.avg("revenue").alias("avg_order_value")
    )
    .orderBy("total_revenue", ascending=False)
)

df_gold_country.display()

# Save as Delta Table
df_gold_country.write.format("delta").mode("overwrite").saveAsTable("gold_sales_country")


# COMMAND ----------

# Aggregate by product
df_gold_products = (
    df_silver
    .groupBy("product_id", "product_name")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_quantity"),
        F.count("invoice_id").alias("total_orders")
    )
    .orderBy("total_revenue", ascending=False)
)

df_gold_products.display()

# Save as Delta Table
df_gold_products.write.format("delta").mode("overwrite").saveAsTable("gold_top_products")


# COMMAND ----------

# Aggregate metrics per customer
df_gold_customers = (
    df_silver
    .groupBy("customer_id")
    .agg(
        F.count("invoice_id").alias("total_orders"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("revenue").alias("avg_order_value")
    )
    .orderBy("total_revenue", ascending=False)
)

df_gold_customers.display()

# Save as Delta Table
df_gold_customers.write.format("delta").mode("overwrite").saveAsTable("gold_customer_metrics")


# COMMAND ----------

# Read the Gold table
df_gold_daily = spark.table("gold_sales_daily")

# Display the table in Databricks
df_gold_daily.display()



# COMMAND ----------


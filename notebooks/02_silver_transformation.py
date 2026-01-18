# =======================================
# Silver Layer - Raw Data Transformation
# Databricks Community Edition
# =======================================
# 1️⃣ Usar la base de datos default
spark.sql("USE default")


# 2️⃣ Tomar los datos de Bronze
df_bronze = spark.table("online_retail")  # tu tabla Bronze ya creada
df_bronze.display()
df_bronze.printSchema()


# 3️⃣ Transformaciones Silver
from pyspark.sql import functions as F
# Convertir tipos, filtrar valores, crear columnas
df_silver = (
    df_bronze
    .withColumn("invoice_date",F.to_timestamp(F.col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("quantity", F.col("Quantity").cast("int"))
    .withColumn("unit_price", F.col("UnitPrice").cast("double"))
    .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
    # Crear columna revenue
    .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
    # Renombrar columnas para consistencia
    .withColumnRenamed("InvoiceNo", "invoice_id") \
    .withColumnRenamed("StockCode", "product_id") \
    .withColumnRenamed("Description", "product_name") \
    .withColumnRenamed("CustomerID", "customer_id") \
    .withColumnRenamed("Country", "country")
)

# Mostrar resultado
df_silver.display()


# 4️⃣ Guardar como Delta Table Silver
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_retail_clean")
# Verificar que la tabla Silver existe
spark.sql("SHOW TABLES").display()

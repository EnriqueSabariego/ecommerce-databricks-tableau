# =====================================
# Bronze Layer - Raw Data Ingestion
# Databricks Community Edition
# =====================================
# Path del archivo
file_path = "/Volumes/workspace/default/proyecto/online_retail.csv"

# Lectura del CSV
df_bronze = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(file_path)
)

df_bronze.display()

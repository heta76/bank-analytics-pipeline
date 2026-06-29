# extract_postgres.py
"""
Чтение справочников из PostgreSQL и сохранение в HDFS.
Запуск:
  spark-submit --jars /path/to/postgresql-42.5.0.jar extract_postgres.py
"""
from pyspark.sql import SparkSession

# Таблицы для выгрузки
TABLES = ["banks", "cards", "mcc", "organizations", "peoples"]
JDBC_URL = "jdbc:postgresql://postgres:5432/retail"
JDBC_PROPS = {
    "user": "student",
    "password": "student_pass",
    "driver": "org.postgresql.Driver"
}
HDFS_BASE_PATH = "hdfs://namenode:9000/raw/postgres"

spark = SparkSession.builder \
    .appName("PostgreSQL Extract") \
    .getOrCreate()

for table in TABLES:
    print(f"Reading {table}...")
    df = spark.read.jdbc(
        url=JDBC_URL,
        table=f"data.{table}",
        properties=JDBC_PROPS
    )
    output_path = f"{HDFS_BASE_PATH}/{table}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved {table} to {output_path}")

print("All tables extracted.")
spark.stop()
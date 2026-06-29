# aggregate_marts.py
"""
Читает обогащённые данные из HDFS, строит три витрины и сохраняет в Hive-таблицы (Parquet).
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ENRICHED_PATH = "hdfs://namenode:9000/clean/enriched_transactions"

spark = SparkSession.builder \
    .appName("Build Marts to Hive") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Создаём базу данных marts, если её нет
spark.sql("CREATE DATABASE IF NOT EXISTS marts")

# Читаем обогащённые данные
enriched = spark.read.parquet(ENRICHED_PATH)

# Витрина 1: Обороты по банкам и MCC (часовые)
turnover = enriched \
    .withColumn("hour", F.date_trunc("hour", F.col("ts"))) \
    .groupBy("bank_name", "bank_abbr", "mcc", "mcc_desc", "hour") \
    .agg(
        F.count("*").alias("txn_cnt"),
        F.round(F.sum("price"), 2).alias("total_amount"),
        F.round(F.avg("price"), 2).alias("avg_receipt")
    ) \
    .orderBy("hour", "bank_name")

# Витрина 2: Топ товаров по городам
top_products = enriched \
    .groupBy("city", "position") \
    .agg(
        F.count("*").alias("txn_cnt"),
        F.round(F.sum("price"), 2).alias("total_amount")
    ) \
    .orderBy(F.desc("total_amount"))

# Витрина 3: Активность по платёжным системам и картам
card_activity = enriched \
    .groupBy("bank_name", "pay_system", "masked_card") \
    .agg(
        F.count("*").alias("txn_cnt"),
        F.round(F.sum("price"), 2).alias("total_amount"),
        F.min("ts").alias("first_txn"),
        F.max("ts").alias("last_txn")
    ) \
    .orderBy(F.desc("total_amount"))

# Сохраняем как Hive-таблицы (формат Parquet по умолчанию)
turnover.write.mode("overwrite").saveAsTable("marts.turnover")
print("Mart turnover saved to Hive.")
top_products.write.mode("overwrite").saveAsTable("marts.top_products")
print("Mart top_products saved to Hive.")
card_activity.write.mode("overwrite").saveAsTable("marts.card_activity")
print("Mart card_activity saved to Hive.")

spark.stop()
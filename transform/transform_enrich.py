# transform_enrich.py
"""
Читает сырые транзакции из HDFS, парсит JSON, обогащает справочниками
и сохраняет очищенные/обогащённые данные в HDFS.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

RAW_KAFKA_PATH = "hdfs://namenode:9000/raw/kafka_transactions"
RAW_POSTGRES_BASE = "hdfs://namenode:9000/raw/postgres"
ENRICHED_PATH = "hdfs://namenode:9000/clean/enriched_transactions"

spark = SparkSession.builder \
    .appName("Transform and Enrich") \
    .getOrCreate()

# Схема JSON-сообщения (как в Jupyter)
transaction_schema = StructType([
    StructField("bank_id", StringType(), True),
    StructField("info_client", StructType([
        StructField("card", StringType(), True),
        StructField("bank", StringType(), True),
        StructField("pay_system", StringType(), True)
    ])),
    StructField("info_transaction", StructType([
        StructField("terminal", StringType(), True),
        StructField("transaction", StringType(), True),
        StructField("datetime", StringType(), True)
    ])),
    StructField("info_market", StructType([
        StructField("market", StringType(), True),
        StructField("mcc", StringType(), True),
        StructField("address", StringType(), True)
    ])),
    StructField("purchases", ArrayType(StructType([
        StructField("position", StringType(), True),
        StructField("barcode", StringType(), True),
        StructField("price", DoubleType(), True)
    ])))
])

# Читаем сырые транзакции (только value, остальное не нужно)
raw_df = spark.read.parquet(RAW_KAFKA_PATH) \
    .select(F.col("value").cast("string").alias("json_str"))

# Парсим JSON и чиним одинарные кавычки
parsed = raw_df \
    .withColumn("json_str", F.regexp_replace(F.col("json_str"), "'", "\"")) \
    .withColumn("data", F.from_json(F.col("json_str"), transaction_schema)) \
    .select("data.*")

# Разворачиваем массив purchases в отдельные строки
exploded = parsed \
    .withColumn("purchase", F.explode("purchases")) \
    .select(
        F.col("info_transaction.transaction").alias("txn_id"),
        F.to_timestamp("info_transaction.datetime").alias("ts"),
        F.col("info_client.bank").alias("bank_name"),
        F.col("info_client.pay_system").alias("pay_system"),
        F.col("info_client.card").alias("masked_card"),
        F.col("info_market.market").alias("market_id"),
        F.col("info_market.mcc").cast("int").alias("mcc"),
        F.col("info_market.address").alias("address"),
        F.col("purchase.position").alias("position"),
        F.col("purchase.price").alias("price")
    )

# Читаем справочники из HDFS
banks = spark.read.parquet(f"{RAW_POSTGRES_BASE}/banks")
mcc = spark.read.parquet(f"{RAW_POSTGRES_BASE}/mcc")
organizations = spark.read.parquet(f"{RAW_POSTGRES_BASE}/organizations")

# Переименовываем столбцы, чтобы избежать конфликтов при join
organizations = organizations.select(
    F.col("id").alias("org_id"),
    F.col("address").alias("market_address"),
    F.col("name_market"),
    F.col("type_budget")
)

# Обогащаем
enriched = exploded \
    .join(banks, exploded.bank_name == banks.name, "left") \
    .join(mcc, exploded.mcc == mcc.mcc, "left") \
    .join(organizations, exploded.market_id == organizations.org_id, "left") \
    .select(
        exploded["*"],
        banks.abbreviation.alias("bank_abbr"),
        mcc.type_organization.alias("mcc_desc"),
        organizations.name_market,
        organizations.type_budget,
        F.regexp_extract(F.col("address"), r"г\.\s*([\p{L}\-]+)", 1).alias("city")
    )

# Сохраняем обогащённые данные в HDFS (перезаписываем)
enriched.write \
    .mode("overwrite") \
    .parquet(ENRICHED_PATH)

print(f"Enriched data saved to {ENRICHED_PATH}")
spark.stop()
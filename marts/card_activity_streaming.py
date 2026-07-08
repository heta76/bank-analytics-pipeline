import sys
import os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.config_loader import load_config
from utils.spark_session import create_spark_session
from utils.logger import setup_logger
from loaders.hive_loader import HiveLoader
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

logger = setup_logger(__name__)

def main():
    config_path = os.path.join(PROJECT_ROOT, "config.yaml")
    config = load_config(config_path)
    spark = create_spark_session("Mart: Card Activity Streaming")
    loader = HiveLoader(spark, config)

    # Читаем ТОЛЬКО streaming-слой
    streaming_path = config["hdfs"]["raw_kafka_streaming_path"]
    raw_df = spark.read.parquet(streaming_path)
    raw_df = raw_df.filter(F.col("kafka_timestamp") >= F.current_timestamp() - F.expr("INTERVAL 5 MINUTES"))

    transaction_schema = StructType([
        StructField("bank_id", StringType(), True),
        StructField("info_client", StructType([
            StructField("card", StringType(), True),
            StructField("bank", StringType(), True),
            StructField("pay_system", StringType(), True)
        ])),
        StructField("info_transaction", StructType([
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

    # Парсим и разворачиваем purchases
    parsed = raw_df.select(F.col("value").cast("string").alias("json_str")) \
        .withColumn("json_str", F.regexp_replace(F.col("json_str"), "'", "\"")) \
        .withColumn("data", F.from_json(F.col("json_str"), transaction_schema)) \
        .select("data.*")

    exploded = parsed.withColumn("purchase", F.explode("purchases")) \
        .select(
            F.col("info_client.bank").alias("bank_name"),
            F.col("info_client.pay_system").alias("pay_system"),
            F.col("info_client.card").alias("masked_card"),
            F.to_timestamp("info_transaction.datetime").alias("ts"),
            F.col("purchase.price").alias("price")
        )

    # Сумма последней транзакции через оконную функцию
    window_spec = Window.partitionBy("bank_name", "pay_system", "masked_card").orderBy(F.col("ts").desc())
    last_txn_amount = exploded.withColumn("rn", F.row_number().over(window_spec)) \
        .filter(F.col("rn") == 1) \
        .select("bank_name", "pay_system", "masked_card", F.col("price").alias("last_txn_amount"))

    # Агрегация
    card_activity = exploded \
        .groupBy("bank_name", "pay_system", "masked_card") \
        .agg(
            F.count("*").alias("txn_cnt"),
            F.round(F.sum("price"), 2).alias("total_amount"),
            F.min("ts").alias("first_txn"),
            F.max("ts").alias("last_txn")
        )

    card_activity = card_activity.join(last_txn_amount,
                                       ["bank_name", "pay_system", "masked_card"],
                                       "left")

    loader.load(card_activity, config["marts"]["card_activity_streaming_table"])
    spark.stop()

if __name__ == "__main__":
    main()
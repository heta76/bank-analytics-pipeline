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

logger = setup_logger(__name__)

def main():
    config_path = os.path.join(PROJECT_ROOT, "config.yaml")
    config = load_config(config_path)
    spark = create_spark_session("Mart: Card Activity")
    loader = HiveLoader(spark, config)

    enriched = spark.read.parquet(config["hdfs"]["enriched_path"])

    # Сумма последней транзакции через оконную функцию
    window_spec = Window.partitionBy("bank_name", "pay_system", "masked_card").orderBy(F.col("ts").desc())
    last_txn_amount = enriched.withColumn("rn", F.row_number().over(window_spec)) \
        .filter(F.col("rn") == 1) \
        .select("bank_name", "pay_system", "masked_card", F.col("price").alias("last_txn_amount"))

    # Агрегация
    card_activity = enriched \
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

    loader.load(card_activity, config["marts"]["card_activity_table"])
    spark.stop()

if __name__ == "__main__":
    main()
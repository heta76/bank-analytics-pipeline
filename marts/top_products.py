import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import functions as F
from utils.config_loader import load_config
from utils.spark_session import create_spark_session
from utils.logger import setup_logger
from loaders.hive_loader import HiveLoader



logger = setup_logger(__name__)

def main():
    config_path = os.path.join(PROJECT_ROOT, "config.yaml")
    config = load_config(config_path)
    spark = create_spark_session("Mart: Top Products")
    loader = HiveLoader(spark, config)

    enriched = spark.read.parquet(config["hdfs"]["enriched_path"])

    top_products = enriched \
        .groupBy("city", "position") \
        .agg(
            F.count("*").alias("txn_cnt"),
            F.round(F.sum("price"), 2).alias("total_amount")
        )

    loader.load(top_products, config["marts"]["top_products_table"])
    spark.stop()

if __name__ == "__main__":
    main()
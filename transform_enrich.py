import os
import sys
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import functions as F
from utils.config_loader import load_config
from utils.spark_session import create_spark_session
from transformers.enrich import EnrichTransformer

def main():
    config_path = os.path.join(PROJECT_ROOT, "config.yaml")
    config = load_config(config_path)
    spark = create_spark_session("Transform and Enrich")

    streaming_df = spark.read.parquet(config["hdfs"]["raw_kafka_streaming_path"])
    batch_df = spark.read.parquet(config["hdfs"]["raw_kafka_batch_path"])

    combined = streaming_df.unionByName(batch_df) \
        .dropDuplicates(["topic", "partition", "offset"])

    transformer = EnrichTransformer(spark, config)
    transformer.transform(input_df=combined)
    spark.stop()

if __name__ == "__main__":
    main()
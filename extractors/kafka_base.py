from base.extractor import Extractor
from pyspark.sql import DataFrame

class KafkaExtractor(Extractor):
    def __init__(self, spark, config, output_path_key):
        super().__init__(spark, config)
        self.bootstrap = config["kafka"]["bootstrap_servers"]
        self.topics = config["kafka"]["topics"]
        self.output_path = config["hdfs"][output_path_key]

    def _transform(self, df: DataFrame, mode: str = "batch") -> DataFrame:
        from pyspark.sql import functions as F
        return df.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.lit(mode).alias("extract_mode")
        )
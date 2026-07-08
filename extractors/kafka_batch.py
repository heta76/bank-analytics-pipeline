from extractors.kafka_base import KafkaExtractor
from utils.logger import setup_logger

logger = setup_logger(__name__)

class KafkaBatchExtractor(KafkaExtractor):
    def __init__(self, spark, config):
        super().__init__(spark, config, output_path_key="raw_kafka_batch_path")

    def extract(self):
        from pyspark.sql import DataFrame

        df = self.spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap) \
            .option("subscribe", self.topics) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()

        if df.rdd.isEmpty():
            logger.warning("No data fetched from Kafka. Skipping batch write.")
            return

        transformed = self._transform(df, mode="batch")
        transformed.write.mode("append").parquet(self.output_path)
        logger.info("Batch extracted to %s", self.output_path)
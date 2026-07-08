from extractors.kafka_base import KafkaExtractor
from utils.logger import setup_logger

logger = setup_logger(__name__)

class KafkaStreamingExtractor(KafkaExtractor):
    def __init__(self, spark, config):
        super().__init__(spark, config, output_path_key="raw_kafka_streaming_path")
        self.checkpoint = config["hdfs"]["checkpoint_streaming"]

    def extract(self):
        raw_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap) \
            .option("subscribe", self.topics) \
            .option("startingOffsets", self.config["kafka"]["starting_offsets"]) \
            .load()

        transformed = self._transform(raw_stream, mode="streaming")

        query = transformed.writeStream \
            .format("parquet") \
            .option("path", self.output_path) \
            .option("checkpointLocation", self.checkpoint) \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .start()

        logger.info("Streaming started, writing to %s", self.output_path)
        query.awaitTermination()
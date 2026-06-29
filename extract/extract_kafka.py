# extract_kafka.py
"""
Чтение сырых данных из Kafka (поток) и сохранение в HDFS.
Запуск:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 extract_kafka.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

# Конфигурация
KAFKA_BOOTSTRAP = "kafka1:19092"          # внутренний listener
TOPICS = "amur_topic,neva_topic,ob_topic,volga_topic,yenisei_topic"
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/raw/kafka_transactions"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/kafka_raw"

spark = SparkSession.builder \
    .appName("Kafka Extract") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# Читаем поток
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPICS) \
    .option("startingOffsets", "latest") \
    .load()

# Для сырого слоя сохраняем ключ, значение, топик, смещение, временную метку
raw_with_metadata = raw_stream.select(
    F.col("key").cast("string").alias("key"),
    F.col("value").cast("string").alias("value"),
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("timestamp").alias("kafka_timestamp")
)

# Пишем в HDFS в формате Parquet
query = raw_with_metadata.writeStream \
    .format("parquet") \
    .option("path", HDFS_OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

print(f"Stream started, writing to {HDFS_OUTPUT_PATH}")
query.awaitTermination()
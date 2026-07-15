/**
 * Класс извлечения данных из Apache Kafka.
 *
 * Подключается к Kafka, читает поток сообщений,
 * добавляет техническую информацию (дата, час, offset)
 * и сохраняет данные в формате Parquet.
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

class KafkaExtractor(spark: SparkSession) {

  def startStream(): Unit = {

    println("Connecting to Kafka...")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.kafkaBootstrap)
      .option("subscribe", AppConfig.kafkaTopics)
      .option("startingOffsets", AppConfig.kafkaStartingOffsets)
      .load()

    println("Kafka connection established.")

    val rawDF = kafkaDF
      .select(
        col("key").cast("string").as("key"),
        col("value").cast("string").as("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").as("kafka_timestamp")
      )
      .withColumn("kafka_date", to_date(col("kafka_timestamp")))
      .withColumn("kafka_hour", hour(col("kafka_timestamp")))

    val query = rawDF.writeStream
      .format("parquet")
      .option("path", AppConfig.kafkaOutput)
      .option("checkpointLocation", AppConfig.kafkaCheckpoint)
      .partitionBy("kafka_date", "kafka_hour")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(AppConfig.kafkaTrigger))
      .start()

    println("Kafka streaming started.")
    println(s"Writing to: ${AppConfig.kafkaOutput}")

    query.awaitTermination()
  }

}
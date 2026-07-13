import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExtractKafka {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Kafka Extract")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") //Разрешает удалять временные checkpoint-файлы после завершения работы.
      .getOrCreate()

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka1:19092") //Адрес Kafka Broker
      .option(
        "subscribe",
        "amur_topic,neva_topic,ob_topic,volga_topic,yenisei_topic"
      )
      .option("startingOffsets", "latest") //чтение только с новых сообщений
      .load()

    val rawDF = kafkaDF.select(
      col("key").cast("string").as("key"),
      col("value").cast("string").as("value"),
      col("topic"),
      col("partition"),
      col("offset"),
      col("timestamp").as("kafka_timestamp")
    )

    val query = rawDF.writeStream //настройка потоковой записи
      .format("parquet")
      .option("path", "hdfs://namenode:9000/raw/kafka_transactions")
      .option(
        "checkpointLocation",
        "hdfs://namenode:9000/checkpoints/kafka_raw"
      )
      .outputMode("append") //добавляет новые записи в существующие файлы
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("30 seconds")) //обработка выполняется каждые 30 секунд
      .start()

    println("Kafka stream started...")

    query.awaitTermination()
  }
}

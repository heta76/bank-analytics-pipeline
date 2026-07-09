/**
 * Точка входа для извлечения данных из Kafka.
 *
 * Запускает потоковое чтение сообщений,
 * сохраняет полученные данные в формате Parquet
 * в директорию raw HDFS.
 */
object ExtractKafka {

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSessionFactory.create("Kafka Extract")

    val extractor =
      new KafkaExtractor(spark)

    extractor.startStream()

    spark.stop()

  }

}
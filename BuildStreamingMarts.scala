/**
 * Построение streaming-витрины.
 *
 * Последовательно выполняет:
 * - создание SparkSession;
 * - чтение сообщений из Kafka;
 * - разбор JSON;
 * - построение streaming-витрины;
 * - сохранение результата в HDFS.
 */

import reader.KafkaStreamingReader
import service.StreamingMartService
import writer.StreamingMartWriter

object BuildStreamingMarts {

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSessionFactory.create("Build Streaming Marts")

    val kafka =
      new KafkaStreamingReader(spark).read()

    val mart =
      new StreamingMartService().cardActivity(kafka)

    val query =
      new StreamingMartWriter().write(mart)

    query.awaitTermination()

  }

}
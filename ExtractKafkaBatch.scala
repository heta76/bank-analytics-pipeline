/**
 * Batch-экстрактор Kafka.
 *
 * Последовательно выполняет:
 * - создание SparkSession;
 * - чтение сообщений из Kafka;
 * - разбор JSON;
 * - сохранение данных в HDFS.
 */

import reader.KafkaBatchReader
import writer.KafkaBatchWriter

object ExtractKafkaBatch {

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSessionFactory.create("Extract Kafka Batch")

    val transactions =
      new KafkaBatchReader(spark).read()

    new KafkaBatchWriter()
      .write(transactions)

    spark.stop()

  }

}
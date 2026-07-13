/**
 * Сохранение batch-данных Kafka.
 *
 * Записывает разобранные транзакции
 * в формате Parquet в HDFS.
 */
package writer

import org.apache.spark.sql.DataFrame

class KafkaBatchWriter {

  private val path =
    "hdfs://namenode:9000/raw/kafka_batch"

  def write(df: DataFrame): Unit = {

    df.write
      .mode("overwrite")
      .parquet(path)

  }

}
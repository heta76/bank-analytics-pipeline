/**
 * Чтение обогащенных данных.
 *
 * Выполняет чтение подготовленного набора данных
 * из HDFS, который используется для построения витрин.
 */
package reader

import org.apache.spark.sql.{DataFrame, SparkSession}

class EnrichedReader(spark: SparkSession) {

  private val path = "hdfs://namenode:9000/clean/enriched_transactions"

  def read(): DataFrame =
    spark.read.parquet(path)

}
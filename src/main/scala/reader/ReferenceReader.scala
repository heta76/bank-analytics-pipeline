/**
 * Класс чтения справочных данных.
 *
 * Загружает таблицы банков, организаций,
 * MCC-кодов, карт и клиентов,
 * сохраненные в формате Parquet.
 */
package reader

import org.apache.spark.sql.{DataFrame, SparkSession}

class ReferenceReader(spark: SparkSession) {

  private val base = "hdfs://namenode:9000/raw"

  def banks(): DataFrame =
    spark.read.parquet(s"$base/banks")

  def cards(): DataFrame =
    spark.read.parquet(s"$base/cards")

  def organizations(): DataFrame =
    spark.read.parquet(s"$base/organizations")

  def mcc(): DataFrame =
    spark.read.parquet(s"$base/mcc")

  def peoples(): DataFrame =
    spark.read.parquet(s"$base/peoples")

}
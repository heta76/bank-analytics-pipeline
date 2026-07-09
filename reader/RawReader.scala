/**
 * Класс чтения исходных данных.
 *
 * Выполняет загрузку сырых транзакций
 * из директории raw HDFS.
 */
package reader

import org.apache.spark.sql.{DataFrame, SparkSession}

class RawReader(spark: SparkSession) {

  def readTransactions(path: String): DataFrame = {

    spark.read.parquet(path)

  }

}
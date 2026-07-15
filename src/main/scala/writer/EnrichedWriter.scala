/**
 * Класс записи обработанных данных.
 *
 * Сохраняет обогащенный DataFrame
 * в формате Parquet
 * в директорию clean HDFS.
 */
package writer

import org.apache.spark.sql.DataFrame

class EnrichedWriter {

  def write(df: DataFrame, path: String): Unit = {

    df.write
      .mode("append")
      .partitionBy("transaction_date")
      .parquet(path)

  }

}
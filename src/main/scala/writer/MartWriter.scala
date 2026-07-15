/**
 * Запись  витрин.
 *
 * Выполняет сохранение DataFrame в Hive-таблицу формата Parquet.
 * При повторном запуске существующие данные заменяются новыми(временно, дальше возможно допилим нормально).
 */
package writer

import org.apache.spark.sql.DataFrame

class MartWriter {

  def write(df: DataFrame, table: String): Unit = {

    val spark = df.sparkSession

    val tableName = table.split("\\.").last
    val path = s"hdfs://namenode:9000/marts/$tableName"

    // удаляем старую таблицу
    spark.sql(s"DROP TABLE IF EXISTS $table")

    // удаляем старые данные
    val fs = org.apache.hadoop.fs.FileSystem.get(
      spark.sparkContext.hadoopConfiguration
    )

    val hdfsPath = new org.apache.hadoop.fs.Path(path)

    if (fs.exists(hdfsPath))
      fs.delete(hdfsPath, true)

    // записываем parquet в HDFS
    df.write
      .mode("overwrite")
      .parquet(path)

    // регистрируем таблицу в Hive
    spark.sql(
      s"""
         |CREATE TABLE $table
         |USING PARQUET
         |LOCATION '$path'
         |""".stripMargin
    )

  }

}
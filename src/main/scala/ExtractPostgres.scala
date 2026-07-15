/**
 * Точка входа для извлечения данных из PostgreSQL.
 *
 * Загружает таблицы базы данных и сохраняет
 * их в формате Parquet в директорию raw HDFS.
 */
object ExtractPostgres {

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSessionFactory.create("Extract PostgreSQL")

    val extractor =
      new PostgresExtractor(spark)

    extractor.extractAll()

    spark.stop()

  }

}
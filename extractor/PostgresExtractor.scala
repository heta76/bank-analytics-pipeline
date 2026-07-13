/**
 * Класс извлечения данных из PostgreSQL.
 *
 * Загружает справочные таблицы через JDBC
 * и сохраняет их в формате Parquet
 * в HDFS для дальнейшего использования.
 */
import org.apache.spark.sql.SparkSession

class PostgresExtractor(spark: SparkSession) {

  def extractAll(): Unit = {

    AppConfig.tables.foreach(extractTable)

  }

  private def extractTable(table: String): Unit = {

    println(s"Reading table $table")

    val df = spark.read
      .format("jdbc")
      .option("url", AppConfig.jdbcUrl)
      .option("dbtable", s"${AppConfig.schema}.$table")
      .option("user", AppConfig.user)
      .option("password", AppConfig.password)
      .option("driver", "org.postgresql.Driver")
      .load()

    println(s"Loaded $table")

    df.show(5, false)

    df.write
      .mode("overwrite")
      .parquet(s"${AppConfig.hdfsPath}/$table")

    println(s"Saved $table")
  }

}
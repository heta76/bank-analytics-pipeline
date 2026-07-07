import org.apache.spark.sql.SparkSession

object ExtractPostgres {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Extract PostgreSQL")
      .getOrCreate()

    val jdbcUrl = "jdbc:postgresql://postgres_container:5432/retail" //подключение к PostgreSQL.

    val tables = Seq(
      "banks",
      "cards",
      "mcc",
      "organizations",
      "peoples"
    )

    tables.foreach { table =>

      println(s"Reading table: $table")

      val df = spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", s"data.$table")
        .option("user", "student")
        .option("password", "student_pass")
        .option("driver", "org.postgresql.Driver")
        .load()
      println(s"Loaded $table")
      df.show(5, false)
      df.write
        .mode("overwrite")
        .parquet(s"hdfs://namenode:9000/raw/$table")
      println(s"Saved $table to HDFS")
    }

    spark.stop()
  }
}

/**
 * Построение витрин.
 *
 * Последовательно выполняет:
 * - создание SparkSession;
 * - чтение обогащенных данных из HDFS;
 * - построение аналитических витрин;
 * - сохранение результатов в Hive.
 */

import reader.EnrichedReader
import service.MartService
import writer.MartWriter

object BuildMarts {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionFactory.create("Build Marts")
    /** Тестовый вывод для работы над возникшими ошмбками */
    println("==========")
    println("Warehouse = " + spark.conf.get("spark.sql.warehouse.dir"))
    println("DefaultFS = " + spark.sparkContext.hadoopConfiguration.get("fs.defaultFS"))
    println("==========")

    spark.sql("CREATE DATABASE IF NOT EXISTS marts")

    val enriched =
      new EnrichedReader(spark).read()

    val martService =
      new MartService()

    val writer =
      new MartWriter()

    writer.write(
      martService.turnover(enriched),
      "marts.turnover"
    )

    writer.write(
      martService.topProducts(enriched),
      "marts.top_products"
    )

    writer.write(
      martService.cardActivity(enriched),
      "marts.card_activity"
    )

    spark.stop()

  }

}
/**
 * Создание SparkSession.
 *
 * Создает и настраивает экземпляр SparkSession.
 *
 *
 * Включает поддержку Hive и настраивает хранение таблиц в HDFS.
 *
 */
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def create(appName: String): SparkSession = {

    SparkSession.builder()
      .appName(appName)
      .config(
        "spark.sql.warehouse.dir",
        "hdfs://namenode:9000/user/hive/warehouse"
      )
      .config(
        "spark.hadoop.fs.defaultFS",
        "hdfs://namenode:9000"
      )
      .enableHiveSupport()
      .getOrCreate()

  }

}
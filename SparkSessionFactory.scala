/**
 * Создание SparkSession.
 *
 * Создает и настраивает экземпляр SparkSession,
 * который используется всеми этапами ETL-процесса.
 */
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def create(appName: String): SparkSession = {

    SparkSession.builder()
      .appName(appName)
      .getOrCreate()
  }

}
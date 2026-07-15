/**
 * Хранение параметров конфигурации приложения.
 *
 * Загружает настройки из файла application.conf:
 * - параметры подключения к Kafka;
 * - параметры подключения к PostgreSQL;
 * - пути хранения данных в HDFS;
 * - настройки Spark Streaming.
 */
import com.typesafe.config.ConfigFactory

object AppConfig {

  private val config = ConfigFactory.load()

  val jdbcUrl: String =
    config.getString("postgres.url")

  val user: String =
    config.getString("postgres.user")

  val password: String =
    config.getString("postgres.password")

  val schema: String =
    config.getString("postgres.schema")

  val tables: Seq[String] =
    config.getStringList("postgres.tables").toArray.map(_.toString).toSeq

  val hdfsPath: String =
    config.getString("hdfs.rawPath")

  val kafkaBootstrap =
    config.getString("kafka.bootstrapServers")

  val kafkaTopics =
    config.getStringList("kafka.topics")
      .toArray
      .mkString(",")

  val kafkaCheckpoint =
    config.getString("kafka.checkpoint")

  val kafkaOutput =
    config.getString("kafka.output")

  val kafkaTrigger =
    config.getString("kafka.trigger")

  val kafkaStartingOffsets =
    config.getString("kafka.startingOffsets")
}

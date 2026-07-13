/**
 * Batch-чтение сообщений из Kafka.
 *
 * Выполняет однократное чтение сообщений
 * из всех топиков с транзакциями.
 */
package reader

import model.TransactionSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaBatchReader(spark: SparkSession) {

  def read(): DataFrame = {

    val kafka = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka1:19092")
      .option(
        "subscribe",
        "amur_topic,neva_topic,ob_topic,volga_topic,yenisei_topic"
      )
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) AS value")

    TransactionSchema.parse(kafka)

  }

}
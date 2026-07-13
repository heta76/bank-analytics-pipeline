/**
 * Основной этап обработки данных.
 *
 * Выполняет чтение сырых транзакций,
 * обогащение справочными данными,
 * формирование итогового набора данных
 * и сохранение результата в директорию clean HDFS.
 */
import reader._
import service._
import writer._

import org.apache.spark.sql.functions._

object TransformEnrich {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionFactory.create("Transform & Enrich")

    val rawReader = new RawReader(spark)
    val refReader = new ReferenceReader(spark)
    val service = new EnrichmentService()
    val writer = new EnrichedWriter()

    val raw = rawReader
      .readTransactions("hdfs://namenode:9000/raw/kafka_transactions")
      .filter(col("kafka_date") === current_date())

    val enriched = service
      .enrich(
        raw,
        refReader.banks(),
        refReader.cards(),
        refReader.organizations(),
        refReader.mcc(),
        refReader.peoples()
      )
      .withColumn(
        "transaction_date",
        to_date(col("transaction_timestamp"))
      )

    writer.write(
      enriched,
      "hdfs://namenode:9000/clean/enriched_transactions"
    )

    spark.stop()

  }

}
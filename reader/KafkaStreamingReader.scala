package reader

import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaStreamingReader(spark: SparkSession) {

  def read(): DataFrame = {

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka1:19092")
      .option(
        "subscribePattern",
        ".*_topic"
      )
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) AS value")

  }

}
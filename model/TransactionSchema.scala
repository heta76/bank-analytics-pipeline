/**
 * Описание структуры JSON-сообщения транзакции.
 *
 * Содержит схему входящих сообщений Kafka
 * и методы преобразования JSON в DataFrame Spark
 * с выделением необходимых полей транзакции.
 */
package model

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TransactionSchema {

  private val orderSchema = StructType(Seq(
    StructField("position", StringType),
    StructField("category", StringType),
    StructField("cost", DoubleType),
    StructField("barcode", StringType),
    StructField("type", StringType)
  ))

  private val marketSchema = StructType(Seq(
    StructField("market", StringType),
    StructField("name_market", StringType),
    StructField("mcc", IntegerType),
    StructField("address", StringType)
  ))

  val schema = StructType(Seq(
    StructField("bank", StringType),
    StructField("transaction", StringType),
    StructField("transaction_time", StringType),
    StructField("terminal_id", StringType),
    StructField("terminal_imei", StringType),
    StructField("card", StringType),
    StructField("card_bank", StringType),
    StructField("payment_system", StringType),
    StructField("market_data", marketSchema),
    StructField("orders", ArrayType(orderSchema))
  ))

  def parse(df: DataFrame): DataFrame = {

    df
      .select(
        from_json(col("value"), schema).alias("json")
      )
      .select("json.*")

      .withColumn("purchase", explode(col("orders")))

      .select(
        col("transaction").alias("transaction_id"),
        to_timestamp(col("transaction_time")).alias("transaction_timestamp"),

        col("card").alias("card_number"),
        col("card_bank").alias("bank_name"),
        col("payment_system"),

        col("market_data.market").alias("organization_id"),
        col("market_data.name_market").alias("organization_name"),
        col("market_data.mcc").cast("short").alias("mcc"),
        col("market_data.address").alias("address"),

        col("purchase.position").alias("position"),
        col("purchase.cost").alias("price"),
        col("purchase.category"),
        col("purchase.barcode"),
        col("purchase.type")
      )
  }

}
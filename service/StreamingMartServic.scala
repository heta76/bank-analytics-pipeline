package service

import model.TransactionSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class StreamingMartService {

  def cardActivity(df: DataFrame): DataFrame = {

    TransactionSchema
      .parse(df)

      .withWatermark(
        "transaction_timestamp",
        "5 minutes"
      )

      .groupBy(
        window(col("transaction_timestamp"), "1 minute"),
        col("bank_name"),
        col("payment_system"),
        col("card_number")
      )

      .agg(
        count("*").alias("transaction_count"),
        round(sum("price"), 2).alias("total_amount"),
        max("transaction_timestamp").alias("last_transaction"),
        last("price").alias("last_transaction_amount")
      )

  }

}
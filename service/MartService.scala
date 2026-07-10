/**
 * Формирование витрин.
 *
 * Содержит методы построения витрин на основе обогащенных транзакций:
 * - обороты банков по MCC и часам;
 * - топ товаров по городам;
 * - активность банковских карт.
 */
package service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class MartService {

  def turnover(df: DataFrame): DataFrame = {

    df
      .withColumn(
        "hour",
        date_trunc("hour", col("transaction_timestamp"))
      )
      .groupBy(
        "bank_name",
        "bank_abbreviation",
        "mcc",
        "mcc_description",
        "hour"
      )
      .agg(
        count("*").alias("transaction_count"),
        round(sum("price"),2).alias("total_amount"),
        round(avg("price"),2).alias("average_receipt")
      )
      .orderBy("hour","bank_name")

  }

  def topProducts(df: DataFrame): DataFrame = {

    df
      .withColumn(
        "city",
        regexp_extract(
          col("address"),
          "г\\.\\s*([^,]+)",
          1
        )
      )
      .groupBy(
        "city",
        "position"
      )
      .agg(
        count("*").alias("transaction_count"),
        round(sum("price"),2).alias("total_amount")
      )
      .orderBy(desc("total_amount"))

  }

  def cardActivity(df: DataFrame): DataFrame = {

    df
      .groupBy(
        "bank_name",
        "payment_system",
        "card_number"
      )
      .agg(
        count("*").alias("transaction_count"),
        round(sum("price"),2).alias("total_amount"),
        min("transaction_timestamp").alias("first_transaction"),
        max("transaction_timestamp").alias("last_transaction")
      )
      .orderBy(desc("total_amount"))

  }

}
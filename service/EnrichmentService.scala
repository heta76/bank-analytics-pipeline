/**
 * Сервис обогащения транзакций.
 *
 * Выполняет объединение транзакций
 * со справочными данными банков,
 * организаций, MCC и других источников
 * для формирования единого набора данных.
 */
package service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class EnrichmentService {

  def enrich(
              raw: DataFrame,
              banks: DataFrame,
              cards: DataFrame,
              organizations: DataFrame,
              mcc: DataFrame,
              peoples: DataFrame
            ): DataFrame = {

    val transactions = model.TransactionSchema.parse(raw)

    val banksDf = banks
      .withColumnRenamed("id", "bank_id")
      .withColumnRenamed("name", "bank_name")
      .withColumnRenamed("abbreviation", "bank_abbreviation")
      .withColumnRenamed("inn", "bank_inn")
      .withColumnRenamed("ogrn", "bank_ogrn")
      .withColumnRenamed("bik", "bank_bik")
      .withColumnRenamed("phone", "bank_phone")
      .withColumnRenamed("date_open", "bank_date_open")
      .withColumnRenamed("head_office", "bank_head_office")

    val organizationsDf = organizations
      .withColumnRenamed("id", "organization_id")
      .withColumnRenamed("address", "organization_address_ref")
      .withColumnRenamed("name_market", "organization_name_ref")
      .withColumnRenamed("type_budget", "organization_type_budget")
      .withColumnRenamed("mcc", "organization_mcc")
      .withColumnRenamed("date_open", "organization_date_open")
      .withColumnRenamed("date_close", "organization_date_close")

    val mccDf = mcc
      .withColumnRenamed("type_organization", "mcc_description")

    transactions
      .join(organizationsDf, Seq("organization_id"), "left")
      .join(mccDf, transactions("mcc") === mccDf("mcc"), "left")
      .drop(mccDf("mcc"))
      .join(banksDf, Seq("bank_name"), "left")
      .dropDuplicates("transaction_id", "position")
  }

}
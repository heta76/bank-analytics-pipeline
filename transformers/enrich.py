from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from base.transformer import Transformer
from utils.logger import setup_logger

logger = setup_logger(__name__)

class EnrichTransformer(Transformer):
    def transform(self, input_df=None):
        raw_postgres_base = self.config["hdfs"]["raw_postgres_base"]
        enriched_path = self.config["hdfs"]["enriched_path"]

        transaction_schema = StructType([
            StructField("bank_id", StringType(), True),
            StructField("info_client", StructType([
                StructField("card", StringType(), True),
                StructField("bank", StringType(), True),
                StructField("pay_system", StringType(), True)
            ])),
            StructField("info_transaction", StructType([
                StructField("terminal", StringType(), True),
                StructField("transaction", StringType(), True),
                StructField("datetime", StringType(), True)
            ])),
            StructField("info_market", StructType([
                StructField("market", StringType(), True),
                StructField("mcc", StringType(), True),
                StructField("address", StringType(), True)
            ])),
            StructField("purchases", ArrayType(StructType([
                StructField("position", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("price", DoubleType(), True)
            ])))
        ])

        # Чтение сырых данных
        if input_df is not None:
            # Используем переданный DataFrame (объединённый streaming+batch)
            raw_df = input_df.select(
                F.col("value").cast("string").alias("json_str")
            )
            logger.info("Using provided input_df (streaming + batch combined)")
        else:
            # Старый режим — читаем из одного raw-пути
            raw_kafka_path = self.config["hdfs"].get("raw_kafka_path")
            if not raw_kafka_path:
                logger.error("No raw_kafka_path in config and no input_df provided")
                raise ValueError("Either input_df or raw_kafka_path must be provided")
            raw_df = self.spark.read.parquet(raw_kafka_path) \
                .select(F.col("value").cast("string").alias("json_str"))
            logger.info("Reading from single raw path: %s", raw_kafka_path)

        # Парсинг JSON
        parsed = raw_df \
            .withColumn("json_str", F.regexp_replace(F.col("json_str"), "'", "\"")) \
            .withColumn("data", F.from_json(F.col("json_str"), transaction_schema)) \
            .select("data.*")

        # Разворачиваем purchases
        exploded = parsed \
            .withColumn("purchase", F.explode("purchases")) \
            .select(
                F.col("info_transaction.transaction").alias("txn_id"),
                F.to_timestamp("info_transaction.datetime").alias("ts"),
                F.col("info_client.bank").alias("bank_name"),
                F.col("info_client.pay_system").alias("pay_system"),
                F.col("info_client.card").alias("masked_card"),
                F.col("info_market.market").alias("market_id"),
                F.col("info_market.mcc").cast("int").alias("mcc"),
                F.col("info_market.address").alias("address"),
                F.col("purchase.position").alias("position"),
                F.col("purchase.price").alias("price")
            )

        # Справочники
        banks = self.spark.read.parquet(f"{raw_postgres_base}/banks")
        mcc = self.spark.read.parquet(f"{raw_postgres_base}/mcc")
        organizations = self.spark.read.parquet(f"{raw_postgres_base}/organizations") \
            .select(
                F.col("id").alias("org_id"),
                F.col("address").alias("market_address"),
                F.col("name_market"),
                F.col("type_budget")
            )

        # Обогащение
        enriched = exploded \
            .join(banks, exploded.bank_name == banks.name, "left") \
            .join(mcc, exploded.mcc == mcc.mcc, "left") \
            .join(organizations, exploded.market_id == organizations.org_id, "left") \
            .select(
                exploded["*"],
                banks.abbreviation.alias("bank_abbr"),
                mcc.type_organization.alias("mcc_desc"),
                organizations.name_market,
                organizations.type_budget,
                F.regexp_extract(F.col("address"), r"г\.\s*([\p{L}\-]+)", 1).alias("city")
            )

        enriched.write.mode("overwrite").parquet(enriched_path)
        logger.info("Enriched data saved to %s", enriched_path)
        return enriched
from concurrent.futures import ThreadPoolExecutor
from base.extractor import Extractor
from utils.logger import setup_logger

logger = setup_logger(__name__)

class PostgresBatchExtractor(Extractor):
    def extract(self):
        jdbc_url = self.config["postgres"]["url"]
        jdbc_props = {
            "user": self.config["postgres"]["user"],
            "password": self.config["postgres"]["password"],
            "driver": "org.postgresql.Driver"
        }
        base_path = self.config["hdfs"]["raw_postgres_base"]

        tables = self.config["postgres"]["tables"]
        logger.info("Starting extraction of %d tables", len(tables))

        def extract_table(table):
            logger.info("Reading %s", table)
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table=f"data.{table}",
                properties=jdbc_props
            )
            output_path = f"{base_path}/{table}"
            df.write.mode("overwrite").parquet(output_path)
            logger.info("Saved %s to %s", table, output_path)

        # Параллельное выполнение для ускорения (если таблицы большие)
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(extract_table, tables)

        logger.info("All tables extracted.")
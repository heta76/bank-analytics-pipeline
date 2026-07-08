from base.loader import Loader
from utils.logger import setup_logger

logger = setup_logger(__name__)

class HiveLoader(Loader):
    def load(self, df, table_name: str):
        self.spark.sql("CREATE DATABASE IF NOT EXISTS marts")
        df.write.mode("overwrite").saveAsTable(table_name)
        logger.info("Table %s saved to Hive", table_name)
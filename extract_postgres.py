import os
import sys
from utils.config_loader import load_config
from utils.spark_session import create_spark_session
from extractors.postgres_batch import PostgresBatchExtractor

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

def main():
    config_path = os.path.join(PROJECT_ROOT, "config.yaml")
    config = load_config(config_path)
    spark = create_spark_session("PostgreSQL Extract")
    extractor = PostgresBatchExtractor(spark, config)
    extractor.extract()
    spark.stop()

if __name__ == "__main__":
    main()
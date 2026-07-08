from pyspark.sql import SparkSession

def create_spark_session(app_name, extra_config=None):
    builder = SparkSession.builder.appName(app_name)
    if extra_config:
        for key, value in extra_config.items():
            builder = builder.config(key, value)
    builder = builder.config("spark.sql.warehouse.dir",
                             "hdfs://namenode:9000/warehouse")
    return builder.enableHiveSupport().getOrCreate()
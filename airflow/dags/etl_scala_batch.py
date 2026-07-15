from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'student',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='etl_scala_batch',
    default_args=default_args,
    schedule_interval='@hourly',          # каждый час
    start_date=datetime(2026, 7, 14),
    catchup=False,
    tags=['scala', 'batch']
) as dag:

    extract_postgres = SparkSubmitOperator(
        task_id='extract_postgres',
        application='/opt/etl/ETL-assembly-1.0.jar',
        java_class='ExtractPostgres',
        conn_id='spark_default',          # использует spark.master из extra
        packages='org.postgresql:postgresql:42.5.0',
        conf={'spark.executor.memory': '1g'},
    )

    extract_kafka_batch = SparkSubmitOperator(
        task_id='extract_kafka_batch',
        application='/opt/etl/ETL-assembly-1.0.jar',
        java_class='ExtractKafkaBatch',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1',
    )

    transform_enrich = SparkSubmitOperator(
        task_id='transform_enrich',
        application='/opt/etl/ETL-assembly-1.0.jar',
        java_class='TransformEnrich',
        conn_id='spark_default',
        files='/opt/etl/application.conf',   # конфиг передаётся через --files
    )

    build_marts = SparkSubmitOperator(
        task_id='build_marts',
        application='/opt/etl/ETL-assembly-1.0.jar',
        java_class='BuildMarts',
        conn_id='spark_default',
    )

    # Порядок выполнения
    [extract_postgres, extract_kafka_batch] >> transform_enrich >> build_marts
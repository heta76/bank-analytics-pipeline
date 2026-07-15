from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Обязательно объявляем default_args, так как они используются в DAG
default_args = {
    'owner': 'student',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='etl_streaming_scala',
    default_args=default_args,
    schedule_interval=timedelta(seconds=30),   # или None для ручного запуска
    start_date=datetime(2026, 7, 14),
    catchup=False,
    tags=['scala', 'streaming']
) as dag:

    refresh_streaming = SparkSubmitOperator(
        task_id='build_streaming_marts',
        application='/opt/etl/ETL-assembly-1.0.jar',
        java_class='BuildStreamingMarts',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1',
    )
Для использования ExtractPostgres необходимо сделать следующее:
1. Скопировать jar-файл внутрь docker-контейнера
docker cp target\scala-2.12\ETL-assembly-1.0.jar spark-master:/opt/etl/
2. Запустить приложение
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class ExtractPostgres /opt/etl/ETL-assembly-1.0.jar


Для использования ExtractKafka необходимо сделать следующее:
1. Скопировать jar-файл внутрь docker-контейнера
docker cp target\scala-2.12\ETL-assembly-1.0.jar spark-master:/opt/etl/
2. Войти в контейнер
docker exec -it spark-master bash
2. Запустить приложение
/spark/bin/spark-submit --master spark://spark-master:7077 --class ExtractKafka --jars /tmp/spark-sql-kafka-0-10_2.12-3.2.1.jar,/tmp/spark-token-provider-kafka-0-10_2.12-3.2.1.jar,/tmp/kafka-clients-2.8.1.jar /opt/etl/ETL-assembly-1.0.jar

Чтобы проверить доставку сообщений, необходимо выполнить следующие действия:
1. Открыть новую консоль
2. Выполнить команду
docker exec -it kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic volga_topic
3. Ввести несколько тестовых сообщений
4.Проверить что Streaming их обработал:
docker exec -it namenode bash
hdfs dfs -ls /raw/kafka_transactions
Если появился новый part-*.parquet, значит поток записал данные.

Проверить содержимое parquet
1. Зайти в Spark Shell:
docker exec -it spark-master /spark/bin/spark-shell
val df = spark.read.parquet("hdfs://namenode:9000/raw/kafka_transactions")
2. Проверь количество записей и вывести сами записи:
df.count()
df.show(false)

---------------------------------------------------------------------------------------------------------------------------------------------------------------
Для использования необходимо скопировать jar-файл внутрь docker-контейнера:  
docker cp target\scala-2.12\ETL-assembly-1.0.jar spark-master:/opt/etl/
---------------------------------------------------------------------------------------------------------------------------------------------------------------
Для входа в контейнер используется команда:  
docker exec -it spark-master bash  
docker exec -it namenode bash  
Вход в Spark Shell:  
docker exec -it spark-master /spark/bin/spark-shell  
---------------------------------------------------------------------------------------------------------------------------------------------------------------
Для использования ExtractPostgres необходимо сделать следующее:  
1. Запустить приложение  
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --class ExtractPostgres /opt/etl/ETL-assembly-1.0.jar  
---------------------------------------------------------------------------------------------------------------------------------------------------------------
Для использования ExtractKafka необходимо сделать следующее:  
1. Войти в контейнер(spark-master)  
2. Запустить приложение  
/spark/bin/spark-submit --master spark://spark-master:7077 --class ExtractKafka --jars /tmp/spark-sql-kafka-0-10_2.12-3.2.1.jar,/tmp/spark-token-provider-kafka-0-10_2.12-3.2.1.jar,/tmp/kafka-clients-2.8.1.jar /opt/etl/ETL-assembly-1.0.jar  
---------------------------------------------------------------------------------------------------------------------------------------------------------------
Чтобы проверить доставку сообщений, необходимо выполнить следующие действия:  
1. Открыть новую консоль  
2. Выполнить команду  
docker exec -it kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic volga_topic  
3. Ввести несколько тестовых сообщений  
4.Проверить что Streaming их обработал:  
4.1 Войти в контейнер(namenode)  
4.2 hdfs dfs -ls /raw/kafka_transactions  
Если появился новый part-*.parquet, значит поток записал данные.  

Проверить содержимое parquet  
1. Зайти в Spark Shell:  
val df = spark.read.parquet("hdfs://namenode:9000/raw/kafka_transactions")  
2. Проверить количество записей и вывести сами записи:  
df.count()  
df.show(false)  
---------------------------------------------------------------------------------------------------------------------------------------------------------------
Для запуска обработчика необходимо проделать следующий алгоритм:  
1. Скопировать конфигурационный файл  
docker cp src\main\resources\application.conf spark-master:/opt/etl/application.conf  
2. Войти в контейнер(spark-master)  
3. Запустить TransformEnrich  
/spark/bin/spark-submit --master spark://spark-master:7077 --class TransformEnrich --files /opt/etl/application.conf /opt/etl/ETL-assembly-1.0.jar  
---------------------------------------------------------------------------------------------------------------------------------------------------------------
Для запуска построения витрин необходимо сделать следующее:  
1. Войти в контейнер(spark-master)  
2. Запустить построение  
/spark/bin/spark-submit --master spark://spark-master:7077 --class BuildMarts /opt/etl/ETL-assembly-1.0.jar  
3. Проверить результат построения  
3.1 Запустить spark-shell  
3.2 Проверить существование БД  
spark.sql("SHOW DATABASES").show(false)  
3.3 Просмотр таблиц  
spark.sql("SHOW TABLES IN marts").show(false)  
3.4 Просмотр витрин  
spark.sql("SELECT * FROM marts.turnover LIMIT 10").show(false)  
spark.sql("SELECT * FROM marts.top_products LIMIT 10").show(false)  
spark.sql("SELECT * FROM marts.card_activity LIMIT 10").show(false)  
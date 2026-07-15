# Bank Analytics ETL Pipeline

Проект ETL-пайплайна для обработки банковских транзакций на Apache Spark (Scala), Kafka, PostgreSQL, HDFS, Hive и Airflow.

## Требования

- Docker 20.10+ и Docker Compose v2+
- Git
- JDK 17 (если нужно пересобирать Scala-код, например Eclipse Temurin или Microsoft OpenJDK 17)
- SBT (можно использовать встроенный в IntelliJ IDEA)

## Структура проекта
├── airflow/ # Airflow (Dockerfile + docker-compose)  
│ ├── Dockerfile  
│ ├── docker-compose.yml  
│ └── dags/ # Python-файлы DAG'ов  
├── src/main/scala/ # Исходный код Spark-приложений  
├── src/main/resources/  
│ └── application.conf # Конфигурация (Kafka, PostgreSQL, HDFS)  
├── build.sbt # Зависимости и сборка  
├── project/ # Настройки SBT  
├── docs/   # инструкции по ручной отладке  
└── readme.md

## Быстрый старт

### 1. Клонирование и подготовка
```bash
git clone https://github.com/heta76/bank-analytics-pipeline
cd bank-analytics-pipeline
```

### 2. Сборка Scala-приложения
```bash
sbt assembly
```

Полученный JAR появится в target/scala-2.12/ETL-assembly-1.0.jar.
Вам не нужно копировать его куда-либо – он автоматически будет доступен в Airflow благодаря смонтированному тому.
### 3. Запуск основного стека

Основные `docker-compose.yml` с сервисами (Spark, Kafka, HDFS, Hive, PostgreSQL, генератор данных) расположены в другом репозитории.
Запустите их из их корневых папок с помощью команды:
```bash
docker-compose up -d
```
Убедитесь, что контейнеры spark-master, kafka1, namenode, postgres_container, hive-metastore, hive-server запущены.
### 4. Запуск Airflow
```bash
cd airflow
docker-compose up -d --build # --build при первом запуске или после изменения Dockerfile
```
Airflow будет доступен по адресу http://localhost:8082 (логин: admin, пароль: admin).
### 5. Настройка подключения к Spark (один раз)
После первого запуска Airflow выполните команду:
```bash
docker exec -it airflow airflow connections add 'spark_default' \
    --conn-type 'generic' \
    --conn-extra '{"spark.master": "spark://spark-master:7077", "spark.deploy_mode": "client"}'
```
Это подключение используется всеми DAG'ами.

### 6. Запуск ETL-задач
Откройте Airflow UI, включите нужный DAG (переключатель слева) и нажмите Trigger DAG (▶).

 - etl_scala_batch – полный batch‑цикл: извлечение справочников, исторических данных из Kafka, обогащение и построение трёх витрин в Hive.

 - etl_streaming_scala – стриминговая обработка, строит витрину card_activity_streaming в реальном времени (работает постоянно).
### 7. Проверка результатов
**Витрины в Hive**  
Подключитесь к Hive Server:
```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
```
Выполните SQL:
```bash
USE marts;
SHOW TABLES;
SELECT * FROM turnover LIMIT 10;
SELECT * FROM top_products LIMIT 10;
SELECT * FROM card_activity LIMIT 10;
```
**Стриминговая витрина**  
Проверьте файлы в HDFS:
```bash
docker exec namenode hdfs dfs -ls /marts/card_activity_streaming
```

### 8. Полезные команды
**Просмотр логов контейнера:** 
```bash
docker logs airflow
docker logs spark-master
docker logs kafka1
```
### Примечания
- Сеть контейнеров: все сервисы используют общую сеть disable_ipv6. DNS-имена (kafka1, namenode, spark-master) доступны из любого контейнера в этой сети.

- Конфигурация Spark-приложений задана в src/main/resources/application.conf. При необходимости её можно изменить и пересобрать JAR.

- Порты: Airflow UI – 8082, Spark Master – 8080, HDFS NameNode – 9870, Kafka – 19092 (внутри сети).

- Генератор данных может завершаться с ошибкой list index out of range – это нормально, он исчерпывает заложенный датасет. Для постоянной генерации перезапустите его:
```bash
docker restart kafka-generator-generate-kafka-data-1
```
- Удаление чекпоинтов стриминга (при ошибке Concurrent update to the log):
```bash
docker exec namenode hdfs dfs -rm -r /checkpoints/card_activity_streaming
```
- Команды для ручной отладки находятся в docs/manual-run.md

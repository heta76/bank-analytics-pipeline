FROM apache/airflow:2.9.3-python3.10

USER root

RUN sed -i 's/deb.debian.org/mirror.yandex.ru/g' /etc/apt/sources.list.d/debian.sources || true && \
    sed -i 's/deb.debian.org/mirror.yandex.ru/g' /etc/apt/sources.list || true && \
    sed -i 's/security.debian.org/mirror.yandex.ru/g' /etc/apt/sources.list.d/debian.sources || true && \
    sed -i 's/security.debian.org/mirror.yandex.ru/g' /etc/apt/sources.list || true

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

ENV JDK_JAVA_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-opens=java.security.jgss/sun.security.jgss.krb5=ALL-UNNAMED"

RUN mkdir -p /opt/etl && chown -R airflow:root /opt/etl

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    pyspark==3.2.1
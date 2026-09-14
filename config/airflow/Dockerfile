# =============================================================================
# Airflow + клиент Spark (Java 17 + pyspark + provider apache-spark), чтобы
# DAG'и могли запускать spark-submit на кластер spark://spark-master:7077.
# Python образа (3.11) совпадает с Python на Spark-executor'ах.
# =============================================================================
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG SPARK_VERSION=3.5.8
ARG TARGETARCH

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
# spark-defaults.conf монтируется в /opt/spark-conf из config/spark (см. compose)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH} \
    SPARK_CONF_DIR=/opt/spark-conf

USER airflow
# Provider ставим по constraints Airflow, затем pyspark нужной версии
# (constraints-файл Airflow фиксирует другую патч-версию pyspark).
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pyspark==${SPARK_VERSION}"

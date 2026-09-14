"""
Проверочный DAG: Airflow -> spark-submit на кластер -> запись в Iceberg (HDFS).
Запуск: включить DAG в UI и нажать Trigger (или `airflow dags test`).
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_iceberg_smoke",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smoke"],
) as dag:
    SparkSubmitOperator(
        task_id="write_iceberg_table",
        conn_id="spark_default",  # AIRFLOW_CONN_SPARK_DEFAULT в docker-compose.yml
        application="/opt/jobs/iceberg_smoke_job.py",  # каталог ./jobs
        name="airflow-iceberg-smoke",
        # jar-пакеты, каталог iceberg и ресурсы — из config/spark/spark-defaults.conf
    )

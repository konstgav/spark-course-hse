"""
Витрина «выручка по дням и регионам» для Metabase.

Один шаг: spark-submit скрипта jobs/retail/mart_daily_revenue.py. Он читает
iceberg.retail.sales_silver и перезаписывает таблицу dwh.public.mart_daily_revenue.

Шаблон для своих витрин: скопируйте этот файл и скрипт из jobs/retail/,
поменяйте dag_id, application и name.
"""
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="retail_mart_daily_revenue",                     # имя DAG в интерфейсе Airflow
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Moscow"),
    schedule=None,                                          # запуск вручную: кнопка Trigger DAG
    catchup=False,
    tags=["retail"],
) as dag:
    SparkSubmitOperator(
        task_id="build_mart",
        conn_id="spark_default",                            # кластер spark://spark-master:7077
        application="/opt/jobs/retail/mart_daily_revenue.py",   # = ./jobs/retail/ в репозитории
        name="retail-mart-daily-revenue",                   # имя приложения в Spark UI
    )

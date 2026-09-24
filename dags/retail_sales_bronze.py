"""
Почасовая загрузка продаж: data/retail/sales/sales_<час>.csv -> iceberg.retail.sales_bronze.

Каждый запуск DAG отвечает за один час. Airflow сам «догоняет» все часы
от start_date до end_date (catchup=True) — по одному запуску на час, строго
по очереди (max_active_runs=1): кластер у нас один, и одна загрузка занимает его целиком.

Даты совпадают с генератором по умолчанию (--start 2026-09-04 --days 3).
Сгенерировали другой период — поменяйте start_date и end_date.
"""
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="retail_sales_bronze",
    start_date=pendulum.datetime(2026, 9, 4, 0, tz="Europe/Moscow"),   # первый файл
    end_date=pendulum.datetime(2026, 9, 6, 23, tz="Europe/Moscow"),    # последний файл
    schedule="@hourly",
    catchup=True,
    max_active_runs=1,
    tags=["retail"],
) as dag:
    SparkSubmitOperator(
        task_id="load_hour",
        conn_id="spark_default",
        application="/opt/jobs/retail/load_sales_bronze.py",
        # Час, за который отвечает запуск, по московскому времени: 2026-09-04T14
        application_args=["{{ data_interval_start.in_timezone('Europe/Moscow').strftime('%Y-%m-%dT%H') }}"],
        name="retail-bronze",
    )

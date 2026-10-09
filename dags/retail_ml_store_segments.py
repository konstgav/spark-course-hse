"""
Еженедельное переобучение модели сегментации магазинов (k-means на Spark ML).

Один шаг: spark-submit скрипта jobs/retail/ml_store_segments.py. Он собирает
признаки магазинов из iceberg.retail.sales_silver, обучает модель, сохраняет её
в HDFS и пишет сегменты в Postgres: dwh.public.ml_store_segments,
ml_segment_profile и журнал запусков ml_runs. Дашборд Metabase читает эти таблицы.

Число сегментов k — параметр DAG: значение по умолчанию задано ниже (его берут
запуски по расписанию), другое можно передать вручную через Trigger DAG w/ config.
"""
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="retail_ml_store_segments",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Moscow"),
    schedule="0 6 * * 1",                  # cron: каждый понедельник в 06:00
    catchup=False,                         # пропущенные недели не догоняем — только последнюю
    params={"k": 4},                       # число сегментов
    tags=["retail", "ml"],
) as dag:
    SparkSubmitOperator(
        task_id="train_segments",
        conn_id="spark_default",
        application="/opt/jobs/retail/ml_store_segments.py",
        # Дата модели — понедельник, когда сработало расписание (по Москве).
        # Ручной запуск получает понедельник текущей недели и перезаписывает её модель.
        application_args=[
            "--k", "{{ params.k }}",
            "--model-date", "{{ data_interval_end.in_timezone('Europe/Moscow').strftime('%Y-%m-%d') }}",
        ],
        name="retail-ml-store-segments",
    )

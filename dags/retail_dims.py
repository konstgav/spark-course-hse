"""
Справочники розничной сети: магазины, категории, товары -> Iceberg.

Запускается вручную (кнопка Trigger DAG) после генерации данных, до загрузки продаж.
"""
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="retail_dims",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Moscow"),
    schedule=None,          # только ручной запуск
    catchup=False,
    tags=["retail"],
) as dag:
    SparkSubmitOperator(
        task_id="load_dims",
        conn_id="spark_default",
        application="/opt/jobs/retail/load_dims.py",
        name="retail-dims",
    )

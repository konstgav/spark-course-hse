"""
Справочники розничной сети: CSV -> iceberg.retail.{stores, categories, products}.

Паттерн Full Loader: справочники маленькие, поэтому каждый запуск целиком
перезаписывает таблицу из свежего файла (createOrReplace — одна транзакция Iceberg,
читатели видят либо старую версию, либо новую).
Запускается DAG'ом retail_dims, вручную:
    docker compose exec spark-master spark-submit /opt/jobs/retail/load_dims.py
"""
from pyspark.sql import SparkSession

DIMS_DIR = "file:///opt/data/retail/dims"   # каталог ./data/retail/dims репозитория

SCHEMAS = {
    "stores": "store_id INT, store_name STRING, city STRING, region STRING, format STRING, open_date DATE",
    "categories": "category_id INT, category_name STRING, group_name STRING",
    "products": "product_id INT, product_name STRING, category_id INT, base_price_kop BIGINT",
}

spark = SparkSession.builder.appName("retail-dims").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.retail")

for name, schema in SCHEMAS.items():
    # escape='"': в названиях есть запятые и кавычки, pandas экранирует их удвоением
    df = spark.read.csv(f"{DIMS_DIR}/{name}.csv", header=True, schema=schema, escape='"')
    df.writeTo(f"iceberg.retail.{name}").using("iceberg").createOrReplace()
    print(f"iceberg.retail.{name}: {spark.table(f'iceberg.retail.{name}').count()} строк")

spark.stop()

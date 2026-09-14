"""
Проверка стенда с хост-машины: python host/smoke_test.py
(или то же самое построчно в ячейках JupyterLab).
"""
import platform
import socket
import sys

from pyspark.sql import functions as F

from spark_session import get_spark


def check_host_setup() -> None:
    try:
        for name in ("namenode", "datanode", "hive-metastore", "postgres"):
            socket.gethostbyname(name)
    except socket.gaierror:
        sys.exit(f"Имя '{name}' не резолвится: добавьте в hosts-файл строку "
                 "'127.0.0.1 namenode datanode hive-metastore postgres' (см. README.md)")
    if sys.version_info[:2] != (3, 11):
        print(f"ВНИМАНИЕ: Python {sys.version.split()[0]}, в кластере 3.11 — "
              "Python UDF будут падать")


check_host_setup()
spark = get_spark("smoke-test")
sc = spark.sparkContext
print("Spark", spark.version, "| UI приложения:", sc.uiWebUrl)

# 1. Iceberg: namespace + таблица в HDFS + запись/чтение
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo")
spark.sql("DROP TABLE IF EXISTS iceberg.demo.hello PURGE")
spark.sql("CREATE TABLE iceberg.demo.hello (id BIGINT, msg STRING) USING iceberg")
spark.sql("INSERT INTO iceberg.demo.hello VALUES (1, 'it works'), (2, 'from host')")
spark.table("iceberg.demo.hello").show()
spark.sql("SELECT snapshot_id, operation FROM iceberg.demo.hello.snapshots").show()

# 2. Параллельность: задачи распределяются по executor'ам на двух воркерах
hosts = (
    spark.range(0, 1_000_000, numPartitions=8)
    .withColumn("k", F.col("id") % 97)
    .rdd.mapPartitions(lambda it: [(socket.gethostname(), sum(1 for _ in it))])
    .reduceByKey(lambda a, b: a + b)
    .collect()
)
print("Строк обработано на воркерах:", dict(hosts))

# 3. Python UDF (проверка совпадения версий Python на driver и executor'ах)
spark.range(3).select(F.udf(lambda x: x * 10, "long")("id").alias("x10")).show()

# 4. toPandas через Arrow
print(spark.table("iceberg.demo.hello").toPandas())

# 5. Запись витрины в Postgres (база dwh) по JDBC
spark.table("iceberg.demo.hello").write.format("jdbc").mode("overwrite").options(
    url="jdbc:postgresql://postgres:5432/dwh", dbtable="public.hello",
    user="course", password="course_pass", driver="org.postgresql.Driver",
).save()
print("JDBC: записано строк в dwh.public.hello:", spark.read.format("jdbc").options(
    url="jdbc:postgresql://postgres:5432/dwh", dbtable="public.hello",
    user="course", password="course_pass", driver="org.postgresql.Driver",
).load().count())

# 6. CatBoost на Spark (только Linux: на Docker Desktop driver на хосте не видит
#    контейнеры — там используйте jobs/catboost_smoke_job.py, см. README.md)
if platform.system() == "Linux":
    from pyspark.ml.linalg import Vectors
    import catboost_spark

    train = spark.createDataFrame(
        [(Vectors.dense(float(i % 7), float(i % 3)), float((i % 7) > 3)) for i in range(1000)],
        ["features", "label"],
    )
    pool = catboost_spark.Pool(train)
    model = catboost_spark.CatBoostClassifier(iterations=50).fit(pool)
    model.transform(pool.data).groupBy("label", "prediction").count().show()

spark.stop()
print("OK: стенд работает")

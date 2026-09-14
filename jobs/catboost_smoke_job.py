"""
Проверка распределённого обучения CatBoost на кластере (driver внутри docker-сети):
    docker compose exec spark-master spark-submit /opt/jobs/catboost_smoke_job.py
"""
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("catboost-smoke").getOrCreate()
import catboost_spark  # python-обёртка поставляется внутри jar catboost-spark

df = spark.createDataFrame(
    [(Vectors.dense(float(i % 7), float(i % 3)), float((i % 7) > 3)) for i in range(1000)],
    ["features", "label"],
)
pool = catboost_spark.Pool(df)
model = catboost_spark.CatBoostClassifier(iterations=50).fit(pool)
model.transform(pool.data).groupBy("label", "prediction").count().show()

spark.stop()

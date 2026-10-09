"""
Сегментация магазинов (k-means): iceberg.retail.sales_silver -> Postgres dwh.public.ml_*.

Запускается раз в неделю DAG'ом retail_ml_store_segments:

    spark-submit ml_store_segments.py --k 4 --model-date 2026-10-05

1. Spark SQL собирает по каждому магазину четыре признака (одна строка = один магазин).
2. Pipeline Spark ML: VectorAssembler -> StandardScaler -> KMeans.
3. Сегменты перенумеровываются по дневной выручке магазинов (чек × поток): 1 — самая
   низкая. Номера кластеров k-means случайны и меняются от запуска к запуску, а
   графики Metabase и имена сегментов на дашборде — нет.
4. Результаты пишутся в Postgres, модель — в HDFS (/models/store_segments/<дата>).

Функции store_features() и train() импортирует ноутбук занятия 4
(notebooks/lab-04-ml.ipynb), чтобы подбирать k на том же коде, что работает в DAG.
"""
import argparse

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FEATURES = ["receipts_per_day", "avg_check_rub", "items_per_receipt", "cash_share_pct"]

DWH = dict(url="jdbc:postgresql://postgres:5433/dwh", user="course", password="course_pass",
           driver="org.postgresql.Driver")
MODEL_DIR = "hdfs://namenode:9000/models/store_segments"


def store_features(spark):
    """Признаки магазинов: одна строка = один магазин."""
    return spark.sql("""
        WITH receipts AS (                         -- сначала собираем чеки
            SELECT store_id, event_date, receipt_id,
                   FIRST(payment_method)     AS payment_method,
                   COUNT(*)                  AS items,
                   SUM(price_paid_kop) / 100 AS amount_rub
            FROM iceberg.retail.sales_silver
            WHERE operation_type = 'SALE' AND NOT is_cancelled
            GROUP BY store_id, event_date, receipt_id
        )
        SELECT r.store_id, st.store_name, st.city, st.region, st.format,
               COUNT(*) / COUNT(DISTINCT r.event_date)                      AS receipts_per_day,
               AVG(r.amount_rub)                                            AS avg_check_rub,
               AVG(r.items)                                                 AS items_per_receipt,
               100 * AVG(CASE WHEN r.payment_method = 'CASH' THEN 1 ELSE 0 END) AS cash_share_pct
        FROM receipts AS r
        JOIN iceberg.retail.stores AS st ON st.store_id = r.store_id
        GROUP BY r.store_id, st.store_name, st.city, st.region, st.format
    """)


def train(features, k):
    """Обучает k-means и возвращает (модель, магазины с колонкой segment, silhouette)."""
    pipeline = Pipeline(stages=[
        VectorAssembler(inputCols=FEATURES, outputCol="raw_features"),
        # признаки в разных единицах (чеки в тысячах, доля в процентах):
        # без масштабирования расстояние определял бы один receipts_per_day
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
        KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster"),
    ])
    model = pipeline.fit(features)
    clustered = model.transform(features)
    silhouette = ClusteringEvaluator(featuresCol="features", predictionCol="cluster").evaluate(clustered)

    # кластер -> сегмент 1..k по возрастанию дневной выручки магазинов
    order = (clustered.groupBy("cluster")
             .agg(F.avg(F.col("avg_check_rub") * F.col("receipts_per_day")).alias("revenue"))
             .orderBy("revenue").collect())
    mapping = {row["cluster"]: i + 1 for i, row in enumerate(order)}
    segment = F.create_map(*[F.lit(x) for pair in mapping.items() for x in pair])
    segments = clustered.withColumn("segment", segment[F.col("cluster")]).drop(
        "raw_features", "features", "cluster")
    return model, segments, silhouette


def profile(segments):
    """Средний портрет сегмента: по одной строке на сегмент."""
    return (segments.groupBy("segment")
            .agg(F.count("*").alias("stores"),
                 *[F.avg(c).alias(c) for c in FEATURES],
                 F.sum(F.when(F.col("format") == "гипермаркет", 1).otherwise(0)).alias("hypermarkets"),
                 F.sum(F.when(F.col("format") == "супермаркет", 1).otherwise(0)).alias("supermarkets"),
                 F.sum(F.when(F.col("format") == "у дома", 1).otherwise(0)).alias("convenience"))
            .orderBy("segment"))


def rounded(df):
    """Признаки с 2 знаками после запятой — так их удобнее читать в Metabase."""
    return df.select(*[F.col(c).cast("decimal(18,2)").alias(c) if c in FEATURES else c
                       for c in df.columns])


def write_to_dwh(df, table, mode="overwrite"):
    (df.write.format("jdbc").mode(mode)
        .option("truncate", "true")     # overwrite: очистить таблицу, но сохранить её (и графики)
        .options(dbtable=f"public.{table}", **DWH)
        .save())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--k", type=int, default=4, help="число сегментов")
    parser.add_argument("--model-date", required=True, help="дата модели, YYYY-MM-DD")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("retail-ml-store-segments").getOrCreate()

    features = store_features(spark).cache()
    model, segments, silhouette = train(features, args.k)
    segments = segments.withColumn("model_date", F.lit(args.model_date).cast("date"))

    # Модель с датой в пути: каждую неделю — новая версия, старые остаются для сравнения
    model_path = f"{MODEL_DIR}/{args.model_date}"
    model.write().overwrite().save(model_path)

    write_to_dwh(rounded(segments), "ml_store_segments")
    write_to_dwh(rounded(profile(segments)), "ml_segment_profile")
    # журнал запусков дописывается, а не перезаписывается: видно, как менялось качество
    runs = spark.createDataFrame(
        [(args.model_date, args.k, features.count(), round(silhouette, 4), model_path)],
        "model_date STRING, k INT, stores BIGINT, silhouette DOUBLE, model_path STRING",
    ).withColumn("model_date", F.col("model_date").cast("date")) \
     .withColumn("trained_at", F.current_timestamp())
    write_to_dwh(runs, "ml_runs", mode="append")

    print(f"k={args.k}, silhouette={silhouette:.3f}, модель: {model_path}")
    profile(segments).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

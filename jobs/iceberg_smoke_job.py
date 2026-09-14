"""PySpark-джоба для spark_iceberg_smoke: пишет строку в iceberg.demo.airflow_runs."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo")
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.demo.airflow_runs (
        run_ts TIMESTAMP, rows_counted BIGINT
    ) USING iceberg
""")

cnt = spark.range(0, 1_000_000, numPartitions=8).filter(F.col("id") % 3 == 0).count()
spark.createDataFrame([(cnt,)], "rows_counted BIGINT") \
    .select(F.current_timestamp().alias("run_ts"), "rows_counted") \
    .writeTo("iceberg.demo.airflow_runs").append()

spark.table("iceberg.demo.airflow_runs").orderBy(F.desc("run_ts")).show(truncate=False)
spark.stop()

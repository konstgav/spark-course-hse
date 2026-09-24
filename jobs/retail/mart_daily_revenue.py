"""
Витрина «выручка по дням и регионам»: iceberg.retail.sales_silver -> Postgres dwh.public.mart_daily_revenue.

Тот же запрос, что в примере из notebooks/lab-03-analytics.ipynb, оформленный как
скрипт для spark-submit. Запускается DAG'ом retail_mart_daily_revenue.

Витрина маленькая, поэтому каждый запуск перезаписывает её целиком: повторный
запуск даёт тот же результат, а не удвоенные цифры.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("retail-mart-daily-revenue").getOrCreate()

mart = spark.sql("""
    SELECT
        s.event_date,
        st.region,
        -- чистая выручка: продажи без отменённых позиций минус возвраты
        CAST(SUM(CASE
                    WHEN s.operation_type = 'SALE' AND NOT s.is_cancelled THEN s.price_paid_kop
                    WHEN s.operation_type = 'RETURN' THEN -s.price_paid_kop
                    ELSE 0
                  END) / 100 AS DECIMAL(18, 2)) AS revenue_rub,
        COUNT(DISTINCT CASE WHEN s.operation_type = 'SALE' AND NOT s.is_cancelled
                            THEN s.receipt_id END) AS receipts
    FROM iceberg.retail.sales_silver AS s
    JOIN iceberg.retail.stores AS st ON st.store_id = s.store_id
    GROUP BY s.event_date, st.region
""")

# Запись выполняют executor'ы внутри сети Docker, поэтому хост — postgres, а не localhost
(mart.write.format("jdbc").mode("overwrite")
    .option("truncate", "true")     # очистить таблицу, но сохранить её (и графики Metabase)
    .options(url="jdbc:postgresql://postgres:5433/dwh", dbtable="public.mart_daily_revenue",
             user="course", password="course_pass", driver="org.postgresql.Driver")
    .save())

print(f"dwh.public.mart_daily_revenue: записано {mart.count()} строк")
spark.stop()

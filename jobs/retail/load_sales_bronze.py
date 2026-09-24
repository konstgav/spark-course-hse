"""
Bronze-слой: один часовой CSV-файл -> iceberg.retail.sales_bronze.

Данные кладутся как есть, со всеми дублями и битыми строками источника, и получают
три служебных колонки: за какой час пришёл файл, имя файла и время загрузки.

Паттерн Data Overwrite: таблица партиционирована по часу файла, и загрузка
перезаписывает партицию своего часа целиком (overwritePartitions). Поэтому
повторный запуск за тот же час не создаёт дублей — он заменяет прошлую загрузку.

Аргумент — час файла:
    spark-submit /opt/jobs/retail/load_sales_bronze.py 2026-09-04T14
"""
import sys

from pyspark.sql import SparkSession, functions as F

hour = sys.argv[1]                                         # 2026-09-04T14
source_file = f"sales_{hour}.csv"
path = f"file:///opt/data/retail/sales/{source_file}"      # ./data/retail/sales репозитория

SCHEMA = """
    event_id BIGINT, event_ts TIMESTAMP, receipt_id BIGINT, line_no INT,
    store_id INT, product_id INT, category_id INT, customer_id BIGINT,
    is_loyalty BOOLEAN, operation_type STRING, quantity INT,
    price_regular_kop BIGINT, price_paid_kop BIGINT, payment_method STRING,
    ref_event_id BIGINT
"""

spark = SparkSession.builder.appName(f"retail-bronze-{hour}").getOrCreate()
# Файл ~80 МБ по умолчанию читался бы одной задачей (порог 128 МБ) — режем мельче,
# чтобы работали все 4 ядра кластера.
spark.conf.set("spark.sql.files.maxPartitionBytes", "16m")

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.retail")
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.retail.sales_bronze (
        event_id BIGINT, event_ts TIMESTAMP, receipt_id BIGINT, line_no INT,
        store_id INT, product_id INT, category_id INT, customer_id BIGINT,
        is_loyalty BOOLEAN, operation_type STRING, quantity INT,
        price_regular_kop BIGINT, price_paid_kop BIGINT, payment_method STRING,
        ref_event_id BIGINT,
        file_hour TIMESTAMP, source_file STRING, loaded_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (hours(file_hour))
""")

df = (
    spark.read.csv(path, header=True, schema=SCHEMA, timestampFormat="yyyy-MM-dd HH:mm:ss.SSS")
    .withColumn("file_hour", F.to_timestamp(F.lit(hour), "yyyy-MM-dd'T'HH"))
    .withColumn("source_file", F.lit(source_file))
    .withColumn("loaded_at", F.current_timestamp())
)
df.writeTo("iceberg.retail.sales_bronze").overwritePartitions()

loaded = spark.table("iceberg.retail.sales_bronze").where(F.col("source_file") == source_file).count()
print(f"{source_file}: загружено {loaded} строк в iceberg.retail.sales_bronze")
spark.stop()

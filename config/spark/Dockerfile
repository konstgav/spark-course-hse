# =============================================================================
# Образ для spark-master / spark-worker-1 / spark-worker-2.
#
# База — официальный apache/spark (Java 17, Ubuntu 22.04).
# В базовом образе Python 3.10, а версия Python у driver'а (JupyterLab на хосте,
# Airflow) и у executor'ов должна совпадать до minor-версии — иначе любые
# Python UDF / rdd.map / pandas_udf падают с
# "Python in worker has different version". Поэтому ставим отдельный Python
# ${PYTHON_VERSION} (через uv) и кладём его первым в PATH: executor запускает
# python-воркеры командой `python3`.
# =============================================================================
ARG SPARK_VERSION=3.5.8
FROM ghcr.io/astral-sh/uv:0.8.22 AS uv

FROM apache/spark:${SPARK_VERSION}-java17-python3
ARG PYTHON_VERSION=3.11

USER root
COPY --from=uv /uv /usr/local/bin/uv

ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python \
    VIRTUAL_ENV=/opt/python \
    PATH=/opt/python/bin:/opt/spark/bin:${PATH}

# Библиотеки, нужные executor'ам для pandas_udf / applyInPandas / toPandas.
# Домашний каталог пользователя spark (в базовом образе /nonexistent) нужен Ivy
# для spark.jars.packages при запуске spark-submit/pyspark внутри контейнера.
RUN uv venv --python ${PYTHON_VERSION} /opt/python \
    && uv pip install --no-cache "numpy<2" "pandas<2.3" "pyarrow>=11,<19" \
    && mkdir -p /opt/spark/conf /opt/spark/work /home/spark/.ivy2 \
    && chown -R spark:spark /opt/spark/conf /opt/spark/work /home/spark \
    && usermod -d /home/spark spark

# spark-defaults.conf монтируется в /opt/spark/conf из config/spark (см. compose)

USER spark
WORKDIR /opt/spark/work

"""
SparkSession для JupyterLab на хост-машине студента.

Driver (этот Python-процесс + JVM) работает на хосте, executor'ы — в Docker.

    import sys; sys.path.append("<путь к репозиторию>/host")
    from spark_session import get_spark
    spark = get_spark("my-notebook")

Перед первым запуском в hosts-файле должна быть строка
    127.0.0.1 namenode datanode hive-metastore postgres
(см. README.md, раздел «Подготовка хост-машины»).
"""
import os
import platform
import shutil
import subprocess

from pyspark.sql import SparkSession

# Шлюз docker-сети стенда (подсеть задана в docker-compose.yml)
DOCKER_NETWORK_GATEWAY = "172.28.0.1"


def _is_docker_desktop() -> bool:
    if platform.system() != "Linux":
        return True  # Mac / Windows — только Docker Desktop
    if not shutil.which("docker"):
        return False
    try:
        out = subprocess.run(["docker", "info", "--format", "{{.OperatingSystem}}"],
                             capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return False
    return "Docker Desktop" in out  # Docker Desktop for Linux / WSL2


def driver_host() -> str:
    """Адрес, по которому executor'ы из контейнеров подключаются к driver'у.

    Docker Engine на Linux: IP шлюза docker-сети (именно IP, а не имя —
    CatBoost-spark резолвит адрес driver'а на самом хосте).
    Docker Desktop (Mac/Windows/WSL2): host.docker.internal.
    Переопределяется переменной окружения SPARK_DRIVER_HOST.
    """
    if os.environ.get("SPARK_DRIVER_HOST"):
        return os.environ["SPARK_DRIVER_HOST"]
    return "host.docker.internal" if _is_docker_desktop() else DOCKER_NETWORK_GATEWAY


SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1",
    "ai.catboost:catboost-spark_3.5_2.12:1.2.10",
    "org.postgresql:postgresql:42.7.8",
])

HOST_SPARK_CONF = {
    "spark.master": "spark://localhost:7077",

    # --- сеть: executor'ы в контейнерах подключаются обратно к driver'у -----
    # spark.driver.host вычисляется в get_spark(), см. driver_host()
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.port": "7078",
    "spark.blockManager.port": "7079",
    # Результаты задач возвращаются через RPC-соединение с driver'ом, а не
    # скачиваются driver'ом с executor'ов (контейнеры недоступны с хоста
    # на Docker Desktop Mac/Windows).
    "spark.rpc.message.maxSize": "512",
    "spark.task.maxDirectResultSize": "512m",

    # --- Iceberg + Hive Metastore + HDFS ------------------------------------
    "spark.jars.packages": SPARK_PACKAGES,
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hive",
    "spark.sql.catalog.iceberg.uri": "thrift://hive-metastore:9083",
    # Имя namenode (а не localhost) — этот путь сохраняется в metastore и
    # должен одинаково работать и на хосте, и внутри контейнеров.
    "spark.sql.catalog.iceberg.warehouse": "hdfs://namenode:9000/warehouse",
    "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
    "spark.hadoop.dfs.client.use.datanode.hostname": "true",

    # --- ресурсы: 2 executor'а (по одному на воркер) --------------------------
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.executor.cores": "2",
    "spark.cores.max": "4",
    "spark.sql.shuffle.partitions": "8",

    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.sql.session.timeZone": "Europe/Moscow",
}


def _setup_hadoop_home() -> None:
    """Windows: Hadoop ищет winutils.exe по HADOOP_HOME, а hadoop.dll — по PATH.

    Переменные среды, выставленные host/install_winutils.ps1, видны только в
    терминалах, открытых после установки. Чтобы не зависеть от этого, ищем
    winutils.exe в типовых каталогах и настраиваем окружение сами: JVM стартует
    из этого процесса и наследует его (см. README.md, п. 1.3).
    """
    if platform.system() != "Windows":
        return

    candidates = [os.environ["HADOOP_HOME"]] if os.environ.get("HADOOP_HOME") else []
    candidates += [os.path.expandvars(r"%USERPROFILE%\hadoop"), r"D:\hadoop", r"C:\hadoop"]

    for home in candidates:
        bin_dir = os.path.join(home, "bin")
        if not os.path.isfile(os.path.join(bin_dir, "winutils.exe")):
            continue
        os.environ["HADOOP_HOME"] = home
        if bin_dir.lower() not in os.environ.get("PATH", "").lower():
            os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + bin_dir
        return

    print("ВНИМАНИЕ: winutils.exe не найден, Hadoop не сможет работать с локальными "
          "файлами. Запустите host\\install_winutils.ps1 (см. README.md, п. 1.3)")


def get_spark(app_name: str = "student-notebook", **extra_conf: str) -> SparkSession:
    """Создаёт (или возвращает уже созданную) SparkSession на кластере в Docker.

    extra_conf — дополнительные параметры, точки в имени заменяются на "__":
        get_spark("etl", spark__executor__memory="1500m")
    """
    # Executor'ы запускают Python-воркеры своим `python3` (Python 3.11 в образе).
    # Путь к Python хоста (например, из conda/venv) в контейнерах не существует.
    os.environ["PYSPARK_PYTHON"] = "python3"
    _setup_hadoop_home()

    builder = SparkSession.builder.appName(app_name)
    conf = {**HOST_SPARK_CONF, "spark.driver.host": driver_host()}
    conf.update({k.replace("__", "."): v for k, v in extra_conf.items()})
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()

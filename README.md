# Стенд курса: Spark + Iceberg + ML

Docker Compose для практики: HDFS, Hive Metastore (каталог Iceberg), Spark Standalone
(master + 2 worker'а), Postgres и Airflow. JupyterLab в стек **не входит**: студенты
запускают его на своей машине и подключаются к Spark-кластеру через опубликованные порты.

```
 ХОСТ-МАШИНА                                   DOCKER (сеть 172.28.0.0/24)
 ┌──────────────────────────┐   spark://localhost:7077   ┌──────────────┐   ┌────────────────┐
 │ JupyterLab               │ ─────────────────────────▶ │ spark-master │──▶│ spark-worker-1 │
 │  PySpark driver          │ ◀── executor'ы подключаются│  :7077 :8090 │   │ spark-worker-2 │
 │  (host/spark_session.py) │     обратно к driver'у     └──────────────┘   └───────┬────────┘
 └───────────┬──────────────┘     (:7078, :7079)                                   │
             │ thrift://hive-metastore:9083   ┌────────────────┐                   │
             ├───────────────────────────────▶│ hive-metastore │── схема ──┐       │
             │ hdfs://namenode:9000           └────────────────┘           ▼       ▼
             ├───────────────────────────────▶ namenode + datanode ◀──── данные Iceberg
             │ jdbc:postgresql://postgres:5432                       ┌──────────────────────────┐
             └─────────────────────────────────────────────────────▶ │ postgres                 │
                                                                     │ airflow / metastore / dwh│
 Браузер ──▶ Airflow :8080 (webserver + scheduler, spark-submit) ──▶ └──────────────────────────┘
```

* **Данные** Iceberg-таблиц лежат в HDFS (`hdfs://namenode:9000/warehouse`).
* **Каталог** (какие таблицы есть и где их текущий snapshot) — в Hive Metastore, его
  служебная БД в Postgres (`metastore`).
* **Вычисления** выполняет Spark. Каталог `iceberg` = `SparkCatalog` с `type=hive`.

## Состав и порты

| Сервис | Назначение | Адрес с хоста |
|---|---|---|
| spark-master | Spark Standalone master | `spark://localhost:7077`, UI http://localhost:8090 |
| spark-worker-1 / -2 | воркеры: по 2 ядра и 2g, Python 3.11 | UI http://localhost:8081, http://localhost:8082 |
| namenode | HDFS NameNode | `hdfs://namenode:9000`, UI http://localhost:9870 |
| datanode | HDFS DataNode | порт данных `9866`, UI http://localhost:9864 |
| hive-metastore | каталог Iceberg | `thrift://hive-metastore:9083` |
| postgres | базы `airflow`, `metastore`, `dwh` | `localhost:5432`, `course` / `course_pass` |
| airflow-webserver / -scheduler | Airflow 2.10 (LocalExecutor) | http://localhost:8080, `admin` / `admin` |
| Spark UI приложения | поднимается driver'ом на хосте | http://localhost:4040 |

Версии и пароли задаются в [.env](.env), версии jar-пакетов — в
[config/spark/spark-defaults.conf](config/spark/spark-defaults.conf) и
[host/spark_session.py](host/spark_session.py).

## Требования

* Docker Engine 24+ с Compose v2 (Linux) или Docker Desktop (Mac/Windows):
  **не меньше 8 GB RAM, 4 CPU и 10 GB на диске**. Стенд в простое занимает около 3 GB,
  каждый executor добавляет примерно 1.4 GB.
* Свободные порты: 4040, 5432, 7077–7079, 8080–8082, 8090, 9000, 9083, 9864, 9866, 9870.
* На Apple Silicon образы `bde2020/*` (HDFS, Hive) работают через эмуляцию amd64 —
  медленнее, но работают.
* Windows: если на диске `C:` мало места, см. [Решение проблем](#решение-проблем).

## 1. Запуск стенда

```bash
cd iceberg-course-infra
# Linux: чтобы файлы в ./logs принадлежали вам, а не UID 50000
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env

docker compose up -d --build
docker compose ps -a
```

Первый запуск занимает 5–15 минут: скачиваются базовые образы, собираются
`iceberg-course/spark` и `iceberg-course/airflow`. Ожидаемое состояние: все сервисы `Up`,
у сервисов с healthcheck — `(healthy)`, `airflow-init` — `Exited (0)` (одноразовая
инициализация).

Проверка кластера изнутри Docker:

```bash
# HDFS: 1 live datanode
docker compose exec namenode hdfs dfsadmin -report | grep -E "Live datanodes|DFS Remaining"

# Spark: 2 ALIVE worker'а, 4 ядра
curl -s localhost:8090/json/ | python3 -c 'import json,sys; d=json.load(sys.stdin); print("workers:", d["aliveworkers"], "cores:", d["cores"])'

# Spark + Iceberg + CatBoost (driver внутри docker-сети)
docker compose exec spark-master spark-submit /opt/jobs/iceberg_smoke_job.py
docker compose exec spark-master spark-submit /opt/jobs/catboost_smoke_job.py
```

**Airflow → Spark → Iceberg:** на http://localhost:8080 включите DAG `spark_iceberg_smoke`
и нажмите *Trigger DAG*. То же из консоли:
`docker compose exec airflow-scheduler airflow dags test spark_iceberg_smoke`.
Задача должна завершиться `success`, в логе появится таблица `iceberg.demo.airflow_runs`.

> Первый запуск Spark-приложения 1–2 минуты скачивает jar'ы Iceberg, CatBoost и JDBC из
> Maven Central, дальше они берутся из кеша `~/.ivy2`.

## 2. Подготовка хост-машины студента

### 2.1. hosts-файл (обязательно)

Имена `namenode`, `datanode` и `postgres` сохраняются в metastore и в JDBC-адресах, поэтому
должны резолвиться одинаково и в контейнерах, и на хосте:

* Linux / macOS: `sudo sh -c 'echo "127.0.0.1 namenode datanode hive-metastore postgres" >> /etc/hosts'`
* Windows: та же строка в `C:\Windows\System32\drivers\etc\hosts` (Блокнот от имени
  администратора).

### 2.2. Python 3.11 + Java 17 + pyspark 3.5.8

Версии должны совпадать с кластером: PySpark 3.5 не поддерживает Python 3.12+ и Java 21, а
при другой minor-версии Python падают любые Python UDF. Поэтому всё ставится в отдельное
conda-окружение `spark-course`, системный Python и Java можно не трогать.

Скрипт ставит Miniconda (если conda ещё нет), создаёт окружение и устанавливает
[host/requirements.txt](host/requirements.txt). Повторный запуск безопасен — готовые шаги
пропускаются:

```bash
bash host/install_miniconda.sh                                        # Linux / macOS
powershell -ExecutionPolicy Bypass -File host\install_miniconda.ps1   # Windows
```

Каталог установки по умолчанию `~/miniconda3` (`%USERPROFILE%\miniconda3`); меняется через
`MINICONDA_PREFIX=/opt/miniconda3` или `-Prefix D:\miniconda3`. Путь — без пробелов и
кириллицы.

То же вручную:

```bash
conda create -n spark-course --override-channels -c conda-forge python=3.11 openjdk=17
conda activate spark-course
pip install -r host/requirements.txt
```

`--override-channels` берёт пакеты только из `conda-forge`: без него свежая Miniconda
требует принять условия канала `defaults` (`CondaToSNonInteractiveError`). Java pyspark
находит сам, `JAVA_HOME` задавать не нужно. Проверка: `python --version` → `3.11.x`,
`java -version` → `17.x`.

После установки **откройте новый терминал**, чтобы появилась команда `conda`. Если
PowerShell пишет *«выполнение сценариев отключено в этой системе»* — один раз выполните
`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` или используйте *Anaconda Prompt*.

### 2.3. Windows: winutils

Hadoop под Windows требует `winutils.exe` и `hadoop.dll`; в pyspark они не входят, на Linux
и macOS не нужны. Без них сессия падает на `Did not find winutils.exe ... HADOOP_HOME and
hadoop.home.dir are unset`. Скрипт скачивает обе утилиты и прописывает `HADOOP_HOME` и
`PATH` в переменные среды пользователя — прав администратора не требует:

```powershell
powershell -ExecutionPolicy Bypass -File host\install_winutils.ps1
```

По умолчанию ставит в `%USERPROFILE%\hadoop`, на другой диск — `-Prefix D:\hadoop`. После
установки **откройте новый терминал**.

### 2.4. Linux: файрвол

Executor'ы подключаются к driver'у на хосте из подсети `172.28.0.0/24`. Если включён `ufw`,
откройте её, иначе задачи будут висеть без ошибок:

```bash
sudo ufw allow from 172.28.0.0/24
```

### 2.5. Проверка с хоста (smoke test)

```bash
conda activate spark-course
cd host && python smoke_test.py
```

Скрипт проверяет по порядку: hosts-файл, запись и чтение Iceberg-таблицы в HDFS,
распределение задач по `spark-worker-1` и `spark-worker-2`, Python UDF, `toPandas()`,
запись в `dwh.public.hello` по JDBC, обучение CatBoost (на Linux). В конце выводится
`OK: стенд работает`. Файлы таблицы видны в http://localhost:9870 →
*Utilities → Browse the file system* → `/warehouse/demo.db/hello`.

## 3. Подключение из JupyterLab

```python
import sys
sys.path.append("/путь/к/iceberg-course-infra/host")
from spark_session import get_spark

spark = get_spark("lab-01")          # 2 executor'а по 1 ядру/1g — по одному на каждом воркере

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo")
spark.sql("CREATE TABLE IF NOT EXISTS iceberg.demo.t (id BIGINT, name STRING) USING iceberg")
spark.sql("INSERT INTO iceberg.demo.t VALUES (1, 'a')")
spark.table("iceberg.demo.t").show()

spark.stop()                         # обязательно: освобождает ресурсы кластера
```

`get_spark()` из [host/spark_session.py](host/spark_session.py) задаёт master, адрес
driver'а, jar-пакеты, каталог `iceberg`, HDFS и ресурсы. Дополнительные параметры
передаются аргументами: `get_spark("etl", spark__sql__shuffle__partitions="16")`.

Что важно знать:

* **Одно приложение занимает 2 ядра из 4.** Два открытых ноутбука (или ноутбук и DAG)
  работают одновременно, третий ждёт ресурсов. Неиспользуемые сессии закрывайте
  через `spark.stop()`, занятость видна на http://localhost:8090.
* **Запись в Postgres** выполняют executor'ы, поэтому в JDBC URL указывайте имя `postgres`,
  а не `localhost`: `jdbc:postgresql://postgres:5432/dwh`.
* **Сырые файлы** читают executor'ы, поэтому их нужно загрузить в HDFS: положите файл
  в `./data` и выполните
  ```bash
  docker compose exec namenode hdfs dfs -mkdir -p /raw
  docker compose exec namenode hdfs dfs -put -f /data/file.csv /raw/
  ```
  затем читайте как `spark.read.csv("hdfs://namenode:9000/raw/file.csv", header=True)`.
  Каталог `./data` смонтирован в Spark и Airflow как `/opt/data`.
* **Адрес driver'а** `get_spark()` выбирает сам: шлюз `172.28.0.1` на Linux с Docker Engine,
  `host.docker.internal` на Docker Desktop. Переопределяется `SPARK_DRIVER_HOST`.
* **CatBoost на Spark** (`import catboost_spark`) с хоста работает **только на Linux с
  Docker Engine**: master CatBoost подключается к executor'ам по IP контейнеров, а с Docker
  Desktop эти адреса недоступны. На Mac/Windows обучайте внутри кластера — положите скрипт
  в `./jobs` и запустите `docker compose exec spark-master spark-submit /opt/jobs/<script>.py`
  или через Airflow. Пример — [jobs/catboost_smoke_job.py](jobs/catboost_smoke_job.py).

## 4. Airflow

* DAG'и кладите в `./dags`, PySpark-скрипты для них — в `./jobs` (в контейнере `/opt/jobs`).
* Готовые connections: `spark_default` → `spark://spark-master:7077` (для
  `SparkSubmitOperator`) и `dwh_postgres` → база `dwh`.
* Параметры `spark-submit` из Airflow берутся из
  [config/spark/spark-defaults.conf](config/spark/spark-defaults.conf).
* Новые DAG'и создаются на паузе и появляются в UI в течение ~30 секунд.

## Остановка и сброс

```bash
docker compose stop                         # остановить, контейнеры и данные сохраняются
docker compose down                         # удалить контейнеры, данные (volume'ы) сохраняются
docker compose down -v --remove-orphans     # полный сброс: HDFS, Postgres, кеш Ivy Airflow
```

## Решение проблем

| Симптом | Причина / решение |
|---|---|
| `UnknownHostException: namenode` / `datanode`, запись в HDFS висит | нет строки в hosts-файле (п. 2.1) |
| `Python in worker has different version` | на хосте не Python 3.11 (п. 2.2) |
| `JAVA_GATEWAY_EXITED`, ошибки `sun.nio` при старте сессии | нет Java 17 или используется Java 21 (п. 2.2) |
| `Did not find winutils.exe`, `UnsatisfiedLinkError: NativeIO$Windows.access0` | Windows: не установлен winutils (п. 2.3) |
| Задачи висят, executor'ы не могут подключиться к driver'у | Linux: `ufw` (п. 2.4); Docker Desktop: проверьте `SPARK_DRIVER_HOST` |
| Задачи висят в `(0 + 2) / N`, приложение `WAITING` в UI master'а | кластер занят другими сессиями: закройте лишние, см. http://localhost:8090 |
| `Pool overlaps with other one on this address space` при `up` | подсеть `172.28.0.0/24` занята: поменяйте её в `docker-compose.yml` и задайте `SPARK_DRIVER_HOST` |
| Порт уже занят | остановите конфликтующий сервис или поменяйте левую часть `ports:` |
| hive-metastore перезапускается | `docker compose logs hive-metastore`; обычно помогает полный сброс (выше) |
| `failed to compute cache key: failed to send write: ... desktop-containerd` при `up --build` | кончилось место в виртуальном диске Docker Desktop (ниже) |
| `Error 28 No space left on device` при `pip install` | кончилось место на диске с conda-окружением (ниже) |

### Windows: нет места на диске `C:`

Docker Desktop и conda по умолчанию держат всё на `C:`: виртуальный диск с образами
(`%LOCALAPPDATA%\Docker\wsl\disk\docker_data.vhdx`, около 10 ГБ) и окружение с кэшами
(ещё 8–10 ГБ). И то, и другое переносится без прав администратора.

**Docker:** Settings → Resources → Advanced → **Disk image location** → укажите
`D:\DockerData` → Apply & restart (Docker предложит перенести существующие данные). Быстро
освободить место, ничего не перенося: `docker builder prune -a -f` и
`docker image prune -a -f`. Файл VHDX после удаления данных сам не уменьшается — сожмите
его: `wsl --shutdown`, затем `wsl --manage docker-desktop-data --set-sparse true`.

> ⚠️ Не используйте `docker system prune --volumes` и `docker compose down -v`: они удалят
> volume'ы `iceberg-course_pg_data` и `iceberg-course_hadoop_*`, то есть базы
> Airflow/metastore и все Iceberg-таблицы.

**Conda:** переносить нужно и окружение, и кэш пакетов, и `%TEMP%` (туда pip распаковывает
колёса, один `pyspark` — больше 300 МБ), иначе `Error 28` повторится:

```powershell
conda env remove -n spark-course; conda clean --all --yes; pip cache purge
New-Item -ItemType Directory -Force D:\conda\envs, D:\conda\pkgs, D:\conda\tmp, D:\pip-cache
conda config --add envs_dirs D:\conda\envs   # новые окружения -> D:
conda config --add pkgs_dirs D:\conda\pkgs   # кэш пакетов -> тот же диск, иначе hardlink
                                             # между томами не работает и объём удваивается
setx PIP_CACHE_DIR D:\pip-cache
setx TMP D:\conda\tmp
setx TEMP D:\conda\tmp
```

Откройте новый терминал (`setx` действует только на новые процессы) и создайте окружение
заново (п. 2.2). Активация по имени `conda activate spark-course` продолжает работать.

### Windows: «Невозможно запустить Windchill ProductionPoint Client Manager (порт 8989)»

Окно появляется после установки Docker Desktop; к стенду отношения не имеет — порт 8989
занят другим или зависшим экземпляром Client Manager. Смените порт и перезапустите его
из нового терминала:

```powershell
setx WPP_CLIENT_MSG_PORT 8990
```

## Структура

```
iceberg-course-infra/
├── docker-compose.yml
├── .env                          # версии, пароли, UID Airflow, ресурсы воркеров
├── config/
│   ├── spark/Dockerfile          # apache/spark 3.5.8 (Java 17) + Python 3.11, pandas, pyarrow
│   ├── spark/spark-defaults.conf # конфиг Spark для driver'ов внутри docker (spark-submit, Airflow)
│   ├── airflow/Dockerfile        # Airflow 2.10.5 + Java 17 + pyspark + provider apache-spark
│   ├── hive/start-metastore.sh   # инициализация схемы metastore + запуск
│   └── postgres/init-multiple-dbs.sh  # создание баз airflow / metastore / dwh
├── host/                         # для машины студента
│   ├── requirements.txt
│   ├── install_miniconda.sh      # Miniconda + окружение spark-course (Linux / macOS)
│   ├── install_miniconda.ps1     # то же для Windows
│   ├── install_winutils.ps1      # winutils.exe + hadoop.dll + HADOOP_HOME (только Windows)
│   ├── spark_session.py          # get_spark(): SparkSession к кластеру в Docker
│   └── smoke_test.py             # проверка стенда с хоста
├── dags/spark_iceberg_smoke.py   # проверочный DAG
├── jobs/                         # PySpark-скрипты (/opt/jobs в Spark и Airflow)
├── data/                         # сырые файлы (/data в namenode, /opt/data в Spark и Airflow)
└── logs/                         # логи Airflow
```

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
* **Каталог** (какие таблицы есть и где их текущий snapshot) хранится в Hive Metastore,
  служебная БД которого находится в Postgres (`metastore`).
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

* Docker Engine 24+ с Compose v2 (Linux) или Docker Desktop (Mac/Windows).
  Нужно **не меньше 8 GB RAM и 4 CPU** для Docker: сам стенд в простое занимает около 3 GB,
  каждый executor добавляет примерно 1.4 GB. Под образы понадобится около 10 GB на диске.
* Должны быть свободны порты 4040, 5432, 7077–7079, 8080–8082, 8090, 9000, 9083,
  9864, 9866, 9870.
* На Apple Silicon образы `bde2020/*` (HDFS, Hive) работают через эмуляцию amd64.
  Это медленнее, но работает.

## 1. Запуск стенда

```bash
cd iceberg-course-infra
# Linux: чтобы файлы в ./logs принадлежали вам, а не UID 50000
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env

docker compose up -d --build
```

Первый запуск занимает 5–15 минут: скачиваются базовые образы, собираются образы
`iceberg-course/spark` и `iceberg-course/airflow`. Статус смотрите так:

```bash
docker compose ps -a
```

Ожидаемое состояние: все сервисы `Up`, у сервисов с healthcheck `(healthy)`, а
`airflow-init` в статусе `Exited (0)` (это одноразовая инициализация). Сервисы стартуют
в порядке зависимостей: postgres → namenode → datanode → hive-metastore; параллельно
spark-master → worker'ы и airflow-init → webserver/scheduler.

При первом старте metastore создаёт схему в БД `metastore`, это видно в логе:
`docker compose logs hive-metastore | grep -i schema`.

## 2. Проверка кластера (без хоста)

```bash
# HDFS: 1 live datanode
docker compose exec namenode hdfs dfsadmin -report | grep -E "Live datanodes|DFS Remaining"

# Spark: 2 ALIVE worker'а, 4 ядра
curl -s localhost:8090/json/ | python3 -c 'import json,sys; d=json.load(sys.stdin); print("workers:", d["aliveworkers"], "cores:", d["cores"])'

# Hive Metastore слушает порт
docker compose exec hive-metastore bash -c '</dev/tcp/localhost/9083' && echo metastore OK

# Postgres: базы airflow, metastore, dwh
docker compose exec postgres psql -U course -d postgres -c '\l' | grep -E 'airflow|metastore|dwh'

# Spark + Iceberg + CatBoost из контейнера (driver внутри docker-сети)
docker compose exec spark-master spark-submit /opt/jobs/catboost_smoke_job.py
docker compose exec spark-master spark-submit /opt/jobs/iceberg_smoke_job.py
```

**Airflow → Spark → Iceberg:** откройте http://localhost:8080, включите DAG
`spark_iceberg_smoke` и нажмите *Trigger DAG*. То же из консоли:

```bash
docker compose exec airflow-scheduler airflow dags test spark_iceberg_smoke
```

Задача должна завершиться `success`, а в логе появится таблица `iceberg.demo.airflow_runs`.

> Первый запуск Spark-приложения скачивает jar'ы Iceberg, CatBoost и JDBC из Maven
> Central, это занимает 1–2 минуты. Потом они берутся из кеша `~/.ivy2`.

## 3. Подготовка хост-машины студента

### 3.1. hosts-файл (обязательно)

Пути к таблицам сохраняются в metastore с именами `namenode` и `datanode`, а JDBC-адрес
витрин использует имя `postgres`. Эти имена должны одинаково резолвиться и в контейнерах,
и на хосте. Добавьте строку:

```
127.0.0.1 namenode datanode hive-metastore postgres
```

* Linux / macOS: `sudo sh -c 'echo "127.0.0.1 namenode datanode hive-metastore postgres" >> /etc/hosts'`
* Windows: откройте Блокнот от имени администратора и добавьте строку в
  `C:\Windows\System32\drivers\etc\hosts`.

### 3.2. Python 3.11 + Java 17 + pyspark 3.5.8

Версии должны совпадать с кластером. PySpark 3.5 не поддерживает Python 3.12+ и Java 21,
а при другой minor-версии Python любые Python UDF падают с ошибкой
`Python in worker has different version`. Поэтому всё ставится в отдельное conda-окружение
`spark-course`, и системный Python с Java можно не трогать.

#### Вариант А: скрипт (Miniconda + окружение)

Скрипт скачивает Miniconda (если conda ещё не установлена), ставит её в домашний каталог
без прав администратора, создаёт окружение `spark-course` с Python 3.11 и Java 17 из
`conda-forge` и устанавливает пакеты из [host/requirements.txt](host/requirements.txt).
Если что-то уже установлено, этот шаг пропускается, поэтому скрипт можно запускать повторно.

* Linux / macOS ([host/install_miniconda.sh](host/install_miniconda.sh)):
  ```bash
  bash host/install_miniconda.sh
  ```
* Windows ([host/install_miniconda.ps1](host/install_miniconda.ps1)), в PowerShell из
  каталога `iceberg-course-infra`:
  ```powershell
  powershell -ExecutionPolicy Bypass -File host\install_miniconda.ps1
  ```

Каталог установки по умолчанию `~/miniconda3` (`%USERPROFILE%\miniconda3` на Windows). Его
можно изменить: `MINICONDA_PREFIX=/opt/miniconda3 bash host/install_miniconda.sh` или
`... -File host\install_miniconda.ps1 -Prefix D:\miniconda3`. На Windows путь не должен
содержать пробелов.

После установки **откройте новый терминал**, чтобы в нём появилась команда `conda`, и
запустите JupyterLab:

```bash
conda activate spark-course
jupyter lab
```

На Windows, если PowerShell пишет *«выполнение сценариев отключено в этой системе»*, один
раз выполните `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` или используйте
*Anaconda Prompt* из меню «Пуск».

#### Вариант Б: вручную

1. Установите Miniconda (если conda ещё нет):
   * **Linux** (для ARM замените `x86_64` на `aarch64`):
     ```bash
     curl -fsSLo miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
     bash miniconda.sh -b -p ~/miniconda3 && rm miniconda.sh
     ~/miniconda3/bin/conda init "$(basename "$SHELL")"
     ```
   * **macOS**: то же самое с установщиком `Miniconda3-latest-MacOSX-arm64.sh`
     (Apple Silicon) или `Miniconda3-latest-MacOSX-x86_64.sh` (Intel).
   * **Windows**: скачайте и запустите
     [Miniconda3-latest-Windows-x86_64.exe](https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe).
     Выберите *Just Me*, путь без пробелов и кириллицы. Дальнейшие команды выполняйте в
     *Anaconda Prompt*.

   Затем откройте новый терминал и проверьте: `conda --version`.

2. Создайте окружение и запустите JupyterLab (из каталога `iceberg-course-infra`):
   ```bash
   conda create -n spark-course --override-channels -c conda-forge python=3.11 openjdk=17
   conda activate spark-course
   pip install -r host/requirements.txt
   jupyter lab
   ```
   Флаг `--override-channels` берёт пакеты только из `conda-forge`. Без него свежая
   Miniconda может потребовать принять условия использования канала `defaults`
   (`CondaToSNonInteractiveError`).

Java из conda-окружения pyspark находит сам, отдельно `JAVA_HOME` задавать не нужно.
Проверка: `python --version` покажет `3.11.x`, а `java -version` — `17.x`.

### 3.3. Linux: файрвол

Executor'ы подключаются к driver'у на хосте из подсети `172.28.0.0/24`. Если включён
`ufw`, откройте её, иначе задачи будут висеть без ошибок:

```bash
sudo ufw allow from 172.28.0.0/24
```

## 4. Подключение из JupyterLab

```python
import sys
sys.path.append("/путь/к/iceberg-course-infra/host")
from spark_session import get_spark

spark = get_spark("lab-01")          # 2 executor'а по 1 ядру/1g — по одному на каждом воркере

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo")
spark.sql("CREATE TABLE IF NOT EXISTS iceberg.demo.t (id BIGINT, name STRING) USING iceberg")
spark.sql("INSERT INTO iceberg.demo.t VALUES (1, 'a')")
spark.table("iceberg.demo.t").show()

# ... в конце работы обязательно освободить ресурсы кластера
spark.stop()
```

Функция `get_spark()` из [host/spark_session.py](host/spark_session.py) задаёт все нужные
параметры: master, адрес driver'а, jar-пакеты, каталог `iceberg`, HDFS и ресурсы.
Дополнительные параметры можно передать аргументами:
`get_spark("etl", spark__sql__shuffle__partitions="16")`.

Что важно знать:

* **Адрес driver'а.** Executor'ы открывают соединения к driver'у на хосте.
  `get_spark()` выбирает адрес сам: на Linux с Docker Engine это шлюз `172.28.0.1`,
  на Docker Desktop (Mac/Windows/WSL2) — `host.docker.internal`. Переопределить можно
  переменной окружения `SPARK_DRIVER_HOST`.
* **Одно приложение занимает 2 ядра из 4.** Два открытых ноутбука (или ноутбук и DAG)
  работают одновременно, третий будет ждать ресурсов. Неиспользуемые сессии закрывайте
  через `spark.stop()`.
* **Запись в Postgres** выполняют executor'ы, поэтому в JDBC URL указывайте имя
  `postgres`, а не `localhost`: `jdbc:postgresql://postgres:5432/dwh`.
* **Сырые файлы** читаются executor'ами, поэтому их нужно загрузить в HDFS. Положите файл
  в `./data` и выполните:
  ```bash
  docker compose exec namenode hdfs dfs -mkdir -p /raw
  docker compose exec namenode hdfs dfs -put -f /data/file.csv /raw/
  ```
  После этого читайте его как `spark.read.csv("hdfs://namenode:9000/raw/file.csv", header=True)`.
  Каталог `./data` также смонтирован в Spark и Airflow как `/opt/data` для джоб,
  запущенных внутри кластера.
* **CatBoost на Spark** (`import catboost_spark`) из ноутбука на хосте работает **только на
  Linux с Docker Engine**. Процесс master CatBoost на driver'е сам подключается к
  executor'ам по IP контейнеров, а на Docker Desktop эти адреса с хоста недоступны. На
  Mac/Windows запускайте обучение внутри кластера: положите скрипт в `./jobs` и выполните
  `docker compose exec spark-master spark-submit /opt/jobs/<script>.py` (или используйте
  Airflow). Пример — [jobs/catboost_smoke_job.py](jobs/catboost_smoke_job.py).
  Служебные файлы обучения (`learn_error.tsv`, `catboost_training.json`, `tmp/` и т.п.)
  CatBoost пишет в текущий каталог driver'а.

### Проверка с хоста (smoke test)

```bash
conda activate spark-course
cd host && python smoke_test.py
```

Скрипт проверяет по порядку: hosts-файл, запись и чтение Iceberg-таблицы в HDFS,
распределение задач по `spark-worker-1` и `spark-worker-2`, Python UDF, `toPandas()`,
запись в `dwh.public.hello` по JDBC, обучение CatBoost (на Linux). В конце выводится
`OK: стенд работает`. Файлы таблицы можно посмотреть в http://localhost:9870 →
*Utilities → Browse the file system* → `/warehouse/demo.db/hello`.

## 5. Airflow

* DAG'и кладите в `./dags`, PySpark-скрипты для них — в `./jobs` (внутри контейнера это
  `/opt/jobs`).
* Уже настроены connections:
  * `spark_default` → `spark://spark-master:7077`, для `SparkSubmitOperator`;
  * `dwh_postgres` → база `dwh`.
* Параметры `spark-submit` из Airflow (jar-пакеты, каталог `iceberg`, ресурсы) берутся из
  [config/spark/spark-defaults.conf](config/spark/spark-defaults.conf).
* Новые DAG'и создаются на паузе и появляются в UI в течение ~30 секунд.

## Типичные проблемы

| Симптом | Причина / решение |
|---|---|
| `UnknownHostException: namenode` / `datanode`, или запись в HDFS висит | нет строки в hosts-файле (п. 3.1) |
| Задачи висят в `(0 + 2) / N`, в UI master'а приложение `WAITING` | кластер занят другими сессиями: закройте лишние (`spark.stop()`), см. http://localhost:8090 |
| Задачи висят, executor'ы в логах воркера не могут подключиться к driver'у | Linux: `ufw` (п. 3.3); Docker Desktop: проверьте `SPARK_DRIVER_HOST` |
| `Python in worker has different version` | на хосте не Python 3.11 |
| `JAVA_GATEWAY_EXITED` / ошибки `sun.nio` при старте сессии | на хосте нет Java 17 или используется Java 21 |
| `Pool overlaps with other one on this address space` при `up` | подсеть `172.28.0.0/24` занята: поменяйте её в `docker-compose.yml` и задайте `SPARK_DRIVER_HOST` |
| Порт уже занят | остановите конфликтующий сервис или поменяйте левую часть `ports:` |
| hive-metastore перезапускается | `docker compose logs hive-metastore`; обычно помогает полный сброс (ниже) |

## Остановка и сброс

```bash
docker compose stop                         # остановить, контейнеры и данные сохраняются
docker compose down                         # удалить контейнеры, данные (volume'ы) сохраняются
docker compose down -v --remove-orphans     # полный сброс: HDFS, Postgres, кеш Ivy Airflow
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
│   ├── spark_session.py          # get_spark(): SparkSession к кластеру в Docker
│   └── smoke_test.py             # проверка стенда с хоста
├── dags/spark_iceberg_smoke.py   # проверочный DAG
├── jobs/                         # PySpark-скрипты (/opt/jobs в Spark и Airflow)
├── data/                         # сырые файлы (/data в namenode, /opt/data в Spark и Airflow)
└── logs/                         # логи Airflow
```

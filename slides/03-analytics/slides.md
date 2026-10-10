---
marp: true
theme: course
paginate: true
footer: 'Инженерная аналитика больших данных · Пермский кампус ВШЭ × Digital Future Systems · осень 2026'
---

<!-- _class: title -->
<!-- _paginate: false -->
<!-- _footer: "" -->

![Высшая школа экономики](../theme/logo-2.svg) ![Digital Future Systems](../theme/logo_new.png)

# Инженерная аналитика больших данных: Spark, Iceberg, HDFS

## Мини-курс · занятие 3 — аналитика и витрины

|  |  |
|---|---|
| Преподаватель | Константин Гаврилов |
| Индустриальный партнёр | Digital Future Systems (ООО «ДФС») |
| ВУЗ | Пермский кампус Высшей школы экономики |
| Программа | магистратура «Бизнес-аналитика», 2 курс |
| Период | осень 2026 |

---

<!-- _class: lead -->

# Занятие 3. Аналитика

## Витрины в Postgres, регулярный пересчёт в Airflow, графики в Metabase

---

<!-- _class: theory -->

## Путь данных сегодня

```
iceberg.retail.sales_silver   ~35 млн строк, HDFS
        │  Spark SQL: агрегаты
        ▼
Postgres dwh.public.mart_*    сотни строк
        │
        ▼
Metabase                      графики и дашборд
```

* **Витрина** — маленькая таблица под конкретный вопрос бизнеса
* Тяжёлые вычисления — в Spark, где данные. BI-инструмент читает готовый результат
* Metabase не умеет читать Iceberg, а Postgres отвечает за миллисекунды

---

<!-- _class: theory -->

## Метрики: договариваемся заранее

| метрика | формула |
|---|---|
| **Выручка** | продажи без отменённых **минус** возвраты |
| **Чек** | `receipt_id` среди неотменённых продаж |
| **Средний чек** | выручка продаж / число чеков |

```sql
SUM(CASE WHEN operation_type = 'SALE' AND NOT is_cancelled THEN price_paid_kop
         WHEN operation_type = 'RETURN'                    THEN -price_paid_kop
         ELSE 0 END) / 100 AS revenue_rub
```

Одна метрика, посчитанная по-разному в двух отчётах, — самая частая причина споров
«почему у вас цифры не сходятся».

---

<!-- _class: theory -->

## Ловушки агрегации

* **`COUNT(*)` — не число чеков.** В чеке много позиций: `COUNT(DISTINCT receipt_id)`
* **Средний чек — не `AVG(price_paid_kop)`.** Это средняя цена позиции. Сначала соберите
  чеки (`GROUP BY receipt_id`), потом усредните
* **Среднее средних** ≠ среднему. Средний чек региона — не среднее средних чеков его
  магазинов: у магазинов разное число чеков
* **Отмены и возвраты.** Без `NOT is_cancelled` выручка завышена, без возвратов — тоже

---

<!-- _class: theory -->

## Оконные функции

Нумерация, доли, накопительные суммы **внутри группы** без потери строк:

```sql
ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY revenue DESC) AS rank
revenue / SUM(revenue) OVER (PARTITION BY region)                   AS share
```

| category | product | revenue | rank |
|---|---|---|---|
| Сыры | Гауда «Кама» | 912 000 | 1 |
| Сыры | Российский «Утро» | 640 000 | 2 |
| Хлеб | Батон «Своё» | 455 000 | 1 |

`GROUP BY` схлопывает строки группы в одну, окно — нет.

---

<!-- _class: theory -->

## Запись витрины в Postgres

```python
(mart.write.format("jdbc").mode("overwrite")
     .option("truncate", "true")
     .options(url="jdbc:postgresql://postgres:5433/dwh",
              dbtable="public.mart_daily_revenue",
              user="course", password="course_pass",
              driver="org.postgresql.Driver")
     .save())
```

* `overwrite` + `truncate`: таблица очищается и заполняется заново, но **не удаляется** —
  графики Metabase не ломаются
* Витрина маленькая, пересчитать её целиком проще, чем обновлять частями
* Хост `postgres`, а не `localhost`: пишут executor'ы внутри Docker

---

<!-- _class: theory -->

## От ноутбука к DAG

| в ноутбуке | в скрипте для DAG |
|---|---|
| `spark = get_spark("lab-03")` | `spark = SparkSession.builder.getOrCreate()` |
| запрос + `write_to_dwh(...)` | тот же запрос + та же запись |
| `spark.stop()` | `spark.stop()` |

DAG — десять строк, отличаются только имена:

```python
with DAG(dag_id="retail_mart_daily_revenue", schedule=None, ...) as dag:
    SparkSubmitOperator(
        task_id="build_mart",
        conn_id="spark_default",
        application="/opt/jobs/retail/mart_daily_revenue.py",
        name="retail-mart-daily-revenue",
    )
```

`./dags` → Airflow находит DAG сам. `./jobs` → в контейнере это `/opt/jobs`.

---

<!-- _class: practice -->

## Шаг 1. Пример: выручка по дням и регионам

1. Убедиться, что DAG `retail_sales_bronze` загрузил все 72 часа, и обновить silver
   (раздел 3 ноутбука занятия 2)
2. `git pull`, `cp notebooks/lab-03-analytics.ipynb work/` и открыть `work/lab-03-analytics.ipynb`
3. Выполнить пример: запрос → `mart_daily_revenue` в Postgres
4. Открыть `jobs/retail/mart_daily_revenue.py` и `dags/retail_mart_daily_revenue.py`,
   найти в них тот же запрос
5. `spark.stop()`, затем в Airflow включить `retail_mart_daily_revenue` → **Trigger DAG**

---

<!-- _class: practice -->

## Шаг 2. Задача 1: средний чек

Витрина `mart_avg_check`: средний чек по регионам и форматам магазинов, участники программы и остальные.

1. Сначала собрать чеки: `GROUP BY receipt_id`, потом усреднить суммы чеков по группам
2. Регион и формат — из `iceberg.retail.stores`
3. Записать в Postgres: `write_to_dwh(avg_check, "mart_avg_check")`

Колонки витрины и подсказки — в ноутбуке.

---

<!-- _class: practice -->

## Шаг 3. Задача 2: своя витрина в DAG

1. Скопировать `jobs/retail/mart_daily_revenue.py` → `jobs/retail/mart_avg_check.py`,
   вставить свой запрос, `dbtable="public.mart_avg_check"`
2. Скопировать `dags/retail_mart_daily_revenue.py` → `dags/retail_mart_avg_check.py`,
   поменять `dag_id`, `application`, `name`
3. `spark.stop()` в ноутбуке. Через ~30 секунд DAG появится в Airflow → включить → **Trigger DAG**
4. Упало — **Logs** задачи, ошибка Spark в конце лога

---

<!-- _class: practice -->

## Шаг 4. Задача 3: дашборд в Metabase

1. http://localhost:3000. База `dwh` не подключена — README стенда, раздел «Metabase»
2. Новых таблиц не видно — ⚙ **Admin → Databases → dwh → Sync database schema**:
   сам Metabase перечитывает схему только раз в час
3. **New → Question** → `Mart Daily Revenue` → Summarize: сумма `Revenue Rub`
   по `Event Date: Day` и `Region` → **Line**
4. Второй вопрос по своей витрине: `Mart Avg Check` → **Bar**
5. **New → Dashboard** «Розничная сеть» → добавить оба вопроса
6. Запустить DAG витрины ещё раз и обновить дашборд

---

<!-- _class: practice -->

## Шаг 5. Задачи повышенной сложности

На дополнительный балл — после того, как готовы задачи 1–3:

| задача | витрина | о чём вопрос |
|---|---|---|
| 4* | `mart_top_products` | топ-5 товаров по выручке в каждой категории |
| 5* | `mart_payment_mix` | доли способов оплаты в выручке каждого региона |

Обе — на **оконные функции**. Сессию Spark открыть заново (ячейка в ноутбуке), готовые
витрины добавить на дашборд «Розничная сеть».

---

## Итоги занятия

* **Витрина** — маленькая агрегированная таблица под вопрос бизнеса.
  Считает Spark, хранит Postgres, показывает Metabase
* Метрики определяются **один раз** и одинаково во всех отчётах
* Средний чек — через уровень чека, топ-N и доли — через **оконные функции**
* Код из ноутбука превращается в DAG почти без изменений
* Витрина перезаписывается целиком — пересчёт безопасно повторять

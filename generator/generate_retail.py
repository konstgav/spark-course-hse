"""
Генератор синтетических данных розничной сети.

Сеть магазинов в 15 регионах России присылает в хранилище каждый час CSV-файл
с операциями касс: продажи, отмены позиций кассиром и возвраты покупателей.
Описание данных — в docs/retail-data.md.

Запуск из корня репозитория в окружении spark-course:

    python generator/generate_retail.py                  # 3 дня, файлы до ~100 МБ
    python generator/generate_retail.py --scale 0.1      # то же, но в 10 раз меньше
    python generator/generate_retail.py --start 2026-09-04 --days 3 --seed 42

Результат:

    data/retail/dims/stores.csv, categories.csv, products.csv   справочники
    data/retail/sales/sales_2026-09-04T00.csv ...               файл на каждый час

Генерация детерминирована: одинаковые --start, --days, --scale и --seed дают
побайтно одинаковые файлы. Уже существующие файлы перезаписываются.
"""
import argparse
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

# Число строк в самый загруженный час при --scale 1. Строка CSV занимает
# около 87 байт, поэтому самый большой файл получается ~90 МБ.
PEAK_ROWS_PER_HOUR = 950_000

# ---------------------------------------------------------------------------
# Справочники: регионы, города, категории
# ---------------------------------------------------------------------------
# регион: (сдвиг от МСК в часах, ценовой коэффициент, доля наличных, [(город, население, тыс.)])
REGIONS = {
    "Москва":                   (0, 1.15, 0.08, [("Москва", 13000)]),
    "Московская область":       (0, 1.08, 0.12, [("Химки", 260), ("Подольск", 310), ("Балашиха", 520)]),
    "Санкт-Петербург":          (0, 1.10, 0.09, [("Санкт-Петербург", 5600)]),
    "Краснодарский край":       (0, 1.00, 0.20, [("Краснодар", 1100), ("Сочи", 450), ("Новороссийск", 270)]),
    "Ростовская область":       (0, 0.97, 0.22, [("Ростов-на-Дону", 1140), ("Таганрог", 245)]),
    "Республика Татарстан":     (0, 0.98, 0.16, [("Казань", 1320), ("Набережные Челны", 550)]),
    "Нижегородская область":    (0, 0.96, 0.18, [("Нижний Новгород", 1200), ("Дзержинск", 220)]),
    "Самарская область":        (1, 0.96, 0.18, [("Самара", 1150), ("Тольятти", 680)]),
    "Пермский край":            (2, 0.97, 0.19, [("Пермь", 1030), ("Березники", 135), ("Соликамск", 90)]),
    "Свердловская область":     (2, 0.99, 0.17, [("Екатеринбург", 1540), ("Нижний Тагил", 330)]),
    "Республика Башкортостан":  (2, 0.95, 0.21, [("Уфа", 1160), ("Стерлитамак", 280)]),
    "Новосибирская область":    (4, 1.00, 0.17, [("Новосибирск", 1630), ("Бердск", 105)]),
    "Красноярский край":        (4, 1.06, 0.19, [("Красноярск", 1200), ("Норильск", 180)]),
    "Приморский край":          (7, 1.18, 0.21, [("Владивосток", 600), ("Находка", 140)]),
    "Хабаровский край":         (7, 1.16, 0.20, [("Хабаровск", 610)]),
}

# формат магазина: (доля магазинов, чеков в час в пик, средних позиций в чеке, часы работы)
STORE_FORMATS = {
    "гипермаркет":  (0.15, 900, 9.0, (8, 23)),
    "супермаркет":  (0.45, 450, 5.5, (8, 23)),
    "у дома":       (0.40, 260, 3.2, (7, 23)),
}

# группа: [(категория, медианная цена в рублях, вес в продажах, [товары])]
CATEGORIES = {
    "Молочная продукция и яйца": [
        ("Молоко и сливки", 95, 9, ["Молоко 2,5%", "Молоко 3,2%", "Молоко топлёное", "Сливки 10%", "Сливки 20%"]),
        ("Кисломолочные продукты", 85, 7, ["Кефир 1%", "Кефир 3,2%", "Ряженка", "Йогурт питьевой", "Снежок", "Айран"]),
        ("Сыры", 320, 5, ["Сыр Российский", "Сыр Гауда", "Сыр Моцарелла", "Сыр Сулугуни", "Сыр плавленый"]),
        ("Творог и сметана", 120, 6, ["Творог 5%", "Творог 9%", "Сметана 15%", "Сметана 20%", "Творожная масса"]),
        ("Масло и маргарин", 190, 3, ["Масло сливочное 72,5%", "Масло сливочное 82,5%", "Спред", "Маргарин"]),
        ("Яйца", 125, 4, ["Яйцо куриное С1", "Яйцо куриное С0", "Яйцо перепелиное"]),
    ],
    "Мясо, птица, рыба": [
        ("Мясо", 520, 4, ["Свинина шейка", "Говядина лопатка", "Фарш домашний", "Свиные рёбра", "Баранина"]),
        ("Птица", 330, 5, ["Филе куриное", "Бедро куриное", "Голень куриная", "Филе индейки", "Цыплёнок"]),
        ("Колбасы и сосиски", 260, 6, ["Колбаса докторская", "Сервелат", "Сосиски молочные", "Сардельки", "Ветчина"]),
        ("Рыба и морепродукты", 450, 3, ["Сёмга", "Минтай", "Скумбрия", "Креветки", "Крабовые палочки"]),
    ],
    "Овощи и фрукты": [
        ("Овощи", 85, 9, ["Картофель", "Морковь", "Лук репчатый", "Огурцы", "Помидоры", "Капуста белокочанная"]),
        ("Фрукты", 140, 8, ["Бананы", "Яблоки", "Апельсины", "Мандарины", "Груши", "Виноград"]),
        ("Зелень и салаты", 70, 3, ["Укроп", "Петрушка", "Салат листовой", "Руккола", "Лук зелёный"]),
        ("Орехи и сухофрукты", 290, 2, ["Грецкий орех", "Миндаль", "Курага", "Изюм", "Финики"]),
    ],
    "Бакалея": [
        ("Крупы", 95, 4, ["Гречка", "Рис круглозёрный", "Рис басмати", "Овсяные хлопья", "Пшено", "Булгур"]),
        ("Макаронные изделия", 85, 4, ["Спагетти", "Рожки", "Перья", "Лапша яичная", "Вермишель"]),
        ("Консервы", 140, 3, ["Горошек зелёный", "Кукуруза", "Тушёнка", "Шпроты", "Фасоль"]),
        ("Соусы и специи", 110, 3, ["Кетчуп", "Майонез", "Горчица", "Соевый соус", "Перец чёрный"]),
        ("Чай и кофе", 350, 4, ["Чай чёрный", "Чай зелёный", "Кофе молотый", "Кофе в зёрнах", "Кофе растворимый"]),
        ("Сладости", 150, 7, ["Шоколад молочный", "Печенье", "Конфеты", "Вафли", "Зефир", "Мармелад"]),
        ("Снеки", 110, 5, ["Чипсы", "Сухарики", "Попкорн", "Семечки", "Крекеры"]),
    ],
    "Хлеб и выпечка": [
        ("Хлеб", 60, 9, ["Батон нарезной", "Хлеб Бородинский", "Хлеб пшеничный", "Багет", "Лаваш"]),
        ("Выпечка и торты", 210, 3, ["Круассан", "Пирожок с капустой", "Торт Наполеон", "Кекс", "Слойка"]),
    ],
    "Напитки": [
        ("Вода", 55, 6, ["Вода питьевая", "Вода минеральная", "Вода газированная"]),
        ("Соки и морсы", 130, 4, ["Сок яблочный", "Сок апельсиновый", "Нектар персиковый", "Морс клюквенный"]),
        ("Газированные напитки", 110, 4, ["Лимонад", "Кола", "Тоник", "Квас"]),
        ("Энергетики", 115, 2, ["Энергетик классический", "Энергетик без сахара"]),
    ],
    "Бытовая химия и гигиена": [
        ("Средства для стирки", 480, 2, ["Порошок стиральный", "Гель для стирки", "Кондиционер для белья", "Пятновыводитель"]),
        ("Средства для уборки", 210, 2, ["Средство для посуды", "Средство для стёкол", "Чистящий порошок", "Средство для пола"]),
        ("Личная гигиена", 250, 3, ["Шампунь", "Гель для душа", "Зубная паста", "Мыло", "Дезодорант"]),
        ("Бумажная продукция", 180, 3, ["Туалетная бумага", "Бумажные полотенца", "Салфетки", "Ватные диски"]),
    ],
    "Товары для дома": [
        ("Посуда", 450, 1, ["Тарелка", "Кружка", "Сковорода", "Кастрюля", "Контейнер"]),
        ("Товары для кухни", 190, 1, ["Фольга", "Пакеты для заморозки", "Губки", "Плёнка пищевая"]),
        ("Товары для животных", 260, 2, ["Корм для кошек", "Корм для собак", "Наполнитель", "Лакомство для собак"]),
        ("Электротовары", 390, 1, ["Батарейки", "Лампочка светодиодная", "Удлинитель", "Зарядное устройство"]),
    ],
}

BRANDS = [
    "Северная ферма", "Добрый край", "Уральские зори", "Вкусная полка", "Своё",
    "Лугово", "Семейный выбор", "Парма", "Кама", "Приволжье", "Солнечный день",
    "Экономыч", "Премиум Лайн", "Белый берег", "Отборное", "Дары Сибири",
    "Утро", "Мегаполис", "Родные просторы", "Золотая нива",
]
VARIANTS = ["", "", "", " мини", " большая упаковка", " эконом", " фермерский", " премиум"]

# Суточный профиль покупок по местному времени (доля от пикового часа)
HOURLY_PROFILE = np.array([
    0.02, 0.01, 0.01, 0.01, 0.01, 0.02, 0.05, 0.20,   # 00-07
    0.40, 0.50, 0.55, 0.60, 0.70, 0.72, 0.65, 0.62,   # 08-15
    0.70, 0.85, 1.00, 0.98, 0.85, 0.60, 0.35, 0.12,   # 16-23
])
WEEKDAY_FACTOR = [0.92, 0.90, 0.93, 0.97, 1.05, 1.20, 1.10]  # пн..вс

PAYMENT_METHODS = np.array(["CARD", "SBP", "CASH", "BONUS"])

# Доли «грязи» в источнике (см. docs/retail-data.md, «Известные дефекты»)
LATE_RECEIPT_SHARE = 0.02      # чек пришёл с опозданием на 1-3 часа (офлайн-касса)
CANCEL_SHARE = 0.015           # позиция отменена кассиром
RETURN_SHARE = 0.005           # позиция возвращена покупателем позже
DUPLICATE_SHARE = 0.01         # строки продублированы внутри файла
RESEND_SHARE = 0.005           # строки предыдущего часа прислали ещё раз
BROKEN_SHARE = 0.0002          # битые строки

SALES_COLUMNS = [
    "event_id", "event_ts", "receipt_id", "line_no", "store_id", "product_id",
    "category_id", "customer_id", "is_loyalty", "operation_type", "quantity",
    "price_regular_kop", "price_paid_kop", "payment_method", "ref_event_id",
]



# ---------------------------------------------------------------------------
# Справочники
# ---------------------------------------------------------------------------
def build_dims(rng: np.random.Generator, n_stores: int):
    # --- категории ---
    cat_rows, cat_id = [], 1
    for group, cats in CATEGORIES.items():
        for name, median_rub, weight, items in cats:
            cat_rows.append(dict(category_id=cat_id, category_name=name, group_name=group,
                                 _median_rub=median_rub, _weight=weight, _items=items))
            cat_id += 1
    categories = pd.DataFrame(cat_rows)

    # --- товары: для каждой категории базовые товары × бренды × варианты ---
    prod_rows, prod_id = [], 1
    for c in cat_rows:
        n = int(40 + 80 * c["_weight"] / 9)  # 40..120 SKU на категорию
        names = set()
        while len(names) < n:
            names.add(f'{rng.choice(c["_items"])} «{rng.choice(BRANDS)}»{rng.choice(VARIANTS)}')
        for name in sorted(names):
            price_rub = c["_median_rub"] * rng.lognormal(0, 0.35)
            price_kop = max(990, int(round(price_rub)) * 100 - 10)  # цены вида 89,90
            prod_rows.append(dict(product_id=prod_id, product_name=name,
                                  category_id=c["category_id"], base_price_kop=price_kop))
            prod_id += 1
    products = pd.DataFrame(prod_rows)

    # --- магазины: распределяем по городам пропорционально населению ---
    cities = [(region, city, pop) for region, (_, _, _, cs) in REGIONS.items() for city, pop in cs]
    pops = np.array([c[2] for c in cities], dtype=float) ** 0.6  # мегаполисы не забирают всё
    per_city = np.maximum(1, np.round(pops / pops.sum() * n_stores)).astype(int)
    formats = list(STORE_FORMATS)
    format_p = [STORE_FORMATS[f][0] for f in formats]
    store_rows, store_id = [], 1
    for (region, city, _), k in zip(cities, per_city):
        for i in range(k):
            fmt = rng.choice(formats, p=format_p)
            opened = date(2012, 1, 1) + timedelta(days=int(rng.integers(0, 365 * 13)))
            store_rows.append(dict(store_id=store_id, store_name=f"{city}, магазин №{i + 1}",
                                   city=city, region=region, format=fmt, open_date=opened))
            store_id += 1
    stores = pd.DataFrame(store_rows)
    return stores, categories, products


# ---------------------------------------------------------------------------
# Факт: часовые файлы
# ---------------------------------------------------------------------------
class SalesGenerator:
    def __init__(self, rng, stores, categories, products, start: datetime, hours: int, scale: float):
        self.rng = rng
        self.start = np.datetime64(start, "h")
        self.hours = hours
        self.next_event_id = 100_000_001
        self.next_receipt_id = 50_000_001
        self.pending = {}          # час доставки -> список DataFrame (опоздания, отмены, возвраты)
        self.prev_file = None      # предыдущий файл: из него «переотправляются» строки

        self.n_stores = len(stores)
        self.store_ids = stores["store_id"].to_numpy()
        region_info = stores["region"].map(REGIONS)
        self.store_offset = np.array([r[0] for r in region_info])
        self.store_price_k = np.array([r[1] for r in region_info]) * rng.uniform(0.98, 1.02, self.n_stores)
        self.store_cash = np.array([r[2] for r in region_info])
        fmt = stores["format"].map(STORE_FORMATS)
        self.store_rate = np.array([f[1] for f in fmt]) * rng.uniform(0.7, 1.3, self.n_stores)
        self.store_items = np.array([f[2] for f in fmt])
        self.store_open = np.array([f[3][0] for f in fmt])
        self.store_close = np.array([f[3][1] for f in fmt])
        # премиальность магазина: у участников программы и в богатых регионах чек больше
        self.store_basket_k = self.store_price_k ** 2

        self.product_ids = products["product_id"].to_numpy()
        self.product_cat = products["category_id"].to_numpy()
        self.product_price = products["base_price_kop"].to_numpy()
        # популярность: закон Ципфа внутри категории × вес категории
        cat_weight = categories.set_index("category_id")["_weight"]
        pop = np.empty(len(products))
        for cid, idx in products.groupby("category_id").indices.items():
            ranks = rng.permutation(len(idx)) + 1
            z = 1.0 / ranks ** 1.1
            pop[idx] = z / z.sum() * cat_weight[cid]
        self.product_p = pop / pop.sum()

        # пул покупателей
        self.n_customers = 400_000

        # Нормировка: самый загруженный час должен дать PEAK_ROWS_PER_HOUR * scale строк
        expected = np.array([self._expected_lines(self.start + np.timedelta64(h, "h"))
                             for h in range(hours)])
        self.rate_k = PEAK_ROWS_PER_HOUR * scale / expected.max() / 1.06  # запас на дубли и «хвосты»

    # ожидаемое число строк-продаж по каждому магазину в час h (МСК)
    def _store_intensity(self, hour_msk):
        local = hour_msk + self.store_offset.astype("timedelta64[h]")
        local_hour = (local.astype("datetime64[h]") - local.astype("datetime64[D]")).astype(int)
        weekday = (local.astype("datetime64[D]").astype(int) + 3) % 7  # 1970-01-01 — четверг
        is_open = (local_hour >= self.store_open) & (local_hour < self.store_close)
        prof = np.where(is_open, HOURLY_PROFILE[local_hour], 0.0)
        return self.store_rate * prof * np.take(WEEKDAY_FACTOR, weekday)

    def _expected_lines(self, hour_msk):
        return (self._store_intensity(hour_msk) * self.store_items).sum()

    def _ids(self, attr, n):
        start = getattr(self, attr)
        setattr(self, attr, start + n)
        return np.arange(start, start + n, dtype=np.int64)

    def _defer(self, df, deliver_hour):
        """Откладывает строки до файла за час deliver_hour."""
        for h, part in df.groupby(deliver_hour):
            self.pending.setdefault(np.datetime64(h, "h"), []).append(part)

    def generate_hour(self, hour_msk) -> pd.DataFrame:
        rng = self.rng
        # --- чеки ---
        lam = self._store_intensity(hour_msk) * self.rate_k
        n_rec = rng.poisson(lam)
        rec_store = np.repeat(np.arange(self.n_stores), n_rec)
        R = len(rec_store)
        rec_id = self._ids("next_receipt_id", R)
        rec_start = rng.random(R)  # момент начала чека внутри часа, доля от свободного времени
        loyalty = rng.random(R) < 0.55
        customer = np.where(loyalty | (rng.random(R) < 0.12),
                            rng.integers(1, self.n_customers + 1, R), 0)
        # оплата: наличные зависят от региона, баллами — только участники программы
        cash_p = self.store_cash[rec_store]
        u = rng.random(R)
        pay = np.where(u < cash_p, 2, np.where(u < cash_p + 0.22, 1, 0))
        pay = np.where(loyalty & (rng.random(R) < 0.06), 3, pay)
        # размер корзины: формат магазина, регион, участие в программе
        mean_items = self.store_items[rec_store] * self.store_basket_k[rec_store] * np.where(loyalty, 1.25, 0.85)
        n_items = 1 + rng.poisson(np.maximum(mean_items - 1, 0.1))
        # чек начинается так, чтобы последняя позиция (~2,5 с на позицию) попала в тот же час
        free_ms = 3_600_000 - (n_items + 1) * 3500
        rec_ts = hour_msk.astype("datetime64[ms]") + (rec_start * free_ms).astype("timedelta64[ms]")

        # --- позиции чека ---
        L = int(n_items.sum())
        r = np.repeat(np.arange(R), n_items)
        line_no = np.arange(L) - np.repeat(np.cumsum(n_items) - n_items, n_items) + 1
        prod = rng.choice(len(self.product_ids), size=L, p=self.product_p)
        qty = np.where(rng.random(L) < 0.82, 1, 1 + rng.geometric(0.55, L))
        store = rec_store[r]
        unit_price = np.round(self.product_price[prod] * self.store_price_k[store] / 10) * 10
        regular = (unit_price * qty).astype(np.int64)
        # скидки: промо на ~12% товаров (меняется по дням) + скидка участника программы
        day_seed = int(hour_msk.astype("datetime64[D]").astype(int))
        promo = (self.product_ids[prod] * 2654435761 + day_seed * 97) % 100 < 12
        disc = np.where(promo, 0.10 + ((self.product_ids[prod] + day_seed) % 6) * 0.05, 0.0)
        disc = disc + np.where(loyalty[r] & (rng.random(L) < 0.3), 0.05, 0.0)
        paid = np.round(regular * (1 - disc)).astype(np.int64)

        sales = pd.DataFrame({
            "event_id": self._ids("next_event_id", L),
            # позиции пробиваются одна за другой, примерно раз в 2,5 секунды
            "event_ts": rec_ts[r] + (line_no * 2500 + rng.integers(0, 1000, L)).astype("timedelta64[ms]"),
            "receipt_id": rec_id[r],
            "line_no": line_no,
            "store_id": self.store_ids[store],
            "product_id": self.product_ids[prod],
            "category_id": self.product_cat[prod],
            "customer_id": customer[r],
            "is_loyalty": loyalty[r],
            "operation_type": "SALE",
            "quantity": qty,
            "price_regular_kop": regular,
            "price_paid_kop": paid,
            "payment_method": PAYMENT_METHODS[pay[r]],
            "ref_event_id": 0,
        })
        # чек с офлайн-кассы доставляется целиком на 1-3 часа позже
        late_rec = rng.random(R) < LATE_RECEIPT_SHARE
        lag = np.where(late_rec, rng.integers(1, 4, R), 0)[r].astype("timedelta64[h]")

        # --- отмены кассиром: та же позиция через 20 с - 5 мин, тот же чек ---
        c = rng.random(L) < CANCEL_SHARE
        cancels = sales[c].copy()
        cancels["ref_event_id"] = cancels["event_id"]
        cancels["event_id"] = self._ids("next_event_id", len(cancels))
        cancels["event_ts"] += rng.integers(20_000, 300_000, len(cancels)).astype("timedelta64[ms]")
        cancels["operation_type"] = "CANCEL"
        self._defer(cancels, cancels["event_ts"].to_numpy().astype("datetime64[h]") + lag[c])

        # --- возвраты покупателем: через 2 часа - 5 дней, новым чеком ---
        ret = (rng.random(L) < RETURN_SHARE) & ~c
        returns = sales[ret].copy()
        returns["ref_event_id"] = returns["event_id"]
        returns["event_id"] = self._ids("next_event_id", len(returns))
        returns["receipt_id"] = self._ids("next_receipt_id", len(returns))
        returns["line_no"] = 1
        delay_ms = (2 * 3600 + rng.exponential(30 * 3600, len(returns)).clip(0, 5 * 86400)) * 1000
        returns["event_ts"] += delay_ms.astype(np.int64).astype("timedelta64[ms]")
        returns["operation_type"] = "RETURN"
        returns["payment_method"] = np.where(returns["payment_method"] == "BONUS", "CARD",
                                             returns["payment_method"])
        self._defer(returns, returns["event_ts"].to_numpy().astype("datetime64[h]"))

        # --- продажи этого часа: вовремя или с опозданием ---
        late = lag > np.timedelta64(0, "h")
        self._defer(sales[late], hour_msk + lag[late])
        return sales[~late]

    def build_file(self, hour_msk) -> pd.DataFrame:
        rng = self.rng
        on_time = self.generate_hour(hour_msk)
        df = pd.concat([on_time, *self.pending.pop(hour_msk, [])], ignore_index=True)
        df = df.sort_values("event_ts", kind="stable", ignore_index=True)

        # битые строки: отрицательная цена, неизвестный магазин, пустой товар
        broken = np.flatnonzero(rng.random(len(df)) < BROKEN_SHARE)
        kind = rng.integers(0, 3, len(broken))
        df.loc[broken[kind == 0], "price_paid_kop"] *= -1
        df.loc[broken[kind == 1], "store_id"] = 9999
        df.loc[broken[kind == 2], "product_id"] = -1  # записывается пустым полем

        # дубли внутри файла и повторная отправка части предыдущего файла
        extra = [df.sample(frac=DUPLICATE_SHARE, random_state=rng)]
        if self.prev_file is not None:
            extra.append(self.prev_file.sample(frac=RESEND_SHARE, random_state=rng))
        self.prev_file = df
        out = pd.concat([df, *extra], ignore_index=True)
        # дубли вставляются в случайные места файла, а не в конец
        order = np.argsort(np.concatenate([np.arange(len(df)), rng.integers(0, len(df), len(out) - len(df))]),
                           kind="stable")
        return out.iloc[order].reset_index(drop=True)


def write_sales_csv(df: pd.DataFrame, path: Path):
    ts = np.datetime_as_string(df["event_ts"].to_numpy().astype("datetime64[ms]"), unit="ms")
    table = pa.table({
        "event_id": df["event_id"].to_numpy(),
        "event_ts": pa.array(np.char.replace(ts, "T", " ")),
        "receipt_id": df["receipt_id"].to_numpy(),
        "line_no": df["line_no"].to_numpy(),
        "store_id": df["store_id"].to_numpy(),
        "product_id": pa.array(df["product_id"].to_numpy(), mask=df["product_id"].to_numpy() < 0),
        "category_id": df["category_id"].to_numpy(),
        "customer_id": pa.array(df["customer_id"].to_numpy(), mask=df["customer_id"].to_numpy() == 0),
        "is_loyalty": df["is_loyalty"].to_numpy(),
        "operation_type": pa.array(df["operation_type"].to_numpy(dtype=str)),
        "quantity": df["quantity"].to_numpy(),
        "price_regular_kop": df["price_regular_kop"].to_numpy(),
        "price_paid_kop": df["price_paid_kop"].to_numpy(),
        "payment_method": pa.array(df["payment_method"].to_numpy(dtype=str)),
        "ref_event_id": pa.array(df["ref_event_id"].to_numpy(), mask=df["ref_event_id"].to_numpy() == 0),
    })
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "wb") as f:
        f.write((",".join(SALES_COLUMNS) + "\n").encode())  # pyarrow берёт заголовок в кавычки
        pacsv.write_csv(table, f, pacsv.WriteOptions(include_header=False, quoting_style="none"))
    tmp.replace(path)  # файл появляется целиком: загрузчик не увидит половину


def main():
    p = argparse.ArgumentParser(description="Генератор данных розничной сети (см. docs/retail-data.md)")
    p.add_argument("--start", default="2026-09-04", help="первый день, YYYY-MM-DD (по умолчанию 2026-09-04, пятница)")
    p.add_argument("--days", type=int, default=3, help="сколько дней генерировать (по умолчанию 3)")
    p.add_argument("--scale", type=float, default=1.0, help="масштаб объёма: 1.0 — пиковый файл ~90 МБ")
    p.add_argument("--stores", type=int, default=150, help="примерное число магазинов")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="data/retail", help="каталог результата")
    args = p.parse_args()

    out = Path(args.out)
    (out / "dims").mkdir(parents=True, exist_ok=True)
    (out / "sales").mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed)

    stores, categories, products = build_dims(rng, args.stores)
    stores.to_csv(out / "dims" / "stores.csv", index=False)
    categories[["category_id", "category_name", "group_name"]].to_csv(out / "dims" / "categories.csv", index=False)
    products.to_csv(out / "dims" / "products.csv", index=False)
    print(f"справочники: {len(stores)} магазинов, {len(categories)} категорий, "
          f"{len(products)} товаров -> {out / 'dims'}")

    start = datetime.strptime(args.start, "%Y-%m-%d")
    hours = args.days * 24
    gen = SalesGenerator(rng, stores, categories, products, start, hours, args.scale)
    total_rows, total_bytes, t0 = 0, 0, time.time()
    for h in range(hours):
        hour = gen.start + np.timedelta64(h, "h")
        df = gen.build_file(hour)
        name = f"sales_{pd.Timestamp(hour):%Y-%m-%dT%H}.csv"
        path = out / "sales" / name
        write_sales_csv(df, path)
        size = path.stat().st_size
        total_rows += len(df)
        total_bytes += size
        print(f"{name}: {len(df):>9,} строк, {size / 2**20:6.1f} МБ".replace(",", " "), flush=True)
    print(f"итого: {hours} файлов, {total_rows:,} строк, {total_bytes / 2**30:.2f} ГБ "
          f"за {time.time() - t0:.0f} с -> {out / 'sales'}".replace(",", " "))


if __name__ == "__main__":
    main()

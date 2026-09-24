"""
Небольшая выборка данных розничной сети для занятия 1 (notebooks/lab-01-spark.ipynb).

Один день операций в одном CSV-файле (~38 тыс. строк, ~3 МБ) и справочники —
те же, что у полного набора (generator/generate_retail.py с тем же --seed).
Результат хранится в git, в data/sample/, поэтому запускать скрипт студентам не нужно.

    python generator/make_sample.py
"""
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "data" / "sample"

with tempfile.TemporaryDirectory() as tmp:
    subprocess.run([sys.executable, str(REPO / "generator" / "generate_retail.py"),
                    "--start", "2026-09-05", "--days", "1", "--scale", "0.003", "--out", tmp],
                   check=True, stdout=subprocess.DEVNULL)
    OUT.mkdir(parents=True, exist_ok=True)
    for name in ["stores.csv", "categories.csv", "products.csv"]:
        shutil.copy(Path(tmp) / "dims" / name, OUT / name)

    # 24 часовых файла -> один файл с одним заголовком
    rows = 0
    with open(OUT / "sales.csv", "w", encoding="utf-8", newline="") as out:
        for i, part in enumerate(sorted((Path(tmp) / "sales").glob("sales_*.csv"))):
            lines = part.read_text(encoding="utf-8").splitlines(keepends=True)
            out.writelines(lines if i == 0 else lines[1:])
            rows += len(lines) - 1

size = (OUT / "sales.csv").stat().st_size
print(f"{OUT / 'sales.csv'}: {rows} строк, {size / 2**20:.1f} МБ; справочники -> {OUT}")

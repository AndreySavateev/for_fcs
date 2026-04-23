import time
import warnings
from datetime import date, datetime
from pathlib import Path

import pandas as pd

import db
import processing as proc
from config import settings

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


def get_snapshot_folders_from_2026(archive_root: Path) -> list[tuple[date, Path]]:
    """
    Ищет папки дат внутри месячных папок архива.
    Пример структуры:
        Архив/
            Январь/
                09.01.2026/
                10.01.2026/
            Февраль/
                03.02.2026/
            Март/
                04.03.2026/
    """
    result = []

    if not archive_root.exists():
        raise FileNotFoundError(f"Архивная папка не найдена: {archive_root}")

    month_folders = sorted(
        [folder for folder in archive_root.iterdir() if folder.is_dir()]
    )

    for month_folder in month_folders:
        snapshot_folders = sorted(
            [folder for folder in month_folder.iterdir() if folder.is_dir()]
        )

        for snapshot_folder in snapshot_folders:
            try:
                load_date = datetime.strptime(snapshot_folder.name, "%d.%m.%Y").date()
            except ValueError:
                continue

            if load_date >= date(2026, 1, 1):
                result.append((load_date, snapshot_folder))

    result.sort(key=lambda x: x[0])
    return result


def run_raw_mo_backfill() -> None:
    start_time = time.time()

    print("=" * 100)
    print("СТАРТ BACKFILL raw_mo_daily")
    print("=" * 100)
    print(f"Архивная папка: {settings.MO_ARCHIVE_ROOT}")
    print(f"Папка существует: {settings.MO_ARCHIVE_ROOT.exists()}")

    dim_rc = pd.read_parquet(settings.DIM_RC_PATH)
    client = db.get_clickhouse_client()

    db.create_raw_table_mo(client)

    snapshot_folders = get_snapshot_folders_from_2026(settings.MO_ARCHIVE_ROOT)

    total_days = len(snapshot_folders)
    loaded_days = 0
    skipped_days = 0
    total_rows = 0

    print(f"Найдено дат для загрузки: {total_days}")

    for idx, (load_date, snapshot_folder) in enumerate(snapshot_folders, start=1):
        print("-" * 100)
        print(
            f"[{idx}/{total_days}] Дата отчета: {load_date} | Папка: {snapshot_folder}"
        )

        try:
            raw_mo_data = proc.process_raw_mo_backfill(
                dim_rc=dim_rc,
                snapshot_folder=snapshot_folder,
                load_date=load_date,
            )

            row_count = len(raw_mo_data)
            print(f"Подготовлено строк: {row_count}")

            print(f"Удаляю старые данные за {load_date} ...")
            db.delete_raw_table_mo(client, load_date.isoformat())

            print(f"Вставляю новые данные за {load_date} ...")
            db.insert_raw_mo(client, raw_mo_data)

            loaded_days += 1
            total_rows += row_count

            print(f"OK | {load_date} | вставлено строк: {row_count}")

        except FileNotFoundError as e:
            skipped_days += 1
            print(f"SKIP | {load_date} | {e}")
            continue

        except Exception as e:
            print(f"ERROR | {load_date} | {e}")
            raise

    elapsed_minutes = (time.time() - start_time) / 60

    print("=" * 100)
    print("ФИНИШ BACKFILL raw_mo_daily")
    print("=" * 100)
    print(f"Успешно загружено дней: {loaded_days}")
    print(f"Пропущено дней: {skipped_days}")
    print(f"Всего вставлено строк: {total_rows}")
    print(f"Время выполнения: {elapsed_minutes:.2f} мин")


if __name__ == "__main__":
    run_raw_mo_backfill()

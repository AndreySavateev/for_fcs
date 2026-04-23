import warnings

import pandas as pd

import db
import excel_export as exe
import processing as proc
from config import settings

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


def load_weekly_increment(client, click_data: pd.DataFrame) -> None:
    if click_data.empty:
        print("Датафрейм пустой. Загружать нечего.")
        return

    report_week = proc.extract_single_report_week(click_data)

    print(f"Загружаем неделю: {report_week}")

    db.delete_report_week(client, report_week)
    db.insert_report_week(client, click_data)

    print(f"Успешно загружено строк: {len(click_data)} за неделю {report_week}")


def run_pipeline() -> None:
    # Загружаем справочники и подключаемся к БД
    dim_rc = pd.read_parquet(settings.DIM_RC_PATH)
    client = db.get_clickhouse_client()
    dim_goods = db.get_excluded_goods(client)
    dim_district = db.get_dim_district(client)

    # Обрабатываем данные МО и МХ
    data_mo = proc.process_mo_data(dim_rc, dim_goods)
    data_mx = proc.process_mx_data(dim_rc)
    data = proc.build_final_dataset(data_mo, data_mx)

    # Сохранение в Excel
    exe.save_to_excel(data, dim_district)

    # Работа с clikhouse
    click_data = proc.prepare_df_for_clickhouse(data)
    db.create_target_table(client)
    load_weekly_increment(client, click_data)

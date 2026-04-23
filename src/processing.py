import datetime as dt

import pandas as pd

import constants as const
from config import settings

# Удалить read_excel_snapshot_by_folder после загрузки
from io_utils import read_excel_snapshot, read_excel_snapshot_by_folder


def attach_dim_rc(data: pd.DataFrame, dim_rc: pd.DataFrame) -> pd.DataFrame:
    return data.merge(
        dim_rc,
        how="left",
        left_on="source_file",
        right_on="Названия строк",
    )


def process_mo_data(dim_rc, dim_goods):
    data_sg_mo = read_excel_snapshot(settings.MO_ROOT, const.MO_REQUIRED_COLUMNS)
    data_sg_mo = attach_dim_rc(data_sg_mo, dim_rc)

    data_sg_mo = data_sg_mo.drop(columns=const.RC_MERGE_DROP_COLUMNS)

    data_sg_mo["Годен до"] = pd.to_datetime(
        data_sg_mo["Годен до"], format=const.MO_EXPIRY_DATE_FORMAT
    ).dt.normalize()

    # Добавмл как тест
    now = pd.Timestamp.now().normalize()
    data_sg_mo = data_sg_mo[
        (data_sg_mo["Годен до"] < (now + dt.timedelta(days=5000)))
        & (data_sg_mo["Годен до"] > (now - dt.timedelta(days=1)))
    ]

    data_sg_mo = data_sg_mo.groupby(const.MO_GROUPBY_COLUMNS, as_index=False).agg(
        const.MO_AGG_MAP
    )

    data_sg_mo["Код ТП"] = data_sg_mo["Код ТП"].astype(str)
    dim_goods["Код"] = dim_goods["CODE"].astype(str)

    # Фильтрация
    data_sg_mo = data_sg_mo[~data_sg_mo["Код ТП"].isin(dim_goods["Код"])]
    data_sg_mo = data_sg_mo[
        ~data_sg_mo["Наименование ТП"].str.contains("МРЦ", na=False)
    ]

    data_sg_mo.columns = const.MO_FINAL_COLUMNS

    return data_sg_mo


def process_mx_data(dim_rc):
    data_mx = read_excel_snapshot(settings.MX_ROOT, const.MX_REQUIRED_COLUMNS)
    data_mx = attach_dim_rc(data_mx, dim_rc)

    data_mx["Годен до"] = pd.to_datetime(
        data_mx["Годен до"], format=const.MX_EXPIRY_DATE_FORMAT
    ).dt.normalize()
    data_mx["код"] = data_mx["код"].astype(str)

    # Отсавляем только со сроком годности
    # Оставляем только те у которых статус один или 0 при этом "Примечание" = "Смена фасовки"
    data_mx = data_mx[
        (data_mx["Годен до"].notna())
        & (
            (data_mx["актив"] == 1)
            | (
                (data_mx["актив"] == 0)
                & (data_mx["Примечание"].str.contains("Смена фасовки", na=False))
            )
        )
    ]
    # Убираем те товары которые начинаются с (РСБ, СВХ ОПТ)
    data_mx = data_mx[
        ~data_mx["Примечание"].str.startswith(("РСБ", "СВХ", "ОПТ"), na=False)
    ]
    # Убираем все ячейки которые срок годности = 5000
    data_mx = data_mx[
        (data_mx["Срок хранения в КТ"] < 5000) | (data_mx["Срок хранения в КТ"].isna())
    ]
    # Убираем все ячейки которые начинаются на "Корзина"
    data_mx = data_mx[~data_mx["МП"].str.startswith("Корзина", na=False)]
    # Убираем все где Годен до больше чем 5000 (так как это не возможно) и там где уже просрочка

    now = dt.datetime.now()
    data_mx = data_mx[
        (data_mx["Годен до"] < (now + dt.timedelta(days=5000)))
        & (data_mx["Годен до"] > (now - dt.timedelta(days=1)))
    ]
    print(data_mx.shape)

    data_mx = data_mx.drop(columns=const.MX_DROP_COLUMNS_AFTER_FILTER)
    data_mx = data_mx.groupby(const.MX_GROUPBY_COLUMNS, as_index=False).agg(
        const.MX_AGG_MAP
    )

    # Работаем тут
    # Получаю минимальную дату для МХ.
    # Для того что свисти к минимому ошибку при поиске ошибок попнения
    # Получаестя что мы берем минимально низкую дату харенеия для того что бы отсечь все спорные даты на МО

    data_mx = data_mx.loc[
        data_mx.groupby(["Cтандартные наименования РЦ", "код"])["Годен до"].idxmin()
    ]

    data_mx.columns = const.MX_FINAL_COLUMNS

    return data_mx


def build_final_dataset(data_mo, data_mx):

    data = data_mo.merge(
        data_mx, how="inner", on=["Cтандартные наименования РЦ", "Код ТП"]
    )
    data = data[data["Годен до на МХ"] < data["Годен до на МО"]]  # Основной фильтр
    data["Дата отчета"] = dt.datetime.now().date()

    data["Дата отчета"] = pd.to_datetime(
        data["Дата отчета"], dayfirst=True, errors="coerce"
    )

    iso = data["Дата отчета"].dt.isocalendar()

    data["Год-Неделя"] = (
        iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
    )

    data = data.rename(columns={"Cтандартные наименования РЦ": "Наименование РЦ"})

    data = data[const.FINAL_DATA_COLUMNS]

    return data


def prepare_df_for_clickhouse(data: pd.DataFrame) -> pd.DataFrame:

    click_df = data.copy()

    rename_map = const.CLICKHOUSE_RENAME_MAP

    click_df = click_df.rename(columns=rename_map)

    click_df["report_date"] = pd.to_datetime(click_df["report_date"]).dt.date
    click_df["expiry_mo"] = pd.to_datetime(click_df["expiry_mo"]).dt.date
    click_df["expiry_mx"] = pd.to_datetime(click_df["expiry_mx"]).dt.date

    click_df["report_week"] = click_df["report_week"].astype(str).str.strip()
    click_df["product_code"] = click_df["product_code"].astype(str).str.strip()
    click_df["product_name"] = click_df["product_name"].astype(str).str.strip()
    click_df["rc_name"] = click_df["rc_name"].astype(str).str.strip()

    click_df["qty_mo"] = (
        pd.to_numeric(click_df["qty_mo"], errors="coerce").fillna(0).astype(float)
    )
    click_df["qty_mx"] = (
        pd.to_numeric(click_df["qty_mx"], errors="coerce").fillna(0).astype(float)
    )

    return click_df


def extract_single_report_week(click_df: pd.DataFrame) -> str:
    if click_df.empty:
        raise ValueError("Датафрейм пустой.")

    weeks = click_df["report_week"].dropna().unique().tolist()

    if len(weeks) != 1:
        raise ValueError(f"Ожидалась одна неделя в загрузке, а получено: {weeks}")

    return weeks[0]


def process_raw_mo(dim_rc):
    raw_mo_data = read_excel_snapshot(settings.MO_ROOT)
    raw_mo_data = attach_dim_rc(raw_mo_data, dim_rc)
    raw_mo_data = raw_mo_data.drop(columns=const.RC_MERGE_DROP_COLUMNS)
    raw_mo_data = raw_mo_data.rename(columns=const.CLICKHOUSE_RENAME_RAW_MO)

    raw_mo_data["load_date"] = dt.date.today()

    raw_mo_data["rc_name"] = (
        raw_mo_data["rc_name"].astype("string").str.strip().replace({pd.NA: None})
    )
    raw_mo_data["log_region"] = (
        raw_mo_data["log_region"].astype("string").str.strip().replace({pd.NA: None})
    )
    raw_mo_data["product_code"] = (
        raw_mo_data["product_code"].astype("string").str.strip().replace({pd.NA: None})
    )
    raw_mo_data["product_name"] = (
        raw_mo_data["product_name"].astype("string").str.strip().replace({pd.NA: None})
    )
    raw_mo_data["mo_location"] = (
        raw_mo_data["mo_location"].astype("string").str.strip().replace({pd.NA: None})
    )
    raw_mo_data["pe_code"] = (
        raw_mo_data["pe_code"].astype("string").str.strip().replace({pd.NA: None})
    )

    raw_mo_data["expiry_mo"] = pd.to_datetime(
        raw_mo_data["expiry_mo"], format="%d.%m.%Y %H:%M:%S", errors="coerce"
    ).dt.date

    # Добавил как тест
    now = dt.date.today()
    raw_mo_data = raw_mo_data[
        (raw_mo_data["expiry_mo"] < (now + dt.timedelta(days=5000)))
        & (raw_mo_data["expiry_mo"] > (now - dt.timedelta(days=1)))
    ]

    raw_mo_data["last_slot_replenish_dttm"] = pd.to_datetime(
        raw_mo_data["last_slot_replenish_dttm"],
        format=const.MO_EXPIRY_DATE_FORMAT,
        errors="coerce",
    )

    raw_mo_data["last_slot_zero_dttm"] = pd.to_datetime(
        raw_mo_data["last_slot_zero_dttm"], format="%d.%m.%Y %H:%M:%S", errors="coerce"
    )

    raw_mo_data["qty_sg"] = pd.to_numeric(raw_mo_data["qty_sg"], errors="coerce")
    raw_mo_data["total_qty"] = pd.to_numeric(raw_mo_data["total_qty"], errors="coerce")

    raw_mo_data = raw_mo_data[const.FINAL_DATA_COLUMNS_RAW_MO]

    return raw_mo_data


def process_raw_mx():
    raw_mx_data = read_excel_snapshot(settings.MX_ROOT)

    raw_mx_data.columns = (
        raw_mx_data.columns.astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    raw_mx_data["load_date"] = dt.date.today()
    raw_mx_data = raw_mx_data.rename(columns=const.CLICKHOUSE_RENAME_RAW_MX)

    missing_cols = [
        col for col in const.FINAL_DATA_COLUMNS_RAW_MX if col not in raw_mx_data.columns
    ]
    if missing_cols:
        raise KeyError(f"Не найдены колонки после rename: {missing_cols}")

    for col in const.MX_STRING_COLUMNS:
        raw_mx_data[col] = raw_mx_data[col].astype("string").str.strip()
        raw_mx_data[col] = raw_mx_data[col].replace({pd.NA: None})

    raw_mx_data["is_active"] = pd.to_numeric(raw_mx_data["is_active"], errors="coerce")

    raw_mx_data["report_dttm_bd_rc"] = pd.to_datetime(
        raw_mx_data["report_dttm_bd_rc"],
        format="%d.%m.%Y %H:%M:%S",
        errors="coerce",
        dayfirst=True,
    )

    today = dt.date.today()
    min_allowed_date = today - dt.timedelta(days=5010)
    max_allowed_date = today + dt.timedelta(days=5010)

    expiry_ts = pd.to_datetime(
        raw_mx_data["expiry_date"],
        errors="coerce",
        dayfirst=True,
    )
    expiry_ts = expiry_ts.where(
        (expiry_ts >= pd.Timestamp(min_allowed_date))
        & (expiry_ts <= pd.Timestamp(max_allowed_date))
    )
    raw_mx_data["expiry_date"] = expiry_ts.dt.date

    production_ts = pd.to_datetime(
        raw_mx_data["production_date"],
        errors="coerce",
        dayfirst=True,
    )
    production_ts = production_ts.where(
        (production_ts >= pd.Timestamp(min_allowed_date))
        & (production_ts <= pd.Timestamp(max_allowed_date))
    )
    raw_mx_data["production_date"] = production_ts.dt.date

    for col in const.MX_DATETIME_COLUMNS:
        raw_mx_data[col] = pd.to_datetime(
            raw_mx_data[col],
            errors="coerce",
            dayfirst=True,
        )

    for col in const.MX_NUMERIC_COLUMNS:
        raw_mx_data[col] = pd.to_numeric(raw_mx_data[col], errors="coerce")

    raw_mx_data = raw_mx_data[const.FINAL_DATA_COLUMNS_RAW_MX]

    return raw_mx_data


# Пути для загрузки пропущеных дней/данных из архива
def process_raw_mo_backfill(
    dim_rc: pd.DataFrame, snapshot_folder, load_date
) -> pd.DataFrame:
    raw_mo_data = read_excel_snapshot_by_folder(snapshot_folder)
    raw_mo_data = attach_dim_rc(raw_mo_data, dim_rc)
    raw_mo_data = raw_mo_data.drop(columns=const.RC_MERGE_DROP_COLUMNS)
    raw_mo_data = raw_mo_data.rename(columns=const.CLICKHOUSE_RENAME_RAW_MO)

    raw_mo_data["load_date"] = load_date

    string_columns = [
        "rc_name",
        "log_region",
        "product_code",
        "product_name",
        "mo_location",
        "pe_code",
    ]

    for col in string_columns:
        raw_mo_data[col] = (
            raw_mo_data[col].astype("string").str.strip().replace({pd.NA: None})
        )

    raw_mo_data["expiry_mo"] = pd.to_datetime(
        raw_mo_data["expiry_mo"],
        format=const.MO_EXPIRY_DATE_FORMAT,
        errors="coerce",
    ).dt.date

    raw_mo_data["last_slot_replenish_dttm"] = pd.to_datetime(
        raw_mo_data["last_slot_replenish_dttm"],
        format="%d.%m.%Y %H:%M:%S",
        errors="coerce",
    )

    raw_mo_data["last_slot_zero_dttm"] = pd.to_datetime(
        raw_mo_data["last_slot_zero_dttm"],
        format="%d.%m.%Y %H:%M:%S",
        errors="coerce",
    )

    raw_mo_data["qty_sg"] = pd.to_numeric(raw_mo_data["qty_sg"], errors="coerce")
    raw_mo_data["total_qty"] = pd.to_numeric(raw_mo_data["total_qty"], errors="coerce")

    raw_mo_data = raw_mo_data[const.FINAL_DATA_COLUMNS_RAW_MO]

    return raw_mo_data


def process_raw_mx_backfill(snapshot_folder, load_date) -> pd.DataFrame:
    raw_mx_data = read_excel_snapshot_by_folder(snapshot_folder)

    raw_mx_data.columns = (
        raw_mx_data.columns.astype(str)
        .str.replace("\xa0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    raw_mx_data["load_date"] = load_date
    raw_mx_data = raw_mx_data.rename(columns=const.CLICKHOUSE_RENAME_RAW_MX)

    missing_cols = [
        col for col in const.FINAL_DATA_COLUMNS_RAW_MX if col not in raw_mx_data.columns
    ]
    if missing_cols:
        raise KeyError(f"Не найдены колонки после rename: {missing_cols}")

    for col in const.MX_STRING_COLUMNS:
        raw_mx_data[col] = raw_mx_data[col].astype("string").str.strip()
        raw_mx_data[col] = raw_mx_data[col].replace({pd.NA: None})

    raw_mx_data["is_active"] = pd.to_numeric(raw_mx_data["is_active"], errors="coerce")

    raw_mx_data["report_dttm_bd_rc"] = pd.to_datetime(
        raw_mx_data["report_dttm_bd_rc"],
        format="%d.%m.%Y %H:%M:%S",
        errors="coerce",
        dayfirst=True,
    )

    load_date_ts = pd.Timestamp(load_date)
    min_allowed_date = load_date_ts - pd.Timedelta(days=5010)
    max_allowed_date = load_date_ts + pd.Timedelta(days=5010)

    expiry_ts = pd.to_datetime(
        raw_mx_data["expiry_date"],
        errors="coerce",
        dayfirst=True,
    )
    expiry_ts = expiry_ts.where(
        (expiry_ts >= min_allowed_date) & (expiry_ts <= max_allowed_date)
    )
    raw_mx_data["expiry_date"] = expiry_ts.dt.date

    production_ts = pd.to_datetime(
        raw_mx_data["production_date"],
        errors="coerce",
        dayfirst=True,
    )
    production_ts = production_ts.where(
        (production_ts >= min_allowed_date) & (production_ts <= max_allowed_date)
    )
    raw_mx_data["production_date"] = production_ts.dt.date

    for col in const.MX_DATETIME_COLUMNS:
        raw_mx_data[col] = pd.to_datetime(
            raw_mx_data[col],
            errors="coerce",
            dayfirst=True,
        )

    for col in const.MX_NUMERIC_COLUMNS:
        raw_mx_data[col] = pd.to_numeric(raw_mx_data[col], errors="coerce")

    raw_mx_data = raw_mx_data[const.FINAL_DATA_COLUMNS_RAW_MX]

    return raw_mx_data

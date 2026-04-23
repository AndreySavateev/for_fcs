# src/constants.py

# =========================
# Обязательные колонки во входных файлах
# =========================

MO_REQUIRED_COLUMNS = ["Код ТП", "Наименование ТП", "Годен до", "Количество по СГ"]

MX_REQUIRED_COLUMNS = [
    "актив",
    "код",
    "Годен до",
    "колич",
    "Примечание",
    "МП",
    "Срок хранения в КТ",
]

# =========================
# Колонки для удаления
# =========================

RC_MERGE_DROP_COLUMNS = [
    "Названия строк",
    "source_file",
    "Код РЦ",
]


MX_DROP_COLUMNS_AFTER_FILTER = [
    "Названия строк",
    "source_file",
    "Код РЦ",
    "актив",
    "МП",
    "Срок хранения в КТ",
]

# =========================
# Форматы дат во входных данных
# =========================

MO_EXPIRY_DATE_FORMAT = "%d.%m.%Y %H:%M:%S"
MX_EXPIRY_DATE_FORMAT = "%d.%m.%Y"

# =========================
# Колонки для группировок
# =========================

MO_GROUPBY_COLUMNS = [
    "Cтандартные наименования РЦ",
    "Код ТП",
    "Наименование ТП",
    "Годен до",
]

MX_GROUPBY_COLUMNS = [
    "Cтандартные наименования РЦ",
    "код",
    "Годен до",
]

# =========================
# Агрегации
# =========================

MO_AGG_MAP = {
    "Количество по СГ": "sum",
}

MX_AGG_MAP = {
    "колич": "sum",
}


# =========================
# Финальные названия колонок после обработки МО и МХ
# =========================

MO_FINAL_COLUMNS = [
    "Cтандартные наименования РЦ",
    "Код ТП",
    "Наименование ТП",
    "Годен до на МО",
    "Количество на МО",
]

MX_FINAL_COLUMNS = [
    "Cтандартные наименования РЦ",
    "Код ТП",
    "Годен до на МХ",
    "Количество на МХ",
]


# =========================
# Финальный порядок колонок итогового датасета
# =========================

FINAL_DATA_COLUMNS = [
    "Дата отчета",
    "Год-Неделя",
    "Код ТП",
    "Наименование ТП",
    "Наименование РЦ",
    "Годен до на МО",
    "Годен до на МХ",
    "Количество на МО",
    "Количество на МХ",
]

# =========================
# Колонки с датами для Excel-экспорта
# =========================

EXCEL_DATE_COLUMNS = [
    "Годен до на МО",
    "Годен до на МХ",
]

# =========================
# Названия листов Excel
# =========================

SUMMARY_SHEET_NAME = "Сводник"  # Где это ??
FULL_DATASET_SHEET_NAME = "Общий датасет"
UNMAPPED_DISTRICT_NAME = "Не распределено"

# =========================
# Порядок округов для Excel
# =========================

DISTRICT_ORDER = [
    "Волжский",
    "Московский",
    "Северо-Западный",
    "Сибирский",
    "Уральский",
    "Центрально-Черноземный",
    "Южный",
    "Не распределено",
]

DISTRICT_SHEET_NAMES = {
    "Волжский": "Волжский",
    "Московский": "Московский",
    "Северо-Западный": "Северо-Западный",
    "Сибирский": "Сибирский",
    "Уральский": "Уральский",
    "Центрально-Черноземный": "Центр-Чернозем",
    "Южный": "Южный",
    "Не распределено": "Не распределено",
}

# =========================
# SQL / ClickHouse
# =========================

# Сначал разложи запросы в папки SQL а уже потом что то думай.

CLICKHOUSE_TARGET_TABLE = "gemstone.dtl_selection_errors_weekly"
CLICKHOUSE_MO_TABLE = "gemstone.raw_mo_daily"  # МО
CLICKHOUSE_MX_TABLE = "gemstone.raw_mx_daily"  # МХ

CLICKHOUSE_RENAME_MAP = {
    "Дата отчета": "report_date",
    "Год-Неделя": "report_week",
    "Код ТП": "product_code",
    "Наименование ТП": "product_name",
    "Наименование РЦ": "rc_name",
    "Годен до на МО": "expiry_mo",
    "Годен до на МХ": "expiry_mx",
    "Количество на МО": "qty_mo",
    "Количество на МХ": "qty_mx",
}


CLICKHOUSE_RENAME_RAW_MO = {
    "Код ТП": "product_code",
    "Наименование ТП": "product_name",
    "МО": "mo_location",
    "Годен до": "expiry_mo",
    "Количество по СГ": "qty_sg",
    "п/э": "pe_code",
    "Последнее пополнение слота": "last_slot_replenish_dttm",
    "Последнее обнуление слота": "last_slot_zero_dttm",
    "Общее количество": "total_qty",
    "Cтандартные наименования РЦ": "rc_name",
    "Федеральный округ": "log_region",
}


CLICKHOUSE_RENAME_RAW_MX = {
    "Код РЦ": "rc_code",
    "Наименование РЦ": "rc_name",
    "актив": "is_active",
    "Дата формирования": "report_dttm_bd_rc",
    "номер": "pallet_number",
    "Тип паллеты": "pallet_type",
    "SSCC/Центр.ПЭ": "sscc_center_pe",
    "Тип запаса": "stock_type",
    "№ Прихода": "receipt_no",
    "ЦФС": "cfs",
    "код": "product_code",
    "назв": "product_name",
    "МО": "mo_location",
    "МП": "mp_location",
    "колич": "qty",
    "Средний вес коробки": "avg_box_weight",
    "Годен до": "expiry_date",
    "Дата производства": "production_date",
    "Производитель": "manufacturer",
    "Страна": "country",
    "Срок хранения в КТ": "shelf_life_kt",
    "Срок хранения введенный": "shelf_life_entered",
    "Остаточный срок годности": "remaining_shelf_life",
    "Снимается с отборки": "removal_from_selection",
    "Разница расхождения": "deviation_diff",
    "отклонение": "deviation_comment",
    "Нужды_РЦ": "rc_needs",
    "Жёсткость товара": "goods_hardness",
    "Высота паллеты": "pallet_height",
    "Тип поддона": "pallet_base_type",
    "Дата установки": "install_dttm",
    "Пользователь": "install_user",
    "Количество выборки": "sample_qty",
    "Брак в выборке": "defect_sample_qty",
    "Процент брака": "defect_percent",
    "Дата установки (% брака)": "defect_install_dttm",
    "Пользователь (% брака)": "defect_install_user",
    "% ШК без отклонений": "pct_barcodes_without_deviation",
    "% ШК с отклонениями": "pct_barcodes_with_deviation",
    "% Несчитанных ШК": "pct_unscanned_barcodes",
    "Примечание": "note",
    "Вес брутто": "gross_weight",
    "Номер задания приемки": "receiving_task_no",
    "Создана вручную": "created_manually",
    "Приоритет": "priority_flag",
    "Переборка": "rework_flag",
}

# =========================
# Финальный порядок колонок RAW датасета
# =========================

FINAL_DATA_COLUMNS_RAW_MO = [
    "load_date",
    "rc_name",
    "log_region",
    "product_code",
    "product_name",
    "mo_location",
    "expiry_mo",
    "qty_sg",
    "pe_code",
    "last_slot_replenish_dttm",
    "last_slot_zero_dttm",
    "total_qty",
]


FINAL_DATA_COLUMNS_RAW_MX = [
    "load_date",
    "rc_code",
    "rc_name",
    "is_active",
    "report_dttm_bd_rc",
    "pallet_number",
    "pallet_type",
    "sscc_center_pe",
    "stock_type",
    "receipt_no",
    "cfs",
    "product_code",
    "product_name",
    "mo_location",
    "mp_location",
    "qty",
    "avg_box_weight",
    "expiry_date",
    "production_date",
    "manufacturer",
    "country",
    "shelf_life_kt",
    "shelf_life_entered",
    "remaining_shelf_life",
    "removal_from_selection",
    "deviation_diff",
    "deviation_comment",
    "rc_needs",
    "goods_hardness",
    "pallet_height",
    "pallet_base_type",
    "install_dttm",
    "install_user",
    "sample_qty",
    "defect_sample_qty",
    "defect_percent",
    "defect_install_dttm",
    "defect_install_user",
    "pct_barcodes_without_deviation",
    "pct_barcodes_with_deviation",
    "pct_unscanned_barcodes",
    "note",
    "gross_weight",
    "receiving_task_no",
    "created_manually",
    "priority_flag",
    "rework_flag",
]

# =========================
# Типизация RAW_MX
# =========================


MX_STRING_COLUMNS = [
    "rc_code",
    "rc_name",
    "pallet_number",
    "pallet_type",
    "sscc_center_pe",
    "stock_type",
    "receipt_no",
    "cfs",
    "product_code",
    "product_name",
    "mo_location",
    "mp_location",
    "manufacturer",
    "country",
    "deviation_comment",
    "rc_needs",
    "pallet_base_type",
    "install_user",
    "defect_install_user",
    "note",
    "receiving_task_no",
    "created_manually",
    "priority_flag",
    "rework_flag",
]


MX_NUMERIC_COLUMNS = [
    "qty",
    "avg_box_weight",
    "shelf_life_kt",
    "shelf_life_entered",
    "remaining_shelf_life",
    "removal_from_selection",
    "deviation_diff",
    "goods_hardness",
    "pallet_height",
    "sample_qty",
    "defect_sample_qty",
    "defect_percent",
    "pct_barcodes_without_deviation",
    "pct_barcodes_with_deviation",
    "pct_unscanned_barcodes",
    "gross_weight",
]

MX_DATETIME_COLUMNS = [
    "install_dttm",
    "defect_install_dttm",
]

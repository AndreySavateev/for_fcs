import pandas as pd

import constants as const
from config import settings


def save_to_excel(data: pd.DataFrame, dim_district) -> None:

    date_cols = const.EXCEL_DATE_COLUMNS
    for col in date_cols:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], errors="coerce")

    # Код ТП -> число
    if "Код ТП" in data.columns:
        data["Код ТП"] = pd.to_numeric(data["Код ТП"], errors="coerce")

    report_date = pd.Timestamp("2026-03-11")

    # Добавляем округ только для внутренней логики разбивки
    rc_to_district = dict(zip(dim_district["RC_NAME"], dim_district["LOG_REGION"]))

    data_with_district = data.copy()
    data_with_district["Округ"] = data_with_district["Наименование РЦ"].map(
        rc_to_district
    )
    data_with_district["Округ"] = data_with_district["Округ"].fillna("Не распределено")

    # =========================
    # 3. Сводка TOP 20
    # =========================

    pivot = (
        data_with_district.groupby("Наименование РЦ", as_index=False)
        .size()
        .rename(columns={"size": "Количество по полю Код ТП"})
        .sort_values("Количество по полю Код ТП", ascending=False)
        .head(20)
    )

    pivot["Дата отчета"] = report_date

    # =========================
    # 4. Путь сохранения
    # =========================

    output_file = settings.OUTPUT_PATH

    # =========================
    # 5. Порядок округов и названия вкладок
    # =========================

    district_order = const.DISTRICT_ORDER

    district_sheet_names = const.DISTRICT_SHEET_NAMES

    # =========================
    # 6. Сохранение в Excel
    # =========================

    with pd.ExcelWriter(
        output_file, engine="xlsxwriter", datetime_format="dd.mm.yyyy"
    ) as writer:
        workbook = writer.book

        # =========================
        # Форматы
        # =========================

        title_fmt = workbook.add_format({"bold": True, "font_size": 16})

        text_fmt = workbook.add_format({"font_size": 12})

        header_fmt = workbook.add_format(
            {
                "bold": True,
                "bg_color": "#1F4E78",
                "font_color": "white",
                "border": 1,
                "align": "center",
                "valign": "vcenter",
            }
        )

        date_fmt = workbook.add_format({"num_format": "dd.mm.yyyy", "align": "center"})

        int_fmt = workbook.add_format({"num_format": "#,##0"})

        # =========================
        # 6.1 Лист "Сводная" - первый
        # =========================

        pivot_start_row = 14
        pivot_start_col = 3

        pivot.to_excel(
            writer,
            sheet_name="Сводная",
            index=False,
            startrow=pivot_start_row,
            startcol=pivot_start_col,
        )

        ws_pivot = writer.sheets["Сводная"]

        text_col = 2  # C
        date_col = 3  # D
        right_text_col = 5  # F

        ws_pivot.write(1, text_col, "Добрый день.", text_fmt)
        ws_pivot.write(2, text_col, "Коллеги, по состоянию на", text_fmt)
        ws_pivot.write_datetime(2, date_col, report_date.to_pydatetime(), date_fmt)
        ws_pivot.write(
            2, right_text_col, "были выявлены ТП, пополненные не по ротации.", text_fmt
        )

        ws_pivot.write(
            4,
            text_col,
            "Это значит, что у вас на МО есть товар с более поздним СГ, чем на МХ.",
            text_fmt,
        )
        ws_pivot.write(
            5,
            text_col,
            "Необходимо устранить выявленные отклонения и не допускать таких ошибок в дальнейшем.",
            text_fmt,
        )
        ws_pivot.write(
            6,
            text_col,
            "Обратите внимание, что при заполнении Примечаний к паллетам необходимо учитывать корректное",
            text_fmt,
        )
        ws_pivot.write(
            7,
            text_col,
            "написание текста в соответствии с требованиями Инструкции, избегая лишних пробелов/знаков.",
            text_fmt,
        )

        ws_pivot.write(
            10,
            text_col,
            "HOT - на контроль и довести информацию до ответственных сотрудников.",
            title_fmt,
        )
        ws_pivot.write(
            pivot_start_row - 2,
            pivot_start_col,
            "ТОП 20 РЦ по кол-ву пополнений не по ротации",
            title_fmt,
        )

        for col_num, value in enumerate(pivot.columns):
            ws_pivot.write(
                pivot_start_row, pivot_start_col + col_num, value, header_fmt
            )

        ws_pivot.set_column("A:B", 4)
        ws_pivot.set_column("C:C", 22)
        ws_pivot.set_column("D:D", 16)
        ws_pivot.set_column("E:E", 28)
        ws_pivot.set_column("F:F", 18)
        ws_pivot.set_column("G:Z", 18)

        ws_pivot.set_row(1, 22)
        ws_pivot.set_row(2, 22)
        ws_pivot.set_row(4, 22)
        ws_pivot.set_row(5, 22)
        ws_pivot.set_row(6, 22)
        ws_pivot.set_row(7, 22)
        ws_pivot.set_row(10, 28)
        ws_pivot.set_row(pivot_start_row - 2, 28)

        qty_col_idx = pivot.columns.get_loc("Количество по полю Код ТП")
        qty_excel_col = pivot_start_col + qty_col_idx
        ws_pivot.set_column(qty_excel_col, qty_excel_col, 24, int_fmt)

        date_report_col_idx = pivot.columns.get_loc("Дата отчета")
        date_excel_col = pivot_start_col + date_report_col_idx
        ws_pivot.set_column(date_excel_col, date_excel_col, 16, date_fmt)

        first_data_row = pivot_start_row + 1
        last_data_row = pivot_start_row + len(pivot)

        ws_pivot.conditional_format(
            first_data_row,
            qty_excel_col,
            last_data_row,
            qty_excel_col,
            {"type": "data_bar", "bar_color": "#5B9BD5"},
        )

        # =========================
        # 6.2 Вкладки по округам - после сводной
        # =========================

        for district in district_order:
            district_df = data_with_district[
                data_with_district["Округ"] == district
            ].copy()

            if district_df.empty:
                continue

            # колонку "Округ" не выгружаем
            district_df = district_df.drop(columns=["Округ"], errors="ignore")

            sheet_name = district_sheet_names[district]

            district_df.to_excel(writer, sheet_name=sheet_name, index=False)

            ws_district = writer.sheets[sheet_name]
            ws_district.autofilter(0, 0, len(district_df), len(district_df.columns) - 1)

            for idx, col in enumerate(district_df.columns):
                max_len = max(
                    len(str(col)),
                    (
                        district_df[col].astype(str).map(len).max()
                        if not district_df.empty
                        else 0
                    ),
                )
                max_len = min(max_len + 2, 28)

                if col in date_cols:
                    ws_district.set_column(idx, idx, 15, date_fmt)
                elif col == "Код ТП":
                    ws_district.set_column(idx, idx, 14, int_fmt)
                else:
                    ws_district.set_column(idx, idx, max_len)

        # =========================
        # 6.3 Общий датасет - последний
        # =========================

        final_data_export = data_with_district.drop(columns=["Округ"], errors="ignore")

        final_data_export.to_excel(writer, sheet_name="Датасет", index=False)

        ws_data = writer.sheets["Датасет"]
        ws_data.autofilter(
            0, 0, len(final_data_export), len(final_data_export.columns) - 1
        )

        for idx, col in enumerate(final_data_export.columns):
            max_len = max(
                len(str(col)),
                (
                    final_data_export[col].astype(str).map(len).max()
                    if not final_data_export.empty
                    else 0
                ),
            )
            max_len = min(max_len + 2, 28)

            if col in date_cols:
                ws_data.set_column(idx, idx, 15, date_fmt)
            elif col == "Код ТП":
                ws_data.set_column(idx, idx, 14, int_fmt)
            else:
                ws_data.set_column(idx, idx, max_len)

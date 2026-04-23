import clickhouse_connect
import teradatasql
import pandas as pd

import constants as const
from config import settings
from io_utils import load_sql


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_PORT,
        username=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
    )


def get_tddr_client():
    return teradatasql.connect(
        host=settings.TDDR_HOST,
        user=settings.TDDR_USER,
        password=settings.TDDR_PASSWORD,
        logmech=settings.TDDR_LOGMECH,
        dbs_port=settings.TDDR_DBS_PORT,
    )


def get_teradata_client():
    return teradatasql.connect(
        host=settings.TERADATA_HOST,
        user=settings.TDDR_USER,
        password=settings.TDDR_PASSWORD,
        logmech=settings.TDDR_LOGMECH,
        dbs_port=settings.TDDR_DBS_PORT,
    )


def get_excluded_goods(client) -> pd.DataFrame:
    query = load_sql("excluded_goods.sql")
    return client.query_df(query)


def get_dim_district(clien) -> pd.DataFrame:
    query = load_sql("dim_district.sql")
    return clien.query_df(query)


def create_target_table(client) -> None:
    query = load_sql("create_target_table.sql")
    client.command(query)


def delete_report_week(client, report_week) -> None:
    query_template = load_sql("delete_report_week.sql")
    query = query_template.format(report_week=report_week)
    client.command(query)


def insert_report_week(client, click_df):
    client.insert_df(table=const.CLICKHOUSE_TARGET_TABLE, df=click_df)


def create_raw_table_mo(client) -> None:
    query = load_sql("create_raw_table_mo.sql")
    client.command(query)


def delete_raw_table_mo(client, load_date) -> None:
    query_template = load_sql("delete_raw_table_mo.sql")
    query = query_template.format(load_date=load_date)
    client.command(query)


def insert_raw_mo(client, click_raw_df):
    client.insert_df(table=const.CLICKHOUSE_MO_TABLE, df=click_raw_df)


def create_raw_table_mx(client) -> None:
    query = load_sql("create_raw_table_mx.sql")
    client.command(query)


def delete_raw_table_mx(client, load_date) -> None:
    query_template = load_sql("delete_raw_table_mx.sql")
    query = query_template.format(load_date=load_date)
    client.command(query)


def insert_raw_mx(client, raw_mx_df, chunk_size: int = 100000) -> None:
    total_rows = len(raw_mx_df)

    for start in range(0, total_rows, chunk_size):
        end = start + chunk_size
        chunk = raw_mx_df.iloc[start:end]

        client.insert_df(table=const.CLICKHOUSE_MX_TABLE, df=chunk)

        print(
            f"MX chunk загружен: строки {start + 1}-{min(end, total_rows)} из {total_rows}"
        )

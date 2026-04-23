import datetime as dt
import warnings

import pandas as pd

import db
import processing as proc
from config import settings

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


def run_raw_pipeline():
    print("Старт")
    dim_rc = pd.read_parquet(settings.DIM_RC_PATH)
    client = db.get_clickhouse_client()

    today = dt.date.today().isoformat()

    # МО
    db.create_raw_table_mo(client)
    raw_mo_data = proc.process_raw_mo(dim_rc)
    db.delete_raw_table_mo(client, today)
    db.insert_raw_mo(client, raw_mo_data)
    print(f"Подгрузил в МО {len(raw_mo_data)} строк")

    # MX
    db.create_raw_table_mx(client)
    raw_mx_data = proc.process_raw_mx()
    db.delete_raw_table_mx(client, today)
    db.insert_raw_mx(client, raw_mx_data)
    print(f"Подгрузил в МХ {len(raw_mx_data)} строк")

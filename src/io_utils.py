from pathlib import Path

import pandas as pd


# функция чтения sql запросов
def load_sql(sql_file_name: str) -> str:
    project_root = Path(__file__).resolve().parent.parent
    sql_path = project_root / "sql" / sql_file_name

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    return sql_path.read_text(encoding="utf=8")


def read_excel_snapshot(
    root: Path, required_columns: list[str] | None = None
) -> pd.DataFrame:
    root = Path(root)

    subfolders = [p for p in root.iterdir() if p.is_dir()]
    if not subfolders:
        raise FileNotFoundError(f"В папке {root} не найдено ни одной подпапки")

    data_root = max(subfolders, key=lambda p: p.stat().st_mtime)

    excel_files = list(data_root.glob("*.xlsx"))
    if not excel_files:
        raise FileNotFoundError(f"В папке {data_root} не найдено Excel-файлов (*.xlsx)")

    dfs = []

    for file_path in excel_files:
        if required_columns is None:
            df = pd.read_excel(file_path)
        else:
            df = pd.read_excel(file_path, usecols=required_columns)

        df["source_file"] = file_path.stem
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


# Удалить эту функцию после загрузки данных из МО
def read_excel_snapshot_by_folder(snapshot_folder: Path) -> pd.DataFrame:
    excel_files = sorted(
        [
            file_path
            for file_path in snapshot_folder.iterdir()
            if file_path.is_file()
            and file_path.suffix.lower() in {".xlsx", ".xls"}
            and not file_path.name.startswith("~$")
        ]
    )

    if not excel_files:
        raise FileNotFoundError(f"В папке нет Excel-файлов: {snapshot_folder}")

    dataframes = []

    for file_path in excel_files:
        print(f"Читаю файл: {file_path.name}")

        try:
            if file_path.suffix.lower() == ".xlsx":
                df = pd.read_excel(file_path, engine="openpyxl")
            elif file_path.suffix.lower() == ".xls":
                df = pd.read_excel(file_path, engine="xlrd")
            else:
                continue
        except Exception as e:
            raise ValueError(f"Ошибка чтения файла {file_path}: {e}") from e

        df["source_file"] = file_path.stem
        dataframes.append(df)

    return pd.concat(dataframes, ignore_index=True)

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Settings:
    # ClickHouse
    CLICKHOUSE_HOST: str = os.getenv("CLICKHOUSE_HOST")
    CLICKHOUSE_PORT: int = int(os.getenv("CLICKHOUSE_PORT", 8123))
    CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD")

    # tddr и teradata (тут дава хоста)
    TDDR_HOST: str = os.getenv("TDDR_HOST")
    TERADATA_HOST: str = os.getenv("TERADATA_HOST")
    TDDR_USER: str = os.getenv("TDDR_USER")
    TDDR_PASSWORD: str = os.getenv("TDDR_PASSWORD")
    TDDR_LOGMECH: str = os.getenv("TDDR_LOGMECH")
    TDDR_DBS_PORT: int = int(os.getenv("TDDR_DBS_PORT", 1025))

    # Paths
    BASE_DIR: Path = Path(__file__).resolve().parent.parent

    MO_ROOT: Path = BASE_DIR / os.getenv("MO_ROOT")
    MX_ROOT: Path = BASE_DIR / os.getenv("MX_ROOT")

    DIM_RC_PATH: Path = BASE_DIR / os.getenv("DIM_RC_PATH")
    OUTPUT_PATH: Path = BASE_DIR / os.getenv("OUTPUT_PATH")

    # Пути для загрузки пропущеных дней/данных из архива
    MO_ARCHIVE_ROOT: Path = BASE_DIR / os.getenv("MO_ARCHIVE_ROOT")
    MX_ARCHIVE_ROOT: Path = BASE_DIR / os.getenv("MX_ARCHIVE_ROOT")


settings = Settings()

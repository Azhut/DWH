from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    APP_ENV: str
    MONGO_URI: str
    DATABASE_NAME: str
    API_HOST: str
    API_PORT: int
    DEBUG: bool
    ENABLE_PROFILING: bool = False

    MONGO_USE_TRANSACTIONS: bool = True
    MONGO_TRANSACTION_MAX_FLAT_RECORDS: int = 30000
    FLATDATA_BULK_CHUNK_SIZE: int = 3000

    MANUAL_MAP_PATH: Path = Path(__file__).resolve().parent.parent / "app" / "utils" / "manual_map.json"

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parent.parent / ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )


config = Settings()

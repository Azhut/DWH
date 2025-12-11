from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

class Settings(BaseSettings):
    APP_ENV: str
    MONGO_URI: str
    DATABASE_NAME: str
    API_HOST: str
    API_PORT: int
    DEBUG: bool

    MANUAL_MAP_PATH: Path = Path("app/utils/manual_map.json")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

config = Settings()

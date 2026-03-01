from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    APP_ENV: str
    MONGO_URI: str
    DATABASE_NAME: str
    API_HOST: str
    API_PORT: int
    DEBUG: bool

    # Сделать путь абсолютным относительно корня проекта, чтобы
    # при запуске из тестов не создавался tests/.../app/utils/...
    MANUAL_MAP_PATH: Path = Path(__file__).resolve().parent.parent / "app" / "utils" / "manual_map.json"

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parent.parent / ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )


config = Settings()

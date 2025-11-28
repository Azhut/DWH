import os
from pathlib import Path

from pydantic_settings import BaseSettings



class BaseConfig(BaseSettings):
    """Базовая конфигурация приложения - ТОЛЬКО НАСТРОЙКИ"""

    # Режим работы
    APP_ENV: str = "development"
    DEBUG: bool = True
    TESTING: bool = False

    # MongoDB
    DATABASE_URI: str = "mongodb://localhost:27017"
    DATABASE_NAME: str = "sport_data"

    # API
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 2700

    # Пути
    MANUAL_MAP_PATH: Path = Path("app/utils/manual_map.json")

    class Config:
        env_file = ".env"
        case_sensitive = True


class DevelopmentConfig(BaseConfig):
    """Конфигурация для разработки - ТОЛЬКО НАСТРОЙКИ"""
    DEBUG: bool = True
    DATABASE_NAME: str = "sport_data_dev"


class TestingConfig(BaseConfig):
    """Конфигурация для тестирования - ТОЛЬКО НАСТРОЙКИ"""
    APP_ENV: str = "testing"
    TESTING: bool = True
    DEBUG: bool = False
    DATABASE_NAME: str = "sport_data_test"


class ProductionConfig(BaseConfig):
    """Конфигурация для продакшна - ТОЛЬКО НАСТРОЙКИ"""
    APP_ENV: str = "production"
    DEBUG: bool = False
    DATABASE_URI: str = "mongodb://mongo:27017"


def get_config():
    """Фабрика конфигураций - ТОЛЬКО ВОЗВРАТ КОНФИГА"""
    env = os.getenv("APP_ENV", "development").lower()

    config_mapping = {
        "development": DevelopmentConfig,
        "testing": TestingConfig,
        "production": ProductionConfig
    }

    return config_mapping.get(env, DevelopmentConfig)()


config = get_config()
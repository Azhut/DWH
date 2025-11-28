"""
Модуль запуска приложения - отвечает за процесс запуска в разных режимах
"""
from .base import ApplicationLauncher
from .development import DevelopmentLauncher
from .production import ProductionLauncher
from .testing import TestingLauncher

def get_launcher(env: str) -> ApplicationLauncher:
    """Фабрика лаунчеров"""
    launchers = {
        "development": DevelopmentLauncher,
        "production": ProductionLauncher,
        "testing": TestingLauncher
    }
    return launchers.get(env, DevelopmentLauncher)()
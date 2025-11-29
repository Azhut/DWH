"""
Модуль запуска приложения - отвечает за процесс запуска в разных режимах
"""
from .base import ApplicationLauncher
from .development import DevelopmentLauncher
from .production import ProductionLauncher


def get_launcher(env: str) -> ApplicationLauncher:
    """Фабрика лаунчеров"""
    launchers = {
        "development": DevelopmentLauncher,
        "production": ProductionLauncher,
    }
    return launchers.get(env, DevelopmentLauncher)()
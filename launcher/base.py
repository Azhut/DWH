"""
Базовый класс для запуска приложения
"""
from abc import ABC, abstractmethod
from config import config


class ApplicationLauncher(ABC):
    """Абстрактный базовый класс для всех лаунчеров"""

    def __init__(self):
        self.config = config

    @abstractmethod
    def run_checks(self):
        """Запускает проверки перед стартом"""
        pass

    @abstractmethod
    def print_startup_info(self):
        """Выводит информацию о запуске"""
        pass

    def print_success(self, message: str):
        print(f"✅ {message}")

    def print_warning(self, message: str):
        print(f"⚠️  {message}")

    def print_error(self, message: str):
        print(f"❌ {message}")

    def print_info(self, message: str):
        print(f"🔍 {message}")

    def print_step(self, message: str):
        print(f"🚀 {message}")
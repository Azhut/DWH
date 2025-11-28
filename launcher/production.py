"""
Лаунчер для продакшн режима
"""
from .base import ApplicationLauncher


class ProductionLauncher(ApplicationLauncher):
    """Запуск приложения в продакшн режиме"""

    def run_checks(self):
        """Минимальные проверки для продакшна"""
        print("🏭 Запуск в продакшн режиме")
        self._run_critical_checks()

    def print_startup_info(self):
        """Минимальный вывод для продакшна"""
        print(f"🚀 SPORT API запущена на http://{self.config.API_HOST}:{self.config.API_PORT}")
        print(f"📊 База данных: {self.config.DATABASE_NAME}")

    def _run_critical_checks(self):
        """Только критические проверки"""
        try:
            # Проверяем что базовые импорты работают
            from app.core.database import mongo_connection
            from app.core.logger import logger
            self.print_success("Критические компоненты загружены")
        except Exception as e:
            self.print_error(f"Критические проверки не пройдены: {e}")
            raise
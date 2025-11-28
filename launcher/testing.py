"""
Лаунчер для тестового режима
"""
from .base import ApplicationLauncher


class TestingLauncher(ApplicationLauncher):
    """Запуск приложения в тестовом режиме"""

    def run_checks(self):
        """Проверки для тестового режима"""
        print("🧪 Запуск в тестовом режиме")
        self._run_test_checks()

    def print_startup_info(self):
        """Вывод для тестового режима"""
        print(f"🧪 Тестовый режим: http://{self.config.API_HOST}:{self.config.API_PORT}")
        print(f"📊 Тестовая БД: {self.config.DATABASE_NAME}")

    def _run_test_checks(self):
        """Проверки специфичные для тестов"""
        try:
            # Проверяем что тестовая БД доступна
            from app.core.database import mongo_connection
            self.print_success("Тестовая среда настроена")
        except Exception as e:
            self.print_error(f"Тестовая среда не настроена: {e}")
            raise
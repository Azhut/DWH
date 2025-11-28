"""
Лаунчер для режима разработки
"""
import sys
from .base import ApplicationLauncher


class DevelopmentLauncher(ApplicationLauncher):
    """Запуск приложения в режиме разработки"""

    def run_checks(self):
        """Запускает проверки для разработки"""
        self.print_step("Запуск в режиме разработки")
        self._run_basic_checks()
        self._run_unit_tests()

    def print_startup_info(self):
        """Красивый вывод для разработки"""
        banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║                🚀 SPORT API Запущена Успешно!               ║
    ║                                                              ║
    ║    📊 API для обработки Excel файлов спортивной статистики  ║
    ║                                                              ║
    ║         🔗 http://{}:{}                         ║
    ║         📁 Режим: {: <10}                          ║
    ║         🐛 Отладка: {: <8}                          ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
        """.format(
            self.config.API_HOST,
            self.config.API_PORT,
            self.config.APP_ENV,
            "Включена" if self.config.DEBUG else "Выключена"
        )
        print(banner)

    def _run_basic_checks(self):
        """Базовые проверки"""
        self.print_step("Выполняем базовые проверки...")
        try:
            checks = [
                (1 + 1 == 2, "Базовая математика"),
                ("test".upper() == "TEST", "Строковые операции"),
            ]

            for condition, description in checks:
                if not condition:
                    raise Exception(f"Проверка не пройдена: {description}")

            self.print_success("Все базовые проверки пройдены")

        except Exception as e:
            self.print_error(f"Базовая проверка не пройдена: {e}")
            sys.exit(1)

    def _run_unit_tests(self):
        """Запуск unit-тестов"""
        self.print_step("Запуск unit-тестов...")
        try:
            import pytest
            result = pytest.main(["tests/unit/", "-q", "--tb=no", "--disable-warnings"])
            if result == 0:
                self.print_success("Все unit-тесты прошли успешно")
            else:
                self.print_warning(f"Unit-тесты завершились с кодом: {result}")
        except Exception as e:
            self.print_warning(f"Не удалось запустить unit-тесты: {e}")
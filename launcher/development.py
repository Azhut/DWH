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

    def print_startup_info(self):
        """Красивый вывод для разработки"""
        banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║                🚀 SPORT API Запущена Успешно!                ║
    ║                                                              ║
    ║    📊 API для обработки Excel файлов спортивной статистики   ║
    ║                                                              ║
    ║         🔗 http://{}:{}                                      ║
    ║         📁 Режим: {: <10}                                    ║
    ║         🐛 Отладка: {: <8}                                   ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
        """.format(
            self.config.API_HOST,
            self.config.API_PORT,
            self.config.APP_ENV,
            "Включена" if self.config.DEBUG else "Выключена"
        )
        print(banner)


    def _run_unit_tests(self):
        """Запуск unit-тестов"""

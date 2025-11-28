#!/usr/bin/env python3
"""
Универсальный скрипт для запуска тестов
"""
import os
import sys
import subprocess
import argparse


def set_environment(environment):
    """Устанавливает переменные окружения"""
    env_vars = {
        "testing": {
            "APP_ENV": "testing",
            "DATABASE_URI": "mongodb://localhost:27017",
            "DATABASE_NAME": "sport_data_test"
        },
        "development": {
            "APP_ENV": "development",
            "DATABASE_URI": "mongodb://localhost:27017",
            "DATABASE_NAME": "sport_data_dev"
        },
        "production": {
            "APP_ENV": "production",
            "DATABASE_URI": "mongodb://mongo:27017",
            "DATABASE_NAME": "sport_data"
        }
    }

    env_config = env_vars.get(environment, env_vars["development"])
    for key, value in env_config.items():
        os.environ[key] = value

    return env_config


def run_tests(test_type, environment="testing", coverage=False):
    """Запускает тесты определенного типа"""

    # Устанавливаем окружение
    env_config = set_environment(environment)
    print(f"🚀 Запуск {test_type} тестов в окружении: {environment}")
    print(f"📊 База данных: {env_config['DATABASE_URI']}/{env_config['DATABASE_NAME']}")

    # Команды для разных типов тестов
    base_cmd = ["pytest", "-v"]

    if coverage:
        base_cmd = ["pytest", "-v", "--cov=app", "--cov-report=term-missing"]

    commands = {
        "unit": [*base_cmd, "tests/unit", "-m", "unit"],
        "integration": [*base_cmd, "tests/integration", "-m", "integration"],
        "all": [*base_cmd, "tests/"],
        "production": [*base_cmd, "tests/production", "-m", "production"],
        "fast": [*base_cmd, "tests/unit", "tests/integration", "-m", "not slow"]
    }

    if test_type not in commands:
        print(f"❌ Неизвестный тип тестов: {test_type}")
        print(f"   Доступные: {list(commands.keys())}")
        return False

    try:
        print(f"🔧 Команда: {' '.join(commands[test_type])}")
        result = subprocess.run(commands[test_type])
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Ошибка при запуске тестов: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Запуск тестов Sport API")
    parser.add_argument("type",
                        choices=["unit", "integration", "all", "production", "fast"],
                        help="Тип тестов для запуска")
    parser.add_argument("--env",
                        choices=["testing", "development", "production"],
                        default="testing",
                        help="Окружение для тестов")
    parser.add_argument("--coverage", action="store_true",
                        help="Включить отчет о покрытии кода")

    args = parser.parse_args()

    success = run_tests(args.type, args.env, args.coverage)

    if success:
        print("✅ Все тесты прошли успешно!")
        sys.exit(0)
    else:
        print("❌ Некоторые тесты не прошли")
        sys.exit(1)


if __name__ == "__main__":
    main()
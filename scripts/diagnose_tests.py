#!/usr/bin/env python3
"""
Скрипт для диагностики проблем с тестами
"""
import os
import sys
import subprocess


def diagnose():
    # Переходим в корневую директорию проекта
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    os.chdir(project_root)

    print("🔍 Диагностика тестовой среды...")
    print(f"📁 Текущая директория: {os.getcwd()}")

    # Проверяем наличие pytest.ini
    pytest_ini_path = os.path.join(project_root, "pytest.ini")
    if os.path.exists(pytest_ini_path):
        print("✅ pytest.ini найден")
        try:
            # Пробуем разные кодировки
            for encoding in ['utf-8', 'cp1251', 'latin-1']:
                try:
                    with open(pytest_ini_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    print(f"✅ Файл прочитан с кодировкой: {encoding}")
                    print("Содержимое pytest.ini:")
                    print("---")
                    print(content)
                    print("---")
                    break
                except UnicodeDecodeError:
                    continue
            else:
                print("❌ Не удалось прочитать файл с доступными кодировками")
                # Пересоздаем файл
                create_pytest_ini(pytest_ini_path)
        except Exception as e:
            print(f"❌ Ошибка при чтении pytest.ini: {e}")
            create_pytest_ini(pytest_ini_path)
    else:
        print("❌ pytest.ini не найден")
        create_pytest_ini(pytest_ini_path)

    # Проверяем наличие тестов
    tests_dir = os.path.join(project_root, "tests")
    if os.path.exists(tests_dir):
        print("✅ Папка tests найдена")

        # Считаем тестовые файлы
        test_files = []
        for root, dirs, files in os.walk(tests_dir):
            for file in files:
                if file.startswith("test_") and file.endswith(".py"):
                    test_files.append(os.path.join(root, file))

        print(f"📊 Найдено тестовых файлов: {len(test_files)}")
        for tf in test_files:
            print(f"  - {os.path.relpath(tf, project_root)}")

        if not test_files:
            print("❌ В папке tests нет тестовых файлов")
            create_sample_tests(tests_dir)
    else:
        print("❌ Папка tests не найдена")
        os.makedirs(tests_dir, exist_ok=True)
        print("📁 Создана папка tests")
        create_sample_tests(tests_dir)

    # Проверяем pytest
    print("\n🔧 Проверка pytest...")
    try:
        result = subprocess.run([sys.executable, "-m", "pytest", "--version"],
                                capture_output=True, text=True, encoding='utf-8')
        print(f"Версия pytest: {result.stdout.strip()}")
    except Exception as e:
        print(f"❌ Ошибка при запуске pytest: {e}")

    # Запускаем простой тест
    print("\n🧪 Запуск поиска тестов...")
    try:
        result = subprocess.run([
            sys.executable, "-m", "pytest",
            "tests/",
            "--collect-only",
            "-q"
        ], capture_output=True, text=True, encoding='utf-8')

        if result.stdout:
            print("Найдены тесты:")
            print("---")
            print(result.stdout)
            print("---")
        else:
            print("ℹ️ Тесты не найдены")

        if result.stderr:
            print("Предупреждения/Ошибки:")
            print(result.stderr)

        print(f"Код возврата: {result.returncode}")

        # Если тесты найдены, запустим один простой
        if "test" in result.stdout.lower():
            print("\n🚀 Запуск одного теста...")
            test_result = subprocess.run([
                sys.executable, "-m", "pytest",
                "tests/unit/test_basic.py::test_basic_math",
                "-v"
            ], capture_output=True, text=True, encoding='utf-8')

            print("Результат теста:")
            print(test_result.stdout)
            if test_result.stderr:
                print("Ошибки:")
                print(test_result.stderr)

    except Exception as e:
        print(f"❌ Ошибка при запуске теста: {e}")


def create_pytest_ini(file_path):
    """Создает или пересоздает pytest.ini"""
    print("📝 Создаем pytest.ini...")
    content = """[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --strict-markers
    --disable-warnings
    --color=yes
markers =
    unit: Unit tests with mocks
    integration: Integration tests with real DB
    production: Production environment tests
    slow: Slow tests
    database: Tests requiring database
"""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print("✅ pytest.ini создан/перезаписан с кодировкой UTF-8")
    except Exception as e:
        print(f"❌ Ошибка при создании pytest.ini: {e}")


def create_sample_tests(tests_dir):
    """Создает примеры тестов"""
    print("📝 Создаем примеры тестов...")

    # Создаем unit тесты
    unit_dir = os.path.join(tests_dir, "unit")
    os.makedirs(unit_dir, exist_ok=True)

    # Простой тест
    simple_test = '''"""
Simple test for pytest verification
"""
def test_basic_math():
    """Test basic mathematics"""
    assert 1 + 1 == 2

def test_string_operations():
    """Test string operations"""
    text = "hello"
    assert text.upper() == "HELLO"

def test_list_operations():
    """Test list operations"""
    items = [1, 2, 3]
    assert len(items) == 3
    assert 2 in items

class TestSimpleClass:
    """Simple test class"""

    def test_class_method(self):
        """Test method in class"""
        assert True is True
'''

    with open(os.path.join(unit_dir, "test_basic.py"), "w", encoding="utf-8") as f:
        f.write(simple_test)
    print("✅ Создан tests/unit/test_basic.py")

    # Тест с маркерами
    markers_test = '''"""
Tests with markers
"""
import pytest

@pytest.mark.unit
def test_with_unit_marker():
    """Test with unit marker"""
    assert 2 * 2 == 4

@pytest.mark.integration  
def test_with_integration_marker():
    """Test with integration marker"""
    assert "test".replace("e", "a") == "tast"

@pytest.mark.slow
def test_slow_operation():
    """Slow test"""
    result = sum(range(1000))
    assert result == 499500
'''

    with open(os.path.join(unit_dir, "test_markers.py"), "w", encoding="utf-8") as f:
        f.write(markers_test)
    print("✅ Создан tests/unit/test_markers.py")

    # Создаем __init__.py файлы
    init_files = [
        os.path.join(tests_dir, "__init__.py"),
        os.path.join(unit_dir, "__init__.py")
    ]

    for init_file in init_files:
        with open(init_file, "w", encoding="utf-8") as f:
            f.write("# Test package\n")
        print(f"✅ Создан {init_file}")


if __name__ == "__main__":
    diagnose()
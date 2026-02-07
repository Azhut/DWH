"""
Тестовый скрипт для диагностики проблем с чтением UploadFile.

Использование:
    python test_file_reading.py path/to/test_file.xls
"""

import sys
from io import BytesIO
from tempfile import SpooledTemporaryFile


def test_direct_file_reading(filepath: str):
    """Тест прямого чтения файла."""
    print(f"\n{'=' * 60}")
    print(f"ТЕСТ 1: Прямое чтение файла")
    print(f"{'=' * 60}")

    try:
        with open(filepath, 'rb') as f:
            content = f.read()
            print(f"✓ Успешно прочитано: {len(content)} байт")
            print(f"✓ Первые 16 байт (hex): {content[:16].hex()}")

            # Проверка сигнатуры
            if content[:4] == b'\xD0\xCF\x11\xE0':
                print(f"✓ Формат: .xls (OLE2/CFB)")
            elif content[:4] == b'PK\x03\x04':
                print(f"✓ Формат: .xlsx/.xlsm (ZIP)")
            else:
                print(f"⚠ Неизвестная сигнатура: {content[:4].hex()}")

            return content
    except Exception as e:
        print(f"✗ Ошибка: {e}")
        return None


def test_spooled_temp_file(content: bytes, filename: str):
    """Тест чтения через SpooledTemporaryFile (как у FastAPI)."""
    print(f"\n{'=' * 60}")
    print(f"ТЕСТ 2: Чтение через SpooledTemporaryFile")
    print(f"{'=' * 60}")

    # Создаём SpooledTemporaryFile с разными максимальными размерами
    for max_size in [1024, 1024 * 512, 1024 * 1024 * 10]:  # 1KB, 512KB, 10MB
        print(f"\n--- max_size={max_size} ({max_size // 1024}KB) ---")

        try:
            # Создаём файл
            temp_file = SpooledTemporaryFile(max_size=max_size, mode='w+b')
            temp_file.write(content)

            print(f"  Записано: {len(content)} байт")
            print(f"  Файл на диске: {temp_file._rolled}")
            print(f"  Позиция: {temp_file.tell()}")

            # Метод 1: Простой seek + read
            temp_file.seek(0)
            read_content = temp_file.read()

            if read_content and len(read_content) == len(content):
                print(f"  ✓ Метод 1 (seek+read): OK, {len(read_content)} байт")
            else:
                print(f"  ✗ Метод 1 (seek+read): FAIL, {len(read_content) if read_content else 0} байт")

            # Метод 2: Через _file
            if hasattr(temp_file, '_file'):
                temp_file._file.seek(0)
                read_content2 = temp_file._file.read()
                if read_content2 and len(read_content2) == len(content):
                    print(f"  ✓ Метод 2 (_file): OK, {len(read_content2)} байт")
                else:
                    print(f"  ✗ Метод 2 (_file): FAIL, {len(read_content2) if read_content2 else 0} байт")

            # Метод 3: Rollover
            if hasattr(temp_file, 'rollover'):
                temp_file.rollover()
                temp_file.seek(0)
                read_content3 = temp_file.read()
                if read_content3 and len(read_content3) == len(content):
                    print(f"  ✓ Метод 3 (rollover): OK, {len(read_content3)} байт")
                else:
                    print(f"  ✗ Метод 3 (rollover): FAIL, {len(read_content3) if read_content3 else 0} байт")

            temp_file.close()

        except Exception as e:
            print(f"  ✗ Ошибка: {e}")


def test_calamine_reading(content: bytes, filename: str):
    """Тест чтения через python-calamine."""
    print(f"\n{'=' * 60}")
    print(f"ТЕСТ 3: Чтение через python-calamine")
    print(f"{'=' * 60}")

    try:
        import pandas as pd
        from io import BytesIO

        print(f"  Размер содержимого: {len(content)} байт")

        # Читаем через calamine
        df_dict = pd.read_excel(
            BytesIO(content),
            sheet_name=None,
            header=None,
            engine='calamine',
            dtype=object
        )

        print(f"  ✓ Успешно прочитано {len(df_dict)} листов:")
        for sheet_name, df in df_dict.items():
            print(f"    - '{sheet_name}': {len(df)} строк × {len(df.columns)} колонок")

        return True
    except ImportError:
        print(f"  ⚠ pandas или python-calamine не установлены")
        return False
    except Exception as e:
        print(f"  ✗ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    if len(sys.argv) < 2:
        print("Использование: python test_file_reading.py <путь_к_файлу.xls>")
        sys.exit(1)

    filepath = sys.argv[1]

    print(f"\n{'#' * 60}")
    print(f"# ДИАГНОСТИКА ЧТЕНИЯ ФАЙЛА")
    print(f"# Файл: {filepath}")
    print(f"{'#' * 60}")

    # Тест 1: Прямое чтение
    content = test_direct_file_reading(filepath)

    if content:
        # Тест 2: SpooledTemporaryFile
        test_spooled_temp_file(content, filepath)

        # Тест 3: Calamine
        test_calamine_reading(content, filepath)

    print(f"\n{'=' * 60}")
    print(f"ДИАГНОСТИКА ЗАВЕРШЕНА")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Скрипт для настройки тестовых данных
"""
import asyncio
import os
from app.core.database import mongo_connection


async def setup_test_data():
    """Создает тестовые данные в БД"""
    db = mongo_connection.get_database()

    # Тестовые файлы
    test_files = [
        {
            "file_id": "test-file-1",
            "filename": "MOSCOW 2023.xlsx",
            "status": "success",
            "year": 2023,
            "city": "MOSCOW",
            "upload_timestamp": "2023-01-01T00:00:00",
            "updated_at": "2023-01-01T00:00:00"
        },
        {
            "file_id": "test-file-2",
            "filename": "SAINT-PETERSBURG 2022.xlsx",
            "status": "success",
            "year": 2022,
            "city": "SAINT-PETERSBURG",
            "upload_timestamp": "2022-01-01T00:00:00",
            "updated_at": "2022-01-01T00:00:00"
        }
    ]

    # Тестовые flat data
    test_flat_data = [
        {
            "file_id": "test-file-1",
            "year": 2023,
            "city": "MOSCOW",
            "section": "Раздел1",
            "row": "Строка1",
            "column": "Колонка1",
            "value": 100
        },
        {
            "file_id": "test-file-1",
            "year": 2023,
            "city": "MOSCOW",
            "section": "Раздел1",
            "row": "Строка2",
            "column": "Колонка1",
            "value": 200
        }
    ]

    # Очищаем и заполняем данными
    await db.Files.delete_many({})
    await db.FlatData.delete_many({})

    if test_files:
        await db.Files.insert_many(test_files)
        print(f"✅ Добавлено {len(test_files)} тестовых файлов")

    if test_flat_data:
        await db.FlatData.insert_many(test_flat_data)
        print(f"✅ Добавлено {len(test_flat_data)} тестовых записей данных")

    print("🎉 Тестовые данные успешно настроены")


if __name__ == "__main__":
    # Устанавливаем тестовое окружение
    os.environ["APP_ENV"] = "testing"
    asyncio.run(setup_test_data())
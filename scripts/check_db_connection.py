import asyncio
from motor.motor_asyncio import AsyncIOMotorClient


async def check_connection():

    possible_uris = [
        'mongodb://localhost:27017',  # Локальный MongoDB
        'mongodb://localhost:2701',  # Порт из docker-compose
        'mongodb://mongo:27017',  # Внутри Docker сети
        'mongodb://127.0.0.1:27017',
        'mongodb://127.0.0.1:2701',
    ]

    db_name = 'sport_data'

    for uri in possible_uris:
        print(f"\nПробуем подключиться к: {uri}")
        try:
            client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=5000)
            await client.admin.command('ping')
            print(f"✓ Успешное подключение к {uri}")

            # Проверяем базу данных
            db = client[db_name]
            collections = await db.list_collection_names()
            print(f"Коллекции в базе '{db_name}': {collections}")

            # Проверяем конкретные коллекции
            if 'Files' in collections:
                count = await db.Files.count_documents({})
                print(f"  Files: {count} документов")
            else:
                print(f"  Files: коллекция не существует")

            if 'FlatData' in collections:
                count = await db.FlatData.count_documents({})
                print(f"  FlatData: {count} документов")
            else:
                print(f"  FlatData: коллекция не существует")

            client.close()
            return uri

        except Exception as e:
            print(f"✗ Ошибка подключения: {e}")

    print("\nНе удалось подключиться ни к одному из вариантов")
    return None


if __name__ == "__main__":
    print("Тестирование подключения к MongoDB...")
    result = asyncio.run(check_connection())

    if result:
        print(f"\nРекомендуемый URI для использования: {result}")
        print("\nСоздайте или обновите .env файл со следующим содержимым:")
        print(f"MONGO_URI={result}")
        print("DATABASE_NAME=sport_data")
        print("APP_ENV=development")
        print("API_HOST=0.0.0.0")
        print("API_PORT=2700")
        print("DEBUG=True")
    else:
        print("\nУбедитесь, что MongoDB запущена:")
        print("1. Если используете Docker: docker-compose up -d mongo")
        print("2. Если локально: sudo service mongod start")
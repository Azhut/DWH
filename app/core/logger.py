# app/core/logger.py
import logging
from config import config
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import PyMongoError

level = logging.DEBUG if config.DEBUG else logging.INFO
logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger("sport_api")

# В продакшне — попытка отправлять логи в коллекцию Logs, но только если соединение успешно
if config.APP_ENV == "production":
    try:
        mongo_client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
        # Тестируем подключение
        mongo_client.admin.command("ping")
        logs_collection = mongo_client[config.DATABASE_NAME]["Logs"]

        class MongoHandler(logging.Handler):
            def emit(self, record):
                try:
                    log_entry = {
                        "timestamp": datetime.utcnow(),
                        "level": record.levelname,
                        "message": record.getMessage(),
                        "logger": record.name,
                        "pathname": record.pathname,
                        "lineno": record.lineno
                    }
                    # Не блокируем основной поток (insert_one в pymongo — быстрый вызов)
                    logs_collection.insert_one(log_entry)
                except PyMongoError:
                    # При проблемах с Mongo — падаем на консоль (чтобы не терять логи)
                    logging.getLogger("sport_api").exception("Не удалось записать лог в Mongo")

        mongo_handler = MongoHandler()
        mongo_handler.setLevel(logging.INFO)
        logger.addHandler(mongo_handler)
        logger.info("Логирование в Mongo настроено (production).")
    except Exception:
        logger.warning("Не удалось подключиться к Mongo для логирования. Логи будут выводиться в stdout.")

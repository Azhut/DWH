# app/core/logger.py — централизованная настройка логирования (stdout + опционально Mongo по схеме LogEntry).
import logging
from datetime import datetime

from config import config
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from app.domain.log.models import LogEntry

level = logging.DEBUG if config.DEBUG else logging.INFO
logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger("sport_api")

if config.APP_ENV == "production":
    try:
        mongo_client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command("ping")
        logs_collection = mongo_client[config.DATABASE_NAME]["Logs"]

        class MongoHandler(logging.Handler):
            """Пишет записи логгера в MongoDB по схеме LogEntry."""

            def emit(self, record):
                try:
                    entry = LogEntry(
                        timestamp=datetime.utcnow(),
                        level=record.levelname,
                        message=record.getMessage(),
                        logger=record.name,
                        pathname=record.pathname,
                        lineno=record.lineno,
                    )
                    logs_collection.insert_one(entry.to_mongo_doc())
                except PyMongoError:
                    logging.getLogger("sport_api").exception("Не удалось записать лог в Mongo")

        mongo_handler = MongoHandler()
        mongo_handler.setLevel(logging.INFO)
        logger.addHandler(mongo_handler)
        logger.info("Логирование в Mongo настроено (production).")
    except Exception:
        logger.warning("Не удалось подключиться к Mongo для логирования. Логи будут выводиться в stdout.")
